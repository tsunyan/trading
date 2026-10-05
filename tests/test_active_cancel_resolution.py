"""Review an uncertain cancellation without calling it failed or enabling a repeat."""

import hashlib
import json
import socket
import sqlite3
import subprocess
from datetime import timedelta
from types import SimpleNamespace

import httpx
import pytest
from test_account_guard import account, fill, quote
from test_order_resolution import reopen
from test_private_cancel import working
from test_private_order import client
from test_private_order import setup as live_setup

from trading.account_guard import Position, WorkingOrder
from trading.broker_contracts import cancel_request
from trading.execution_lab import fixture_evidence
from trading.live_journal import (
    ACTIVE_CANCEL_RESOLUTION_CONFIRMATIONS,
    ACTIVE_RESOLUTION_CONFIRMATIONS,
    EVIDENCE_KINDS,
    RESOLUTION_CONFIRMATIONS,
    AcceptanceEvidence,
    LiveOrderError,
    OrderResolutionApproval,
)
from trading.paper_runner import python_process_args
from trading.private_order import OrderTransportError


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("real network attempted"))


@pytest.fixture
def setup(tmp_path):
    return live_setup.__wrapped__(tmp_path)


def unknown_cancel(setup, *, pending=False, partial=False):
    clock, _, posts, journal = setup
    order, _ = working(setup)
    if pending:
        with pytest.raises(RuntimeError):
            with posts.operation(
                "cancel", request_sha256=hashlib.sha256(cancel_request(101).body).hexdigest()
            ):
                journal.begin_cancel(order.client_id)
                raise RuntimeError("interrupted after claiming cancellation")
        journal.halt()
    else:
        with client(setup, lambda _: httpx.Response(500)) as sender:
            with pytest.raises(OrderTransportError):
                sender.cancel(order.client_id)
    clock.advance(1)
    executions = [fill(size="400", fee="-2", timestamp=clock.now.isoformat())] if partial else []
    evidence = fixture_evidence(order, 101, 201, "ORDERED", executions, clock.now)
    journal.reconcile(evidence)
    values = {
        "working_orders": (
            WorkingOrder(
                client_id=order.client_id, order_id=201, remaining_units=600 if partial else 1000
            ),
        )
    }
    if partial:
        values.update(
            balance="999998",
            equity="999994",
            required_margin="2400.16",
            available_margin="997593.84",
            positions=(Position(position_id=401, side="BUY", units=400, average_price="150.01"),),
        )
    snapshot = account(clock.now, **values)
    return order, evidence, snapshot


def approval(setup, order):
    clock, _, _, journal = setup
    context = journal.active_cancel_context(order.client_id)
    return OrderResolutionApproval(
        account_id=context["account_id"],
        checkpoint_sha256=context["checkpoint_sha256"],
        accepted_at=clock.now,
        expires_at=clock.now + timedelta(seconds=30),
        evidence=tuple(
            AcceptanceEvidence(
                kind=kind,
                reference=f"reviewed-{kind}",
                sha256=hashlib.sha256(kind.encode()).hexdigest(),
            )
            for kind in sorted(EVIDENCE_KINDS)
        ),
    )


@pytest.mark.parametrize("pending", [False, True])
@pytest.mark.parametrize("partial", [False, True])
def test_active_cancel_review_resolves_only_local_claim_and_permanently_forbids_repeat(
    setup, pending, partial
):
    clock, _, posts, journal = setup
    order, evidence, snapshot = unknown_cancel(setup, pending=pending, partial=partial)
    before = journal.snapshot()
    result = journal.record_active_cancel_account(order.client_id, snapshot, quote(clock.now))
    assert result["cancel_outcome_unknown"] and not result["cancel_retry_allowed"]
    assert journal.snapshot()["account_guard"] == before["account_guard"]
    accepted = approval(setup, order)
    for confirmations in (RESOLUTION_CONFIRMATIONS, ACTIVE_RESOLUTION_CONFIRMATIONS):
        with pytest.raises(LiveOrderError):
            journal.resolve_order_claim(order.client_id, accepted, confirmations=confirmations)
        assert posts.snapshot()["claim"] is not None
    result = journal.resolve_order_claim(
        order.client_id, accepted, confirmations=ACTIVE_CANCEL_RESOLUTION_CONFIRMATIONS
    )
    assert result["post_claim_resolved"] and result["cancel_outcome_unknown"]
    assert not result["cancel_retry_allowed"] and result["restart_required"]
    _, _, posts, journal = reopen(setup)
    after = journal.snapshot()
    assert after["halted"] and after["live_control"] == before["live_control"]
    assert after["orders"][0]["state"] == ("PARTIAL" if partial else "WORKING")
    assert after["orders"][0]["evidence"] == evidence.model_dump(mode="json")
    assert after["account_guard"] == before["account_guard"]
    assert posts.snapshot()["phase"] == "STOPPED" and posts.snapshot()["claim"] is None
    with pytest.raises(LiveOrderError):
        journal.cancel_context(order.client_id)
    assert not any(e["kind"] == "CANCEL_NOT_SENT" for e in after["events"])
    # The old gate cannot authorize restart; refresh from a new complete observation first.
    with pytest.raises(LiveOrderError):
        journal.restart_context()
    clock.advance(1)
    journal.update_account(snapshot.model_copy(update={"observed_at": clock.now}), quote(clock.now))
    assert journal.restart_context()["checkpoint_sha256"]


@pytest.mark.parametrize(
    "change",
    [
        "no_observation",
        "incomplete_history",
        "incomplete_account",
        "old_account",
        "wrong_order",
        "missing_working",
    ],
)
def test_review_requires_positive_complete_current_account_and_order_evidence(setup, change):
    clock, _, posts, journal = setup
    order, evidence, snapshot = unknown_cancel(setup, pending=True)
    if change == "no_observation":
        with pytest.raises(LiveOrderError, match="active_cancel_account_required"):
            journal.active_cancel_context(order.client_id)
        return
    if change == "incomplete_history":
        journal.reconcile(
            evidence.model_copy(
                update={
                    "observed_at": clock.now + timedelta(microseconds=1),
                    "executions_complete": False,
                }
            )
        )
    elif change == "incomplete_account":
        snapshot = snapshot.model_copy(update={"complete": False})
    elif change == "old_account":
        snapshot = snapshot.model_copy(update={"observed_at": clock.now - timedelta(seconds=2)})
    elif change == "wrong_order":
        order = order.model_copy(update={"client_id": "Other001"})
    else:
        snapshot = snapshot.model_copy(update={"working_orders": ()})
    before, post = journal.snapshot(), posts.snapshot()
    with pytest.raises((ValueError, TypeError)):
        journal.record_active_cancel_account(order.client_id, snapshot, quote(clock.now))
    assert journal.snapshot() == before and posts.snapshot() == post


def test_post_commit_interruption_blocks_mutations_and_finishes_the_stored_decision(
    setup, monkeypatch
):
    clock, _, posts, journal = setup
    order, evidence, snapshot = unknown_cancel(setup, pending=True, partial=True)
    journal.record_active_cancel_account(order.client_id, snapshot, quote(clock.now))
    accepted = approval(setup, order)

    def fail(*args):
        raise RuntimeError("lost live commit")

    monkeypatch.setattr(journal, "_complete_active_cancel", fail)
    with pytest.raises(RuntimeError):
        journal.resolve_order_claim(
            order.client_id, accepted, confirmations=ACTIVE_CANCEL_RESOLUTION_CONFIRMATIONS
        )
    _, _, posts, journal = reopen(setup)
    assert posts.snapshot()["claim"] is None and posts.snapshot()["phase"] == "STOPPED"
    with pytest.raises(LiveOrderError, match="active_cancel_resolution_incomplete"):
        journal.reconcile(evidence)
    with pytest.raises(LiveOrderError, match="active_cancel_resolution_incomplete"):
        journal.update_account(snapshot, quote(clock.now))
    with pytest.raises(LiveOrderError, match="active_cancel_resolution_incomplete"):
        journal.restart_context()
    with pytest.raises(LiveOrderError, match="active_cancel_resolution_incomplete"):
        journal.prepare(order.model_copy(update={"client_id": "Other001"}))
    with pytest.raises(LiveOrderError, match="active_cancel_resolution_incomplete"):
        journal.record_active_cancel_account(order.client_id, snapshot, quote(clock.now))
    # Emergency stop remains available while normal mutations are fenced.
    journal.halt()
    assert journal.snapshot()["halted"]
    # Completion is the previously committed decision, even if observations and approval expired.
    clock.advance(3600)
    fresh = reopen(setup)
    result = fresh[3].resolve_order_claim(
        order.client_id, accepted, confirmations=ACTIVE_CANCEL_RESOLUTION_CONFIRMATIONS
    )
    assert result["completed_interrupted_resolution"] and result["cancel_outcome_unknown"]
    assert result["post_revision"] == fresh[2].snapshot()["revision"]
    assert fresh[3].snapshot()["orders"][0]["state"] == "PARTIAL"
    assert fresh[3].snapshot()["halted"]


def test_completed_review_record_must_match_the_post_committed_reference(setup):
    clock, _, _, journal = setup
    order, _, snapshot = unknown_cancel(setup)
    journal.record_active_cancel_account(order.client_id, snapshot, quote(clock.now))
    journal.resolve_order_claim(
        order.client_id,
        approval(setup, order),
        confirmations=ACTIVE_CANCEL_RESOLUTION_CONFIRMATIONS,
    )
    with sqlite3.connect(journal.path) as conn:
        row = conn.execute(
            "SELECT id,payload_json FROM events WHERE kind='ACTIVE_CANCEL_RESOLVED'"
        ).fetchone()
        payload = json.loads(row[1])
        payload["active_state"] = "PARTIAL"
        conn.execute("UPDATE events SET payload_json=? WHERE id=?", (json.dumps(payload), row[0]))
    with pytest.raises(LiveOrderError, match="integrity"):
        journal.snapshot()


@pytest.mark.parametrize("phase", ["before_post", "after_post", "after_live_commit"])
def test_process_exit_across_both_commits_keeps_stops_and_never_repeats_cancel(setup, phase):
    clock, reads, posts, journal = setup
    order, _, snapshot = unknown_cancel(setup, pending=True)
    journal.record_active_cancel_account(order.client_id, snapshot, quote(clock.now))
    accepted = approval(setup, order)
    code = f"""
import os, sys
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / "tests"))
from test_private_order import Clock
from trading.read_control import PersistentReadLimiter
from trading.post_control import PersistentPostLimiter
from trading.live_journal import (
    LiveOrderJournal, OrderResolutionApproval, ACTIVE_CANCEL_RESOLUTION_CONFIRMATIONS,
)
clock = Clock()
clock.now, clock.mono = datetime.fromisoformat({clock.now.isoformat()!r}), {clock.mono!r}
reads = PersistentReadLimiter(Path({str(reads.path.parent)!r}), {reads.scope!r})
posts = PersistentPostLimiter(Path({str(posts.path.parent)!r}), reads, **clock.post_args())
journal = LiveOrderJournal(Path({str(journal.path.parent)!r}), posts, clock=lambda: clock.now)
original_post = posts._resolve_trade
def resolve(*args, **kwargs):
    if {phase!r} == "before_post": os._exit(21)
    result = original_post(*args, **kwargs)
    if {phase!r} == "after_post": os._exit(22)
    return result
posts._resolve_trade = resolve
original_transaction = journal._transaction
@contextmanager
def transaction():
    with original_transaction() as conn:
        yield conn
        finished = conn.execute(
            "SELECT COUNT(*) FROM events WHERE kind='ACTIVE_CANCEL_RESOLVED'"
        ).fetchone()[0]
    if finished and {phase!r} == "after_live_commit": os._exit(23)
journal._transaction = transaction
accepted = OrderResolutionApproval.model_validate_json({accepted.model_dump_json()!r})
journal.resolve_order_claim(
    {order.client_id!r}, accepted, confirmations=ACTIVE_CANCEL_RESOLUTION_CONFIRMATIONS,
)
os._exit(99)
"""
    process = subprocess.run(python_process_args(code), capture_output=True, text=True, timeout=30)
    assert (
        process.returncode == {"before_post": 21, "after_post": 22, "after_live_commit": 23}[phase]
    ), process.stderr
    fresh = reopen(setup)
    assert fresh[3].snapshot()["halted"] and fresh[2].snapshot()["phase"] == "STOPPED"
    if phase == "before_post":
        assert fresh[2].snapshot()["claim"] is not None
        accepted = approval(fresh, order)
        fresh[3].resolve_order_claim(
            order.client_id, accepted, confirmations=ACTIVE_CANCEL_RESOLUTION_CONFIRMATIONS
        )
        fresh = reopen(setup)
    elif phase == "after_post":
        assert fresh[2].snapshot()["claim"] is None
        with pytest.raises(LiveOrderError, match="active_cancel_resolution_incomplete"):
            fresh[3].restart_context()
        result = fresh[3].resolve_order_claim(
            order.client_id, accepted, confirmations=ACTIVE_CANCEL_RESOLUTION_CONFIRMATIONS
        )
        assert result["completed_interrupted_resolution"]
    else:
        with pytest.raises(LiveOrderError):
            fresh[3].resolve_order_claim(
                order.client_id, accepted, confirmations=ACTIVE_CANCEL_RESOLUTION_CONFIRMATIONS
            )
    assert fresh[3].snapshot()["orders"][0]["state"] == "WORKING"
    assert len(fresh[2].trade_resolutions()) == 1
    with pytest.raises(LiveOrderError):
        fresh[3].cancel_context(order.client_id)


@pytest.mark.parametrize("partial", [False, True])
def test_delayed_broker_cancellation_is_reconciled_after_the_review(setup, partial):
    clock, _, _, journal = setup
    order, evidence, snapshot = unknown_cancel(setup, partial=partial)
    journal.record_active_cancel_account(order.client_id, snapshot, quote(clock.now))
    journal.resolve_order_claim(
        order.client_id,
        approval(setup, order),
        confirmations=ACTIVE_CANCEL_RESOLUTION_CONFIRMATIONS,
    )
    fresh = reopen(setup)
    clock.advance(1)
    canceled = evidence.model_copy(update={"status": "CANCELED", "observed_at": clock.now})
    assert fresh[3].reconcile(canceled) == "CANCELED"
    fresh[3].update_account(
        snapshot.model_copy(update={"observed_at": clock.now, "working_orders": ()}),
        quote(clock.now),
    )
    assert fresh[3].snapshot()["halted"] and fresh[2].snapshot()["phase"] == "STOPPED"
    assert fresh[3].restart_context()["checkpoint_sha256"]


def test_builder_and_cli_require_review_confirmation_without_http(
    setup, monkeypatch, capsys, tmp_path
):
    from test_live_acceptance_checkpoints import evidence
    from test_order_resolution import recovery_cli

    from trading.live_acceptance import checkpoint_approval
    from trading.private_order_recovery import main

    clock, _, _, journal = setup
    order, _, snapshot = unknown_cancel(setup)
    journal.record_active_cancel_account(order.client_id, snapshot, quote(clock.now))
    args = recovery_cli(setup, monkeypatch)
    main(["active-cancel-context", *args])
    context = json.loads(capsys.readouterr().out)
    assert context == journal.active_cancel_context(order.client_id)
    built = checkpoint_approval(
        journal,
        "active-cancel",
        order.client_id,
        evidence(tmp_path, EVIDENCE_KINDS, journal, clock.now),
        minutes=5,
        now=clock.now,
    )
    path = tmp_path / "active-cancel-approval.json"
    path.write_text(built.model_dump_json())
    confirmations = [
        arg
        for item in sorted(ACTIVE_CANCEL_RESOLUTION_CONFIRMATIONS)
        for arg in ("--confirm", item)
    ]
    main(["resolve", *args, "--approval", str(path), *confirmations])
    result = json.loads(capsys.readouterr().out)
    assert result["post_claim_resolved"] and result["cancel_outcome_unknown"]
    assert not result["cancel_retry_allowed"]


@pytest.mark.parametrize("change", ["expiry", "checkpoint", "read_stop", "future_approval"])
def test_review_authorization_is_invalidated_without_clearing_the_claim(setup, change):
    clock, reads, posts, journal = setup
    order, _, snapshot = unknown_cancel(setup)
    journal.record_active_cancel_account(order.client_id, snapshot, quote(clock.now))
    accepted = approval(setup, order)
    if change == "expiry":
        clock.advance(30)
    elif change == "checkpoint":
        journal.record_active_cancel_account(order.client_id, snapshot, quote(clock.now))
    elif change == "read_stop":
        reads.stop("operator_stop")
    else:
        accepted = accepted.model_copy(update={"accepted_at": clock.now + timedelta(seconds=1)})
    before, post = journal.snapshot(), posts.snapshot()
    with pytest.raises(LiveOrderError):
        journal.resolve_order_claim(
            order.client_id, accepted, confirmations=ACTIVE_CANCEL_RESOLUTION_CONFIRMATIONS
        )
    assert journal.snapshot() == before and posts.snapshot() == post
    assert post["claim"] is not None


@pytest.mark.parametrize("filled", [600, 1000])
def test_more_executions_after_review_are_accounted_without_enabling_posts(setup, filled):
    clock, _, _, journal = setup
    order, _, snapshot = unknown_cancel(setup, pending=True, partial=True)
    journal.record_active_cancel_account(order.client_id, snapshot, quote(clock.now))
    journal.resolve_order_claim(
        order.client_id,
        approval(setup, order),
        confirmations=ACTIVE_CANCEL_RESOLUTION_CONFIRMATIONS,
    )
    fresh = reopen(setup)
    clock.advance(1)
    executions = [
        fill(size="400", fee="-2", timestamp=snapshot.observed_at.isoformat()),
        {
            **fill(size=str(filled - 400), fee="-1", timestamp=clock.now.isoformat()),
            "executionId": 302,
        },
    ]
    evidence = fixture_evidence(
        order, 101, 201, "EXECUTED" if filled == 1000 else "ORDERED", executions, clock.now
    )
    assert fresh[3].reconcile(evidence) == ("FILLED" if filled == 1000 else "PARTIAL")
    after = fresh[3].snapshot()
    assert len(after["orders"][0]["evidence"]["executions"]) == 2
    assert after["halted"] and fresh[2].snapshot()["phase"] == "STOPPED"
    with pytest.raises(LiveOrderError):
        fresh[3].cancel_context(order.client_id)


def test_acceptance_cli_builds_active_cancel_approval(setup, tmp_path, monkeypatch, capsys):
    from test_live_acceptance import pinned
    from test_live_acceptance_checkpoints import evidence

    from trading import live_acceptance

    clock, reads, _, journal = setup
    order, _, snapshot = unknown_cancel(setup)
    journal.record_active_cancel_account(order.client_id, snapshot, quote(clock.now))
    items = evidence(tmp_path, EVIDENCE_KINDS, journal, clock.now)
    monkeypatch.setattr(live_acceptance, "datetime", pinned(clock.now))
    monkeypatch.setattr(
        live_acceptance,
        "PrivateOrderRecovery",
        lambda *args: SimpleNamespace(journal=journal, reads=reads),
    )
    path = tmp_path / "built-active-cancel.json"
    args = [
        "active-cancel-approval",
        "--directory",
        str(journal.path.parent),
        "--read-control-directory",
        str(reads.path.parent),
        "--scope",
        reads.scope,
        "--client-id",
        order.client_id,
        "--minutes",
        "5",
        "--output",
        str(path),
    ]
    for item in items:
        args.extend(["--evidence", f"{item.kind}={tmp_path / item.reference}"])
    live_acceptance.main(args)
    assert json.loads(capsys.readouterr().out)["applied"] is False
    built = OrderResolutionApproval.model_validate_json(path.read_bytes())
    assert (
        built.checkpoint_sha256
        == journal.active_cancel_context(order.client_id)["checkpoint_sha256"]
    )


@pytest.mark.parametrize("change", ["expiry", "read_stop", "implementation"])
def test_last_post_commit_gate_refuses_changes_after_live_preparation(setup, monkeypatch, change):
    clock, reads, posts, journal = setup
    order, _, snapshot = unknown_cancel(setup)
    journal.record_active_cancel_account(order.client_id, snapshot, quote(clock.now))
    accepted = approval(setup, order)
    original = posts._resolve_trade

    def changed(**kwargs):
        if change == "expiry":
            clock.advance(30)
        elif change == "read_stop":
            reads.stop("operator_stop")
        else:
            monkeypatch.setattr(journal, "_current_implementation", lambda: "f" * 64)
        return original(**kwargs)

    monkeypatch.setattr(posts, "_resolve_trade", changed)
    post = posts.snapshot()
    with pytest.raises(LiveOrderError, match="order_resolution_checkpoint_changed"):
        journal.resolve_order_claim(
            order.client_id, accepted, confirmations=ACTIVE_CANCEL_RESOLUTION_CONFIRMATIONS
        )
    assert posts.snapshot() == post and not posts.trade_resolutions()
    assert journal.snapshot()["halted"]
