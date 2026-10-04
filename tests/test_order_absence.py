"""Unknown submissions with no broker trace, resolved only by a complete account."""

import hashlib
import socket
import sqlite3
from datetime import timedelta

import httpx
import pytest
from test_account_guard import account, quote
from test_order_resolution import event_rows, reopen, terminal
from test_order_resolution_active import active, unknown_submission
from test_private_order import ready
from test_private_order import setup as live_setup

from trading.account_guard import Position, WorkingOrder
from trading.live_journal import (
    ABSENCE_MIN_DELAY_SECONDS,
    ABSENCE_RESOLUTION_CONFIRMATIONS,
    EVIDENCE_KINDS,
    RESOLUTION_CONFIRMATIONS,
    AcceptanceEvidence,
    LiveOrderError,
    OrderResolutionApproval,
)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("real network attempted"))


@pytest.fixture
def setup(tmp_path):
    return live_setup.__wrapped__(tmp_path)


def observe(setup, order, *, wait=ABSENCE_MIN_DELAY_SECONDS, **values):
    clock, _, _, journal = setup
    clock.advance(wait)
    return journal.record_absence_account(
        order.client_id, account(clock.now, **values), quote(clock.now), now=clock.now
    )


def approval(setup, order, *, lifetime=30):
    clock, _, _, journal = setup
    context = journal.order_absence_context(order.client_id)
    return OrderResolutionApproval(
        account_id=context["account_id"],
        checkpoint_sha256=context["checkpoint_sha256"],
        accepted_at=clock.now,
        expires_at=clock.now + timedelta(seconds=lifetime),
        evidence=tuple(
            AcceptanceEvidence(
                kind=kind,
                reference=f"synthetic-{kind}",
                sha256=hashlib.sha256(kind.encode()).hexdigest(),
            )
            for kind in sorted(EVIDENCE_KINDS)
        ),
    )


def test_absent_order_resolves_to_abandoned_and_keeps_every_stop(setup):
    clock, reads, posts, journal = setup
    order = unknown_submission(setup)
    with sqlite3.connect(journal.path) as conn:
        conn.execute("UPDATE account_gate SET entry_halted=1")
    observed = observe(setup, order)
    assert observed["account_shows_no_effect"] and not observed["absence_proven"]
    context = journal.order_absence_context(order.client_id)
    assert context["absent_state"] == "UNKNOWN"
    assert context["checkpoint_sha256"] == observed["checkpoint_sha256"]
    approved = approval(setup, order)
    # Neither the terminal set nor a bare claim resolution accepts an absent order.
    with pytest.raises(LiveOrderError, match="absence_confirmations_required"):
        journal.resolve_absent_order(
            order.client_id, approved, confirmations=RESOLUTION_CONFIRMATIONS
        )
    with pytest.raises(LiveOrderError, match="terminal_evidence_required"):
        journal.order_resolution_context(order.client_id)
    result = journal.resolve_absent_order(
        order.client_id, approved, confirmations=ABSENCE_RESOLUTION_CONFIRMATIONS
    )
    assert result["post_claim_resolved"] and result["order_state"] == "ABANDONED"
    assert not result["absence_proven"] and result["restart_required"]
    _, _, fresh, journal = reopen(setup)
    post = fresh.snapshot()
    assert post["phase"] == "STOPPED" and post["claim"] is None
    after = journal.snapshot()
    assert after["halted"] and after["orders"][0]["state"] == "ABANDONED"
    assert len(event_rows(journal, "ORDER_ABSENCE_RESOLVED")) == 1
    with sqlite3.connect(journal.path) as conn:
        assert conn.execute("SELECT entry_halted FROM account_gate").fetchone()[0] == 1
    # The ordinary account gate works again, and the client ID is never reused.
    journal.update_account(account(clock.now), quote(clock.now), now=clock.now)
    with pytest.raises(ValueError):
        journal.order_recovery_context(order.client_id)
    assert journal.restart_context()["checkpoint_sha256"]


def test_observation_too_soon_after_the_claim_is_refused_and_not_kept(setup):
    _, _, posts, journal = setup
    order = unknown_submission(setup)
    before = posts.snapshot()
    with pytest.raises(LiveOrderError, match="complete_account_required"):
        observe(setup, order, wait=ABSENCE_MIN_DELAY_SECONDS - 5)
    assert event_rows(journal, "ABSENCE_ACCOUNT_OBSERVED") == []
    with pytest.raises(LiveOrderError, match="absence_account_required"):
        journal.order_absence_context(order.client_id)
    assert posts.snapshot() == before


@pytest.mark.parametrize("effect", ["position", "working", "balance"])
def test_any_account_effect_of_the_order_refuses_absence(setup, effect):
    _, _, posts, journal = setup
    order = unknown_submission(setup)
    values = {
        "position": {
            "balance": "999998",
            "equity": "999994",
            "required_margin": "6000.4",
            "available_margin": "993993.6",
            "positions": (
                Position(position_id=401, side="BUY", units=1000, average_price="150.01"),
            ),
        },
        "working": {
            "working_orders": (
                WorkingOrder(client_id=order.client_id, order_id=201, remaining_units=1000),
            )
        },
        "balance": {"balance": "999990", "equity": "999990", "available_margin": "999990"},
    }[effect]
    with pytest.raises(LiveOrderError, match="complete_account_required"):
        observe(setup, order, **values)
    assert event_rows(journal, "ABSENCE_ACCOUNT_OBSERVED") == []
    assert posts.snapshot()["claim"] is not None


def test_any_broker_trace_or_cancel_claim_refuses_absence(setup):
    clock, _, _, journal = setup
    order = unknown_submission(setup)
    active(setup, order)  # Found by GET: reconcile it, never call it absent.
    with pytest.raises(LiveOrderError, match="unknown_submission_required"):
        observe(setup, order)


def test_cancel_claim_is_never_an_absence(tmp_path):
    setup = live_setup.__wrapped__(tmp_path)
    order, _ = terminal(setup, operation="cancel", status="CANCELED")
    with pytest.raises(LiveOrderError, match="unknown_submission_required"):
        observe(setup, order)


def test_changed_checkpoint_or_expired_approval_is_refused(setup):
    clock, _, posts, journal = setup
    order = unknown_submission(setup)
    observe(setup, order)
    approved = approval(setup, order, lifetime=5)
    observe(setup, order, wait=1)  # A newer observation replaces the reviewed one.
    with pytest.raises(LiveOrderError, match="acceptance_refused"):
        journal.resolve_absent_order(
            order.client_id, approved, confirmations=ABSENCE_RESOLUTION_CONFIRMATIONS
        )
    approved = approval(setup, order, lifetime=5)
    clock.advance(5)
    with pytest.raises(LiveOrderError, match="acceptance_refused"):
        journal.resolve_absent_order(
            order.client_id, approved, confirmations=ABSENCE_RESOLUTION_CONFIRMATIONS
        )
    assert posts.snapshot()["claim"] is not None
    assert journal.snapshot()["orders"][0]["state"] == "UNKNOWN"


def test_lost_live_commit_after_the_post_commit_is_completed_on_the_next_call(setup, monkeypatch):
    _, _, posts, journal = setup
    order = unknown_submission(setup)
    observe(setup, order)
    approved = approval(setup, order)
    original = type(posts)._resolve_trade

    def committed_then_lost(self, **kwargs):
        original(self, **kwargs)
        raise OSError("process ended before the live commit")

    monkeypatch.setattr(type(posts), "_resolve_trade", committed_then_lost)
    with pytest.raises(OSError):
        journal.resolve_absent_order(
            order.client_id, approved, confirmations=ABSENCE_RESOLUTION_CONFIRMATIONS
        )
    monkeypatch.undo()
    _, _, fresh, journal = reopen(setup)
    assert fresh.snapshot()["claim"] is None
    assert journal.snapshot()["orders"][0]["state"] == "UNKNOWN"
    result = journal.resolve_absent_order(
        order.client_id, approved, confirmations=ABSENCE_RESOLUTION_CONFIRMATIONS
    )
    assert result["completed_interrupted_resolution"]
    _, _, _, journal = reopen(setup)
    assert journal.snapshot()["orders"][0]["state"] == "ABANDONED"
    assert len(event_rows(journal, "ORDER_ABSENCE_RESOLVED")) == 1


def test_forged_absence_without_a_post_resolution_fails_integrity(setup):
    _, _, _, journal = setup
    order = ready(setup)
    with sqlite3.connect(journal.path) as conn:
        conn.execute("UPDATE orders SET state='ABANDONED'")
        conn.execute(
            "INSERT INTO events(recorded_at,client_id,kind,payload_json) VALUES "
            "('x',?,'ORDER_ABSENCE_RESOLVED',?)",
            (order.client_id, '{"prepared_id":1,"prepared_sha256":"' + "a" * 64 + '"}'),
        )
    with pytest.raises(LiveOrderError, match="integrity"):
        reopen(setup)


def test_unknown_order_still_responding_500_is_not_resent(setup):
    clock, _, _, journal = setup
    order = unknown_submission(setup)
    observe(setup, order)
    calls = []
    with pytest.raises(ValueError):
        with __import__("test_private_order").client(
            setup, lambda request: calls.append(request) or httpx.Response(500)
        ) as sender:
            sender.submit(order.client_id, quote=quote(clock.now))
    assert calls == []


def test_absence_approval_builder_and_cli_resolve_without_http(
    setup, monkeypatch, capsys, tmp_path
):
    import json

    from test_live_acceptance_checkpoints import evidence
    from test_order_resolution import recovery_cli

    from trading.live_acceptance import checkpoint_approval
    from trading.private_order_recovery import main

    clock, _, _, journal = setup
    order = unknown_submission(setup)
    observe(setup, order)
    args = recovery_cli(setup, monkeypatch)
    main(["absence-context", *args])
    context = json.loads(capsys.readouterr().out)
    assert context == journal.order_absence_context(order.client_id)
    built = checkpoint_approval(
        journal,
        "absence",
        order.client_id,
        evidence(tmp_path, EVIDENCE_KINDS, journal, clock.now),
        minutes=5,
        now=clock.now,
    )
    assert built.checkpoint_sha256 == context["checkpoint_sha256"]
    path = tmp_path / "absence-approval.json"
    path.write_text(built.model_dump_json(), encoding="utf-8")
    confirms = [
        value for item in sorted(ABSENCE_RESOLUTION_CONFIRMATIONS) for value in ("--confirm", item)
    ]
    # The terminal "resolve" command never accepts an absence approval.
    with pytest.raises(SystemExit):
        main(["resolve", *args, "--approval", str(path), *confirms])
    capsys.readouterr()
    main(["resolve-absence", *args, "--approval", str(path), *confirms])
    result = json.loads(capsys.readouterr().out)
    assert result["order_state"] == "ABANDONED" and result["post_claim_resolved"]
    assert reopen(setup)[3].snapshot()["orders"][0]["state"] == "ABANDONED"
