"""Explicit terminal claim resolution, cross-store crashes, and generation fencing."""

import hashlib
import json
import socket
import sqlite3
import subprocess
import sys
from contextlib import contextmanager
from datetime import timedelta

import httpx
import pytest
from test_account_guard import account, fill, intent, quote
from test_private_cancel import working
from test_private_order import client, ready
from test_private_order import setup as live_setup

from trading.account_guard import Position
from trading.broker_contracts import Settlement
from trading.execution_lab import fixture_evidence
from trading.live_journal import (
    EVIDENCE_KINDS,
    RESOLUTION_CONFIRMATIONS,
    AcceptanceEvidence,
    LiveOrderJournal,
    OrderResolutionApproval,
)
from trading.post_control import PersistentPostLimiter, PostControlError
from trading.private_order import OrderTransportError
from trading.private_order_recovery import PrivateOrderRecovery, main


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("real network attempted"))


@pytest.fixture
def setup(tmp_path):
    return live_setup.__wrapped__(tmp_path)


def terminal(setup, *, operation="order", status="CANCELED", partial=False):
    clock, _, _, journal = setup
    order = working(setup, partial=partial)[0] if operation == "cancel" else ready(setup)
    with client(setup, lambda _: httpx.Response(500)) as sender:
        with pytest.raises(OrderTransportError):
            if operation == "cancel":
                sender.cancel(order.client_id)
            else:
                sender.submit(order.client_id, quote=quote(clock.now))
    clock.advance(1)
    old = journal.snapshot()["orders"][0]["evidence"]
    fills = []
    if status == "EXECUTED":
        fills = [fill(timestamp=clock.now.isoformat())]
    elif partial:
        if old:
            # Preserve the original execution exactly across the terminal observation.
            fills = [fill(size="400", fee="-2", timestamp=old["executions"][0]["timestamp"])]
        else:
            fills = [fill(size="400", fee="-2", timestamp=clock.now.isoformat())]
    evidence = fixture_evidence(order, 101, 201, status, fills, clock.now)
    journal.reconcile(evidence)
    refresh_account(setup, status=status, partial=partial)
    return order, evidence


def refresh_account(setup, *, status="CANCELED", partial=False):
    clock, _, _, journal = setup
    values = {}
    if status == "EXECUTED" or partial:
        full = status == "EXECUTED"
        values = {
            "balance": "999997" if full else "999998",
            "equity": "999987" if full else "999994",
            "required_margin": "6000.4" if full else "2400.16",
            "available_margin": "993986.6" if full else "997593.84",
            "positions": (
                Position(
                    position_id=401, side="BUY", units=1000 if full else 400, average_price="150.01"
                ),
            ),
        }
    journal.update_account(account(clock.now, **values), quote(clock.now), now=clock.now)


def acceptance(setup, order, *, lifetime=30):
    clock, _, _, journal = setup
    context = journal.order_resolution_context(order.client_id)
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


def reopen(setup):
    clock, reads, posts, journal = setup
    posts = PersistentPostLimiter(posts.path.parent, reads, **clock.post_args())
    journal = LiveOrderJournal(journal.path.parent, posts, clock=lambda: clock.now)
    return clock, reads, posts, journal


def event_rows(journal, kind):
    with sqlite3.connect(journal.path) as conn:
        return conn.execute("SELECT payload_json FROM events WHERE kind=?", (kind,)).fetchall()


@pytest.mark.parametrize("operation", ["order", "cancel"])
@pytest.mark.parametrize(
    "status,partial",
    [("CANCELED", False), ("CANCELED", True), ("EXPIRED", True), ("EXECUTED", False)],
)
def test_resolution_clears_only_claim_and_fences_all_old_handles(setup, operation, status, partial):
    clock, reads, posts, journal = setup
    order, evidence = terminal(setup, operation=operation, status=status, partial=partial)
    with sqlite3.connect(journal.path) as conn:
        conn.execute("UPDATE account_gate SET entry_halted=1")
    peer = PersistentPostLimiter(posts.path.parent, reads, **clock.post_args())
    before, post_before = journal.snapshot(), posts.snapshot()
    approved = acceptance(setup, order)
    assert journal.snapshot() == before  # Context inspection is local and read-only.
    result = journal.resolve_order_claim(
        order.client_id, approved, confirmations=RESOLUTION_CONFIRMATIONS
    )
    assert result["post_claim_resolved"] and result["restart_required"]
    for old in (posts, peer):
        with pytest.raises(PostControlError):
            old.snapshot()
        with pytest.raises(PostControlError):
            old.stop()
        with pytest.raises(PostControlError):
            with old.token_slot("POST"):
                pytest.fail("old client resumed")
    setup = reopen(setup)
    _, _, fresh, journal = setup
    post = fresh.snapshot()
    assert post["phase"] == "STOPPED" and post["claim"] is None
    assert post["operation"] is None and post["request_sha256"] is None
    assert post["reason"] == post_before["reason"]
    after = journal.snapshot()
    assert after["live_control"] == before["live_control"]
    assert after["account_guard"] == before["account_guard"] and after["halted"]
    assert after["orders"] == before["orders"]
    assert after["orders"][0]["evidence"] == evidence.model_dump(mode="json")
    assert len(event_rows(journal, "ORDER_RESOLUTION_PREPARED")) == 1
    history = fresh.trade_resolutions()
    assert len(history) == 1 and history[0]["post_before"]["claim"] == post_before["claim"]
    assert history[0]["reference"]["client_id"] == order.client_id
    with client(setup, lambda _: pytest.fail("resolved order resent")) as sender:
        with pytest.raises(OrderTransportError):
            sender.submit(order.client_id, quote=quote(clock.now))
        with pytest.raises(OrderTransportError):
            sender.cancel(order.client_id)
    with pytest.raises(ValueError):
        journal.resolve_order_claim(
            order.client_id, approved, confirmations=RESOLUTION_CONFIRMATIONS
        )


@pytest.mark.parametrize("reason", ["operator_stop", "clock_invalid", "order_cleanup_failed"])
def test_resolution_preserves_explicit_post_stop_reason(setup, reason):
    _, _, posts, journal = setup
    order, _ = terminal(setup)
    posts.stop(reason)
    applied_reason = posts.snapshot()["reason"]
    # Account acceptance must be at or after the last POST stop checkpoint.
    refresh_account(setup)
    journal.resolve_order_claim(
        order.client_id, acceptance(setup, order), confirmations=RESOLUTION_CONFIRMATIONS
    )
    _, _, posts, journal = reopen(setup)
    assert posts.snapshot()["reason"] == applied_reason and posts.snapshot()["phase"] == "STOPPED"
    assert journal.snapshot()["halted"]


@pytest.mark.parametrize(
    "change",
    [
        "incomplete",
        "active",
        "missing",
        "row_state",
        "account",
        "stale",
        "read_stop",
        "gate_damage",
    ],
)
def test_resolution_context_refuses_weak_or_inconsistent_proofs(setup, change):
    clock, reads, posts, journal = setup
    order, evidence = terminal(setup)
    before = posts.snapshot()
    if change in {"incomplete", "active", "missing", "row_state"}:
        with sqlite3.connect(journal.path) as conn:
            if change == "row_state":
                conn.execute("UPDATE orders SET state='UNKNOWN'")
            elif change == "missing":
                conn.execute("UPDATE orders SET evidence_json=NULL")
            else:
                damaged = evidence.model_copy(
                    update={"executions_complete": False}
                    if change == "incomplete"
                    else {"status": "ORDERED"}
                )
                conn.execute("UPDATE orders SET evidence_json=?", (damaged.model_dump_json(),))
    elif change == "account":
        with sqlite3.connect(journal.path) as conn:
            conn.execute("UPDATE account_gate SET proof_json=NULL")
    elif change == "stale":
        clock.advance(61)
    elif change == "read_stop":
        reads.stop()
    else:
        with sqlite3.connect(journal.path) as conn:
            conn.execute("UPDATE account_gate SET peak='999'")
        # Gate peak changes invalidate explicit acceptance, even if the account still matches.
        accepted = acceptance(setup, order)
        with sqlite3.connect(journal.path) as conn:
            conn.execute("UPDATE account_gate SET entry_halted=1")
        with pytest.raises(ValueError):
            journal.resolve_order_claim(
                order.client_id, accepted, confirmations=RESOLUTION_CONFIRMATIONS
            )
        assert posts.snapshot() == before
        return
    with pytest.raises(ValueError):
        journal.order_resolution_context(order.client_id)
    assert posts.snapshot() == before and not event_rows(journal, "ORDER_RESOLUTION_PREPARED")


@pytest.mark.parametrize(
    "change",
    [
        "account",
        "checkpoint",
        "expired",
        "future",
        "confirm",
        "code",
        "halt",
        "account_refresh",
        "post_stop",
    ],
)
def test_resolution_acceptance_fences_changes_before_preparation(setup, monkeypatch, change):
    clock, _, posts, journal = setup
    order, _ = terminal(setup)
    approved = acceptance(setup, order)
    if change == "account":
        approved = approved.model_copy(update={"account_id": "foreign"})
    elif change == "checkpoint":
        approved = approved.model_copy(update={"checkpoint_sha256": "b" * 64})
    elif change == "expired":
        clock.advance(30)
    elif change == "future":
        approved = approved.model_copy(update={"accepted_at": clock.now + timedelta(seconds=1)})
    elif change == "code":
        monkeypatch.setattr(journal, "_current_implementation", lambda: "b" * 64)
    elif change == "halt":
        journal.halt()
    elif change == "account_refresh":
        clock.advance(1)
        refresh_account(setup)
    elif change == "post_stop":
        posts.stop("operator_stop")
    before = posts.snapshot()
    with pytest.raises(ValueError):
        journal.resolve_order_claim(
            order.client_id,
            approved,
            confirmations={"terminal-order"} if change == "confirm" else RESOLUTION_CONFIRMATIONS,
        )
    assert posts.snapshot() == before and not event_rows(journal, "ORDER_RESOLUTION_PREPARED")


@pytest.mark.parametrize("change", ["code", "expiry", "halt", "post_stop", "prepared_damage"])
def test_changes_between_commits_retain_original_claim_and_prepared_audit(
    setup, monkeypatch, change
):
    clock, _, posts, journal = setup
    order, _ = terminal(setup)
    approved = acceptance(setup, order)
    claim = posts.snapshot()["claim"]
    original = journal._transaction
    changed = False

    @contextmanager
    def transaction():
        nonlocal changed
        with original() as conn:
            yield conn
        if not changed and event_rows(journal, "ORDER_RESOLUTION_PREPARED"):
            changed = True
            if change == "code":
                monkeypatch.setattr(journal, "_current_implementation", lambda: "b" * 64)
            elif change == "expiry":
                clock.advance(30)
            elif change == "halt":
                journal.halt()
            elif change == "post_stop":
                posts.stop("operator_stop")
            else:
                with sqlite3.connect(journal.path) as damaged:
                    damaged.execute(
                        "UPDATE events SET payload_json='{}' WHERE kind='ORDER_RESOLUTION_PREPARED'"
                    )

    monkeypatch.setattr(journal, "_transaction", transaction)
    with pytest.raises(ValueError):
        journal.resolve_order_claim(
            order.client_id, approved, confirmations=RESOLUTION_CONFIRMATIONS
        )
    assert posts.snapshot()["claim"] == claim and posts.snapshot()["phase"] == "STOPPED"
    assert len(event_rows(journal, "ORDER_RESOLUTION_PREPARED")) == 1
    assert posts.trade_resolutions() == []


def test_resolution_after_code_update_keeps_old_live_approval_and_binding(setup, monkeypatch):
    _, _, _, journal = setup
    order, _ = terminal(setup)
    before = journal.snapshot()["live_control"]
    monkeypatch.setattr("trading.live_journal.implementation_sha256", lambda: "b" * 64)
    journal.resolve_order_claim(
        order.client_id, acceptance(setup, order), confirmations=RESOLUTION_CONFIRMATIONS
    )
    _, _, posts, journal = reopen(setup)
    assert journal.snapshot()["live_control"] == before
    assert (
        not journal.snapshot()["implementation_matches"] and not journal.snapshot()["live_enabled"]
    )
    assert posts.execution_binding()["instance"] == before["instance"]


def test_closing_order_resolution_checks_realized_cash_and_flat_account(setup):
    clock, _, _, journal = setup
    opened, _ = working(setup)
    clock.advance(1)
    journal.reconcile(
        fixture_evidence(
            opened, 101, 201, "EXECUTED", [fill(timestamp=clock.now.isoformat())], clock.now
        )
    )
    refresh_account(setup, status="EXECUTED")
    closing = intent(
        client_id="Close001",
        side="SELL",
        effect="CLOSE",
        price="150.02",
        positions=(Settlement(position_id=401, units=1000),),
    )
    journal.prepare(closing)
    with client(setup, lambda _: httpx.Response(500)) as sender:
        with pytest.raises(OrderTransportError):
            sender.submit(closing.client_id, quote=quote(clock.now))
    clock.advance(1)
    journal.reconcile(
        fixture_evidence(
            closing,
            111,
            211,
            "EXECUTED",
            [
                fill(
                    execution_id=302,
                    price="150.02",
                    fee="-2",
                    lossGain="10",
                    timestamp=clock.now.isoformat(),
                )
            ],
            clock.now,
        )
    )
    journal.update_account(
        account(clock.now, balance="1000005", equity="1000005", available_margin="1000005"),
        quote(clock.now),
        now=clock.now,
    )
    journal.resolve_order_claim(
        closing.client_id, acceptance(setup, closing), confirmations=RESOLUTION_CONFIRMATIONS
    )
    _, _, posts, journal = reopen(setup)
    assert posts.trade_resolutions()[0]["post_before"]["operation"] == "close_order"
    assert journal.snapshot()["account_guard"]["last_proof"]["snapshot"]["balance"] == "1000005"


def test_live_os_owner_refuses_resolution_without_preparing(setup):
    _, _, posts, journal = setup
    order, _ = terminal(setup)
    approved = acceptance(setup, order)
    with posts._ownership(), pytest.raises(PostControlError):
        journal.resolve_order_claim(
            order.client_id, approved, confirmations=RESOLUTION_CONFIRMATIONS
        )
    assert not event_rows(journal, "ORDER_RESOLUTION_PREPARED") and posts.snapshot()["claim"]


@pytest.mark.parametrize("change", ["code", "expiry", "stale", "read_stop"])
def test_last_commit_validation_rolls_back_post_resolution(setup, monkeypatch, change):
    clock, reads, posts, journal = setup
    order, _ = terminal(setup)
    approved = acceptance(setup, order)
    before = posts.snapshot()
    original = posts._write

    def write(*args, **kwargs):
        result = original(*args, **kwargs)
        if args[2] == "TRADE_RESOLVED":
            if change == "code":
                monkeypatch.setattr(journal, "_current_implementation", lambda: "b" * 64)
            elif change == "read_stop":
                reads.stop()
            else:
                clock.advance(30 if change == "expiry" else 61)
        return result

    monkeypatch.setattr(posts, "_write", write)
    with pytest.raises(ValueError):
        journal.resolve_order_claim(
            order.client_id, approved, confirmations=RESOLUTION_CONFIRMATIONS
        )
    assert posts.snapshot() == before and posts.trade_resolutions() == []
    assert event_rows(journal, "ORDER_RESOLUTION_PREPARED")


def recovery_cli(setup, monkeypatch):
    clock, reads, posts, journal = setup
    recovery = PrivateOrderRecovery(
        journal.path.parent,
        reads.path.parent,
        reads.scope,
        clock=lambda: clock.now,
        monotonic=lambda: clock.mono,
    )
    monkeypatch.setattr(
        "trading.private_order_recovery.PrivateOrderRecovery", lambda *a, **k: recovery
    )
    return [
        "--directory",
        str(journal.path.parent),
        "--read-control-directory",
        str(reads.path.parent),
        "--scope",
        reads.scope,
        "--client-id",
        "Buy001",
    ]


def test_cli_context_and_resolution_use_no_http_or_credentials(
    setup, monkeypatch, capsys, tmp_path
):
    _, _, _, journal = setup
    order, _ = terminal(setup)
    args = recovery_cli(setup, monkeypatch)
    before = journal.snapshot()
    main(["resolution-context", *args])
    context = json.loads(capsys.readouterr().out)
    assert (
        context == journal.order_resolution_context(order.client_id)
        and journal.snapshot() == before
    )
    approved = acceptance(setup, order)
    path = tmp_path / "resolution-approval.json"
    path.write_text(approved.model_dump_json(), encoding="utf-8")
    confirms = [value for item in sorted(RESOLUTION_CONFIRMATIONS) for value in ("--confirm", item)]
    main(["resolve", *args, "--approval", str(path), *confirms])
    result = json.loads(capsys.readouterr().out)
    assert result["post_claim_resolved"] and result["post_phase"] == "STOPPED"
    assert reopen(setup)[2].snapshot()["claim"] is None


@pytest.mark.parametrize(
    "failure", ["missing", "oversize", "invalid", "confirmation", "saved_proof"]
)
def test_cli_refuses_invalid_approval_without_exposing_file_contents(
    setup, monkeypatch, capsys, tmp_path, failure
):
    _, _, posts, journal = setup
    order, _ = terminal(setup)
    args = recovery_cli(setup, monkeypatch)
    approved = acceptance(setup, order)
    path = tmp_path / "approval.json"
    path.write_text(
        approved.model_dump_json()
        if failure == "confirmation"
        else "private-file-secret" * (4000 if failure == "oversize" else 1),
        encoding="utf-8",
    )
    before = posts.snapshot()
    if failure == "saved_proof":
        path.write_text(approved.model_dump_json(), encoding="utf-8")
        with sqlite3.connect(journal.path) as conn:
            conn.execute("UPDATE account_gate SET proof_json='{}'")
    with pytest.raises(SystemExit) as error:
        main(
            [
                "resolve",
                *args,
                *([] if failure == "missing" else ["--approval", str(path)]),
                *(
                    [
                        value
                        for item in sorted(RESOLUTION_CONFIRMATIONS)
                        for value in ("--confirm", item)
                    ]
                    if failure == "saved_proof"
                    else []
                ),
            ]
        )
    output = capsys.readouterr()
    assert error.value.code == 2 and output.err == "private_order_recovery_failed\n"
    assert "private-file-secret" not in output.out + output.err
    assert posts.snapshot() == before and not event_rows(journal, "ORDER_RESOLUTION_PREPARED")


@pytest.mark.parametrize(
    "damage", ["drop", "delete", "body", "marker", "prepared_delete", "prepared_body"]
)
def test_missing_or_changed_resolution_records_refuse_reopen(setup, damage):
    _, _, _, journal = setup
    order, _ = terminal(setup)
    journal.resolve_order_claim(
        order.client_id, acceptance(setup, order), confirmations=RESOLUTION_CONFIRMATIONS
    )
    setup = reopen(setup)
    _, _, posts, journal = setup
    path = journal.path if damage.startswith("prepared") else posts.path
    with sqlite3.connect(path) as conn:
        if damage == "drop":
            conn.execute("DROP TABLE trade_resolutions")
        elif damage == "delete":
            conn.execute("DELETE FROM trade_resolutions")
        elif damage == "body":
            conn.execute("UPDATE trade_resolutions SET body='{}'")
        elif damage == "marker":
            conn.execute("DELETE FROM events WHERE kind='TRADE_RESOLVED'")
        elif damage == "prepared_delete":
            conn.execute("DELETE FROM events WHERE kind='ORDER_RESOLUTION_PREPARED'")
        else:
            conn.execute(
                "UPDATE events SET payload_json='{}' WHERE kind='ORDER_RESOLUTION_PREPARED'"
            )
    with pytest.raises(ValueError):
        reopen(setup)


@pytest.mark.parametrize("phase", ["prepared", "post_before_commit", "post_after_commit"])
def test_actual_process_exit_across_resolution_commits_is_recoverable_without_resend(setup, phase):
    clock, reads, posts, journal = setup
    order, _ = terminal(setup, operation="cancel", partial=True)
    approved = acceptance(setup, order)
    claim = posts.snapshot()["claim"]
    script = r"""
import ctypes,os,socket,sys
from datetime import datetime
sys.path.insert(0,"tests")
from test_private_order import Clock
from trading.read_control import PersistentReadLimiter
from trading.post_control import PersistentPostLimiter
from trading.live_journal import LiveOrderJournal,OrderResolutionApproval,RESOLUTION_CONFIRMATIONS
def forbidden(*a,**k): raise AssertionError("real network/native credentials forbidden")
socket.socket=forbidden; ctypes.WinDLL=forbidden
c=Clock(); c.now=datetime.fromisoformat(sys.argv[4]); c.mono=3.2
r=PersistentReadLimiter(sys.argv[1],"synthetic")
p=PersistentPostLimiter(sys.argv[2],r,**c.post_args())
j=LiveOrderJournal(sys.argv[3],p,clock=lambda:c.now)
phase=sys.argv[6]
if phase=="post_before_commit":
    original=p._write
    def write(*a,**k):
        result=original(*a,**k)
        if a[2]=="TRADE_RESOLVED": os._exit(75)
        return result
    p._write=write
else:
    original=p._resolve_trade
    def release(*a,**k):
        if phase=="prepared": os._exit(75)
        result=original(*a,**k); os._exit(75)
    p._resolve_trade=release
j.resolve_order_claim("Buy001",OrderResolutionApproval.model_validate_json(sys.argv[5]),
    confirmations=RESOLUTION_CONFIRMATIONS)
"""
    process = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(reads.path.parent),
            str(posts.path.parent),
            str(journal.path.parent),
            clock.now.isoformat(),
            approved.model_dump_json(),
            phase,
        ],
        capture_output=True,
        timeout=15,
    )
    assert process.returncode == 75, process.stderr.decode()
    setup = reopen(setup)
    _, _, posts, journal = setup
    assert journal.snapshot()["halted"] and not journal.snapshot()["live_enabled"]
    assert len(event_rows(journal, "ORDER_RESOLUTION_PREPARED")) == 1
    if phase == "post_after_commit":
        assert posts.snapshot()["claim"] is None and len(posts.trade_resolutions()) == 1
    else:
        assert posts.snapshot()["claim"] == claim and posts.trade_resolutions() == []
        with pytest.raises(ValueError):
            journal.resolve_order_claim(
                order.client_id, approved, confirmations=RESOLUTION_CONFIRMATIONS
            )
        # A new explicit checkpoint resolves the retained claim exactly once.
        journal.resolve_order_claim(
            order.client_id, acceptance(setup, order), confirmations=RESOLUTION_CONFIRMATIONS
        )
        setup = reopen(setup)
        assert setup[2].snapshot()["claim"] is None and len(setup[2].trade_resolutions()) == 1
    with client(setup, lambda _: pytest.fail("resend after resolver exit")) as sender:
        with pytest.raises(OrderTransportError):
            sender.cancel(order.client_id)


def test_orphan_in_flight_claim_can_resolve_to_stopped_after_actual_sender_death(setup):
    clock, reads, posts, journal = setup
    order = ready(setup)
    script = r"""
import ctypes,os,socket,sys,httpx
from datetime import datetime
from pydantic import SecretStr
sys.path.insert(0,"tests")
from test_private_order import Clock
from test_account_guard import quote
from trading.read_control import PersistentReadLimiter
from trading.post_control import PersistentPostLimiter
from trading.live_journal import LiveOrderJournal
from trading.private_order import PrivateOrderClient
def forbidden(*a,**k): raise AssertionError("real network/native credentials forbidden")
socket.socket=forbidden; ctypes.WinDLL=forbidden
c=Clock(); c.now=datetime.fromisoformat(sys.argv[4])
r=PersistentReadLimiter(sys.argv[1],"synthetic")
p=PersistentPostLimiter(sys.argv[2],r,**c.post_args())
j=LiveOrderJournal(sys.argv[3],p,clock=lambda:c.now)
sender=PrivateOrderClient(SecretStr("fixture-key"),SecretStr("fixture-secret"),journal=j,
    transport=httpx.MockTransport(lambda request: os._exit(76)),clock=lambda:c.now,
    monotonic=lambda:c.mono)
sender.submit("Buy001",quote=quote(c.now))
"""
    process = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            str(reads.path.parent),
            str(posts.path.parent),
            str(journal.path.parent),
            clock.now.isoformat(),
        ],
        capture_output=True,
        timeout=15,
    )
    assert process.returncode == 76, process.stderr.decode()
    assert posts.snapshot()["phase"] == "IN_FLIGHT" and posts.snapshot()["claim"]
    clock.advance(2)
    journal.halt()
    journal.reconcile(fixture_evidence(order, 101, 201, "CANCELED", [], clock.now))
    refresh_account(setup)
    journal.resolve_order_claim(
        order.client_id, acceptance(setup, order), confirmations=RESOLUTION_CONFIRMATIONS
    )
    _, _, posts, journal = reopen(setup)
    assert posts.snapshot()["phase"] == "STOPPED" and posts.snapshot()["claim"] is None
    assert posts.snapshot()["reason"] == "operation_unknown" and journal.snapshot()["halted"]
