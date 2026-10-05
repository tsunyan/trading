"""Explicit live restart, whole-account gates, old-client fencing and cross-store crashes."""

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
from test_account_guard import account, intent, quote
from test_order_resolution import acceptance as resolution_acceptance
from test_order_resolution import event_rows, refresh_account, reopen, terminal
from test_private_cancel import envelope, working
from test_private_order import client, ready, response
from test_private_order import setup as live_setup

from trading.account_guard import WorkingOrder
from trading.broker_contracts import Settlement
from trading.live_journal import (
    EVIDENCE_KINDS,
    RESOLUTION_CONFIRMATIONS,
    RESTART_CONFIRMATIONS,
    AcceptanceEvidence,
    LiveApproval,
    LiveRestartApproval,
)
from trading.post_control import PersistentPostLimiter, PostControlError
from trading.private_order import OrderTransportError
from trading.private_order_recovery import PrivateOrderRecovery
from trading.private_order_restart import main


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("real network attempted"))


@pytest.fixture
def setup(tmp_path):
    return live_setup.__wrapped__(tmp_path)


def stopped(setup, *, reason="operator_stop", resolved=False, status="CANCELED"):
    clock, _, posts, journal = setup
    if resolved:
        order, _ = terminal(setup, status=status)
        journal.resolve_order_claim(
            order.client_id,
            resolution_acceptance(setup, order),
            confirmations=RESOLUTION_CONFIRMATIONS,
        )
        setup = reopen(setup)
        clock, _, posts, journal = setup
    else:
        ready(setup)
        journal.halt()
    if reason:
        posts.stop(reason)
    clock.advance(1)
    refresh_account(setup, status=status)
    return setup


def acceptance(setup, *, lifetime=3600):
    clock, _, _, journal = setup
    context = journal.restart_context()
    return LiveRestartApproval(
        checkpoint_sha256=context["checkpoint_sha256"],
        stop_review_reference="synthetic-stop-review",
        stop_review_sha256="a" * 64,
        approval=LiveApproval(
            account_id=context["account_id"],
            configuration_sha256=context["configuration_sha256"],
            implementation_sha256=context["implementation_sha256"],
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
        ),
    )


def confirmations(setup):
    return RESTART_CONFIRMATIONS | (
        {"clock-repaired"} if setup[2].snapshot()["reason"] == "clock_invalid" else set()
    )


@pytest.mark.parametrize(
    "resolved,reason",
    [
        (False, None),
        (True, "operation_unknown"),
        (False, "order_cleanup_failed"),
    ],
)
def test_restart_requires_new_approval_preserves_history_and_dispatches_only_with_fresh_objects(
    setup, resolved, reason
):
    setup = stopped(setup, reason=reason, resolved=resolved)
    clock, reads, posts, journal = setup
    peer = PersistentPostLimiter(posts.path.parent, reads, **clock.post_args())
    before, post_before = journal.snapshot(), posts.snapshot()
    accepted = acceptance(setup)
    assert journal.snapshot() == before
    result = journal.restart(accepted, confirmations=confirmations(setup))
    assert result["live_restarted"] and result["new_clients_required"]
    for old in (posts, peer):
        with pytest.raises(PostControlError):
            old.snapshot()
        with pytest.raises(PostControlError):
            with old.token_slot("POST"):
                pytest.fail("old client resumed")
    setup = reopen(setup)
    _, _, posts, journal = setup
    after = journal.snapshot()
    assert after["live_enabled"] and not after["halted"]
    assert after["orders"] == before["orders"] and after["account_guard"] == before["account_guard"]
    assert after["live_control"]["approval"] == accepted.approval.model_dump(mode="json")
    assert posts.snapshot()["phase"] == "READY" and posts.snapshot()["claim"] is None
    assert posts.execution_restarts()[0]["post_before"] == {
        k: v for k, v in post_before.items() if k not in {"blocked", "live_enabled", "complete"}
    }
    assert len(posts.execution_restarts()) == 1
    assert event_rows(journal, "LIVE_STOPPED") and event_rows(journal, "LIVE_RESTARTED")
    if resolved:
        with client(setup, lambda _: pytest.fail("old intent replay")) as sender:
            with pytest.raises(OrderTransportError):
                sender.submit("Buy001", quote=quote(clock.now))
        order = intent(client_id="Next001")
        journal.prepare(order)
    else:
        order = intent()

    def handler(request):
        payload = response(clock, request).json()
        payload["data"][0]["rootOrderId"] = 102
        payload["data"][0]["orderId"] = 202
        return httpx.Response(200, json=payload)

    with client(setup, handler) as sender:
        assert sender.submit(order.client_id, quote=quote(clock.now)).order_id == 202


def test_restart_reapproves_current_code_without_replacing_bindings_or_stores(setup, monkeypatch):
    setup = stopped(setup)
    _, reads, posts, journal = setup
    before = journal.snapshot()["live_control"]
    binding = posts.execution_binding(), reads.post_binding()
    monkeypatch.setattr("trading.live_journal.implementation_sha256", lambda: "b" * 64)
    accepted = acceptance(setup)
    assert accepted.approval.implementation_sha256 != before["implementation_sha256"]
    journal.restart(accepted, confirmations=confirmations(setup))
    _, _, posts, journal = reopen(setup)
    assert journal.snapshot()["implementation_matches"] and journal.snapshot()["live_enabled"]
    assert journal.snapshot()["live_control"]["instance"] == before["instance"]
    assert (posts.execution_binding(), reads.post_binding()) == binding


def test_restart_preserves_loss_stop_rejects_opening_and_allows_position_close(setup):
    setup = stopped(setup, resolved=True, status="EXECUTED")
    clock, _, _, journal = setup
    with sqlite3.connect(journal.path) as conn:
        conn.execute("UPDATE account_gate SET entry_halted=1,peak='1050000'")
    before = journal.snapshot()["account_guard"]
    journal.restart(acceptance(setup), confirmations=confirmations(setup))
    setup = reopen(setup)
    _, _, _, journal = setup
    assert journal.snapshot()["account_guard"] == before
    new = intent(client_id="Next001")
    journal.prepare(new)
    with client(setup, lambda _: pytest.fail("entry halt bypassed")) as sender:
        with pytest.raises(OrderTransportError):
            sender.submit(new.client_id, quote=quote(clock.now))
    journal.abandon(new.client_id)
    closing = intent(
        client_id="Close001",
        side="SELL",
        effect="CLOSE",
        price="150",
        positions=(Settlement(position_id=401, units=1000),),
    )
    journal.prepare(closing)
    with client(setup, lambda request: response(clock, request)) as sender:
        assert sender.submit(closing.client_id, quote=quote(clock.now)).order_id == 211
    assert journal.snapshot()["account_guard"]["entry_halted"]


def test_restart_known_working_order_after_cleanup_failure_enables_its_cancellation(setup):
    clock, _, posts, journal = setup
    order, evidence = working(setup, complete=True)
    journal.halt()
    posts.stop("order_cleanup_failed")
    clock.advance(1)
    journal.reconcile(evidence.model_copy(update={"observed_at": clock.now}))
    journal.update_account(
        account(
            clock.now,
            required_margin="6000.4",
            available_margin="993999.6",
            working_orders=(
                WorkingOrder(client_id=order.client_id, order_id=201, remaining_units=1000),
            ),
        ),
        quote(clock.now),
        now=clock.now,
    )
    journal.restart(acceptance(setup), confirmations=confirmations(setup))
    setup = reopen(setup)
    with client(setup, lambda _: httpx.Response(200, json=envelope(clock))) as sender:
        assert sender.cancel(order.client_id).accepted
    assert setup[3].snapshot()["orders"][0]["state"] == "CANCEL_PENDING"


@pytest.mark.parametrize(
    "change",
    [
        "checkpoint",
        "implementation",
        "expired",
        "confirm",
        "clock_confirm",
        "reuse_approval",
        "review_reference",
    ],
)
def test_restart_rejects_invalid_operator_acceptance_without_changes(setup, change):
    setup = stopped(setup, reason="clock_invalid" if change == "clock_confirm" else "operator_stop")
    clock, _, posts, journal = setup
    accepted = acceptance(setup)
    confirms = confirmations(setup)
    if change == "checkpoint":
        accepted = accepted.model_copy(update={"checkpoint_sha256": "b" * 64})
    elif change == "implementation":
        accepted = accepted.model_copy(
            update={
                "approval": accepted.approval.model_copy(update={"implementation_sha256": "b" * 64})
            }
        )
    elif change == "expired":
        clock.advance(3600)
    elif change == "reuse_approval":
        accepted = accepted.model_copy(
            update={
                "approval": LiveApproval.model_validate_json(
                    json.dumps(journal.snapshot()["live_control"]["approval"])
                )
            }
        )
    elif change == "review_reference":
        accepted = accepted.model_copy(update={"stop_review_reference": "private-review\nnote"})
    else:
        confirms = (
            confirms - {"clock-repaired"} if change == "clock_confirm" else {"restart-orders"}
        )
    before, post_before = journal.snapshot(), posts.snapshot()
    with pytest.raises(ValueError):
        journal.restart(accepted, confirmations=confirms)
    assert journal.snapshot() == before and posts.snapshot() == post_before


@pytest.mark.parametrize(
    "change",
    [
        "claim",
        "token_failed",
        "read_stop",
        "missing_proof",
        "incomplete_proof",
        "stale",
        "order_state",
        "order_incomplete",
    ],
)
def test_restart_context_refuses_unresolved_or_incomplete_state(setup, change):
    setup = stopped(setup)
    clock, reads, posts, journal = setup
    if change == "claim":
        # A real unresolved token claim is not eligible, regardless of account equality.
        setup = stopped(live_setup.__wrapped__(journal.path.parent.parent / "another"), reason=None)
        clock, reads, posts, journal = setup
        with pytest.raises(RuntimeError), posts.token_slot("POST"):
            raise RuntimeError("synthetic ambiguity")
    elif change == "token_failed":
        posts.stop("token_failed")  # Existing operator stop takes precedence; use a READY domain.
        setup = stopped(live_setup.__wrapped__(journal.path.parent.parent / "another"), reason=None)
        clock, reads, posts, journal = setup
        posts.stop("token_failed")
    elif change == "read_stop":
        reads.stop()
    elif change == "stale":
        clock.advance(61)
    else:
        with sqlite3.connect(journal.path) as conn:
            if change == "missing_proof":
                conn.execute("UPDATE account_gate SET proof_json=NULL")
            elif change == "incomplete_proof":
                proof = json.loads(
                    conn.execute("SELECT proof_json FROM account_gate").fetchone()[0]
                )
                proof["snapshot"]["complete"] = False
                conn.execute("UPDATE account_gate SET proof_json=?", (json.dumps(proof),))
            else:
                conn.execute(
                    "UPDATE orders SET state=?",
                    ("UNKNOWN" if change == "order_state" else "WORKING",),
                )
    before = posts.snapshot()
    with pytest.raises(ValueError):
        journal.restart_context()
    assert posts.snapshot() == before and not event_rows(journal, "LIVE_RESTART_PREPARED")


@pytest.mark.parametrize(
    "change", ["halt", "code", "account", "expiry", "post_stop", "prepared_damage"]
)
def test_checkpoint_changes_between_preparation_and_restart_leave_both_stopped(
    setup, monkeypatch, change
):
    setup = stopped(setup)
    clock, _, posts, journal = setup
    accepted = acceptance(setup, lifetime=30)
    original = journal._transaction
    changed = False

    @contextmanager
    def transaction():
        nonlocal changed
        with original() as conn:
            yield conn
        if not changed and event_rows(journal, "LIVE_RESTART_PREPARED"):
            changed = True
            if change == "halt":
                journal.halt()
            elif change == "code":
                monkeypatch.setattr(journal, "_current_implementation", lambda: "b" * 64)
            elif change == "account":
                clock.advance(1)
                refresh_account(setup)
            elif change == "expiry":
                clock.advance(30)
            elif change == "post_stop":
                posts.stop("clock_invalid")
            else:
                with sqlite3.connect(journal.path) as conn:
                    conn.execute(
                        "UPDATE events SET payload_json='{}' WHERE kind='LIVE_RESTART_PREPARED'"
                    )

    monkeypatch.setattr(journal, "_transaction", transaction)
    with pytest.raises(ValueError):
        journal.restart(accepted, confirmations=confirmations(setup))
    assert journal.snapshot()["halted"] and posts.snapshot()["phase"] == "STOPPED"
    assert not posts.execution_restarts() and event_rows(journal, "LIVE_RESTART_PREPARED")


@pytest.mark.parametrize("change", ["code", "expiry"])
def test_last_post_validation_rolls_back_live_permission_and_post_restart(
    setup, monkeypatch, change
):
    setup = stopped(setup)
    clock, reads, posts, journal = setup
    accepted = acceptance(setup, lifetime=30)
    before, post_before = journal.snapshot(), posts.snapshot()
    original = posts._write

    def write(*args, **kwargs):
        result = original(*args, **kwargs)
        if args[2] == "EXECUTION_RESTARTED":
            if change == "code":
                monkeypatch.setattr(journal, "_current_implementation", lambda: "b" * 64)
            elif change == "expiry":
                clock.advance(30)
            else:
                reads.stop()
        return result

    monkeypatch.setattr(posts, "_write", write)
    with pytest.raises(ValueError):
        journal.restart(accepted, confirmations=confirmations(setup))
    assert posts.snapshot() == post_before and not posts.execution_restarts()
    assert (
        journal.snapshot()["live_control"] == before["live_control"]
        and journal.snapshot()["halted"]
    )


@pytest.mark.parametrize("phase", ["post_before_commit", "post_after_commit"])
def test_actual_process_exit_keeps_stopped_live_permission_or_completed_restart(setup, phase):
    setup = stopped(setup)
    clock, reads, posts, journal = setup
    accepted = acceptance(setup)
    script = r"""
import ctypes,os,socket,sys
from datetime import datetime
sys.path.insert(0,"tests")
from test_private_order import Clock
from trading.read_control import PersistentReadLimiter
from trading.post_control import PersistentPostLimiter
from trading.live_journal import LiveOrderJournal,LiveRestartApproval,RESTART_CONFIRMATIONS
def forbidden(*a,**k): raise AssertionError("real network/native credentials forbidden")
socket.socket=forbidden; ctypes.WinDLL=forbidden
c=Clock(); c.now=datetime.fromisoformat(sys.argv[4]); c.mono=1
r=PersistentReadLimiter(sys.argv[1],"synthetic")
p=PersistentPostLimiter(sys.argv[2],r,**c.post_args())
j=LiveOrderJournal(sys.argv[3],p,clock=lambda:c.now)
phase=sys.argv[6]
if phase=="post_before_commit":
    original=p._write
    def write(*a,**k):
        result=original(*a,**k)
        if a[2]=="EXECUTION_RESTARTED": os._exit(77)
        return result
    p._write=write
elif phase in {"prepared","post_after_commit"}:
    original=p._restart_execution
    def restart(*a,**k):
        if phase=="prepared": os._exit(77)
        result=original(*a,**k); os._exit(77)
    p._restart_execution=restart
j.restart(LiveRestartApproval.model_validate_json(sys.argv[5]),confirmations=RESTART_CONFIRMATIONS)
os._exit(77)
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
            accepted.model_dump_json(),
            phase,
        ],
        capture_output=True,
        timeout=15,
    )
    assert process.returncode == 77, process.stderr.decode()
    setup = reopen(setup)
    _, _, posts, journal = setup
    view = journal.snapshot()
    assert view["live_enabled"] == (phase == "completed")
    assert view["halted"] == (phase != "completed")
    assert (posts.snapshot()["phase"] == "READY") == (phase in {"post_after_commit", "completed"})
    if phase != "completed":
        with client(setup, lambda _: pytest.fail("partial restart dispatched")) as sender:
            with pytest.raises(OrderTransportError):
                sender.submit("Buy001", quote=quote(clock.now))
        with pytest.raises(ValueError):
            journal.restart(accepted, confirmations=confirmations(setup))
        clock.advance(1)
        refresh_account(setup)
        journal.restart(acceptance(setup), confirmations=confirmations(setup))
        assert reopen(setup)[3].snapshot()["live_enabled"]


def test_cli_is_local_and_requires_explicit_restart_file_and_confirmations(
    setup, monkeypatch, capsys, tmp_path
):
    setup = stopped(setup)
    clock, reads, _, journal = setup
    operations = PrivateOrderRecovery(
        journal.path.parent,
        reads.path.parent,
        reads.scope,
        clock=lambda: clock.now,
        monotonic=lambda: clock.mono,
    )
    monkeypatch.setattr(
        "trading.private_order_restart.PrivateOrderRecovery", lambda *a, **k: operations
    )
    args = [
        "--directory",
        str(journal.path.parent),
        "--read-control-directory",
        str(reads.path.parent),
        "--scope",
        reads.scope,
    ]
    main(["context", *args])
    context = json.loads(capsys.readouterr().out)
    assert context == journal.restart_context()
    accepted = acceptance(setup)
    path = tmp_path / "restart-approval.json"
    path.write_text(accepted.model_dump_json(), encoding="utf-8")
    with pytest.raises(SystemExit):
        main(["restart", *args, "--approval", str(path)])
    assert capsys.readouterr().err == "private_order_restart_failed\n"
    confirms = [v for item in sorted(confirmations(setup)) for v in ("--confirm", item)]
    main(["restart", *args, "--approval", str(path), *confirms])
    assert json.loads(capsys.readouterr().out)["new_clients_required"]
    assert reopen(setup)[3].snapshot()["live_enabled"]


@pytest.mark.parametrize("change", ["code", "expiry"])
def test_failure_after_post_commit_keeps_live_stopped_with_persistent_preparation(
    setup, monkeypatch, change
):
    setup = stopped(setup)
    clock, reads, posts, journal = setup
    accepted = acceptance(setup, lifetime=30)
    before = journal.snapshot()["live_control"]
    original = posts._restart_execution

    def restart(*args, **kwargs):
        result = original(*args, **kwargs)
        if change == "code":
            monkeypatch.setattr(journal, "_current_implementation", lambda: "b" * 64)
        elif change == "expiry":
            clock.advance(30)
        else:
            reads.stop()
        return result

    monkeypatch.setattr(posts, "_restart_execution", restart)
    with pytest.raises(ValueError):
        journal.restart(accepted, confirmations=confirmations(setup))
    setup = reopen(setup)
    _, _, posts, journal = setup
    assert posts.snapshot()["phase"] == "READY" and len(posts.execution_restarts()) == 1
    assert journal.snapshot()["live_control"] == before and journal.snapshot()["halted"]
    assert not event_rows(journal, "LIVE_RESTARTED")


@pytest.mark.parametrize("damage", ["delete", "body", "marker", "prepared", "completed"])
def test_restart_history_missing_or_changed_refuses_reopen(setup, damage):
    setup = stopped(setup)
    journal = setup[3]
    journal.restart(acceptance(setup), confirmations=confirmations(setup))
    setup = reopen(setup)
    _, _, posts, journal = setup
    path = journal.path if damage in {"prepared", "completed"} else posts.path
    with sqlite3.connect(path) as conn:
        if damage == "delete":
            conn.execute("DELETE FROM execution_restarts")
        elif damage == "body":
            conn.execute("UPDATE execution_restarts SET body='{}'")
        elif damage == "marker":
            conn.execute("DELETE FROM events WHERE kind='EXECUTION_RESTARTED'")
        elif damage == "prepared":
            conn.execute("DELETE FROM events WHERE kind='LIVE_RESTART_PREPARED'")
        else:
            conn.execute("DELETE FROM events WHERE kind='LIVE_RESTARTED'")
    with pytest.raises(ValueError):
        reopen(setup)


@pytest.mark.parametrize("failure", ["oversize", "invalid", "saved_proof"])
def test_cli_failure_preserves_stop_and_never_exposes_supplied_contents(
    setup, monkeypatch, capsys, tmp_path, failure
):
    setup = stopped(setup)
    clock, reads, posts, journal = setup
    operations = PrivateOrderRecovery(
        journal.path.parent,
        reads.path.parent,
        reads.scope,
        clock=lambda: clock.now,
        monotonic=lambda: clock.mono,
    )
    monkeypatch.setattr(
        "trading.private_order_restart.PrivateOrderRecovery", lambda *a, **k: operations
    )
    monkeypatch.setattr(
        "trading.credential_store.CredentialVault.load",
        lambda *a, **k: pytest.fail("native credentials touched"),
    )
    args = [
        "--directory",
        str(journal.path.parent),
        "--read-control-directory",
        str(reads.path.parent),
        "--scope",
        reads.scope,
    ]
    before = posts.snapshot()
    path = tmp_path / "approval.json"
    if failure == "saved_proof":
        with sqlite3.connect(journal.path) as conn:
            conn.execute("UPDATE account_gate SET proof_json='{}'")
        command = ["context", *args]
    else:
        path.write_text(
            "private-review-note" * (4000 if failure == "oversize" else 1), encoding="utf-8"
        )
        command = ["restart", *args, "--approval", str(path)]
    with pytest.raises(SystemExit) as error:
        main(command)
    output = capsys.readouterr()
    assert error.value.code == 2 and output.err == "private_order_restart_failed\n"
    assert "private-review-note" not in output.out + output.err
    assert posts.snapshot() == before and not event_rows(journal, "LIVE_RESTART_PREPARED")
