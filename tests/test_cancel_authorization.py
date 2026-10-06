"""Restricted cancellation acceptance preserves all trading stops and replay fences."""

import hashlib
import json
import socket
import sqlite3
import subprocess
import sys
from datetime import timedelta

import httpx
import pytest
from test_account_guard import quote
from test_private_cancel import envelope, working
from test_private_order import client
from test_private_order import setup as live_setup

from trading.live_journal import (
    CANCEL_CONFIRMATIONS,
    CANCEL_EVIDENCE_KINDS,
    AcceptanceEvidence,
    CancelApproval,
    LiveOrderJournal,
)
from trading.private_order import OrderTransportError


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("real network attempted"))


@pytest.fixture
def setup(tmp_path):
    return live_setup.__wrapped__(tmp_path)


def approval(setup, order, *, lifetime=30):
    clock, _, _, journal = setup
    context = journal.cancel_context(order.client_id)
    return CancelApproval(
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
            for kind in sorted(CANCEL_EVIDENCE_KINDS)
        ),
    )


def permit(setup, order, *, lifetime=30):
    return setup[3].authorize_cancel(
        order.client_id,
        approval(setup, order, lifetime=lifetime),
        confirmations=CANCEL_CONFIRMATIONS,
    )


def events(journal, kind):
    with sqlite3.connect(journal.path) as conn:
        return conn.execute("SELECT payload_json FROM events WHERE kind=?", (kind,)).fetchall()


@pytest.mark.parametrize("condition", ["stopped", "expired", "code_changed", "entry_halted"])
def test_restricted_cancel_preserves_permissions_risk_and_incomplete_evidence(
    setup, monkeypatch, condition
):
    clock, _, posts, journal = setup
    order, evidence = working(setup, partial=True)
    if condition == "expired":
        clock.advance(3600)
        evidence = evidence.model_copy(update={"observed_at": clock.now})
        journal.reconcile(evidence)
    elif condition == "code_changed":
        monkeypatch.setattr("trading.live_journal.implementation_sha256", lambda: "b" * 64)
    else:
        if condition == "stopped":
            journal.halt()
        with sqlite3.connect(journal.path) as conn:
            conn.execute("UPDATE account_gate SET entry_halted=1")
    before = journal.snapshot()
    context = journal.cancel_context(order.client_id)
    assert journal.snapshot() == before  # Inspection is local and read-only.
    token = permit(setup, order)
    issued = journal.snapshot()
    assert {k: v for k, v in issued.items() if k != "events"} == {
        k: v for k, v in before.items() if k != "events"
    }  # Issuance changes only append-only permission history.
    assert len(events(journal, "CANCEL_AUTHORIZED")) == 1
    reopened = LiveOrderJournal(journal.path.parent, posts, clock=lambda: clock.now)
    setup = (*setup[:3], reopened)
    calls = []

    def handler(request):
        calls.append(request)
        assert request.content == b'{"rootOrderIds":[101]}'
        assert reopened.snapshot()["live_control"] == before["live_control"]
        return httpx.Response(200, json=envelope(clock))

    with client(setup, handler) as sender:
        assert sender.cancel(order.client_id, authorization_sha256=token).accepted
        with pytest.raises(OrderTransportError):
            sender.cancel(order.client_id, authorization_sha256=token)
        with pytest.raises(OrderTransportError):
            sender.submit(order.client_id, quote=quote(clock.now))
    after = reopened.snapshot()
    assert len(calls) == 1 and after["orders"][0]["state"] == "CANCEL_PENDING"
    assert after["orders"][0]["evidence"] == evidence.model_dump(mode="json")
    assert not after["orders"][0]["evidence"]["executions_complete"]
    assert after["live_control"] == before["live_control"]
    assert after["halted"] == before["halted"]
    assert after["account_guard"] == before["account_guard"]
    assert posts.snapshot()["phase"] == "READY"
    assert context["root_order_id"] == 101


@pytest.mark.parametrize(
    "change",
    ["account", "checkpoint", "expired", "future", "confirmation", "post_stop", "read_stop"],
)
def test_authorization_refusals_write_no_permission_or_cancel_claim(setup, change):
    clock, reads, posts, journal = setup
    order, _ = working(setup)
    journal.halt()
    accepted = approval(setup, order)
    confirmations = CANCEL_CONFIRMATIONS
    if change == "account":
        accepted = accepted.model_copy(update={"account_id": "foreign"})
    elif change == "checkpoint":
        accepted = accepted.model_copy(update={"checkpoint_sha256": "b" * 64})
    elif change == "expired":
        clock.advance(30)
    elif change == "future":
        accepted = accepted.model_copy(update={"accepted_at": clock.now + timedelta(seconds=1)})
    elif change == "confirmation":
        confirmations = {"cancel-only"}
    elif change == "post_stop":
        posts.stop()
    elif change == "read_stop":
        reads.stop()
    with pytest.raises(ValueError):
        journal.authorize_cancel(order.client_id, accepted, confirmations=confirmations)
    assert not events(journal, "CANCEL_AUTHORIZED") and not events(journal, "CANCEL_CLAIMED")
    assert journal.snapshot()["halted"]


def test_acceptance_contract_requires_bounded_time_and_distinct_evidence(setup, subtests):
    order, _ = working(setup)
    accepted = approval(setup, order).model_dump()
    for change, update in {
        "too_long": {"expires_at": accepted["accepted_at"] + timedelta(minutes=10, seconds=1)},
        "naive": {"accepted_at": accepted["accepted_at"].replace(tzinfo=None)},
        "missing": {"evidence": accepted["evidence"][:-1]},
        "duplicate": {"evidence": (accepted["evidence"][0],) * 3},
        "extra": {"enable_orders": True},
    }.items():
        with subtests.test(change=change), pytest.raises(ValueError):
            CancelApproval.model_validate({**accepted, **update})


@pytest.mark.parametrize(
    "change", ["halt", "code", "get", "gate", "post", "new_permission", "wrong_token"]
)
def test_permission_fences_changes_before_post_claim(setup, monkeypatch, change):
    clock, _, posts, journal = setup
    order, evidence = working(setup)
    journal.halt()
    token = permit(setup, order)
    if change == "halt":
        journal.halt()
    elif change == "code":
        monkeypatch.setattr(journal, "_current_implementation", lambda: "b" * 64)
    elif change == "get":
        clock.advance(1)
        journal.reconcile(evidence.model_copy(update={"observed_at": clock.now}))
    elif change == "gate":
        with sqlite3.connect(journal.path) as conn:
            conn.execute("UPDATE account_gate SET entry_halted=1")
    elif change == "post":
        with posts.token_slot("POST"):
            pass
    elif change == "new_permission":
        assert permit(setup, order) != token
    else:
        token = "f" * 64
    with client(setup, lambda _: pytest.fail("stale permission HTTP")) as sender:
        with pytest.raises(OrderTransportError, match="cancel_preflight_refused"):
            sender.cancel(order.client_id, authorization_sha256=token)
    assert not events(journal, "CANCEL_CLAIMED")


@pytest.mark.parametrize("change", ["expiry", "halt", "code", "stale_evidence"])
def test_permission_checked_after_pacing_without_consuming_cancel_attempt(
    setup, monkeypatch, change
):
    clock, _, posts, journal = setup
    order, _ = working(setup)
    journal.halt()
    token = permit(setup, order, lifetime=1 if change == "expiry" else 120)

    def wait(seconds):
        clock.advance(seconds)
        if change == "halt":
            journal.halt()
        elif change == "code":
            monkeypatch.setattr(journal, "_current_implementation", lambda: "b" * 64)
        elif change == "stale_evidence":
            clock.now += timedelta(seconds=60)

    posts._sleep = wait
    with client(setup, lambda _: pytest.fail("pacing refusal HTTP")) as sender:
        with pytest.raises(OrderTransportError, match="cancel_preflight_refused"):
            sender.cancel(order.client_id, authorization_sha256=token)
    assert posts.snapshot()["phase"] == "READY" and not events(journal, "CANCEL_CLAIMED")
    assert journal.snapshot()["orders"][0]["state"] == "RECONCILING"


@pytest.mark.parametrize("change", ["expiry", "halt", "code"])
def test_permission_final_gate_failure_closes_the_claim_and_spends_the_permission(
    setup, monkeypatch, change
):
    clock, _, posts, journal = setup
    order, _ = working(setup)
    journal.halt()
    token = permit(setup, order)
    original = journal.begin_cancel

    def begin(*args, **kwargs):
        result = original(*args, **kwargs)
        if change == "expiry":
            clock.advance(30)
        elif change == "halt":
            journal.halt()
        else:
            monkeypatch.setattr(journal, "_current_implementation", lambda: "b" * 64)
        return result

    monkeypatch.setattr(journal, "begin_cancel", begin)
    with client(setup, lambda _: pytest.fail("final gate refusal HTTP")) as sender:
        with pytest.raises(OrderTransportError, match="cancel_not_sent:"):
            sender.cancel(order.client_id, authorization_sha256=token)
    assert events(journal, "CANCEL_CLAIMED") and events(journal, "CANCEL_NOT_SENT")
    assert posts.snapshot()["claim"] is None and posts.snapshot()["phase"] == "READY"
    assert journal.snapshot()["orders"][0]["state"] == "WORKING"
    monkeypatch.undo()
    # The permission was spent by its claim; the same token can never cancel again.
    with client(setup, lambda _: pytest.fail("replayed permission HTTP")) as sender:
        with pytest.raises(OrderTransportError, match="cancel_preflight_refused"):
            sender.cancel(order.client_id, authorization_sha256=token)


@pytest.mark.parametrize("change", ["expiry", "halt", "code"])
def test_acceptance_after_dispatch_saves_receipt_without_reauthorizing(setup, monkeypatch, change):
    clock, _, posts, journal = setup
    order, _ = working(setup)
    journal.halt()
    token = permit(setup, order, lifetime=2 if change == "expiry" else 30)

    def handler(request):
        assert request.content == b'{"rootOrderIds":[101]}'
        if change == "expiry":
            # Approval can expire during HTTP while the HTTP deadline remains valid.
            clock.advance(1)
        elif change == "halt":
            journal.halt()
        else:
            monkeypatch.setattr(journal, "_current_implementation", lambda: "b" * 64)
        return httpx.Response(200, json=envelope(clock))

    with client(setup, handler) as sender:
        assert sender.cancel(order.client_id, authorization_sha256=token).accepted
    view = journal.snapshot()
    assert view["orders"][0]["cancellation_receipt"]
    assert view["halted"] and view["live_control"]["phase"] == "STOPPED"
    assert not view["live_enabled"] and posts.snapshot()["claim"] is None


@pytest.mark.parametrize("state", ["UNKNOWN", "CANCELED"])
def test_permission_cannot_authorize_unidentified_or_terminal_order(setup, state):
    _, _, _, journal = setup
    order, _ = working(setup)
    accepted = approval(setup, order)
    with sqlite3.connect(journal.path) as conn:
        conn.execute("UPDATE orders SET state=?", (state,))
    with pytest.raises(ValueError):
        journal.authorize_cancel(order.client_id, accepted, confirmations=CANCEL_CONFIRMATIONS)
    assert not events(journal, "CANCEL_AUTHORIZED")


def test_permission_does_not_release_ambiguous_post_claim(setup):
    _, _, posts, journal = setup
    order, _ = working(setup)
    journal.halt()
    accepted = approval(setup, order)
    with pytest.raises(RuntimeError), posts.operation("cancel", request_sha256="a" * 64):
        raise RuntimeError("synthetic ambiguity")
    before = posts.snapshot()
    with pytest.raises(ValueError):
        journal.authorize_cancel(order.client_id, accepted, confirmations=CANCEL_CONFIRMATIONS)
    assert posts.snapshot() == before and not events(journal, "CANCEL_AUTHORIZED")


@pytest.mark.parametrize("phase", ["authorization", "claim", "receipt"])
def test_real_process_exit_keeps_restricted_permission_and_consumed_attempt(setup, phase):
    clock, reads, posts, journal = setup
    order, _ = working(setup)
    journal.halt()
    accepted = approval(setup, order)
    script = r"""
import ctypes,os,socket,sys,httpx
from datetime import datetime
from pydantic import SecretStr
sys.path.insert(0,"tests")
from test_private_order import Clock
from trading.read_control import PersistentReadLimiter
from trading.post_control import PersistentPostLimiter
from trading.live_journal import LiveOrderJournal,CancelApproval,CANCEL_CONFIRMATIONS
from trading.private_order import PrivateOrderClient
def forbidden(*a,**k): raise AssertionError("real network/native credentials forbidden")
socket.socket=forbidden; ctypes.WinDLL=forbidden
c=Clock(); c.now=datetime.fromisoformat(sys.argv[4]); c.mono=1.1
r=PersistentReadLimiter(sys.argv[1],"synthetic")
p=PersistentPostLimiter(sys.argv[2],r,**c.post_args())
j=LiveOrderJournal(sys.argv[3],p,clock=lambda:c.now)
token=j.authorize_cancel("Buy001",CancelApproval.model_validate_json(sys.argv[5]),
    confirmations=CANCEL_CONFIRMATIONS)
phase=sys.argv[6]
if phase=="authorization": os._exit(74)
original=j.begin_cancel if phase=="claim" else j.acknowledge_cancel
def die(*a,**k):
    original(*a,**k); os._exit(74)
if phase=="claim": j.begin_cancel=die
else: j.acknowledge_cancel=die
def handler(request):
    assert request.content==b'{"rootOrderIds":[101]}'
    return httpx.Response(200,json={"status":0,"data":{"success":[
        {"rootOrderId":101,"clientOrderId":"Buy001"}]},"responsetime":c.now.isoformat()})
sender=PrivateOrderClient(SecretStr("fixture-key"),SecretStr("fixture-secret"),journal=j,
    transport=httpx.MockTransport(handler),clock=lambda:c.now,monotonic=lambda:c.mono)
sender.cancel("Buy001",authorization_sha256=token)
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
    assert process.returncode == 74, process.stderr.decode()
    journal = LiveOrderJournal(journal.path.parent, posts, clock=lambda: clock.now)
    setup = (*setup[:3], journal)
    payload = json.loads(events(journal, "CANCEL_AUTHORIZED")[0][0])
    token = journal._checkpoint(payload)
    if phase == "authorization":
        with client(setup, lambda _: httpx.Response(200, json=envelope(clock))) as sender:
            assert sender.cancel(order.client_id, authorization_sha256=token).accepted
    else:
        assert posts.snapshot()["claim"]
        assert journal.order_recovery_context(order.client_id)["post_operation"] == "cancel"
        with client(setup, lambda _: pytest.fail("retry after process death")) as sender:
            with pytest.raises(OrderTransportError):
                sender.cancel(order.client_id, authorization_sha256=token)
        assert bool(journal.snapshot()["orders"][0]["cancellation_receipt"]) == (phase == "receipt")
    assert journal.snapshot()["halted"] and not journal.snapshot()["live_enabled"]
