"""Single live cancellation, positive receipt, durable fencing and GET investigation."""

import hashlib
import hmac
import json
import socket
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import httpx
import pytest
from pydantic import SecretStr
from test_account_guard import account, fill, intent, quote
from test_order_recovery import Vault, checks
from test_order_recovery import transport as get_transport
from test_private_order import client, ready, response
from test_private_order import setup as live_setup

from trading.account_guard import Position
from trading.account_reader import AccountReader
from trading.broker_contracts import Settlement
from trading.execution_lab import fixture_evidence
from trading.live_journal import LiveOrderError, LiveOrderJournal
from trading.order_journal import OrderBlocked
from trading.order_receipts import MAX_RECEIPT_BYTES, ReceiptError, parse_cancellation_receipt
from trading.post_control import PostControlError
from trading.private_order import OrderTransportError
from trading.private_order_recovery import PrivateOrderRecovery
from trading.private_read import PrivateReadClient
from trading.read_control import PersistentReadLimiter


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("real network attempted"))


@pytest.fixture
def setup(tmp_path):
    return live_setup.__wrapped__(tmp_path)


def working(setup, *, partial=False, complete=False):
    clock, _, _, journal = setup
    order = ready(setup)
    with client(setup, lambda request: response(clock, request)) as sender:
        sender.submit(order.client_id, quote=quote(clock.now))
    fills = (
        []
        if not partial
        else [
            {
                "executionId": 301,
                "positionId": 401,
                "size": "400",
                "price": "150.01",
                "fee": "-2",
                "lossGain": "0",
                "settledSwap": "0",
                "timestamp": clock.now.isoformat(),
            }
        ]
    )
    evidence = fixture_evidence(order, 101, 201, "ORDERED", fills, clock.now).model_copy(
        update={"executions_complete": complete}
    )
    journal.reconcile(evidence)
    return order, evidence


def envelope(clock, client_id="Buy001"):
    return {
        "status": 0,
        "data": {"success": [{"rootOrderId": 101, "clientOrderId": client_id}]},
        "responsetime": clock.now.isoformat(),
    }


def raw(value):
    return json.dumps(value, separators=(",", ":")).encode()


@pytest.mark.parametrize(
    "partial,complete", [(False, False), (True, False), (False, True), (True, True)]
)
def test_cancel_signs_once_with_owned_claim_and_retains_pending_fills(setup, partial, complete):
    clock, _, posts, journal = setup
    order, evidence = working(setup, partial=partial, complete=complete)
    calls = []

    def handler(request):
        calls.append(request)
        assert (
            request.method == "POST"
            and str(request.url) == "https://forex-api.coin.z.com/private/v1/cancelOrders"
        )
        assert request.content == b'{"rootOrderIds":[101]}'
        payload = (
            request.headers["API-TIMESTAMP"] + "POST/v1/cancelOrders"
        ).encode() + request.content
        assert (
            request.headers["API-SIGN"]
            == hmac.new(b"fixture-secret", payload, hashlib.sha256).hexdigest()
        )
        post = posts.snapshot()
        assert post["phase"] == "IN_FLIGHT" and post["operation"] == "cancel"
        assert post["request_sha256"] == hashlib.sha256(request.content).hexdigest()
        row = journal.snapshot()["orders"][0]
        assert row["state"] == "CANCEL_PENDING" and row["evidence"] == evidence.model_dump(
            mode="json"
        )
        return httpx.Response(200, json=envelope(clock))

    with client(setup, handler) as sender:
        receipt = sender.cancel(order.client_id)
        with pytest.raises(OrderTransportError):
            sender.cancel(order.client_id)
    assert len(calls) == 1 and receipt.accepted and receipt.root_order_id == 101
    assert "API-KEY" not in calls[0].headers and "API-SIGN" not in calls[0].headers
    view = journal.snapshot()
    assert (
        view["orders"][0]["state"] == "CANCEL_PENDING" and view["orders"][0]["cancellation_receipt"]
    )
    assert posts.snapshot()["phase"] == "READY" and posts.snapshot()["claim"] is None
    assert not view["halted"] and view["live_enabled"]
    # Incomplete or unchanged working GETs do not imply final cancellation.
    assert (
        journal.reconcile(evidence.model_copy(update={"observed_at": clock.now}))
        == "CANCEL_PENDING"
    )
    with pytest.raises(OrderBlocked):
        journal.prepare(order.model_copy(update={"client_id": "Next001"}))


@pytest.mark.parametrize(
    "failure",
    [
        "empty",
        "root",
        "client",
        "status",
        "extension",
        "duplicate",
        "boolean",
        "future",
        "stale",
        "utf8",
        "too_large",
        "wrong_type",
    ],
)
def test_cancel_receipt_requires_exact_documented_positive_envelope(setup, failure):
    clock = setup[0]
    data = envelope(clock)
    if failure == "empty":
        data["data"]["success"] = []
    elif failure == "root":
        data["data"]["success"][0]["rootOrderId"] = 102
    elif failure == "client":
        data["data"]["success"][0]["clientOrderId"] = "Other"
    elif failure == "status":
        data["status"] = 1
    elif failure == "extension":
        data["data"]["failed"] = []
    elif failure == "boolean":
        data["data"]["success"][0]["rootOrderId"] = True
    elif failure == "future":
        data["responsetime"] = (clock.now + timedelta(seconds=1)).isoformat()
    elif failure == "stale":
        data["responsetime"] = (clock.now - timedelta(seconds=1)).isoformat()
    body = raw(data)
    if failure == "duplicate":
        body = body.replace(b'"status":0', b'"status":1,"status":0')
    elif failure == "utf8":
        body = b"\xff"
    elif failure == "too_large":
        body = b" " * (MAX_RECEIPT_BYTES + 1)
    elif failure == "wrong_type":
        body = data
    with pytest.raises(ReceiptError, match="^invalid_cancellation_receipt$"):
        parse_cancellation_receipt("Buy001", 101, body, started_at=clock.now, received_at=clock.now)


def test_cancel_receipt_has_digest_clock_skew_and_strict_local_clocks(setup):
    clock = setup[0]
    data = envelope(clock)
    data["responsetime"] = (clock.now + timedelta(milliseconds=500)).isoformat()
    receipt = parse_cancellation_receipt(
        "Buy001", 101, raw(data), started_at=clock.now, received_at=clock.now, clock_skew_ms=500
    )
    assert receipt.payload_sha256 == hashlib.sha256(raw(data)).hexdigest()
    with pytest.raises(ReceiptError):
        parse_cancellation_receipt(
            "Buy001", 101, raw(data), started_at=clock.now.isoformat(), received_at=clock.now
        )


@pytest.mark.parametrize(
    "failure", [401, 302, "timeout", "api", "empty", "root", "duplicate", "oversize", "cleanup"]
)
def test_cancel_failure_keeps_both_claims_and_never_retries(setup, failure):
    clock, _, posts, journal = setup
    order, _ = working(setup)
    calls = []

    def handler(request):
        calls.append(request)
        if type(failure) is int:
            return httpx.Response(failure, content=b"remote secret")
        if failure == "timeout":
            raise httpx.ReadTimeout("remote secret", request=request)
        data = envelope(clock)
        if failure == "api":
            data.update(status=1, messages=[{"message": "remote secret"}])
        elif failure == "empty":
            data["data"]["success"] = []
        elif failure == "root":
            data["data"]["success"][0]["rootOrderId"] = 102
        result = httpx.Response(200, json=data)
        if failure == "duplicate":
            result = httpx.Response(
                200,
                headers={"content-type": "application/json"},
                content=raw(data).replace(b'"status":0', b'"status":0,"status":1'),
            )
        elif failure == "oversize":
            result.headers["content-length"] = str(MAX_RECEIPT_BYTES + 1)
        elif failure == "cleanup":
            result.close = lambda: (_ for _ in ()).throw(RuntimeError("remote secret"))
        return result

    with client(setup, handler) as sender:
        with pytest.raises(OrderTransportError) as caught:
            sender.cancel(order.client_id)
        assert "remote secret" not in str(caught.value)
        with pytest.raises(OrderTransportError):
            sender.cancel(order.client_id)
    assert len(calls) == 1 and journal.snapshot()["orders"][0]["state"] == "UNKNOWN"
    assert (
        journal.snapshot()["halted"]
        and journal.snapshot()["orders"][0]["cancellation_receipt"] is None
    )
    assert posts.snapshot()["phase"] == "STOPPED" and posts.snapshot()["operation"] == "cancel"
    assert posts.snapshot()["claim"] and b"remote secret" not in journal.path.read_bytes()


@pytest.mark.parametrize(
    "refusal",
    ["no_get", "expired", "code", "read_stop", "live_stop", "stale", "future", "terminal"],
)
def test_cancel_preflight_refuses_without_any_cancel_http_or_claim(setup, monkeypatch, refusal):
    clock, reads, posts, journal = setup
    if refusal == "no_get":
        order = ready(setup)
        with client(setup, lambda request: response(clock, request)) as sender:
            sender.submit(order.client_id, quote=quote(clock.now))
    else:
        order, evidence = working(setup)
        if refusal == "expired":
            clock.advance(3600)
        elif refusal == "code":
            monkeypatch.setattr("trading.live_journal.implementation_sha256", lambda: "b" * 64)
        elif refusal == "read_stop":
            reads.stop()
        elif refusal == "live_stop":
            journal.halt()
        elif refusal == "stale":
            clock.advance(61)
        elif refusal == "future":
            clock.now -= timedelta(seconds=1)
        elif refusal == "terminal":
            clock.advance(0.1)
            journal.reconcile(
                evidence.model_copy(
                    update={
                        "status": "CANCELED",
                        "executions_complete": True,
                        "observed_at": clock.now,
                    }
                )
            )
    before = posts.snapshot()
    with client(setup, lambda _: pytest.fail("cancel HTTP on invalid preflight")) as sender:
        with pytest.raises(OrderTransportError, match="cancel_preflight_refused"):
            sender.cancel(order.client_id)
    assert posts.snapshot() == before
    assert not any(e["kind"] == "CANCEL_CLAIMED" for e in journal.snapshot()["events"])


def test_cancel_rechecks_evidence_after_pacing_and_known_refusal_does_not_consume_attempt(setup):
    clock, _, posts, journal = setup
    order, _ = working(setup)
    clock.advance(59)
    with client(setup, lambda _: pytest.fail("stale after wait")) as sender:
        with pytest.raises(OrderTransportError, match="cancel_preflight_refused"):
            sender.cancel(order.client_id)
    assert posts.snapshot()["phase"] == "READY" and not journal.snapshot()["halted"]
    assert not any(e["kind"] == "CANCEL_CLAIMED" for e in journal.snapshot()["events"])


def test_claim_history_prevents_resend_after_row_state_reset(setup):
    clock, _, _, journal = setup
    order, _ = working(setup, complete=True)
    with client(setup, lambda _: httpx.Response(200, json=envelope(clock))) as sender:
        sender.cancel(order.client_id)
    with sqlite3.connect(journal.path) as conn:
        conn.execute("UPDATE orders SET state='WORKING'")
    with client(setup, lambda _: pytest.fail("cancel resent after reset")) as sender:
        with pytest.raises(OrderTransportError):
            sender.cancel(order.client_id)


@pytest.mark.parametrize("damage", ["missing", "changed"])
def test_cancel_requires_matching_recorded_get_evidence_before_http(setup, damage):
    _, _, posts, journal = setup
    order, evidence = working(setup)
    with journal._transaction() as conn:
        if damage == "missing":
            conn.execute("DELETE FROM events WHERE kind='RECONCILED'")
        else:
            conn.execute(
                "UPDATE orders SET evidence_json=?",
                (evidence.model_copy(update={"status": "WAITING"}).model_dump_json(),),
            )
    saved = posts.snapshot()
    with client(setup, lambda _: pytest.fail("cancel without recorded evidence")) as sender:
        with pytest.raises(OrderTransportError, match="cancel_preflight_refused"):
            sender.cancel(order.client_id)
    assert posts.snapshot() == saved


def test_owned_claim_is_required_and_raw_offline_cancel_ack_is_refused(setup):
    _, _, _, journal = setup
    order, _ = working(setup)
    with pytest.raises(PostControlError):
        journal.begin_cancel(order.client_id)
    with pytest.raises(LiveOrderError, match="explicit_live_cancel_receipt_required"):
        journal.cancellation_response(order.client_id, {"status": 0, "data": {"success": []}})
    assert not any(e["kind"] == "CANCEL_CLAIMED" for e in journal.snapshot()["events"])


def test_confirmed_market_order_can_cancel_without_releasing_submission(setup):
    clock, _, _, journal = setup
    order = ready(setup, intent(kind="MARKET", price=None, bound="150.02"))
    with client(setup, lambda request: response(clock, request)) as sender:
        sender.submit(order.client_id, quote=quote(clock.now))
    journal.reconcile(
        fixture_evidence(order, 101, 201, "WAITING", [], clock.now).model_copy(
            update={"executions_complete": False}
        )
    )
    with client(setup, lambda _: httpx.Response(200, json=envelope(clock))) as sender:
        assert sender.cancel(order.client_id).accepted
    assert journal.snapshot()["orders"][0]["state"] == "CANCEL_PENDING"
    with client(setup, lambda _: pytest.fail("submission repeated")) as sender:
        with pytest.raises(OrderTransportError):
            sender.submit(order.client_id, quote=quote(clock.now))


def test_closing_order_cancel_targets_its_own_root_and_preserves_positions(setup):
    clock, _, _, journal = setup
    opened, _ = working(setup)
    clock.advance(0.1)
    journal.reconcile(
        fixture_evidence(
            opened,
            101,
            201,
            "EXECUTED",
            [fill(timestamp=(clock.now - timedelta(seconds=0.1)).isoformat())],
            clock.now,
        )
    )
    position = Position(position_id=401, side="BUY", units=1000, average_price="150.01")
    journal.update_account(
        account(
            clock.now,
            balance="999997",
            equity="999987",
            required_margin="6000.4",
            available_margin="993986.6",
            positions=(position,),
        ),
        quote(clock.now),
        now=clock.now,
    )
    close = intent(
        client_id="Close001",
        side="SELL",
        effect="CLOSE",
        price="150",
        positions=(Settlement(position_id=401, units=1000),),
    )
    journal.prepare(close)
    with client(setup, lambda request: response(clock, request)) as sender:
        sender.submit(close.client_id, quote=quote(clock.now))
    journal.reconcile(
        fixture_evidence(close, 111, 211, "ORDERED", [], clock.now).model_copy(
            update={"executions_complete": False}
        )
    )
    before = journal.snapshot()["account_guard"]

    def handler(request):
        assert request.content == b'{"rootOrderIds":[111]}'
        data = envelope(clock, "Close001")
        data["data"]["success"][0]["rootOrderId"] = 111
        return httpx.Response(200, json=data)

    with client(setup, handler) as sender:
        assert sender.cancel(close.client_id).root_order_id == 111
    assert journal.snapshot()["account_guard"] == before
    assert journal.snapshot()["orders"][0]["state"] == "FILLED"
    assert journal.snapshot()["orders"][1]["state"] == "CANCEL_PENDING"


def test_real_get_report_can_cancel_without_promoting_account_or_clearing_entry_halt(setup):
    clock, reads, _, journal = setup
    order = ready(setup)
    with client(setup, lambda request: response(clock, request)) as sender:
        sender.submit(order.client_id, quote=quote(clock.now))
    bound_reads = PersistentReadLimiter(
        reads.path.parent,
        reads.scope,
        wall_ns=lambda: int(clock.now.timestamp() * 1e9),
        monotonic_ns=lambda: int(clock.mono * 1e9),
        sleep=clock.advance,
    )
    with PrivateReadClient(
        SecretStr("fixture-key"),
        SecretStr("fixture-secret"),
        limiter=bound_reads,
        transport=get_transport(clock, order, []),
        clock=lambda: clock.now,
        monotonic=lambda: clock.mono,
    ) as reader:
        report = AccountReader(reader, clock=lambda: clock.now).collect_order(order, 201)
    assert not report.evidence.executions_complete
    journal.reconcile(report.evidence)
    with journal._transaction() as conn:
        conn.execute("UPDATE account_gate SET entry_halted=1")
    proof = journal.snapshot()["account_guard"]
    with client(setup, lambda _: httpx.Response(200, json=envelope(clock))) as sender:
        assert sender.cancel(order.client_id).accepted
    assert journal.snapshot()["account_guard"] == proof and proof["entry_halted"]


def test_code_change_at_final_cancel_gate_sends_no_http_and_keeps_consumed_claim(
    setup, monkeypatch
):
    clock, _, posts, journal = setup
    order, _ = working(setup)
    original = journal.begin_cancel

    def changed(*args, **kwargs):
        result = original(*args, **kwargs)
        monkeypatch.setattr("trading.live_journal.implementation_sha256", lambda: "b" * 64)
        return result

    monkeypatch.setattr(journal, "begin_cancel", changed)
    with client(setup, lambda _: pytest.fail("HTTP after code drift")) as sender:
        with pytest.raises(OrderTransportError):
            sender.cancel(order.client_id)
    assert posts.snapshot()["operation"] == "cancel" and posts.snapshot()["claim"]
    assert journal.snapshot()["orders"][0]["state"] == "UNKNOWN" and journal.snapshot()["halted"]


@pytest.mark.parametrize("failure", ["timeout", "empty"])
def test_stopped_cancel_failure_can_be_investigated_repeatedly_without_resuming(setup, failure):
    clock, reads, posts, journal = setup
    order, _ = working(setup)

    def handler(request):
        if failure == "timeout":
            raise httpx.ReadTimeout("synthetic", request=request)
        data = envelope(clock)
        data["data"]["success"] = []
        return httpx.Response(200, json=data)

    with client(setup, handler) as sender:
        with pytest.raises(OrderTransportError):
            sender.cancel(order.client_id)
    saved = posts.snapshot()
    recovery = PrivateOrderRecovery(
        journal.path.parent,
        reads.path.parent,
        reads.scope,
        clock=lambda: clock.now,
        monotonic=lambda: clock.mono,
        read_clocks={
            "wall_ns": lambda: int(clock.now.timestamp() * 1e9),
            "monotonic_ns": lambda: int(clock.mono * 1e9),
            "sleep": clock.advance,
        },
    )
    for _ in range(2):
        recovery.reconcile(
            order.client_id,
            201,
            **checks(recovery, order),
            vault=Vault(),
            transport=get_transport(clock, order, []),
        )
        assert posts.snapshot() == saved and journal.snapshot()["halted"]
    with client(setup, lambda _: pytest.fail("cancel resent after investigation")) as sender:
        with pytest.raises(OrderTransportError):
            sender.cancel(order.client_id)


def test_two_cancel_callers_produce_only_one_request(setup):
    clock, _, _, _ = setup
    order, _ = working(setup)
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json=envelope(clock))

    def cancel(sender):
        try:
            return sender.cancel(order.client_id).accepted
        except OrderTransportError:
            return False

    with client(setup, handler) as first, client(setup, handler) as second:
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(cancel, (first, second)))
    assert outcomes.count(True) == 1 and len(calls) == 1


def test_cancel_receipt_survives_post_dispatch_code_change_and_emergency_stop(setup, monkeypatch):
    clock, _, posts, journal = setup
    order, _ = working(setup)

    def handler(_):
        journal.halt()
        posts.stop("operator_stop")
        monkeypatch.setattr("trading.live_journal.implementation_sha256", lambda: "b" * 64)
        return httpx.Response(200, json=envelope(clock))

    with client(setup, handler) as sender:
        sender.cancel(order.client_id)
    assert journal.snapshot()["orders"][0]["cancellation_receipt"]
    assert posts.snapshot()["phase"] == "STOPPED" and posts.snapshot()["reason"] == "operator_stop"
    assert not journal.snapshot()["live_enabled"] and posts.snapshot()["claim"] is None


@pytest.mark.parametrize("terminal", ["CANCELED", "EXECUTED"])
def test_cancel_racing_with_terminal_get_keeps_partial_and_complete_account_requirements(
    setup, terminal
):
    clock, _, _, journal = setup
    order, _ = working(setup, partial=True)
    with client(setup, lambda _: httpx.Response(200, json=envelope(clock))) as sender:
        sender.cancel(order.client_id)
    fills = [
        {
            "executionId": 301,
            "positionId": 401,
            "size": "400",
            "price": "150.01",
            "fee": "-2",
            "lossGain": "0",
            "settledSwap": "0",
            "timestamp": (clock.now - timedelta(seconds=1.1)).isoformat(),
        }
    ]
    if terminal == "EXECUTED":
        fills.append(
            {**fills[0], "executionId": 302, "size": "600", "timestamp": clock.now.isoformat()}
        )
    evidence = fixture_evidence(order, 101, 201, terminal, fills, clock.now)
    assert (
        journal.reconcile(evidence.model_copy(update={"executions_complete": False}))
        == "CANCEL_PENDING"
    )
    clock.advance(0.1)
    assert journal.reconcile(evidence.model_copy(update={"observed_at": clock.now})) == (
        "FILLED" if terminal == "EXECUTED" else "CANCELED"
    )
    with (
        pytest.raises(OrderTransportError),
        client(setup, lambda _: pytest.fail("terminal cancel")) as sender,
    ):
        sender.cancel(order.client_id)


@pytest.mark.parametrize("phase", ["claim", "response", "receipt", "post_completion"])
def test_process_death_retains_cancel_attempt_and_get_investigation_preserves_claim(setup, phase):
    clock, reads, posts, journal = setup
    order, _ = working(setup)
    sent = journal.path.parent / "mock-cancel-sent"
    script = r"""
import ctypes,os,socket,sys,httpx
from pathlib import Path
from datetime import datetime
from pydantic import SecretStr
sys.path.insert(0,"tests")
from test_private_order import Clock
from trading.read_control import PersistentReadLimiter
from trading.post_control import PersistentPostLimiter
from trading.live_journal import LiveOrderJournal
from trading.private_order import PrivateOrderClient
def forbidden(*a,**k): raise AssertionError("real network/native credentials forbidden")
socket.socket=forbidden; ctypes.WinDLL=forbidden
c=Clock(); c.now=datetime.fromisoformat(sys.argv[4]); c.mono=1.1
r=PersistentReadLimiter(sys.argv[1],"synthetic")
p=PersistentPostLimiter(sys.argv[2],r,**c.post_args())
j=LiveOrderJournal(sys.argv[3],p,clock=lambda:c.now)
phase=sys.argv[5]
if phase=="claim":
    original=j.begin_cancel
    def claimed(*a,**k):
        result=original(*a,**k); os._exit(73)
    j.begin_cancel=claimed
if phase=="response": j.acknowledge_cancel=lambda receipt: os._exit(73)
if phase=="receipt":
    original=j.acknowledge_cancel
    def ack(receipt):
        original(receipt); os._exit(73)
    j.acknowledge_cancel=ack
if phase=="post_completion":
    original=p._write
    def write(conn,state,kind,**changes):
        result=original(conn,state,kind,**changes)
        if kind=="COMPLETED": os._exit(73)
        return result
    p._write=write
def handler(request):
    assert request.url.path=="/private/v1/cancelOrders"
    assert request.content==b'{"rootOrderIds":[101]}'
    Path(sys.argv[6]).write_text("once")
    data={"success":[{"rootOrderId":101,"clientOrderId":"Buy001"}]}
    return httpx.Response(200,json={"status":0,"data":data,"responsetime":c.now.isoformat()})
sender=PrivateOrderClient(SecretStr("fixture-key"),SecretStr("fixture-secret"),journal=j,
    transport=httpx.MockTransport(handler),clock=lambda:c.now,monotonic=lambda:c.mono)
sender.cancel("Buy001")
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
            phase,
            str(sent),
        ],
        capture_output=True,
        timeout=15,
    )
    assert process.returncode == 73, process.stderr.decode()
    view = LiveOrderJournal(journal.path.parent, posts, clock=lambda: clock.now).snapshot()
    assert view["orders"][0]["state"] == "CANCEL_PENDING" and sent.exists() == (phase != "claim")
    assert bool(view["orders"][0]["cancellation_receipt"]) == (
        phase in {"receipt", "post_completion"}
    )
    saved = posts.snapshot()
    assert saved["phase"] == "IN_FLIGHT" and saved["operation"] == "cancel" and saved["claim"]
    with client(setup, lambda _: pytest.fail("cancel resend after death")) as sender:
        with pytest.raises(OrderTransportError):
            sender.cancel(order.client_id)
    clock.advance(3)
    recovery = PrivateOrderRecovery(
        journal.path.parent,
        reads.path.parent,
        reads.scope,
        clock=lambda: clock.now,
        monotonic=lambda: clock.mono,
        read_clocks={
            "wall_ns": lambda: int(clock.now.timestamp() * 1e9),
            "monotonic_ns": lambda: int(clock.mono * 1e9),
            "sleep": clock.advance,
        },
    )
    for _ in range(2):
        assert recovery.context(order.client_id)["post_operation"] == "cancel"
        result = recovery.reconcile(
            order.client_id,
            201,
            **checks(recovery, order),
            vault=Vault(),
            transport=get_transport(clock, order, []),
        )
        assert result["post_claim_retained"] and posts.snapshot() == saved
    assert (
        journal.snapshot()["halted"]
        and journal.snapshot()["orders"][0]["state"] == "CANCEL_PENDING"
    )
