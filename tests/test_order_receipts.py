"""Real wire shapes keep POST acceptance separate from durable execution evidence."""

import copy
import hashlib
import json
import socket
import sqlite3
import subprocess
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from pydantic import SecretStr

from trading.account_reader import AccountReader
from trading.broker_contracts import OrderIntent, OrderLimits, Settlement
from trading.execution_lab import fixture_evidence
from trading.order_journal import OrderBlocked, OrderJournal
from trading.order_receipts import MAX_RECEIPT_BYTES, ReceiptError, parse_submission_receipt
from trading.paper_runner import python_process_args
from trading.private_read import AccountReadLimiter, PrivateReadClient

NOW = datetime(2026, 10, 3, 3, tzinfo=UTC)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("real network attempted"))


def intent(*, effect="OPEN", kind="LIMIT", client_id="Receipt001"):
    return OrderIntent(
        client_id=client_id,
        side="BUY",
        effect=effect,
        units=1000,
        kind=kind,
        price="150" if kind == "LIMIT" else None,
        bound="151" if kind == "MARKET" else None,
        positions=(Settlement(position_id=401, units=1000),) if effect == "CLOSE" else (),
    )


def envelope(order, *, status="WAITING"):
    row = {
        "rootOrderId": 101,
        "orderId": 201,
        "clientOrderId": order.client_id,
        "symbol": "USD_JPY",
        "side": order.side,
        "orderType": "NORMAL",
        "executionType": order.kind,
        "settleType": order.effect,
        "size": str(order.units),
        "status": status,
        "timestamp": NOW.isoformat(),
        "expiry": "20261004",
    }
    if order.kind == "LIMIT":
        row["price"] = str(order.price)
    return {"status": 0, "data": [row], "responsetime": NOW.isoformat()}


def raw(value):
    return json.dumps(value, separators=(",", ":")).encode()


def parse(order, response=None, **kwargs):
    return parse_submission_receipt(
        order, raw(response or envelope(order)), started_at=NOW, received_at=NOW, **kwargs
    )


def journal(tmp_path):
    return OrderJournal.create(
        tmp_path / "lab",
        OrderLimits(
            min_units=100,
            max_units=1000,
            unit_step=100,
            price_tick="0.001",
            max_reference_notional="200000",
        ),
    )


def claimed(book, order):
    book.prepare(order)
    book.begin_submission(order.client_id)


def filled(order, *, root_id=101, order_id=201, status="EXECUTED", complete=True, seconds=1):
    fills = (
        [
            {
                "executionId": 301,
                "positionId": 401,
                "size": "1000",
                "price": "150",
                "fee": "-1",
                "lossGain": "0",
                "settledSwap": "0",
                "timestamp": NOW.isoformat(),
            }
        ]
        if status == "EXECUTED"
        else []
    )
    return fixture_evidence(
        order, root_id, order_id, status, fills, NOW + timedelta(seconds=seconds)
    ).model_copy(update={"executions_complete": complete})


@pytest.mark.parametrize("effect", ["OPEN", "CLOSE"])
@pytest.mark.parametrize("kind", ["MARKET", "LIMIT"])
@pytest.mark.parametrize("status", ["WAITING", "EXECUTED", "EXPIRED"])
def test_documented_post_status_and_shape_are_retained(effect, kind, status):
    order = intent(effect=effect, kind=kind)
    response = envelope(order, status=status)
    receipt = parse(order, response)
    assert receipt.intent == order and receipt.broker_status == status
    assert receipt.root_order_id == 101 and receipt.order_id == 201
    assert receipt.payload_sha256 == hashlib.sha256(raw(response)).hexdigest()
    assert receipt.order_at == receipt.response_at == NOW
    assert not hasattr(receipt, "executions") and not hasattr(receipt, "executions_complete")
    assert not hasattr(receipt, "live_enabled")


@pytest.mark.parametrize(
    "key,value",
    [
        ("rootOrderId", True),
        ("orderId", 1.0),
        ("orderId", "201"),
        ("orderId", 0),
        ("orderId", 2**63),
        ("clientOrderId", "Other"),
        ("symbol", "EUR_JPY"),
        ("side", "SELL"),
        ("orderType", "OCO"),
        ("executionType", "STOP"),
        ("settleType", "CLOSE"),
        ("size", "999"),
        ("size", 1000),
        ("size", "1e3"),
        ("size", "NaN"),
        ("size", "Infinity"),
        ("price", "149"),
        ("price", "NaN"),
        ("status", "ORDERED"),
        ("status", "CANCELED"),
        ("expiry", None),
        ("expiry", "20260230"),
        ("expiry", "secret"),
        ("cancelType", "OCO"),
        ("cancelType", None),
        ("cancelType", "PRICE_BOUND"),
        ("surprise", "secret"),
        ("timestamp", 1790996400),
        ("timestamp", "2026-10-03T03:00:00"),
    ],
)
def test_mismatched_or_malformed_order_fields_are_sanitized(key, value):
    order = intent()
    response = envelope(order)
    response["data"][0][key] = value
    with pytest.raises(ReceiptError, match="^invalid_submission_receipt$"):
        parse(order, response)


def test_price_bound_expiry_receipt_is_only_a_market_acknowledgement():
    order = intent(kind="MARKET")
    response = envelope(order, status="EXPIRED")
    response["data"][0]["cancelType"] = "PRICE_BOUND"
    assert parse(order, response).cancel_type == "PRICE_BOUND"
    response["data"][0]["price"] = "150"
    with pytest.raises(ReceiptError):
        parse(order, response)


@pytest.mark.parametrize(
    "kind",
    [
        "boolean_status",
        "api_error",
        "missing_time",
        "get_shape",
        "empty",
        "multiple",
        "unknown_envelope",
        "missing_client",
        "missing_price",
        "bad_data",
        "bad_row",
    ],
)
def test_invalid_envelope_never_proves_nonacceptance(kind):
    order = intent()
    response = envelope(order)
    if kind == "boolean_status":
        response["status"] = False
    elif kind == "api_error":
        response.update(status=1, messages=[{"message": "remote secret"}])
    elif kind == "missing_time":
        del response["responsetime"]
    elif kind == "get_shape":
        response["data"] = {"list": response["data"]}
    elif kind == "empty":
        response["data"] = []
    elif kind == "multiple":
        response["data"].append(copy.deepcopy(response["data"][0]))
    elif kind == "unknown_envelope":
        response["extra"] = "remote secret"
    elif kind in {"missing_client", "missing_price"}:
        del response["data"][0]["clientOrderId" if kind == "missing_client" else "price"]
    elif kind == "bad_data":
        response["data"] = None
    else:
        response["data"] = [None]
    with pytest.raises(ReceiptError, match="^invalid_submission_receipt$"):
        parse(order, response)


@pytest.mark.parametrize(
    "payload",
    [
        b'{"status":NaN}',
        b'{"status":Infinity}',
        b"\xff",
        b"",
        b"[]",
        b"{",
        pytest.param(b"x" * (MAX_RECEIPT_BYTES + 1), id="too_large"),
        {},
        "{}",
    ],
)
def test_raw_json_is_required_and_duplicate_keys_are_rejected(payload):
    with pytest.raises(ReceiptError, match="^invalid_submission_receipt$"):
        parse_submission_receipt(intent(), payload, started_at=NOW, received_at=NOW)


@pytest.mark.parametrize("kind", ["envelope", "row"])
def test_duplicate_key_with_valid_last_value_cannot_be_silently_adopted(kind):
    order = intent()
    payload = raw(envelope(order))
    payload = payload.replace(
        b'"status":0' if kind == "envelope" else b'"orderId":201',
        b'"status":1,"status":0' if kind == "envelope" else b'"orderId":202,"orderId":201',
    )
    with pytest.raises(ReceiptError):
        parse_submission_receipt(order, payload, started_at=NOW, received_at=NOW)


@pytest.mark.parametrize("value", [NOW.replace(tzinfo=None), 1790996400, True])
def test_explicit_local_clocks_must_be_aware_datetimes(value):
    order = intent()
    with pytest.raises(ReceiptError):
        parse_submission_receipt(order, raw(envelope(order)), started_at=value, received_at=NOW)


@pytest.mark.parametrize(
    "kind", ["old_order", "future_response", "order_after_response", "reverse_local"]
)
def test_impossible_receipt_times_are_rejected(kind):
    order = intent()
    response = envelope(order)
    start = receive = NOW
    if kind == "old_order":
        response["data"][0]["timestamp"] = (NOW - timedelta(milliseconds=1)).isoformat()
    elif kind == "future_response":
        response["responsetime"] = (NOW + timedelta(milliseconds=1)).isoformat()
    elif kind == "order_after_response":
        response["data"][0]["timestamp"] = (NOW + timedelta(milliseconds=1)).isoformat()
        receive += timedelta(seconds=1)
    else:
        start += timedelta(milliseconds=1)
    with pytest.raises(ReceiptError):
        parse_submission_receipt(order, raw(response), started_at=start, received_at=receive)


def test_explicit_bounded_clock_skew_and_model_copy_revalidation(tmp_path):
    order = intent()
    response = envelope(order)
    response["data"][0]["timestamp"] = (NOW - timedelta(milliseconds=1000)).isoformat()
    receipt = parse(order, response, clock_skew_ms=1000)
    with pytest.raises(ReceiptError):
        parse(order, response, clock_skew_ms=1001)
    book = journal(tmp_path)
    claimed(book, order)
    with pytest.raises(OrderBlocked, match="^submission_receipt_invalid$"):
        book.acknowledge_submission(receipt.model_copy(update={"broker_status": "ORDERED"}))
    assert book.snapshot()["halted"]


@pytest.mark.parametrize("status", ["WAITING", "EXECUTED", "EXPIRED"])
def test_persisted_ack_does_not_release_the_claim_or_create_fills(tmp_path, status):
    book, order = journal(tmp_path), intent()
    claimed(book, order)
    payload = raw(envelope(order, status=status))
    assert (
        book.submission_response(order.client_id, payload, started_at=NOW, received_at=NOW)
        == "RECONCILING"
    )
    reopened = OrderJournal(book.path.parent)
    row = reopened.snapshot()["orders"][0]
    assert row["submission_receipt"]["broker_status"] == status and row["evidence"] is None
    assert not reopened.snapshot()["live_enabled"]
    with pytest.raises(OrderBlocked):
        reopened.begin_submission(order.client_id)
    with pytest.raises(OrderBlocked):
        reopened.prepare(intent(client_id="Other001"))
    assert reopened.snapshot()["orders"][0]["state"] == "RECONCILING"


def test_identical_receipt_is_idempotent_even_after_final_get_evidence(tmp_path):
    book, order = journal(tmp_path), intent()
    claimed(book, order)
    receipt = parse(order, envelope(order, status="EXECUTED"))
    book.acknowledge_submission(receipt)
    assert book.reconcile(filled(order, complete=False)) == "RECONCILING"
    assert book.reconcile(filled(order, complete=True, seconds=2)) == "FILLED"
    before = book.snapshot()
    assert OrderJournal(book.path.parent).acknowledge_submission(receipt) == "FILLED"
    assert book.snapshot() == before


def test_actual_private_get_reader_retains_receipt_and_does_not_claim_completeness(tmp_path):
    book, order = journal(tmp_path), intent()
    claimed(book, order)
    book.acknowledge_submission(parse(order, envelope(order, status="EXECUTED")))
    calls = []

    def handler(request):
        calls.append((request.method, request.url.path, str(request.url.params)))
        rows = (
            [envelope(order, status="EXECUTED")["data"][0]]
            if request.url.path.endswith("/orders")
            else [
                {
                    "executionId": 301,
                    "positionId": 401,
                    "orderId": 201,
                    "clientOrderId": order.client_id,
                    "symbol": "USD_JPY",
                    "side": "BUY",
                    "settleType": "OPEN",
                    "size": "1000",
                    "price": "150",
                    "fee": "-1",
                    "lossGain": "0",
                    "settledSwap": "0",
                    "timestamp": NOW.isoformat(),
                }
            ]
        )
        return httpx.Response(
            200,
            json={
                "status": 0,
                "data": {"list": rows},
                "responsetime": (NOW + timedelta(seconds=1)).isoformat(),
            },
        )

    with PrivateReadClient(
        SecretStr("fixture-key"),
        SecretStr("fixture-secret"),
        limiter=AccountReadLimiter(),
        transport=httpx.MockTransport(handler),
        clock=lambda: NOW + timedelta(seconds=1),
    ) as client:
        report = AccountReader(client, clock=lambda: NOW + timedelta(seconds=1)).collect_order(
            order, 201
        )
    assert not report.evidence.executions_complete
    assert book.reconcile(report.evidence) == "RECONCILING"
    row = OrderJournal(book.path.parent).snapshot()["orders"][0]
    assert len(row["evidence"]["executions"]) == 1
    assert row["submission_receipt"]["broker_status"] == "EXECUTED"
    assert (
        calls
        == [
            ("GET", "/private/v1/orders", "orderId=201"),
            ("GET", "/private/v1/executions", "orderId=201"),
        ]
        * 2
    )


@pytest.mark.parametrize("kind", ["order_id", "root_id", "terminal_hint", "stale_time", "old_fill"])
def test_get_evidence_must_agree_with_receipt_before_any_final_state(tmp_path, kind):
    book, order = journal(tmp_path), intent()
    claimed(book, order)
    receipt = parse(order, envelope(order, status="EXECUTED"))
    book.acknowledge_submission(receipt)
    evidence = filled(
        order,
        root_id=102 if kind == "root_id" else 101,
        order_id=202 if kind == "order_id" else 201,
        status="EXPIRED" if kind == "terminal_hint" else "EXECUTED",
    )
    if kind == "stale_time":
        evidence = evidence.model_copy(
            update={"observed_at": NOW - timedelta(seconds=1), "executions": ()}
        )
    elif kind == "old_fill":
        evidence = evidence.model_copy(
            update={
                "executions": (
                    evidence.executions[0].model_copy(
                        update={"timestamp": NOW - timedelta(seconds=1)}
                    ),
                )
            }
        )
    with pytest.raises(OrderBlocked, match="submission_receipt_evidence_mismatch"):
        book.reconcile(evidence)
    row = book.snapshot()["orders"][0]
    assert book.snapshot()["halted"] and row["state"] == "RECONCILING"
    assert row["evidence"] is None and row["submission_receipt"] is not None


def test_conflicting_receipt_preserves_original_and_halts(tmp_path):
    book, order = journal(tmp_path), intent()
    claimed(book, order)
    receipt = parse(order)
    book.acknowledge_submission(receipt)
    response = envelope(order)
    response["data"][0]["orderId"] = 202
    with pytest.raises(OrderBlocked):
        book.acknowledge_submission(parse(order, response))
    saved = book.snapshot()["orders"][0]["submission_receipt"]
    assert saved == receipt.model_dump(mode="json") and book.snapshot()["halted"]


def test_broker_ids_cannot_bind_another_intent_after_the_first_is_final(tmp_path):
    book, first = journal(tmp_path), intent()
    claimed(book, first)
    book.acknowledge_submission(parse(first))
    book.reconcile(filled(first))
    second = intent(client_id="Receipt002")
    claimed(book, second)
    with pytest.raises(OrderBlocked):
        book.acknowledge_submission(parse(second))
    assert book.snapshot()["orders"][1]["submission_receipt"] is None
    assert book.snapshot()["halted"]


def test_invalid_post_response_persists_unknown_without_remote_text(tmp_path):
    book, order = journal(tmp_path), intent()
    claimed(book, order)
    with pytest.raises(OrderBlocked, match="^submission_response_invalid$"):
        book.submission_response(
            order.client_id,
            b'{"status":1,"messages":[{"message":"remote secret"}]}',
            started_at=NOW,
            received_at=NOW,
        )
    reopened = OrderJournal(book.path.parent)
    assert reopened.snapshot()["orders"][0]["state"] == "UNKNOWN"
    assert reopened.snapshot()["halted"] and b"remote secret" not in book.path.read_bytes()
    with pytest.raises(OrderBlocked):
        reopened.begin_submission(order.client_id)


def test_unclaimed_response_is_never_adopted_as_a_sent_order(tmp_path):
    book, order = journal(tmp_path), intent()
    book.prepare(order)
    with pytest.raises(OrderBlocked):
        book.acknowledge_submission(parse(order))
    row = book.snapshot()["orders"][0]
    assert row["state"] == "PREPARED" and row["submission_receipt"] is None
    assert book.snapshot()["halted"]


def test_corrupt_or_duplicate_persisted_receipt_refuses_reconciliation(tmp_path):
    book, order = journal(tmp_path), intent()
    claimed(book, order)
    book.acknowledge_submission(parse(order))
    with sqlite3.connect(book.path) as conn:
        conn.execute(
            "INSERT INTO events(recorded_at,client_id,kind,payload_json) "
            "SELECT recorded_at,client_id,kind,payload_json FROM events WHERE kind='SUBMISSION_ACK'"
        )
    with pytest.raises(OrderBlocked, match="submission_receipt_conflict"):
        book.reconcile(filled(order))
    with sqlite3.connect(book.path) as conn:
        assert conn.execute("SELECT halted FROM metadata").fetchone()[0] == 1
        assert conn.execute("SELECT state FROM orders").fetchone()[0] == "RECONCILING"


@pytest.mark.parametrize("phase", ["before_commit", "after_commit"])
def test_actual_process_exit_around_receipt_commit_never_releases_submission(tmp_path, phase):
    book, order = journal(tmp_path), intent()
    claimed(book, order)
    code = """
import os
from datetime import datetime
from pathlib import Path
from trading.order_journal import OrderJournal
p = OrderJournal(Path(sys.argv[1]))
stamp = datetime.fromisoformat(sys.argv[3])
if sys.argv[4] == 'before_commit':
    original = p._event
    def event(conn, client_id, kind, payload):
        original(conn, client_id, kind, payload)
        if kind == 'SUBMISSION_ACK':
            os._exit(37)
    p._event = event
p.submission_response('Receipt001', sys.argv[2].encode(), started_at=stamp, received_at=stamp)
os._exit(37)
"""
    result = subprocess.run(
        python_process_args(
            code, book.path.parent, raw(envelope(order)).decode(), NOW.isoformat(), phase
        ),
        capture_output=True,
        timeout=10,
    )
    assert result.returncode == 37, result.stderr
    reopened = OrderJournal(book.path.parent)
    row = reopened.snapshot()["orders"][0]
    assert row["state"] == ("SUBMITTING" if phase == "before_commit" else "RECONCILING")
    assert (row["submission_receipt"] is not None) == (phase == "after_commit")
    with pytest.raises(OrderBlocked):
        reopened.begin_submission(order.client_id)
