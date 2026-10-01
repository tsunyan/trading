import json
from datetime import UTC, datetime, timedelta

import pytest

from trading.account_events import MAX_FRAME_BYTES, EventError, parse_event

NOW = datetime(2026, 9, 30, tzinfo=UTC)


@pytest.mark.parametrize(
    "changes",
    [{"size": "４００"}, {"size": "٤٠٠"}, {"timestamp": 1700000000}, {"timestamp": "1700000000"}],
)
def test_noncanonical_wire_values_rejected(changes):
    with pytest.raises(EventError):
        parse_event(raw(position(**changes)), NOW)


@pytest.mark.parametrize("offset", [50, 100, 101])
def test_event_clock_skew_is_explicit_and_bounded(offset):
    payload = raw(position(timestamp=(NOW + timedelta(milliseconds=offset)).isoformat()))
    with pytest.raises(EventError, match="future_event"):
        parse_event(payload, NOW)
    if offset <= 100:
        assert parse_event(payload, NOW, clock_skew_ms=100).entity_id == 401
    else:
        with pytest.raises(EventError, match="future_event"):
            parse_event(payload, NOW, clock_skew_ms=100)


@pytest.mark.parametrize("value", [-1, 1001, True, 0.1])
def test_invalid_clock_skew_rejected(value):
    with pytest.raises(EventError):
        parse_event(raw(position()), NOW, clock_skew_ms=value)


def position(**changes):
    return {
        "channel": "positionEvents",
        "positionId": 401,
        "symbol": "USD_JPY",
        "side": "BUY",
        "size": "400",
        "orderdSize": "0",
        "price": "150",
        "lossGain": "-40",
        "totalSwap": "0",
        "timestamp": NOW.isoformat(),
        "msgType": "UPR",
        **changes,
    }


def order(**changes):
    return {
        "channel": "orderEvents",
        "rootOrderId": 201,
        "orderId": 201,
        "clientOrderId": "DemoOpen",
        "symbol": "USD_JPY",
        "settleType": "OPEN",
        "orderType": "NORMAL",
        "executionType": "LIMIT",
        "side": "BUY",
        "orderStatus": "ORDERED",
        "orderTimestamp": NOW.isoformat(),
        "orderPrice": "150",
        "orderSize": "1000",
        "expiry": "20261001",
        "msgType": "NOR",
        **changes,
    }


def execution(**changes):
    result = order()
    result.pop("orderStatus")
    result.pop("expiry")
    result.update(
        channel="executionEvents",
        msgType="ER",
        amount="-2",
        executionId=501,
        executionPrice="150",
        executionSize="400",
        positionId=401,
        lossGain="0",
        settledSwap="0",
        fee="-2",
        orderExecutedSize="400",
        executionTimestamp=NOW.isoformat(),
    )
    return {**result, **changes}


def raw(row):
    return json.dumps(row).encode()


@pytest.mark.parametrize("factory", [position, order, execution])
def test_valid_events_are_immutable_detached_and_digest_stable(factory):
    row = factory()
    event = parse_event(raw(row), NOW)
    assert event == parse_event(json.dumps(row, sort_keys=True, indent=2).encode(), NOW)
    row["symbol"] = "other"
    assert event.channel in {"positionEvents", "orderEvents", "executionEvents"}
    with pytest.raises(ValueError):
        event.entity_id = 9


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"[]",
        b"null",
        b"\xff",
        b'{"channel":"x","channel":"y"}',
        b"{" * 2000,
        b"x" * (MAX_FRAME_BYTES + 1),
    ],
)
def test_bad_raw_frames_redacted(payload):
    with pytest.raises(EventError) as caught:
        parse_event(payload, NOW)
    assert str(caught.value) in {"invalid_event_frame", "unsupported_event"}


@pytest.mark.parametrize(
    "changes",
    [
        {"symbol": "EUR_USD"},
        {"side": "bad"},
        {"positionId": True},
        {"size": "NaN"},
        {"size": "1.5"},
        {"size": "0"},
        {"price": 150.0},
        {"orderdSize": "401"},
        {"orderedSize": "0"},
        {"secret": "never-echo"},
        {"msgType": "UNKNOWN"},
        {"timestamp": "2026-09-30"},
        {"timestamp": (NOW + timedelta(seconds=1)).isoformat()},
    ],
)
def test_invalid_position_fields(changes):
    with pytest.raises(EventError) as caught:
        parse_event(raw(position(**changes)), NOW)
    assert "never-echo" not in str(caught.value)


def test_position_timestamp_is_not_delivery_time_and_cpr_does_not_require_zero_size():
    event = parse_event(raw(position()), NOW + timedelta(days=30))
    assert event.position.units == 400
    closed = parse_event(raw(position(msgType="CPR")), NOW)
    assert closed.removed and closed.position is None


@pytest.mark.parametrize(
    "changes",
    [
        {"clientOrderId": None},
        {"orderType": "OCO"},
        {"executionType": "STOP"},
        {"orderStatus": "FILLED"},
        {"orderSize": "0"},
        {"expiry": "20260230"},
        {"cancelType": "USER"},
        {"orderStatus": "CANCELED"},
    ],
)
def test_unsupported_orders_fail_closed(changes):
    with pytest.raises(EventError):
        parse_event(raw(order(**changes)), NOW)


@pytest.mark.parametrize("status", ["CANCELED", "EXPIRED"])
def test_terminal_order_expects_absence(status):
    event = parse_event(raw(order(orderStatus=status, msgType="COR", cancelType="USER")), NOW)
    assert event.removed and event.order is None


@pytest.mark.parametrize(
    "changes",
    [
        {"executionId": 0},
        {"executionSize": "401"},
        {"orderExecutedSize": "1001"},
        {"fee": float("nan")},
        {"executionTimestamp": (NOW - timedelta(seconds=1)).isoformat()},
    ],
)
def test_invalid_execution_fields(changes):
    with pytest.raises(EventError):
        parse_event(raw(execution(**changes)), NOW)


def test_execution_not_promoted_to_booking():
    event = parse_event(raw(execution()), NOW)
    assert event.entity_id == 501 and event.execution_order_id == 201
    assert event.position is None and event.order is None
