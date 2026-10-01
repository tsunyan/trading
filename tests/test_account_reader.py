import copy
import socket
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from trading.account_reader import (
    ASSETS,
    ORDERS,
    POSITIONS,
    AccountReader,
    CollectionError,
)
from trading.broker_contracts import OrderIntent, RequestPlan

NOW = datetime(2026, 9, 30, 1, tzinfo=UTC)
STAMP = NOW.isoformat()


def assets():
    return [
        {
            "balance": "1000000",
            "equity": "999900",
            "availableAmount": "993900",
            "margin": "6000",
            "estimatedTradeFee": "2.5",
            "positionLossGain": "-110",
            "totalSwap": "10",
            "transferableAmount": "993800",
            "marginRatio": "16665",
        }
    ]


def position(pid=401, **changes):
    return {
        "positionId": pid,
        "symbol": "USD_JPY",
        "side": "BUY",
        "size": "1000",
        "orderedSize": "0",
        "price": "150.01",
        "lossGain": "-110",
        "totalSwap": "10",
        "timestamp": STAMP,
        **changes,
    }


def order(oid=201, **changes):
    return {
        "rootOrderId": oid,
        "orderId": oid,
        "clientOrderId": "Buy001",
        "symbol": "USD_JPY",
        "side": "BUY",
        "orderType": "NORMAL",
        "executionType": "LIMIT",
        "settleType": "OPEN",
        "size": "1000",
        "price": "150.01",
        "status": "ORDERED",
        "timestamp": STAMP,
        **changes,
    }


def fill(**changes):
    return {
        "executionId": 301,
        "positionId": 401,
        "orderId": 201,
        "clientOrderId": "Buy001",
        "symbol": "USD_JPY",
        "side": "BUY",
        "settleType": "OPEN",
        "size": "400",
        "price": "150.01",
        "amount": "-2",
        "fee": "-2",
        "lossGain": "0",
        "settledSwap": "0",
        "timestamp": STAMP,
        **changes,
    }


class FixtureTransport:
    def __init__(self, mutate=None, positions=None, orders=None):
        self.calls = []
        self.mutate = mutate
        self.positions = positions if positions is not None else [position()]
        self.orders = orders if orders is not None else [order()]

    def get(self, request):
        self.calls.append(request)
        query = dict(request.query)
        if request.path == ASSETS:
            data = assets()
        elif request.path in {POSITIONS, ORDERS}:
            rows = self.positions if request.path == POSITIONS else self.orders
            key = "positionId" if request.path == POSITIONS else "orderId"
            rows = sorted(rows, key=lambda r: r[key], reverse=True)
            rows = [r for r in rows if r[key] < int(query.get("prevId", 10**10))]
            data = {"list": rows[: int(query["count"])]}
        elif request.path == "/v1/orders":
            data = {"list": [order()]}
        elif request.path == "/v1/executions":
            data = {"list": [fill()]}
        else:
            raise AssertionError("unexpected endpoint")
        response = {"status": 0, "data": copy.deepcopy(data), "responsetime": STAMP}
        if self.mutate:
            self.mutate(request, response, len(self.calls))
        return response


def reader(transport=None, **kwargs):
    return AccountReader(transport or FixtureTransport(), clock=lambda: NOW, **kwargs)


def intent():
    return OrderIntent(
        client_id="Buy001", side="BUY", effect="OPEN", units=1000, kind="LIMIT", price="150.01"
    )


def test_normalization_is_read_only_and_cannot_promote(monkeypatch):
    def no_network(*args, **kwargs):
        pytest.fail("network attempted")

    monkeypatch.setattr(socket, "socket", no_network)
    transport = FixtureTransport()
    report = reader(transport).collect_account()
    assert report.assets.position_loss_gain == Decimal("-110")
    assert report.assets.estimated_trade_fee == Decimal("2.5")
    assert report.positions[0].ordered_units == 0
    assert report.active_orders[0].units == 1000
    assert report.traversal_exhausted and report.repeated_state_equal
    assert not report.live_enabled and not report.atomic_snapshot_verified
    assert not report.account_identity_verified
    assert len(report.blockers) == 4
    assert "complete" not in report.model_dump()
    assert all(c.method == "GET" and not c.body for c in transport.calls)
    assert all("symbol" not in dict(c.query) for c in transport.calls)
    assert len(report.observations) == 12
    assert all(len(o.sha256) == 64 for o in report.observations)


def test_all_pages_including_extra_empty_after_short_page():
    transport = FixtureTransport(positions=[position(9), position(7), position(3)])
    report = reader(transport, page_size=2).collect_account()
    assert [p.position_id for p in report.positions] == [3, 7, 9]
    plans = [c for c in transport.calls if c.path == POSITIONS]
    assert [dict(c.query).get("prevId") for c in plans] == [None, "7", "3"] * 2


def test_empty_account_still_has_unproven_identity_and_consistency():
    report = reader(FixtureTransport(positions=[], orders=[])).collect_account()
    assert report.positions == report.active_orders == ()
    assert len(report.observations) == 8
    assert report.blockers


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity", "", True, 1.2, None])
def test_invalid_financial_values(value):
    def mutate(req, response, number):
        if req.path == ASSETS:
            response["data"][0]["balance"] = value

    with pytest.raises(CollectionError):
        reader(FixtureTransport(mutate)).collect_account()


@pytest.mark.parametrize(
    "field,value",
    [
        ("symbol", "EUR_JPY"),
        ("size", "0"),
        ("size", "1.5"),
        ("positionId", True),
        ("orderedSize", "1001"),
        ("side", "UNKNOWN"),
        ("timestamp", "2026-09-30T01:00:01Z"),
        ("timestamp", "2026-09-30T01:00:00"),
    ],
)
def test_bad_or_foreign_positions_never_silently_ignored(field, value):
    with pytest.raises(CollectionError):
        reader(FixtureTransport(positions=[position(**{field: value})])).collect_account()


@pytest.mark.parametrize(
    "changes",
    [
        {"symbol": "EUR_JPY"},
        {"clientOrderId": ""},
        {"clientOrderId": None},
        {"executionType": "STOP"},
        {"orderType": "OCO"},
        {"status": "EXECUTED"},
        {"timestamp": "2026-09-30T01:00:01Z"},
    ],
)
def test_unsupported_orders(changes):
    with pytest.raises(CollectionError):
        reader(FixtureTransport(orders=[order(**changes)])).collect_account()


def test_duplicate_client_id():
    with pytest.raises(CollectionError, match="duplicate_client"):
        reader(FixtureTransport(orders=[order(201), order(202)])).collect_account()


@pytest.mark.parametrize("case", ["duplicate", "oversized", "cursor", "missing_list"])
def test_bad_pagination(case):
    def mutate(req, response, number):
        if req.path != POSITIONS:
            return
        if case == "duplicate":
            response["data"]["list"] = [position(), position()]
        elif case == "oversized":
            response["data"]["list"] = [position(1), position(2), position(3)]
        elif case == "cursor" and "prevId" in dict(req.query):
            response["data"]["list"] = [position(999)]
        elif case == "missing_list":
            response["data"] = {}

    with pytest.raises(CollectionError):
        reader(FixtureTransport(mutate), page_size=2).collect_account()


def test_page_budget_is_not_completeness():
    with pytest.raises(CollectionError, match="page_limit"):
        reader(max_pages=1).collect_account()


@pytest.mark.parametrize("case", ["error", "missing_status", "missing_data", "bad_time"])
def test_api_envelope_failures(case):
    def mutate(req, response, number):
        if case == "error":
            response["status"] = 1
        elif case == "bad_time":
            response["responsetime"] = "invalid"
        else:
            response.pop(case.removeprefix("missing_"))

    with pytest.raises(CollectionError, match="invalid_api_envelope"):
        reader(FixtureTransport(mutate)).collect_account()


@pytest.mark.parametrize("delta", [-6, 1])
def test_stale_and_future_response(delta):
    def mutate(req, response, number):
        response["responsetime"] = (NOW + timedelta(seconds=delta)).isoformat()

    with pytest.raises(CollectionError, match="stale_or_future"):
        reader(FixtureTransport(mutate)).collect_account()


def test_response_clock_moves_backwards():
    def mutate(req, response, number):
        if number > 1:
            response["responsetime"] = (NOW - timedelta(seconds=1)).isoformat()

    with pytest.raises(CollectionError, match="moved_backwards"):
        reader(FixtureTransport(mutate)).collect_account()


@pytest.mark.parametrize("delta", [-1, 31])
def test_local_clock_and_deadline(delta):
    times = iter([NOW, NOW, NOW + timedelta(seconds=delta)])
    with pytest.raises(CollectionError, match="clock_or_deadline"):
        AccountReader(FixtureTransport(), clock=lambda: next(times)).collect_account()


def test_transport_error_is_sanitized():
    def mutate(req, response, number):
        raise RuntimeError("PRIVATE-SECRET-DO-NOT-LOG")

    with pytest.raises(CollectionError) as caught:
        reader(FixtureTransport(mutate)).collect_account()
    assert str(caught.value) == "read_transport_failed"
    assert caught.value.__suppress_context__


def test_assets_changing_within_sweep():
    def mutate(req, response, number):
        if req.path == ASSETS and number > 1:
            response["data"][0]["balance"] = "999999"

    with pytest.raises(CollectionError, match="assets_changed"):
        reader(FixtureTransport(mutate)).collect_account()


def test_state_changing_between_sweeps():
    def mutate(req, response, number):
        if req.path == POSITIONS and number > 6 and response["data"]["list"]:
            response["data"]["list"][0]["size"] = "900"

    with pytest.raises(CollectionError, match="changed_between"):
        reader(FixtureTransport(mutate)).collect_account()


def test_only_one_assets_row_supported():
    def mutate(req, response, number):
        if req.path == ASSETS:
            response["data"] = []

    with pytest.raises(CollectionError, match="one_asset_record"):
        reader(FixtureTransport(mutate)).collect_account()


@pytest.mark.parametrize(
    "kwargs",
    [{"page_size": 101}, {"page_size": True}, {"max_pages": 0}, {"max_duration_seconds": -1}],
)
def test_limits(kwargs):
    with pytest.raises(ValueError, match="limits"):
        reader(**kwargs)


def test_partial_fill_retains_total_order_size_and_incomplete_flag():
    report = reader().collect_order(intent(), 201)
    assert report.evidence.intent.units == 1000
    assert sum(f.units for f in report.evidence.executions) == 400
    assert report.evidence.executions[0].fee == 2
    assert not report.evidence.executions_complete
    assert len(report.observations) == 4


@pytest.mark.parametrize("case", ["missing", "wrong_id", "duplicate_fill", "changed", "fee"])
def test_order_evidence_failures(case):
    def mutate(req, response, number):
        if req.path == "/v1/orders":
            if case == "missing":
                response["data"]["list"] = []
            if case == "wrong_id":
                response["data"]["list"][0]["orderId"] = 999
        if req.path == "/v1/executions":
            if case == "duplicate_fill":
                response["data"]["list"] *= 2
            if case == "changed" and number == 4:
                response["data"]["list"][0]["size"] = "500"
            if case == "fee":
                response["data"]["list"][0]["fee"] = "2"

    with pytest.raises(CollectionError):
        reader(FixtureTransport(mutate)).collect_order(intent(), 201)


def test_no_post_or_arbitrary_endpoint_even_at_transport_boundary():
    transport = FixtureTransport()
    for plan in [
        RequestPlan("POST", ASSETS),
        RequestPlan("GET", "/v1/order"),
        RequestPlan("GET", ASSETS, body=b"payload"),
    ]:
        with pytest.raises(CollectionError, match="endpoint_not_allowed"):
            reader(transport)._read(plan, NOW, [])
    assert not transport.calls


@pytest.mark.parametrize("changes", [{"price": "NaN"}, {"price": "bad"}, {"status": "EXECUTED"}])
def test_invalid_limit_price_and_incomplete_executed_order(changes):
    def mutate(req, response, number):
        if req.path == "/v1/orders":
            response["data"]["list"][0].update(changes)

    with pytest.raises(CollectionError):
        reader(FixtureTransport(mutate)).collect_order(intent(), 201)


def test_complete_fill_still_does_not_assert_account_consistency():
    def mutate(req, response, number):
        if req.path == "/v1/orders":
            response["data"]["list"][0]["status"] = "EXECUTED"
        if req.path == "/v1/executions":
            response["data"]["list"][0]["size"] = "1000"

    report = reader(FixtureTransport(mutate)).collect_order(intent(), 201)
    assert report.evidence.status == "EXECUTED"
    assert not report.evidence.executions_complete


@pytest.mark.parametrize(
    "field",
    [
        "equity",
        "availableAmount",
        "margin",
        "positionLossGain",
        "transferableAmount",
        "marginRatio",
    ],
)
def test_market_values_may_change_but_latest_values_are_retained(field):
    def mutate(req, response, number):
        if req.path == ASSETS:
            response["data"][0][field] = str(number)
        if req.path == POSITIONS and response["data"]["list"]:
            response["data"]["list"][0]["lossGain"] = str(number)

    report = reader(FixtureTransport(mutate)).collect_account()
    assert report.positions[0].loss_gain == 8
    if field == "equity":
        assert report.assets.equity == 12
    assert not report.atomic_snapshot_verified


@pytest.mark.parametrize("field", ["balance", "totalSwap", "estimatedTradeFee", "unknownField"])
def test_non_market_asset_changes_still_rejected(field):
    def mutate(req, response, number):
        if req.path == ASSETS:
            response["data"][0][field] = str(number)

    with pytest.raises(CollectionError, match="assets_changed"):
        reader(FixtureTransport(mutate)).collect_account()


def test_invalid_early_market_value_cannot_be_hidden_by_latest_sample():
    def mutate(req, response, number):
        if number == 1:
            response["data"][0]["equity"] = "NaN"

    with pytest.raises(CollectionError):
        reader(FixtureTransport(mutate)).collect_account()


def test_fill_after_first_order_response_uses_final_observation_time():
    later = NOW + timedelta(milliseconds=100)

    def mutate(req, response, number):
        if number > 1:
            response["responsetime"] = later.isoformat()
        if req.path == "/v1/executions":
            response["data"]["list"][0]["timestamp"] = later.isoformat()

    report = AccountReader(FixtureTransport(mutate), clock=lambda: later).collect_order(
        intent(), 201
    )
    assert report.evidence.observed_at == later
    assert report.evidence.executions[0].timestamp == later


@pytest.mark.parametrize("value", ["４００", "٤٠٠", "4e2", "+400"])
def test_non_ascii_or_non_decimal_wire_numbers_rejected(value):
    with pytest.raises(CollectionError):
        reader(FixtureTransport(positions=[position(size=value)])).collect_account()


@pytest.mark.parametrize("value", [1700000000, "1700000000", True])
@pytest.mark.parametrize("target", ["response", "position", "order", "fill"])
def test_wire_times_require_iso_strings(value, target):
    def mutate(req, response, number):
        if target == "response":
            response["responsetime"] = value
        elif req.path == {"position": POSITIONS, "order": ORDERS, "fill": "/v1/executions"}[target]:
            for row in response["data"]["list"]:
                row["timestamp"] = value

    with pytest.raises(CollectionError):
        subject = reader(FixtureTransport(mutate))
        subject.collect_order(intent(), 201) if target == "fill" else subject.collect_account()


@pytest.mark.parametrize("offset,accepted", [(0, True), (50, True), (100, True), (101, False)])
def test_response_future_skew_boundary(offset, accepted):
    def mutate(req, response, number):
        response["responsetime"] = (NOW + timedelta(milliseconds=offset)).isoformat()

    subject = reader(FixtureTransport(mutate), clock_skew_ms=100)
    if accepted:
        assert subject.collect_account().observations[0].response_at >= NOW
    else:
        with pytest.raises(CollectionError, match="stale_or_future"):
            subject.collect_account()
