import hashlib
import hmac
import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import SecretStr, ValidationError

from trading.broker_contracts import (
    BrokerResponseError,
    OrderIntent,
    OrderLimits,
    RequestPlan,
    Settlement,
    cancel_request,
    cancellation_accepted,
    lookup_request,
    order_request,
    parse_evidence,
    response_data,
    sign_request,
)


@pytest.fixture
def limits():
    return OrderLimits(
        min_units=100,
        max_units=1000,
        unit_step=100,
        price_tick="0.001",
        max_reference_notional="160000",
    )


@pytest.fixture
def intent():
    return OrderIntent(
        client_id="Test001",
        side="BUY",
        effect="OPEN",
        units=1000,
        kind="MARKET",
        bound="150.010",
    )


def test_open_close_cancel_and_lookup_shapes(intent, limits):
    request = order_request(intent, limits)
    assert request.path == "/v1/order"
    assert json.loads(request.body) == {
        "symbol": "USD_JPY",
        "side": "BUY",
        "clientOrderId": "Test001",
        "executionType": "MARKET",
        "size": "1000",
        "upperBound": "150.010",
    }
    close = OrderIntent(
        client_id="Close001",
        side="SELL",
        effect="CLOSE",
        units=1000,
        kind="MARKET",
        bound="149.990",
        positions=(Settlement(position_id=1, units=600), Settlement(position_id=2, units=400)),
    )
    request = order_request(close, limits)
    body = json.loads(request.body)
    assert request.path == "/v1/closeOrder"
    assert "size" not in body and body["lowerBound"] == "149.990"
    assert body["settlePosition"] == [
        {"positionId": 1, "size": "600"},
        {"positionId": 2, "size": "400"},
    ]
    request = cancel_request(123)
    assert request.path == "/v1/cancelOrders"
    assert json.loads(request.body) == {"rootOrderIds": [123]}
    assert lookup_request("orders", 1).query == (("orderId", "1"),)
    assert lookup_request("executions", 1).path == "/v1/executions"


def test_sign_exact_post_bytes_and_get_excludes_query(intent, limits):
    key, secret = SecretStr("dummy-key"), SecretStr("dummy-secret")
    post = order_request(intent, limits)
    signed = sign_request(post, key, secret, 123456789)
    assert (
        signed.headers["API-SIGN"]
        == hmac.new(
            b"dummy-secret",
            b"123456789POST/v1/order" + post.body,
            hashlib.sha256,
        ).hexdigest()
    )
    signed = sign_request(lookup_request("orders", 987), key, secret, 123456789)
    assert (
        signed.headers["API-SIGN"]
        == hmac.new(
            b"dummy-secret",
            b"123456789GET/v1/orders",
            hashlib.sha256,
        ).hexdigest()
    )
    assert "dummy-key" not in repr(signed) and "dummy-secret" not in repr(signed)
    assert "API-SIGN" not in repr(signed)


@pytest.mark.parametrize(
    "updates",
    [
        {"units": True},
        {"units": "1000"},
        {"units": 0},
        {"units": -1},
        {"symbol": "BTC_JPY"},
        {"client_id": "a-b"},
        {"client_id": "a" * 37},
        {"bound": "NaN"},
        {"bound": "Infinity"},
        {"bound": None},
        {"price": "150"},
        {"effect": "CLOSE"},
        {"kind": "STOP"},
        {"kind": "LIMIT", "bound": None},
    ],
)
def test_invalid_intents_fail(intent, updates):
    with pytest.raises(ValidationError):
        OrderIntent.model_validate({**intent.model_dump(), **updates})


@pytest.mark.parametrize(
    "updates",
    [
        {"units": 1100},
        {"units": 50},
        {"units": 101},
        {"bound": Decimal("150.0001")},
        {"bound": Decimal("170")},
        {"bound": Decimal("NaN")},
    ],
)
def test_limits_and_model_copy_bypass_revalidated(intent, limits, updates):
    with pytest.raises(ValueError):
        order_request(intent.model_copy(update=updates), limits)


def test_position_sum_and_duplicates():
    params = dict(
        client_id="Close", side="SELL", effect="CLOSE", units=1000, kind="LIMIT", price="150"
    )
    with pytest.raises(ValueError, match="size"):
        OrderIntent(**params, positions=(Settlement(position_id=1, units=500),))
    with pytest.raises(ValueError, match="distinct"):
        OrderIntent(**params, positions=(Settlement(position_id=1, units=500),) * 2)


@pytest.mark.parametrize(
    "payload",
    [
        {"status": True, "data": []},
        {"data": []},
        {"status": 0},
        {"status": 1, "messages": [{"message_string": "secret-token"}]},
    ],
)
def test_bad_envelopes_are_not_empty_success(payload):
    with pytest.raises(BrokerResponseError) as exc:
        response_data(payload)
    assert "secret-token" not in str(exc.value)


@pytest.mark.parametrize(
    "plan",
    [
        RequestPlan("POST", "/v1/transfer", b"{}"),
        RequestPlan("GET", "/v1/orders", b"{}"),
        RequestPlan("POST", "/v1/order"),
    ],
)
def test_signing_allowlist(plan):
    with pytest.raises(ValueError):
        sign_request(plan, SecretStr("dummy"), SecretStr("dummy"), 1)


def test_empty_orders_never_prove_rejection(intent):
    with pytest.raises(BrokerResponseError):
        parse_evidence(
            intent,
            {"status": 0, "data": []},
            {"status": 0, "data": []},
            datetime.now(UTC),
            executions_complete=True,
        )


def test_cancel_acceptance_matches_target():
    response = {"status": 0, "data": {"success": [{"rootOrderId": 123, "clientOrderId": "a"}]}}
    assert cancellation_accepted(response, 123, "a")
    assert not cancellation_accepted(response, 124, "a")
    assert not cancellation_accepted(response, 123, "b")


@pytest.fixture
def raw_responses(intent):
    identity = {
        "clientOrderId": intent.client_id,
        "symbol": "USD_JPY",
        "side": "BUY",
        "settleType": "OPEN",
    }
    order = {
        **identity,
        "rootOrderId": 1,
        "orderId": 2,
        "orderType": "NORMAL",
        "executionType": "MARKET",
        "size": "1000",
        "status": "EXECUTED",
    }
    execution = {
        **identity,
        "orderId": 2,
        "executionId": 3,
        "positionId": 4,
        "size": "1000",
        "price": "150",
        "fee": "3",
        "lossGain": "0",
        "settledSwap": "0",
        "timestamp": "2026-09-29T00:00:00Z",
    }
    return {"status": 0, "data": [order]}, {"status": 0, "data": [execution]}


@pytest.mark.parametrize(
    "target,key,value",
    [
        (0, "clientOrderId", "Other"),
        (0, "orderType", "OCO"),
        (0, "size", "999"),
        (0, "orderId", True),
        (0, "status", "NEWUNKNOWNSTATUS"),
        (1, "side", "SELL"),
        (1, "orderId", 99),
        (1, "size", "1001"),
        (1, "size", "NaN"),
        (1, "price", "NaN"),
        (1, "price", "151"),
        (1, "fee", "Infinity"),
        (1, "timestamp", "2099-01-01T00:00:00Z"),
        (1, "timestamp", "2026-09-29T00:00:00"),
    ],
)
def test_response_mismatch_overfill_and_invalid_values(intent, raw_responses, target, key, value):
    raw_responses[target]["data"][0][key] = value
    with pytest.raises(ValueError):
        parse_evidence(intent, *raw_responses, datetime(2026, 9, 29, tzinfo=UTC))


def test_conflicting_fill_id_and_default_completeness(intent, raw_responses):
    result = parse_evidence(intent, *raw_responses, datetime(2026, 9, 29, tzinfo=UTC))
    assert not result.executions_complete
    raw_responses[1]["data"].append({**raw_responses[1]["data"][0], "fee": "4"})
    with pytest.raises(ValueError, match="conflicting duplicate"):
        parse_evidence(intent, *raw_responses, datetime(2026, 9, 29, tzinfo=UTC))
