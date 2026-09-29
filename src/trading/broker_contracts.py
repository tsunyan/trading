"""Offline GMO FX request/response contracts. No HTTP, credentials loading or sending.

Source: https://api.coin.z.com/fxdocs/ (reviewed 2026-09-29).
Only single NORMAL USD_JPY orders are supported; not OCO/IFD/speed orders.
"""

import hashlib
import hmac
import json
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, SecretStr, model_validator

Units = Annotated[int, Field(strict=True, gt=0)]
Money = Annotated[Decimal, Field(gt=0, allow_inf_nan=False)]
ClientId = Annotated[str, Field(pattern=r"^[A-Za-z0-9]{1,36}$")]


class Contract(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)


class Settlement(Contract):
    position_id: Units
    units: Units


class OrderIntent(Contract):
    client_id: ClientId
    symbol: Literal["USD_JPY"] = "USD_JPY"
    side: Literal["BUY", "SELL"]
    effect: Literal["OPEN", "CLOSE"]
    units: Units
    kind: Literal["MARKET", "LIMIT"]
    price: Money | None = None
    bound: Money | None = None
    positions: tuple[Settlement, ...] = ()

    @model_validator(mode="after")
    def coherent(self):
        if self.kind == "MARKET" and (self.bound is None or self.price is not None):
            raise ValueError("market orders require a bound, not a limit price")
        if self.kind == "LIMIT" and (self.price is None or self.bound is not None):
            raise ValueError("limit orders require a price, not a market bound")
        if self.effect == "OPEN" and self.positions:
            raise ValueError("opening orders cannot specify positions")
        if self.effect == "CLOSE":
            ids = [p.position_id for p in self.positions]
            if not 1 <= len(ids) <= 10 or len(ids) != len(set(ids)):
                raise ValueError("closing requires 1..10 distinct position IDs")
            if sum(p.units for p in self.positions) != self.units:
                raise ValueError("closing size must equal specified position sizes")
        return self


class OrderLimits(Contract):
    """Explicit fixture/operator limits, NOT current broker rules or portfolio limits."""

    min_units: Units
    max_units: Units
    unit_step: Units
    price_tick: Money
    max_reference_notional: Money

    @model_validator(mode="after")
    def coherent(self):
        if self.min_units > self.max_units:
            raise ValueError("min_units exceeds max_units")
        return self


@dataclass(frozen=True)
class RequestPlan:
    method: str
    path: str
    body: bytes = b""
    query: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class SignedPlan:
    request: RequestPlan
    headers: dict[str, str] = field(repr=False)


def _post(path: str, body: dict) -> RequestPlan:
    return RequestPlan("POST", path, json.dumps(body, separators=(",", ":")).encode("ascii"))


def order_request(intent: OrderIntent, limits: OrderLimits) -> RequestPlan:
    # Revalidate, including callers that used Pydantic model_copy/model_construct.
    intent = OrderIntent.model_validate(intent.model_dump())
    limits = OrderLimits.model_validate(limits.model_dump())
    if not limits.min_units <= intent.units <= limits.max_units:
        raise ValueError("order size outside limits")
    if intent.units % limits.unit_step or any(p.units % limits.unit_step for p in intent.positions):
        raise ValueError("order size is off step")
    reference = intent.price if intent.kind == "LIMIT" else intent.bound
    if reference % limits.price_tick:
        raise ValueError("price is off tick")
    # SELL lowerBound is not an upper bound on proceeds/notional. This is only
    # a reference-size check; a live preflight must use a fresh conservative quote.
    if reference * intent.units > limits.max_reference_notional:
        raise ValueError("reference notional exceeds limit")
    body = {
        "symbol": intent.symbol,
        "side": intent.side,
        "clientOrderId": intent.client_id,
        "executionType": intent.kind,
    }
    if intent.kind == "LIMIT":
        body["limitPrice"] = format(intent.price, "f")
    else:
        body["upperBound" if intent.side == "BUY" else "lowerBound"] = format(intent.bound, "f")
    if intent.effect == "CLOSE":
        body["settlePosition"] = [
            {"positionId": p.position_id, "size": str(p.units)} for p in intent.positions
        ]
    else:
        body["size"] = str(intent.units)
    return _post("/v1/order" if intent.effect == "OPEN" else "/v1/closeOrder", body)


def cancel_request(root_order_id: int) -> RequestPlan:
    if type(root_order_id) is not int or root_order_id <= 0:
        raise ValueError("invalid root order ID")
    return _post("/v1/cancelOrders", {"rootOrderIds": [root_order_id]})


def lookup_request(kind: Literal["orders", "executions"], order_id: int) -> RequestPlan:
    if kind not in {"orders", "executions"} or type(order_id) is not int or order_id <= 0:
        raise ValueError("invalid lookup")
    return RequestPlan("GET", f"/v1/{kind}", query=(("orderId", str(order_id)),))


def sign_request(
    request: RequestPlan, api_key: SecretStr, secret: SecretStr, timestamp_ms: int
) -> SignedPlan:
    """Pure signing only. Never persist/log the returned authentication headers."""
    allowed = {
        ("POST", "/v1/order"),
        ("POST", "/v1/closeOrder"),
        ("POST", "/v1/cancelOrders"),
        ("GET", "/v1/orders"),
        ("GET", "/v1/executions"),
    }
    if (request.method, request.path) not in allowed:
        raise ValueError("unsupported request")
    if (request.method == "GET" and request.body) or (
        request.method == "POST" and (not request.body or request.query)
    ):
        raise ValueError("invalid request body/query")
    if type(timestamp_ms) is not int or timestamp_ms <= 0:
        raise ValueError("invalid timestamp")
    if not api_key.get_secret_value() or not secret.get_secret_value():
        raise ValueError("empty credentials")
    timestamp = str(timestamp_ms)
    # GMO GET signatures exclude query parameters. Sign the exact POST bytes.
    payload = (timestamp + request.method + request.path).encode("ascii") + request.body
    signature = hmac.new(secret.get_secret_value().encode(), payload, hashlib.sha256).hexdigest()
    return SignedPlan(
        request,
        {
            "API-KEY": api_key.get_secret_value(),
            "API-TIMESTAMP": timestamp,
            "API-SIGN": signature,
            "Content-Type": "application/json",
        },
    )


class BrokerResponseError(ValueError):
    """Response rejected without echoing potentially sensitive raw server messages."""


def response_data(response: dict):
    if not isinstance(response, dict) or type(response.get("status")) is not int:
        raise BrokerResponseError("invalid broker envelope")
    if response["status"] != 0:
        # Nonzero/HTTP errors are not automatically proof of non-acceptance.
        raise BrokerResponseError("broker reported an error; reconciliation required")
    if "data" not in response:
        raise BrokerResponseError("missing broker data")
    return response["data"]


def _units(value) -> int:
    if not isinstance(value, str):
        raise BrokerResponseError("expected quantity string")
    decimal = Decimal(value)
    if not decimal.is_finite() or decimal <= 0 or decimal != decimal.to_integral_value():
        raise BrokerResponseError("invalid quantity")
    return int(decimal)


class Execution(Contract):
    execution_id: Units
    position_id: Units
    units: Units
    price: Money
    fee: Decimal
    loss_gain: Decimal
    settled_swap: Decimal
    timestamp: AwareDatetime


class OrderEvidence(Contract):
    intent: OrderIntent
    root_order_id: Units
    order_id: Units
    status: Literal["WAITING", "ORDERED", "MODIFYING", "CANCELED", "EXECUTED", "EXPIRED"]
    observed_at: AwareDatetime
    executions: tuple[Execution, ...]
    executions_complete: bool = Field(strict=True)


def parse_evidence(
    intent: OrderIntent,
    orders: dict,
    executions: dict,
    observed_at: datetime,
    *,
    executions_complete: bool = False,
) -> OrderEvidence:
    """Join explicit /orders and /executions responses; absence proves nothing.

    Completeness is a caller assertion, not inferred from an empty response.
    A future collector must establish a coherent complete snapshot before setting it.
    """
    rows, fills = response_data(orders), response_data(executions)
    if not isinstance(rows, list) or len(rows) != 1 or not isinstance(fills, list):
        raise BrokerResponseError("expected one order and an execution list")
    row = rows[0]
    expected = {
        "clientOrderId": intent.client_id,
        "symbol": intent.symbol,
        "side": intent.side,
        "settleType": intent.effect,
    }
    if not isinstance(row, dict) or any(row.get(k) != v for k, v in expected.items()):
        raise BrokerResponseError("order identity mismatch")
    if row.get("orderType") != "NORMAL" or row.get("executionType") != intent.kind:
        raise BrokerResponseError("unsupported or mismatched order type")
    if _units(row.get("size")) != intent.units:
        raise BrokerResponseError("order size mismatch")
    if intent.kind == "LIMIT" and Decimal(row.get("price", "NaN")) != intent.price:
        raise BrokerResponseError("order price mismatch")
    parsed = {}
    for fill in fills:
        if not isinstance(fill, dict) or any(fill.get(k) != v for k, v in expected.items()):
            raise BrokerResponseError("execution identity mismatch")
        if type(fill.get("orderId")) is not int or fill["orderId"] != row.get("orderId"):
            raise BrokerResponseError("execution order mismatch")
        item = Execution(
            execution_id=fill["executionId"],
            position_id=fill["positionId"],
            units=_units(fill["size"]),
            price=fill["price"],
            fee=fill["fee"],
            loss_gain=fill["lossGain"],
            settled_swap=fill["settledSwap"],
            timestamp=fill["timestamp"],
        )
        if item.execution_id in parsed and parsed[item.execution_id] != item:
            raise BrokerResponseError("conflicting duplicate execution")
        parsed[item.execution_id] = item
    evidence = OrderEvidence(
        intent=intent,
        root_order_id=row["rootOrderId"],
        order_id=row["orderId"],
        status=row["status"],
        observed_at=observed_at,
        executions=tuple(parsed[k] for k in sorted(parsed)),
        executions_complete=executions_complete,
    )
    validate_evidence(evidence)
    return evidence


def validate_evidence(evidence: OrderEvidence) -> None:
    intent = evidence.intent
    if len({e.execution_id for e in evidence.executions}) != len(evidence.executions):
        raise BrokerResponseError("duplicate execution IDs")
    if sum(e.units for e in evidence.executions) > intent.units:
        raise BrokerResponseError("overfilled order")
    positions = {p.position_id: p.units for p in intent.positions}
    for fill in evidence.executions:
        if fill.timestamp > evidence.observed_at:
            raise BrokerResponseError("future execution")
        if intent.effect == "CLOSE":
            if fill.position_id not in positions:
                raise BrokerResponseError("unexpected closing position")
            positions[fill.position_id] -= fill.units
            if positions[fill.position_id] < 0:
                raise BrokerResponseError("position overclosed")
        bound = intent.price if intent.kind == "LIMIT" else intent.bound
        if (intent.side == "BUY" and fill.price > bound) or (
            intent.side == "SELL" and fill.price < bound
        ):
            raise BrokerResponseError("execution violates price protection")


def cancellation_accepted(response: dict, root_id: int, client_id: str) -> bool:
    data = response_data(response)
    if not isinstance(data, dict) or not isinstance(data.get("success"), list):
        raise BrokerResponseError("invalid cancellation response")
    return any(
        isinstance(item, dict)
        and type(item.get("rootOrderId")) is int
        and item["rootOrderId"] == root_id
        and item.get("clientOrderId") == client_id
        for item in data["success"]
    )
