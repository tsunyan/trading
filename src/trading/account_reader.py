"""Offline-testable GMO FX GET collector; no HTTP, keys, or order transport.

REST exhaustion and repeated equality are diagnostics, NOT atomic completeness.
Source: https://api.coin.z.com/fxdocs/ (reviewed 2026-09-30).
"""

import hashlib
import json
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Literal, Protocol

from pydantic import AwareDatetime, Field, TypeAdapter

from trading.broker_contracts import (
    Contract,
    OrderEvidence,
    OrderIntent,
    RequestPlan,
    Units,
    lookup_request,
    parse_evidence,
    response_data,
)
from trading.wire_validation import clock_skew, decimal_string, positive_id, timestamp_string

ASSETS = "/v1/account/assets"
POSITIONS = "/v1/openPositions"
ORDERS = "/v1/activeOrders"
TIME = TypeAdapter(AwareDatetime)


class CollectionError(ValueError):
    """Safe diagnostic without raw account data or transport exception text."""


class ReadTransport(Protocol):
    def get(self, request: RequestPlan) -> dict:
        """Return decoded JSON for a GET plan; enforce timeouts in the adapter."""


class Assets(Contract):
    balance: Decimal
    equity: Decimal
    available_amount: Decimal
    margin: Decimal
    estimated_trade_fee: Decimal
    position_loss_gain: Decimal
    total_swap: Decimal
    transferable_amount: Decimal


class HeldPosition(Contract):
    position_id: Units
    symbol: Literal["USD_JPY"]
    side: Literal["BUY", "SELL"]
    units: Units
    ordered_units: int = Field(strict=True, ge=0)
    price: Decimal = Field(gt=0)
    loss_gain: Decimal
    total_swap: Decimal
    timestamp: AwareDatetime


class ActiveOrder(Contract):
    root_order_id: Units
    order_id: Units
    client_id: str = Field(pattern=r"^[A-Za-z0-9]{1,36}$")
    symbol: Literal["USD_JPY"]
    side: Literal["BUY", "SELL"]
    effect: Literal["OPEN", "CLOSE"]
    kind: Literal["LIMIT"]
    # Wire size is total order quantity, NEVER assumed to be unfilled quantity.
    units: Units
    price: Decimal = Field(gt=0)
    status: Literal["WAITING", "ORDERED", "MODIFYING"]
    timestamp: AwareDatetime


class Observation(Contract):
    path: str
    query: tuple[tuple[str, str], ...]
    response_at: AwareDatetime
    received_at: AwareDatetime
    sha256: str


class AccountReadReport(Contract):
    assets: Assets
    positions: tuple[HeldPosition, ...]
    active_orders: tuple[ActiveOrder, ...]
    observations: tuple[Observation, ...]
    traversal_exhausted: Literal[True] = True
    repeated_state_equal: Literal[True] = True
    live_enabled: Literal[False] = False
    # Deliberately not an AccountSnapshot; no automatic promotion to its gate.
    account_identity_verified: Literal[False] = False
    atomic_snapshot_verified: Literal[False] = False
    blockers: tuple[str, ...] = (
        "account_identity_not_in_rest_response",
        "atomic_snapshot_not_proven",
        "execution_history_not_proven",
        "broker_margin_fee_rounding_not_verified",
    )


class OrderReadReport(Contract):
    evidence: OrderEvidence
    observations: tuple[Observation, ...]
    repeated_state_equal: Literal[True] = True
    live_enabled: Literal[False] = False


def _number(row: dict, key: str) -> Decimal:
    try:
        return decimal_string(row.get(key))
    except ValueError:
        raise CollectionError("invalid_numeric_field:" + key) from None


def _size(row: dict, key: str, *, zero: bool = False) -> int:
    value = _number(row, key)
    if value != value.to_integral_value() or value < (0 if zero else 1):
        raise CollectionError("invalid_quantity:" + key)
    return int(value)


def _id(row: dict, key: str) -> int:
    try:
        return positive_id(row.get(key))
    except ValueError:
        raise CollectionError("invalid_identity:" + key) from None


def _rows(data: object) -> list[dict]:
    if not isinstance(data, dict) or not isinstance(data.get("list"), list):
        raise CollectionError("missing_list")
    if any(not isinstance(row, dict) for row in data["list"]):
        raise CollectionError("invalid_list_row")
    return data["list"]


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _asset_structure(data):
    if not isinstance(data, list) or len(data) != 1 or not isinstance(data[0], dict):
        raise CollectionError("expected_one_asset_record")
    # Exclude only documented market-valued fields, after validating each sample.
    # Unknown fields, cash, swaps and estimated fees still participate in equality.
    volatile = {
        "equity",
        "availableAmount",
        "margin",
        "positionLossGain",
        "transferableAmount",
        "marginRatio",
    }
    for key in volatile:
        if key != "marginRatio" or key in data[0]:
            _number(data[0], key)
    return {key: value for key, value in data[0].items() if key not in volatile}


def _account_structure(sweep):
    assets, positions, orders = sweep
    for row in positions:
        _number(row, "lossGain")
    return (
        _asset_structure(assets),
        [{key: value for key, value in row.items() if key != "lossGain"} for row in positions],
        orders,
    )


class AccountReader:
    """Bounded reads against an explicitly injected transport, normally fixtures.

    No retries silently join different attempts. Restart the entire collection on
    transient failure. A network adapter must impose its own per-request timeout
    and shared-account rate limit; this layer cannot interrupt a blocking callback.
    """

    def __init__(
        self,
        transport: ReadTransport,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        page_size: int = 100,
        max_pages: int = 100,
        max_duration_seconds: int = 30,
        max_response_age_seconds: int = 5,
        clock_skew_ms: int = 0,
    ):
        for value, upper in (
            (page_size, 100),
            (max_pages, 1000),
            (max_duration_seconds, 300),
            (max_response_age_seconds, 60),
        ):
            if type(value) is not int or not 1 <= value <= upper:
                raise ValueError("invalid collection limits")
        self.transport = transport
        self.clock = clock
        self.page_size = page_size
        self.max_pages = max_pages
        self.max_duration = max_duration_seconds
        self.max_response_age = max_response_age_seconds
        self.clock_skew = clock_skew(clock_skew_ms)

    def _read(self, request, started, observations):
        if (
            request.method != "GET"
            or request.body
            or request.path not in {ASSETS, POSITIONS, ORDERS, "/v1/orders", "/v1/executions"}
        ):
            raise CollectionError("read_endpoint_not_allowed")
        before = TIME.validate_python(self.clock())
        last = observations[-1].received_at if observations else started
        if before < last or (before - started).total_seconds() > self.max_duration:
            raise CollectionError("collection_clock_or_deadline")
        try:
            response = self.transport.get(request)
        except Exception:
            raise CollectionError("read_transport_failed") from None
        received = TIME.validate_python(self.clock())
        if received < before or (received - started).total_seconds() > self.max_duration:
            raise CollectionError("collection_clock_or_deadline")
        try:
            data = response_data(response)
            stamp = timestamp_string(response["responsetime"])
            digest = hashlib.sha256(_canonical(response)).hexdigest()
        except (ValueError, TypeError, KeyError):
            raise CollectionError("invalid_api_envelope") from None
        if (
            not -self.clock_skew.total_seconds()
            <= (received - stamp).total_seconds()
            <= self.max_response_age
        ):
            raise CollectionError("stale_or_future_response")
        if observations and stamp < observations[-1].response_at:
            raise CollectionError("response_time_moved_backwards")
        observations.append(
            Observation(
                path=request.path,
                query=request.query,
                response_at=stamp,
                received_at=received,
                sha256=digest,
            )
        )
        # Detach mutable fixtures/transport buffers before subsequent calls.
        return json.loads(_canonical(data))

    def _pages(self, path, key, started, observations):
        rows, seen, cursor = [], set(), None
        for _ in range(self.max_pages):
            query = (("count", str(self.page_size)),)
            if cursor is not None:
                query += (("prevId", str(cursor)),)
            # No symbol filter: foreign/manual exposure must not be hidden.
            batch = _rows(self._read(RequestPlan("GET", path, query=query), started, observations))
            if len(batch) > self.page_size:
                raise CollectionError("oversized_page")
            if not batch:
                return sorted(rows, key=lambda row: row[key])
            ids = [_id(row, key) for row in batch]
            if len(set(ids)) != len(ids) or seen.intersection(ids):
                raise CollectionError("duplicate_page_identity")
            if cursor is not None and any(value >= cursor for value in ids):
                raise CollectionError("nondecreasing_cursor")
            rows.extend(batch)
            seen.update(ids)
            cursor = min(ids)
            # Even a short page is followed to an explicit empty page.
        raise CollectionError("page_limit_without_exhaustion")

    def _sweep(self, started, observations):
        before = self._read(RequestPlan("GET", ASSETS), started, observations)
        positions = self._pages(POSITIONS, "positionId", started, observations)
        orders = self._pages(ORDERS, "orderId", started, observations)
        after = self._read(RequestPlan("GET", ASSETS), started, observations)
        if _asset_structure(before) != _asset_structure(after):
            raise CollectionError("assets_changed_during_collection")
        return after, positions, orders

    def collect_account(self) -> AccountReadReport:
        started = TIME.validate_python(self.clock())
        observations = []
        first = self._sweep(started, observations)
        second = self._sweep(started, observations)
        if _account_structure(first) != _account_structure(second):
            raise CollectionError("account_changed_between_sweeps")
        assets, positions, orders = second
        if not isinstance(assets, list) or len(assets) != 1 or not isinstance(assets[0], dict):
            raise CollectionError("expected_one_asset_record")
        try:
            a = assets[0]
            parsed_assets = Assets(
                **{
                    field: _number(a, wire)
                    for field, wire in (
                        ("balance", "balance"),
                        ("equity", "equity"),
                        ("available_amount", "availableAmount"),
                        ("margin", "margin"),
                        ("estimated_trade_fee", "estimatedTradeFee"),
                        ("position_loss_gain", "positionLossGain"),
                        ("total_swap", "totalSwap"),
                        ("transferable_amount", "transferableAmount"),
                    )
                }
            )
            parsed_positions = []
            for p in positions:
                item = HeldPosition(
                    position_id=_id(p, "positionId"),
                    symbol=p["symbol"],
                    side=p["side"],
                    units=_size(p, "size"),
                    ordered_units=_size(p, "orderedSize", zero=True),
                    price=_number(p, "price"),
                    loss_gain=_number(p, "lossGain"),
                    total_swap=_number(p, "totalSwap"),
                    timestamp=timestamp_string(p["timestamp"]),
                )
                if item.ordered_units > item.units or item.timestamp > observations[-1].response_at:
                    raise CollectionError("invalid_position_quantity_or_time")
                parsed_positions.append(item)
            parsed_orders = []
            for o in orders:
                if o.get("orderType") != "NORMAL":
                    raise CollectionError("unsupported_order_type")
                item = ActiveOrder(
                    root_order_id=_id(o, "rootOrderId"),
                    order_id=_id(o, "orderId"),
                    client_id=o["clientOrderId"],
                    symbol=o["symbol"],
                    side=o["side"],
                    effect=o["settleType"],
                    kind=o["executionType"],
                    units=_size(o, "size"),
                    price=_number(o, "price"),
                    status=o["status"],
                    timestamp=timestamp_string(o["timestamp"]),
                )
                if item.timestamp > observations[-1].response_at:
                    raise CollectionError("future_order")
                parsed_orders.append(item)
            if len({o.client_id for o in parsed_orders}) != len(parsed_orders):
                raise CollectionError("duplicate_client_identity")
            return AccountReadReport(
                assets=parsed_assets,
                positions=tuple(parsed_positions),
                active_orders=tuple(parsed_orders),
                observations=tuple(observations),
            )
        except CollectionError:
            raise
        except (ValueError, TypeError, KeyError):
            raise CollectionError("unsupported_or_invalid_account_fields") from None

    def collect_order(self, intent: OrderIntent, order_id: int) -> OrderReadReport:
        """Read an already known broker ID; missing IDs never prove rejection."""
        intent = OrderIntent.model_validate(intent.model_dump())
        order_plan = lookup_request("orders", order_id)
        fill_plan = lookup_request("executions", order_id)
        started = TIME.validate_python(self.clock())
        observations, results = [], []
        try:
            for _ in range(2):
                order_data = self._read(order_plan, started, observations)
                fill_data = self._read(fill_plan, started, observations)
                orders, fills = _rows(order_data), _rows(fill_data)
                if len(orders) != 1 or _id(orders[0], "orderId") != order_id:
                    raise CollectionError("requested_order_missing_or_mismatched")
                fill_ids = [_id(f, "executionId") for f in fills]
                if len(set(fill_ids)) != len(fill_ids):
                    raise CollectionError("duplicate_execution_identity")
                fills = sorted(fills, key=lambda f: f["executionId"])
                results.append((orders, fills))
            if results[0] != results[1]:
                raise CollectionError("order_changed_during_collection")
            orders, fills = results[-1]
            # Reject malformed numeric wire fields before Decimal comparisons in
            # the shared parser (e.g. a NaN limit price).
            if intent.kind == "LIMIT":
                _number(orders[0], "price")
            if (
                orders[0].get("status") == "EXECUTED"
                and sum(_size(f, "size") for f in fills) != intent.units
            ):
                raise CollectionError("executed_order_missing_fills")
            for row in (*orders, *fills):
                timestamp_string(row["timestamp"])
                for key in ("size", "price", "fee", "lossGain", "settledSwap", "amount"):
                    if key in row:
                        _number(row, key)
            evidence = parse_evidence(
                intent,
                {"status": 0, "data": {"list": orders}},
                {"status": 0, "data": {"list": fills}},
                observed_at=observations[-1].response_at,
                executions_complete=False,
            )
            return OrderReadReport(evidence=evidence, observations=tuple(observations))
        except CollectionError:
            raise
        except (ValueError, TypeError, KeyError, InvalidOperation):
            raise CollectionError("invalid_order_evidence") from None
