"""Pure, narrow GMO FX private-event parsing. No WebSocket or token transport.

Source: https://api.coin.z.com/fxdocs/#private-ws-api (reviewed 2026-09-30).
Order/position timestamps are NOT stream sequence numbers or delivery times.
"""

import hashlib
import json
import re
from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import AwareDatetime, Field, TypeAdapter

from trading.account_reader import ActiveOrder, HeldPosition
from trading.broker_contracts import Contract, Execution, Units
from trading.wire_validation import (
    clock_skew,
    decimal_string,
    positive_id,
    timestamp_string,
    unique_object,
)

TIME = TypeAdapter(AwareDatetime)
MAX_FRAME_BYTES = 16_384


class EventError(ValueError):
    """Fixed diagnostic only; never echo an account event or server error."""


class AccountEvent(Contract):
    channel: Literal["positionEvents", "orderEvents", "executionEvents"]
    entity_id: Units
    payload_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    occurred_at: AwareDatetime
    position: HeldPosition | None = None
    order: ActiveOrder | None = None
    removed: bool = False
    # Executions invalidate observations but are not booked or promoted to fills.
    execution_order_id: Units | None = None
    execution_order_complete: bool = False
    execution_order: ActiveOrder | None = None
    execution: Execution | None = None
    execution_amount: Decimal | None = None
    execution_cumulative_units: Units | None = None


def _object(pairs):
    try:
        return unique_object(pairs)
    except ValueError:
        raise EventError("invalid_event_frame") from None


def _number(row, key, *, positive=False, integer=False, zero=False):
    number = decimal_string(row[key])
    if not number.is_finite() or (positive and number <= 0):
        raise EventError("invalid_event_frame")
    if integer:
        if number != number.to_integral_value() or number < (0 if zero else 1):
            raise EventError("invalid_event_frame")
        return int(number)
    return number


def _id(row, key):
    return positive_id(row[key])


def _shape(row, required, optional=()):
    if not set(required) <= row.keys() or row.keys() - set(required) - set(optional):
        raise EventError("unsupported_event_fields")


def parse_event(payload: bytes, received_at: datetime, *, clock_skew_ms: int = 0) -> AccountEvent:
    """Decode supported USD/JPY NORMAL LIMIT events; reject unknown schemas.

    Only raw bounded JSON is accepted so duplicate keys are detectable. All raw
    values are detached; the returned object contains no mutable dictionaries.
    """
    try:
        received_at = TIME.validate_python(received_at)
        skew = clock_skew(clock_skew_ms)
        if type(payload) is not bytes or not 0 < len(payload) <= MAX_FRAME_BYTES:
            raise EventError("invalid_event_frame")
        row = json.loads(payload.decode("utf-8"), object_pairs_hook=_object)
        if not isinstance(row, dict) or row.get("symbol") != "USD_JPY":
            raise EventError("unsupported_event")
        if row.get("side") not in {"BUY", "SELL"}:
            raise EventError("unsupported_event")
        digest = hashlib.sha256(
            json.dumps(row, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        ).hexdigest()
        channel = row.get("channel")
        if channel == "positionEvents":
            _shape(
                row,
                (
                    "channel",
                    "positionId",
                    "symbol",
                    "side",
                    "size",
                    "orderdSize",
                    "price",
                    "lossGain",
                    "timestamp",
                    "totalSwap",
                    "msgType",
                ),
            )
            if row["msgType"] not in {"OPR", "UPR", "CPR"}:
                raise EventError("unsupported_event")
            # The documented WS key is orderdSize, unlike REST orderedSize.
            units = _number(row, "size", integer=True, zero=True)
            ordered = _number(row, "orderdSize", integer=True, zero=True)
            stamp = timestamp_string(row["timestamp"])
            price = _number(row, "price", positive=True)
            loss = _number(row, "lossGain")
            swap = _number(row, "totalSwap")
            removed = row["msgType"] == "CPR"
            if ordered > units or (not removed and units == 0):
                raise EventError("invalid_event_frame")
            identity = _id(row, "positionId")
            result = AccountEvent(
                channel=channel,
                entity_id=identity,
                payload_sha256=digest,
                occurred_at=stamp,
                removed=removed,
                position=None
                if removed
                else HeldPosition(
                    position_id=identity,
                    symbol=row["symbol"],
                    side=row["side"],
                    units=units,
                    ordered_units=ordered,
                    price=price,
                    loss_gain=loss,
                    total_swap=swap,
                    timestamp=stamp,
                ),
            )
        elif channel in {"orderEvents", "executionEvents"}:
            common = (
                "channel",
                "rootOrderId",
                "clientOrderId",
                "orderId",
                "symbol",
                "settleType",
                "orderType",
                "executionType",
                "side",
                "orderTimestamp",
                "orderPrice",
                "orderSize",
                "msgType",
            )
            if (
                row.get("orderType") != "NORMAL"
                or row.get("executionType") != "LIMIT"
                or row.get("settleType") not in {"OPEN", "CLOSE"}
            ):
                raise EventError("unsupported_event")
            # Require a client ID, as does AccountReader: manual/foreign orders
            # cannot silently disappear from the reconciliation boundary.
            order = ActiveOrder(
                root_order_id=_id(row, "rootOrderId"),
                order_id=_id(row, "orderId"),
                client_id=row["clientOrderId"],
                symbol=row["symbol"],
                side=row["side"],
                effect=row["settleType"],
                kind=row["executionType"],
                units=_number(row, "orderSize", integer=True),
                price=_number(row, "orderPrice", positive=True),
                status="ORDERED",
                timestamp=timestamp_string(row["orderTimestamp"]),
            )
            if channel == "orderEvents":
                _shape(row, (*common, "orderStatus", "expiry"), ("cancelType",))
                if (
                    row["msgType"] not in {"NOR", "ROR", "COR"}
                    or row["orderStatus"] not in {"WAITING", "ORDERED", "CANCELED", "EXPIRED"}
                    or not isinstance(row["expiry"], str)
                    or not re.fullmatch(r"[0-9]{8}", row["expiry"])
                ):
                    raise EventError("unsupported_event")
                datetime.strptime(row["expiry"], "%Y%m%d")
                removed = row["orderStatus"] in {"CANCELED", "EXPIRED"}
                if removed:
                    if row.get("cancelType") not in {
                        "USER",
                        "INSUFFICIENT_COLLATERAL",
                        "INSUFFICIENT_MARGIN",
                        "SPEED",
                        "OCO",
                        "EXPIRATION",
                        "PRICE_BOUND",
                        "OUT_OF_SLIPPAGE_RANGE",
                    }:
                        raise EventError("unsupported_event")
                elif "cancelType" in row:
                    raise EventError("unsupported_event")
                result = AccountEvent(
                    channel=channel,
                    entity_id=order.order_id,
                    payload_sha256=digest,
                    occurred_at=order.timestamp,
                    removed=removed,
                    order=None
                    if removed
                    else order.model_copy(update={"status": row["orderStatus"]}),
                )
            else:
                _shape(
                    row,
                    (
                        *common,
                        "amount",
                        "executionId",
                        "executionPrice",
                        "executionSize",
                        "positionId",
                        "lossGain",
                        "settledSwap",
                        "fee",
                        "orderExecutedSize",
                        "executionTimestamp",
                    ),
                )
                if row["msgType"] != "ER":
                    raise EventError("unsupported_event")
                _id(row, "positionId")
                _number(row, "executionPrice", positive=True)
                size = _number(row, "executionSize", integer=True)
                executed = _number(row, "orderExecutedSize", integer=True)
                if not size <= executed <= order.units:
                    raise EventError("invalid_event_frame")
                for field in ("amount", "lossGain", "settledSwap", "fee"):
                    _number(row, field)
                stamp = timestamp_string(row["executionTimestamp"])
                if stamp < order.timestamp:
                    raise EventError("invalid_event_frame")
                result = AccountEvent(
                    channel=channel,
                    entity_id=_id(row, "executionId"),
                    payload_sha256=digest,
                    occurred_at=stamp,
                    execution_order_id=order.order_id,
                    execution_order_complete=executed == order.units,
                    execution_order=order,
                    execution=Execution(
                        execution_id=_id(row, "executionId"),
                        position_id=_id(row, "positionId"),
                        units=size,
                        price=_number(row, "executionPrice", positive=True),
                        # copy_negate never rounds; unary minus uses the caller's context.
                        fee=_number(row, "fee").copy_negate(),
                        loss_gain=_number(row, "lossGain"),
                        settled_swap=_number(row, "settledSwap"),
                        timestamp=stamp,
                    ),
                    execution_amount=_number(row, "amount"),
                    execution_cumulative_units=executed,
                )
        else:
            raise EventError("unsupported_event")
        if result.occurred_at > received_at + skew:
            raise EventError("future_event_timestamp")
        return result
    except EventError:
        raise
    except (ValueError, TypeError, KeyError, OverflowError, RecursionError):
        raise EventError("invalid_event_frame") from None
