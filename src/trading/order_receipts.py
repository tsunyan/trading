"""Strict GMO single-order POST receipts; no fill, completeness or trade permission."""

import hashlib
import json
import re
from datetime import datetime
from typing import Literal

from pydantic import AwareDatetime, Field, TypeAdapter, model_validator

from trading.broker_contracts import Contract, OrderIntent
from trading.wire_validation import (
    clock_skew,
    decimal_string,
    positive_id,
    timestamp_string,
    unique_object,
)

MAX_RECEIPT_BYTES = 64_000
TIME = TypeAdapter(AwareDatetime)


class ReceiptError(ValueError):
    """Fixed local reasons only; never expose raw broker messages."""


class SubmissionReceipt(Contract):
    intent: OrderIntent
    root_order_id: int = Field(strict=True, gt=0, lt=2**63)
    order_id: int = Field(strict=True, gt=0, lt=2**63)
    broker_status: Literal["WAITING", "EXECUTED", "EXPIRED"]
    order_at: AwareDatetime = Field(strict=True)
    response_at: AwareDatetime = Field(strict=True)
    started_at: AwareDatetime = Field(strict=True)
    received_at: AwareDatetime = Field(strict=True)
    clock_skew_ms: int = Field(default=0, strict=True, ge=0, le=1000)
    expiry: str | None = None
    cancel_type: Literal["PRICE_BOUND"] | None = None
    payload_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def coherent(self):
        skew = clock_skew(self.clock_skew_ms)
        if (
            self.started_at > self.received_at
            or self.order_at < self.started_at - skew
            or self.order_at > self.response_at
            or self.response_at > self.received_at + skew
        ):
            raise ValueError("invalid_receipt_time")
        if self.expiry is not None:
            if not re.fullmatch(r"[0-9]{8}", self.expiry):
                raise ValueError("invalid_receipt_expiry")
            datetime.strptime(self.expiry, "%Y%m%d")
        if self.cancel_type is not None and (
            self.broker_status != "EXPIRED" or self.intent.kind != "MARKET"
        ):
            raise ValueError("invalid_receipt_cancel_type")
        return self


def _reject_constant(_):
    raise ReceiptError("invalid_submission_receipt")


def parse_submission_receipt(
    intent, payload, *, started_at, received_at, clock_skew_ms=0
) -> SubmissionReceipt:
    """Accept raw bounded JSON so duplicate keys cannot disappear before validation.

    POST data is a one-row array, unlike GET data.list. EXECUTED/EXPIRED remain
    acknowledgement hints until independent order and execution evidence agrees.
    """
    try:
        intent = OrderIntent.model_validate(intent.model_dump())
        if not isinstance(started_at, datetime) or not isinstance(received_at, datetime):
            raise ValueError
        started_at, received_at = (
            TIME.validate_python(started_at),
            TIME.validate_python(received_at),
        )
        clock_skew(clock_skew_ms)
        if type(payload) is not bytes or not 0 < len(payload) <= MAX_RECEIPT_BYTES:
            raise ValueError
        envelope = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=unique_object,
            parse_constant=_reject_constant,
        )
        if (
            not isinstance(envelope, dict)
            or set(envelope) != {"status", "data", "responsetime"}
            or type(envelope["status"]) is not int
            or envelope["status"] != 0
            or not isinstance(envelope["data"], list)
            or len(envelope["data"]) != 1
        ):
            raise ValueError
        row = envelope["data"][0]
        required = {
            "rootOrderId",
            "clientOrderId",
            "orderId",
            "symbol",
            "side",
            "orderType",
            "executionType",
            "settleType",
            "size",
            "status",
            "timestamp",
        }
        if intent.kind == "LIMIT":
            required.add("price")
        if (
            not isinstance(row, dict)
            or not required <= row.keys()
            or (row.keys() - required - {"expiry", "cancelType"})
        ):
            raise ValueError
        expected = {
            "clientOrderId": intent.client_id,
            "symbol": intent.symbol,
            "side": intent.side,
            "orderType": "NORMAL",
            "executionType": intent.kind,
            "settleType": intent.effect,
        }
        if any(row[key] != value for key, value in expected.items()):
            raise ValueError
        if any(key in row and not isinstance(row[key], str) for key in ("expiry", "cancelType")):
            raise ValueError
        if decimal_string(row["size"]) != intent.units:
            raise ValueError
        if intent.kind == "LIMIT" and decimal_string(row["price"]) != intent.price:
            raise ValueError
        return SubmissionReceipt(
            intent=intent,
            root_order_id=positive_id(row["rootOrderId"]),
            order_id=positive_id(row["orderId"]),
            broker_status=row["status"],
            order_at=timestamp_string(row["timestamp"]),
            response_at=timestamp_string(envelope["responsetime"]),
            started_at=started_at,
            received_at=received_at,
            clock_skew_ms=clock_skew_ms,
            expiry=row.get("expiry"),
            cancel_type=row.get("cancelType"),
            payload_sha256=hashlib.sha256(payload).hexdigest(),
        )
    except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError, RecursionError):
        raise ReceiptError("invalid_submission_receipt") from None
