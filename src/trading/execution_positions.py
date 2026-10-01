"""Exact replay of declared starting positions and individually booked executions."""

from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction
from typing import Annotated, Literal

from pydantic import Field

from trading.broker_contracts import Contract, Money

MAX_RATIO_BITS = 4096


class PositionAccountingError(ValueError):
    """Fixed diagnostic codes; no source data."""


class OpeningPosition(Contract):
    position_id: Annotated[int, Field(strict=True, gt=0, lt=2**63)]
    symbol: Literal["USD_JPY"] = "USD_JPY"
    side: Literal["BUY", "SELL"]
    units: Annotated[int, Field(strict=True, gt=0, le=10**12)]
    average_price: Money


class PositionBasis(Contract):
    # Required even for (), so an absent declaration never means flat.
    positions: tuple[OpeningPosition, ...] = Field(max_length=1000)
    realized_pnl_tolerance: Decimal = Field(default=Decimal(0), ge=0, le=1)


@dataclass(frozen=True)
class DerivedPosition:
    side: str
    units: int
    average_price: Fraction


@dataclass(frozen=True)
class PositionState:
    positions: dict[int, DerivedPosition]
    realized_pnl: Fraction
    reported_pnl: Fraction


def _bounded(value):
    if max(abs(value.numerator).bit_length(), value.denominator.bit_length()) > MAX_RATIO_BITS:
        raise PositionAccountingError("position_precision_capacity")
    return value


def rational(value):
    return {"numerator": str(value.numerator), "denominator": str(value.denominator)}


def decimal_display(value):
    """Eight places, half-even; only a display, never used as a replay input."""
    scaled = value * 100_000_000
    whole, remainder = divmod(abs(scaled.numerator), scaled.denominator)
    if 2 * remainder > scaled.denominator or (2 * remainder == scaled.denominator and whole % 2):
        whole += 1
    sign = "-" if scaled < 0 and whole else ""
    return f"{sign}{whole // 100_000_000}.{whole % 100_000_000:08d}"


def position_result(state):
    if state is None:
        return {}
    return {
        "position_accounting_applied": True,
        "positions": tuple(
            {
                "position_id": pid,
                "symbol": "USD_JPY",
                "side": p.side,
                "units": p.units,
                "average_price": decimal_display(p.average_price),
                "average_price_exact": rational(p.average_price),
            }
            for pid, p in sorted(state.positions.items())
        ),
        "position_realized_pnl_exact": rational(state.realized_pnl),
        "position_pnl_rounding_difference_exact": rational(state.reported_pnl - state.realized_pnl),
    }


def rebuild_positions(basis: PositionBasis, records):
    """Replay chronologically; mixed open/close at the same time is ambiguous.

    Records have already passed cash-book schema, bounds and evidence checks.
    Fraction preserves weighted cost through partial closes independently of
    the caller's Decimal context. Reported P&L never supplies the cost basis.
    """
    positions = {}
    for initial in basis.positions:
        if initial.position_id in positions:
            raise PositionAccountingError("position_opening_identity_duplicate")
        positions[initial.position_id] = DerivedPosition(
            initial.side, initial.units, _bounded(Fraction(initial.average_price))
        )
    effects = {}
    for record in records:
        fill = record.execution
        key = (fill.timestamp, fill.position_id)
        effects.setdefault(key, set()).add(record.intent.effect)
    if any(len(kinds) > 1 for kinds in effects.values()):
        raise PositionAccountingError("position_event_order_ambiguous")
    retired, order_units, close_units = set(), {}, {}
    realized = reported = Fraction(0)
    tolerance = Fraction(basis.realized_pnl_tolerance)
    for record in sorted(records, key=lambda r: (r.execution.timestamp, r.execution.execution_id)):
        fill, intent = record.execution, record.intent
        pid = fill.position_id
        prior = positions.get(pid)
        order_units[record.order_id] = order_units.get(record.order_id, 0) + fill.units
        if order_units[record.order_id] > intent.units:
            raise PositionAccountingError("position_order_overfilled")
        price = Fraction(fill.price)
        if intent.effect == "OPEN":
            if pid in retired or (prior is not None and prior.side != intent.side):
                raise PositionAccountingError("position_open_identity_conflict")
            if fill.loss_gain != 0 or fill.settled_swap != 0:
                raise PositionAccountingError("position_open_pnl_or_swap_invalid")
            units = fill.units + (prior.units if prior else 0)
            if units > 10**12:
                raise PositionAccountingError("position_quantity_capacity")
            cost = price * fill.units + (prior.average_price * prior.units if prior else 0)
            positions[pid] = DerivedPosition(intent.side, units, _bounded(cost / units))
        else:
            allocated = {p.position_id: p.units for p in intent.positions}
            key = (record.order_id, pid)
            close_units[key] = close_units.get(key, 0) + fill.units
            if pid not in allocated or close_units[key] > allocated[pid]:
                raise PositionAccountingError("position_close_allocation_invalid")
            if prior is None or prior.side == intent.side or fill.units > prior.units:
                raise PositionAccountingError("position_close_without_matching_inventory")
            pnl = _bounded(
                (price - prior.average_price) * fill.units * (1 if prior.side == "BUY" else -1)
            )
            if abs(Fraction(fill.loss_gain) - pnl) > tolerance:
                raise PositionAccountingError("position_realized_pnl_mismatch")
            realized = _bounded(realized + pnl)
            reported = _bounded(reported + Fraction(fill.loss_gain))
            if fill.units == prior.units:
                del positions[pid]
                retired.add(pid)
            else:
                positions[pid] = DerivedPosition(
                    prior.side, prior.units - fill.units, prior.average_price
                )
    _bounded(reported - realized)
    return PositionState(positions, realized, reported)
