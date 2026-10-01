"""Declared local held-position valuation; no claims about broker formulas."""

from decimal import Decimal
from fractions import Fraction
from typing import Literal

from pydantic import AwareDatetime, Field, model_validator

from trading.broker_contracts import Contract, Money
from trading.execution_positions import MAX_RATIO_BITS, decimal_display, rational


class ValuationError(ValueError):
    """Fixed diagnostic for bounded exact arithmetic."""


def _bounded(value):
    if max(value.numerator.bit_length(), value.denominator.bit_length()) > MAX_RATIO_BITS:
        raise ValuationError("valuation_precision_capacity")
    return value


def _sum(values):
    total = Fraction(0)
    for value in values:
        total = _bounded(total + value)
    return total


class ValuationPolicy(Contract):
    margin_rate: Decimal = Field(gt=0, le=1)
    margin_quantum_jpy: Decimal = Field(gt=0, le=1)
    margin_rounding: Literal["CEILING", "FLOOR", "HALF_EVEN"]
    margin_rounding_scope: Literal["ACCOUNT", "POSITION"]
    include_reported_swap: bool = Field(strict=True)
    subtract_reported_estimated_fee: bool = Field(strict=True)
    tolerance_jpy: Decimal = Field(ge=0, le=1)
    price_tolerance_jpy: Decimal = Field(default=Decimal(0), ge=0, le=Decimal("0.01"))
    max_quote_age_seconds: int = Field(default=60, strict=True, ge=1, le=300)
    max_report_age_seconds: int = Field(default=60, strict=True, ge=1, le=300)


class ValuationQuote(Contract):
    symbol: Literal["USD_JPY"] = "USD_JPY"
    bid: Money
    ask: Money
    observed_at: AwareDatetime

    @model_validator(mode="after")
    def ordered(self):
        if self.bid > self.ask:
            raise ValueError("valuation_quote_crossed")
        return self


def _round_margin(amount, policy):
    quantum = Fraction(policy.margin_quantum_jpy)
    scaled = amount / quantum
    whole, remainder = divmod(scaled.numerator, scaled.denominator)
    if policy.margin_rounding == "CEILING" and remainder:
        whole += 1
    if policy.margin_rounding == "HALF_EVEN" and (
        2 * remainder > scaled.denominator or (2 * remainder == scaled.denominator and whole % 2)
    ):
        whole += 1
    return whole * quantum


def _amount(value):
    _bounded(value)
    return {"display_jpy": decimal_display(value), "exact": rational(value)}


def value_account(state, cash, account, quote, policy):
    """Mark book cost at bid/ask, gross margin at ask, no netting or order relief."""
    observed = {p.position_id: p for p in account.positions}
    problems, rows, margins = [], [], []
    pnl = gross = Fraction(0)
    tolerance = Fraction(policy.tolerance_jpy)
    for pid in sorted(state.positions.keys() | observed.keys()):
        position, actual = state.positions.get(pid), observed.get(pid)
        if position is None:
            problems.append(f"position_unexpected:{pid}")
            continue
        mark = Fraction(quote.bid if position.side == "BUY" else quote.ask)
        gain = (mark - position.average_price) * position.units
        if position.side == "SELL":
            gain = -gain
        notional = Fraction(quote.ask) * position.units
        pnl = _bounded(pnl + gain)
        gross = _bounded(gross + notional)
        margins.append(notional * Fraction(policy.margin_rate))
        if actual is None:
            problems.append(f"position_missing:{pid}")
        else:
            if position.side != actual.side:
                problems.append(f"position_side_mismatch:{pid}")
            if position.units != actual.units:
                problems.append(f"position_units_mismatch:{pid}")
            if abs(position.average_price - Fraction(actual.price)) > Fraction(
                policy.price_tolerance_jpy
            ):
                problems.append(f"position_price_mismatch:{pid}")
            if abs(Fraction(actual.loss_gain) - gain) > tolerance:
                problems.append(f"position_valuation_difference:{pid}")
        rows.append(
            {
                "position_id": pid,
                "side": position.side,
                "units": position.units,
                "mark": _amount(mark),
                "loss_gain": _amount(gain),
                "gross_notional": _amount(notional),
            }
        )
    margin = (
        _round_margin(_sum(margins), policy)
        if policy.margin_rounding_scope == "ACCOUNT"
        else _sum(_round_margin(m, policy) for m in margins)
    )
    swap = Fraction(account.assets.total_swap) if policy.include_reported_swap else Fraction(0)
    fee = (
        Fraction(account.assets.estimated_trade_fee)
        if policy.subtract_reported_estimated_fee
        else Fraction(0)
    )
    equity = cash + pnl + swap - fee
    expected = {
        "balance": cash,
        "position_loss_gain": pnl,
        "equity": equity,
        "margin": margin,
        "available_amount": equity - margin,
    }
    comparisons = {}
    for field, modeled in expected.items():
        actual = Fraction(getattr(account.assets, field))
        difference = actual - modeled
        match = abs(difference) <= tolerance
        comparisons[field] = {
            "modeled": _amount(modeled),
            "observed": _amount(actual),
            "difference": _amount(difference),
            "within_tolerance": match,
        }
        if not match:
            problems.append(f"local_valuation_difference:{field}")
    if policy.include_reported_swap and sum(
        (Fraction(p.total_swap) for p in account.positions), Fraction(0)
    ) != Fraction(account.assets.total_swap):
        problems.append("reported_swap_aggregate_difference")
    active = tuple(sorted(o.order_id for o in account.active_orders))
    if active:
        problems.append("active_order_margin_not_modeled")
    return {
        "model": "declared-held-gross-ask-v1",
        "margin_scope": "held_positions_only",
        "diagnostics_match": not problems,
        "mismatches": tuple(problems),
        "positions": tuple(rows),
        "gross_notional": _amount(gross),
        "reported_swap_input": _amount(swap),
        "reported_estimated_fee_input": _amount(fee),
        "active_order_ids_not_modeled": active,
        "comparisons": comparisons,
    }
