"""Normalized OFFLINE account reconciliation and pre-submission risk checks.

Not a GMO wire decoder: fees are explicitly normalized as nonnegative debits.
Bootstrap is flat, with fixed cash and no deposits/withdrawals thereafter.
"""

import hashlib
import json
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import AwareDatetime, Field, model_validator

from trading.broker_contracts import Contract, Money, OrderEvidence, OrderIntent, Units
from trading.order_states import TERMINAL

Nonnegative = Annotated[Decimal, Field(ge=0, allow_inf_nan=False)]
RETRYABLE_ACCOUNT_ERRORS = frozenset(
    {
        "unresolved_order",
        "incomplete_executions",
        "stale_or_future_quote",
        "stale_or_future_account",
        "account_precedes_order_evidence",
        "account_time_moved_backwards",
    }
)


class AccountPolicy(Contract):
    account_id: str = Field(min_length=1, max_length=100)
    bootstrap_at: AwareDatetime
    starting_balance: Money
    max_order_notional: Money
    max_gross_notional: Money
    max_leverage: Money
    max_loss_jpy: Money
    max_drawdown: Annotated[Decimal, Field(gt=0, lt=1)]
    margin_rate: Annotated[Decimal, Field(gt=0, le=1)]
    min_margin_ratio: Annotated[Decimal, Field(ge=1)]
    min_available_margin: Nonnegative
    fee_buffer_rate: Nonnegative
    max_spread: Money
    max_quote_age_seconds: Units = 60
    max_snapshot_age_seconds: Units = 60
    max_pending_orders: Units = 1
    tolerance_jpy: Nonnegative = Decimal("0.01")
    tolerance_price: Nonnegative = Decimal("0.000001")


class AccountQuote(Contract):
    symbol: Literal["USD_JPY"] = "USD_JPY"
    bid: Money
    ask: Money
    observed_at: AwareDatetime
    market_open: bool = Field(strict=True)

    @model_validator(mode="after")
    def valid_spread(self):
        if self.bid > self.ask:
            raise ValueError("crossed quote")
        return self


class Position(Contract):
    position_id: Units
    symbol: Literal["USD_JPY"] = "USD_JPY"
    side: Literal["BUY", "SELL"]
    units: Units
    average_price: Money


class WorkingOrder(Contract):
    client_id: str
    order_id: Units
    remaining_units: Units


class AccountSnapshot(Contract):
    account_id: str
    observed_at: AwareDatetime
    # Completeness must be established by a future collector, not inferred.
    complete: bool = Field(default=False, strict=True)
    balance: Decimal
    equity: Decimal
    unrealized_swap: Decimal = Decimal(0)
    required_margin: Nonnegative
    available_margin: Nonnegative
    positions: tuple[Position, ...] = ()
    working_orders: tuple[WorkingOrder, ...] = ()

    @model_validator(mode="after")
    def unique(self):
        for ids in (
            [p.position_id for p in self.positions],
            [o.order_id for o in self.working_orders],
            [o.client_id for o in self.working_orders],
        ):
            if len(ids) != len(set(ids)):
                raise ValueError("duplicate position/order identity")
        return self


def revision(rows: list[dict]) -> str:
    # Unclaimed intents do not change the broker account. Any claimed order,
    # state change or execution evidence invalidates the account proof.
    relevant = [
        {k: row[k] for k in ("client_id", "intent_json", "state", "evidence_json")}
        for row in rows
        if row["state"] not in {"PREPARED", "ABANDONED"}
    ]
    raw = json.dumps(sorted(relevant, key=lambda r: r["client_id"]), sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()


def fresh(timestamp: datetime, now: datetime, maximum: int) -> bool:
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    age = (now - timestamp).total_seconds()
    return 0 <= age <= maximum


def expected_account(policy: AccountPolicy, rows: list[dict]):
    """Rebuild from immutable execution IDs, not from broker position totals."""
    executions, working, seen = [], {}, set()
    for row in rows:
        if row["state"] in {"PREPARED", "ABANDONED"}:
            continue
        if row["state"] not in TERMINAL | {"WORKING", "PARTIAL"} or not row["evidence_json"]:
            raise ValueError("unresolved_order")
        evidence = OrderEvidence.model_validate_json(row["evidence_json"])
        if not evidence.executions_complete:
            raise ValueError("incomplete_executions")
        for execution in evidence.executions:
            if execution.execution_id in seen:
                raise ValueError("duplicate_execution")
            seen.add(execution.execution_id)
            executions.append(
                (execution.timestamp, execution.execution_id, evidence.intent, execution)
            )
        remaining = evidence.intent.units - sum(e.units for e in evidence.executions)
        if row["state"] not in TERMINAL:
            if remaining <= 0:
                raise ValueError("working_order_without_remaining_size")
            working[row["client_id"]] = WorkingOrder(
                client_id=row["client_id"],
                order_id=evidence.order_id,
                remaining_units=remaining,
            )
    balance = policy.starting_balance
    positions = {}
    for time, _, intent, execution in sorted(executions, key=lambda item: (item[0], item[1])):
        if time < policy.bootstrap_at or execution.fee < 0:
            raise ValueError("invalid_normalized_execution")
        pid = execution.position_id
        existing = positions.get(pid)
        realized = Decimal(0)
        if intent.effect == "OPEN":
            if existing and existing.side != intent.side:
                raise ValueError("position_side_changed")
            old_units = existing.units if existing else 0
            old_cost = existing.average_price * old_units if existing else Decimal(0)
            positions[pid] = Position(
                position_id=pid,
                side=intent.side,
                units=old_units + execution.units,
                average_price=(old_cost + execution.price * execution.units)
                / (old_units + execution.units),
            )
            if execution.settled_swap != 0:
                raise ValueError("unexpected_open_swap")
        else:
            if not existing or existing.side == intent.side or existing.units < execution.units:
                raise ValueError("invalid_position_close")
            sign = 1 if existing.side == "BUY" else -1
            realized = (execution.price - existing.average_price) * execution.units * sign
            if existing.units == execution.units:
                del positions[pid]
            else:
                positions[pid] = existing.model_copy(
                    update={"units": existing.units - execution.units}
                )
        if abs(execution.loss_gain - realized) > policy.tolerance_jpy:
            raise ValueError("execution_realized_pnl_mismatch")
        balance += realized + execution.settled_swap - execution.fee
    return balance, positions, working


def marked_equity(snapshot: AccountSnapshot, quote: AccountQuote) -> Decimal:
    result = snapshot.balance + snapshot.unrealized_swap
    for position in snapshot.positions:
        mark = quote.bid if position.side == "BUY" else quote.ask
        sign = 1 if position.side == "BUY" else -1
        result += (mark - position.average_price) * position.units * sign
    return result


def reconcile_account(
    policy: AccountPolicy,
    rows: list[dict],
    snapshot: AccountSnapshot,
    quote: AccountQuote,
    now: datetime,
) -> list[str]:
    errors = []
    if snapshot.account_id != policy.account_id:
        errors.append("account_mismatch")
    if not snapshot.complete:
        errors.append("incomplete_account_snapshot")
    if snapshot.observed_at < policy.bootstrap_at:
        errors.append("snapshot_before_bootstrap")
    if not fresh(snapshot.observed_at, now, policy.max_snapshot_age_seconds):
        errors.append("stale_or_future_account")
    if not fresh(quote.observed_at, now, policy.max_quote_age_seconds):
        errors.append("stale_or_future_quote")
    # Only compare account contents when the observations can describe the same state.
    for row in rows:
        if row["evidence_json"]:
            evidence = OrderEvidence.model_validate_json(row["evidence_json"])
            if evidence.observed_at > snapshot.observed_at:
                errors.append("account_precedes_order_evidence")
                break
    try:
        balance, positions, working = expected_account(policy, rows)
    except ValueError as exc:
        if str(exc) not in RETRYABLE_ACCOUNT_ERRORS:
            raise
        errors.append(str(exc))
    if errors:
        return errors
    if not positions and snapshot.unrealized_swap != 0:
        errors.append("swap_without_positions")
    if abs(balance - snapshot.balance) > policy.tolerance_jpy:
        errors.append("balance_mismatch")
    actual_positions = {p.position_id: p for p in snapshot.positions}
    if actual_positions.keys() != positions.keys() or any(
        actual_positions[pid].side != p.side
        or actual_positions[pid].units != p.units
        or abs(actual_positions[pid].average_price - p.average_price) > policy.tolerance_price
        for pid, p in positions.items()
        if pid in actual_positions
    ):
        errors.append("positions_mismatch")
    if {o.client_id: o for o in snapshot.working_orders} != working:
        errors.append("working_orders_mismatch")
    if abs(marked_equity(snapshot, quote) - snapshot.equity) > policy.tolerance_jpy:
        errors.append("equity_mismatch")
    if (
        snapshot.available_margin
        > max(snapshot.equity - snapshot.required_margin, Decimal(0)) + policy.tolerance_jpy
    ):
        errors.append("inconsistent_available_margin")
    return errors


def opening_mark_loss(side: str, reference: Decimal, quote: AccountQuote) -> Decimal:
    """Per-unit loss when a new position is marked to its liquidation side."""
    return max(reference - quote.bid if side == "BUY" else quote.ask - reference, Decimal(0))


def evaluate_risk(
    policy: AccountPolicy,
    snapshot: AccountSnapshot,
    quote: AccountQuote,
    rows: list[dict],
    intent: OrderIntent,
    now: datetime,
    peak: Decimal,
    entry_halted: bool,
) -> dict:
    errors = []
    if not fresh(snapshot.observed_at, now, policy.max_snapshot_age_seconds):
        errors.append("stale_or_future_account")
    if not fresh(quote.observed_at, now, policy.max_quote_age_seconds):
        errors.append("stale_or_future_quote")
    if errors:
        # Invalid clocks must not move the saved peak or trigger a financial stop.
        return {
            "allowed": False,
            "reasons": errors,
            "entry_halted": entry_halted,
            "peak": str(peak),
        }
    if not quote.market_open:
        errors.append("market_closed")
    if quote.ask - quote.bid > policy.max_spread:
        errors.append("spread_exceeds_limit")
    equity = min(snapshot.equity, marked_equity(snapshot, quote))
    peak = max(peak, equity)
    drawdown = (peak - equity) / peak
    loss = policy.starting_balance - equity
    entry_halted = entry_halted or loss >= policy.max_loss_jpy or drawdown >= policy.max_drawdown
    if intent.effect == "OPEN" and entry_halted:
        errors.append("entry_loss_halt")
    # Never net opposite sides or count pending closes as already reducing risk.
    position_gross = sum((p.units * quote.ask for p in snapshot.positions), Decimal(0))
    local = {row["client_id"]: OrderIntent.model_validate_json(row["intent_json"]) for row in rows}
    pending_gross = Decimal(0)
    pending_fee_notional = Decimal(0)
    opening_mark_buffer = Decimal(0)
    reserved_closes = {}
    for order in snapshot.working_orders:
        pending = local[order.client_id]
        reference = pending.price if pending.kind == "LIMIT" else pending.bound
        pending_fee_notional += order.remaining_units * max(reference, quote.ask)
        if pending.effect == "OPEN":
            pending_gross += order.remaining_units * max(reference, quote.ask)
            opening_mark_buffer += order.remaining_units * opening_mark_loss(
                pending.side, reference, quote
            )
        else:
            # Conservatively reserve the original full closing quantity until resolved.
            for position in pending.positions:
                reserved_closes[position.position_id] = (
                    reserved_closes.get(position.position_id, 0) + position.units
                )
    reference = intent.price if intent.kind == "LIMIT" else intent.bound
    order_notional = intent.units * max(reference, quote.ask)
    if intent.effect == "OPEN":
        opening_mark_buffer += intent.units * opening_mark_loss(intent.side, reference, quote)
    if order_notional > policy.max_order_notional:
        errors.append("order_notional_limit")
    if len(snapshot.working_orders) + 1 > policy.max_pending_orders:
        errors.append("pending_order_count_limit")
    if intent.effect == "CLOSE":
        positions = {p.position_id: p for p in snapshot.positions}
        for close in intent.positions:
            position = positions.get(close.position_id)
            if not position or position.side == intent.side:
                errors.append("invalid_closing_position")
            elif close.units + reserved_closes.get(close.position_id, 0) > position.units:
                errors.append("closing_quantity_reserved_or_exceeded")
    projected_gross = (
        position_gross + pending_gross + (order_notional if intent.effect == "OPEN" else 0)
    )
    # Count a pending close as zero relief. Reserve fresh open-order margin and
    # costs even if broker availableMargin already includes some reservations.
    fee_buffer = (order_notional + pending_fee_notional) * policy.fee_buffer_rate
    projected_margin = max(
        snapshot.required_margin, (position_gross + pending_gross) * policy.margin_rate
    )
    if intent.effect == "OPEN":
        projected_margin += order_notional * policy.margin_rate
    after_cost_equity = equity - fee_buffer - opening_mark_buffer
    if intent.effect == "OPEN" and (
        policy.starting_balance - after_cost_equity >= policy.max_loss_jpy
        or (peak - after_cost_equity) / peak >= policy.max_drawdown
    ):
        # A hypothetical execution cost may reject this order but must not latch a stop for
        # a loss that has not happened. A smaller order may still be admissible.
        errors.append("projected_loss_limit")
    available = min(snapshot.available_margin - fee_buffer, after_cost_equity - projected_margin)
    if intent.effect == "OPEN":
        available = min(
            available, snapshot.available_margin - order_notional * policy.margin_rate - fee_buffer
        )
        if projected_gross > policy.max_gross_notional:
            errors.append("gross_notional_limit")
        if after_cost_equity <= 0 or projected_gross > after_cost_equity * policy.max_leverage:
            errors.append("leverage_limit")
        if available < policy.min_available_margin:
            errors.append("available_margin_limit")
        if after_cost_equity < projected_margin * policy.min_margin_ratio:
            errors.append("margin_ratio_limit")
    # Risk-reducing closes may pass exposure/loss limits, but never identity,
    # freshness, quantity, unresolved-order, price or market checks.
    return {
        "allowed": not errors,
        "reasons": sorted(set(errors)),
        "entry_halted": entry_halted,
        "peak": str(peak),
        "equity": str(equity),
        "drawdown": str(drawdown),
        "loss_jpy": str(loss),
        "order_notional": str(order_notional),
        "projected_gross": str(projected_gross),
        "projected_margin": str(projected_margin),
        "fee_buffer": str(fee_buffer),
        "opening_mark_buffer": str(opening_mark_buffer),
        "available_after_reserve": str(available),
    }
