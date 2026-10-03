"""Compare captured executions with explicitly collected known-order REST evidence.

Matched cash components are diagnostics, not bookings or proof of account history.
"""

from decimal import Decimal
from typing import Literal

from trading.account_events import AccountEvent
from trading.account_reader import OrderReadReport
from trading.broker_contracts import Contract, validate_evidence
from trading.wire_validation import exact_decimal


class ExecutionReconciliation(Contract):
    matched_execution_ids: tuple[int, ...]
    unverified_execution_ids: tuple[int, ...]
    rest_only_execution_ids: tuple[int, ...]
    mismatches: tuple[str, ...]
    # Totals cover only matched, deduplicated notices in this observation epoch.
    matched_loss_gain: Decimal
    matched_fee_debit: Decimal
    matched_settled_swap: Decimal
    matched_cash_amount: Decimal
    reports: tuple[OrderReadReport, ...]
    complete: Literal[False] = False
    live_enabled: Literal[False] = False
    accounting_applied: Literal[False] = False


def reconcile_executions(
    events: tuple[AccountEvent, ...], reports: tuple[OrderReadReport, ...]
) -> ExecutionReconciliation:
    """Pure bounded comparison. Receipt freshness is checked by the sync monitor."""
    if not isinstance(events, tuple) or not isinstance(reports, tuple):
        raise ValueError("invalid_execution_reconciliation_input")
    if len(events) > 100_000 or len(reports) > 1000:
        raise ValueError("execution_reconciliation_capacity")
    notices = {}
    for source in events:
        event = AccountEvent.model_validate(source.model_dump())
        if (
            event.channel != "executionEvents"
            or event.execution is None
            or event.execution_order is None
            or event.execution_amount is None
            or event.execution_cumulative_units is None
            or event.entity_id != event.execution.execution_id
            or event.execution_order_id != event.execution_order.order_id
        ):
            raise ValueError("invalid_execution_notice")
        prior = notices.get(event.entity_id)
        if prior is not None and prior != event:
            raise ValueError("execution_identity_conflict")
        notices[event.entity_id] = event
    orders, rest_ids, normalized = {}, set(), []
    requested = {e.execution_order_id for e in notices.values()}
    for source in reports:
        report = OrderReadReport.model_validate(source.model_dump())
        evidence = report.evidence
        validate_evidence(evidence)
        if evidence.executions_complete or (
            evidence.status == "EXECUTED"
            and sum(f.units for f in evidence.executions) != evidence.intent.units
        ):
            raise ValueError("invalid_execution_completeness")
        if evidence.order_id in orders or evidence.order_id not in requested:
            raise ValueError("unexpected_or_duplicate_order_report")
        ids = {f.execution_id for f in evidence.executions}
        if rest_ids.intersection(ids) or any(f.fee < 0 for f in evidence.executions):
            raise ValueError("invalid_rest_execution_identity_or_fee")
        rest_ids.update(ids)
        if len(rest_ids) > 100_000:
            raise ValueError("execution_reconciliation_capacity")
        orders[evidence.order_id] = (
            evidence,
            {f.execution_id: f for f in evidence.executions},
            sum(f.units for f in evidence.executions),
        )
        normalized.append(report)
    matched, problems = [], []
    for identity, event in sorted(notices.items()):
        collected = orders.get(event.execution_order_id)
        if collected is None:
            problems.append(f"execution_order_not_collected:{identity}")
            continue
        evidence, rest_fills, rest_units = collected
        order, fill, intent = event.execution_order, event.execution, evidence.intent
        if (
            order.root_order_id,
            order.client_id,
            order.symbol,
            order.side,
            order.effect,
            order.kind,
            order.units,
            order.price,
        ) != (
            evidence.root_order_id,
            intent.client_id,
            intent.symbol,
            intent.side,
            intent.effect,
            intent.kind,
            intent.units,
            intent.price,
        ):
            problems.append(f"execution_order_mismatch:{identity}")
            continue
        rest = rest_fills.get(identity)
        if rest is None:
            problems.append(f"execution_missing_from_rest:{identity}")
            continue
        with exact_decimal():
            amount = fill.loss_gain - fill.fee + fill.settled_swap
        if rest != fill:
            problems.append(f"execution_fields_mismatch:{identity}")
        elif fill.fee < 0 or event.execution_amount != amount:
            problems.append(f"execution_amount_mismatch:{identity}")
        elif not fill.units <= event.execution_cumulative_units <= rest_units:
            problems.append(f"execution_cumulative_size_mismatch:{identity}")
        else:
            matched.append(identity)
    fills = [notices[i].execution for i in matched]
    # The caller's Decimal context must not round a match or its totals.
    with exact_decimal():
        loss = sum((f.loss_gain for f in fills), Decimal(0))
        fee = sum((f.fee for f in fills), Decimal(0))
        swap = sum((f.settled_swap for f in fills), Decimal(0))
        cash = loss - fee + swap
    return ExecutionReconciliation(
        matched_execution_ids=tuple(matched),
        unverified_execution_ids=tuple(sorted(notices.keys() - set(matched))),
        rest_only_execution_ids=tuple(sorted(rest_ids - notices.keys())),
        mismatches=tuple(problems),
        matched_loss_gain=loss,
        matched_fee_debit=fee,
        matched_settled_swap=swap,
        matched_cash_amount=cash,
        reports=tuple(normalized),
    )
