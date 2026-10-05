"""Independent swap recalculation for open positions; a diagnostic, never a halt.

Each held position reports its open time and accumulated swap. The official swap
history (fetch-swap) gives the credit per 10,000 units at each rollover, so the expected
swap is the sum over rollovers after the open time. GMO's rounding and the exact
rollover boundary are not yet confirmed on a real account, hence a report and an
explicit tolerance instead of an account gate.
"""

from decimal import Decimal

import pandas as pd

from trading.account_reader import POSITIONS, AccountReadReport
from trading.config import Settings
from trading.swap import read_swap_schedule, require_swap_coverage, swap_credit_between


def read_schedule(path):
    """The fetch-swap CSV/Parquet for the live USD/JPY account."""
    return read_swap_schedule(path, Settings(market="fx", symbol="USD_JPY", bar_seconds=3600))


def swap_check(report: AccountReadReport, schedule: pd.DataFrame, *, tolerance_jpy="1") -> dict:
    if not isinstance(report, AccountReadReport):
        raise ValueError("account_read_report_required")
    tolerance = Decimal(str(tolerance_jpy))
    if not tolerance.is_finite() or tolerance < 0:
        raise ValueError("invalid_swap_tolerance")
    # The returned positions come from the second sweep. Its positions read, not the
    # first assets read, is when their totalSwap was observed (rollover boundary).
    positions_read = [o.response_at for o in report.observations if o.path == POSITIONS]
    if not positions_read:
        raise ValueError("positions_observation_required")
    observed = pd.Timestamp(max(positions_read))
    rows, uncovered = [], []
    for position in report.positions:
        opened = pd.Timestamp(position.timestamp)
        try:
            require_swap_coverage(schedule, opened, observed)
        except ValueError:
            uncovered.append(position.position_id)
            continue
        units = position.units if position.side == "BUY" else -position.units
        expected = Decimal(str(round(swap_credit_between(schedule, opened, observed, units), 6)))
        difference = position.total_swap - expected
        rows.append(
            {
                "position_id": position.position_id,
                "side": position.side,
                "units": position.units,
                "opened_at": opened.isoformat(),
                "broker_swap": str(position.total_swap),
                "expected_swap": str(expected),
                "difference": str(difference),
                "within_tolerance": abs(difference) <= tolerance,
            }
        )
    return {
        "observed_at": observed.isoformat(),
        "tolerance_jpy": str(tolerance),
        "positions": len(report.positions),
        "checked": len(rows),
        "schedule_not_covering": uncovered,
        "outside_tolerance": [r["position_id"] for r in rows if not r["within_tolerance"]],
        "rows": rows,
        "diagnostic_only": True,
    }
