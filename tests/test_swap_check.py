"""Independent swap recalculation for held positions, reported and never enforced."""

from datetime import UTC, datetime
from decimal import Decimal

import pandas as pd
import pytest
from test_live_account import report

from trading.account_reader import HeldPosition
from trading.gmo import rollover_time
from trading.swap_check import swap_check

OPENED = datetime(2026, 9, 28, 1, 0, tzinfo=UTC)  # Monday 10:00 JST
OBSERVED = datetime(2026, 10, 1, 1, 0, tzinfo=UTC)  # Thursday 10:00 JST


def schedule(days=("2026-09-28", "2026-09-29", "2026-09-30"), long="150", short="-200"):
    rows = [
        {
            "timestamp": rollover_time(pd.Timestamp(day).date()),
            "symbol": "USD_JPY",
            "long_jpy_per_10k": float(long),
            "short_jpy_per_10k": float(short),
            "days": 1,
        }
        for day in days
    ]
    frame = pd.DataFrame(rows)
    frame["timestamp"] = pd.to_datetime(frame.timestamp, utc=True)
    return frame


def held(position_id=401, side="BUY", units=2000, swap="90"):
    return HeldPosition(
        position_id=position_id,
        symbol="USD_JPY",
        side=side,
        units=units,
        ordered_units=0,
        price="150.01",
        loss_gain="0",
        total_swap=swap,
        timestamp=OPENED,
    )


def test_matching_and_mismatching_positions_are_reported(cfg):
    positions = (
        held(),  # 3 rollovers x 150 x 0.2 = 90
        held(402, side="SELL", units=1000, swap="-55"),  # expected -60, off by 5
    )
    result = swap_check(
        report(OBSERVED, positions=positions, swap="35"), schedule(), tolerance_jpy="1"
    )
    rows = {r["position_id"]: r for r in result["rows"]}
    assert Decimal(rows[401]["expected_swap"]) == 90 and rows[401]["within_tolerance"]
    assert Decimal(rows[402]["expected_swap"]) == -60
    assert Decimal(rows[402]["difference"]) == 5
    assert result["outside_tolerance"] == [402] and result["diagnostic_only"]


def test_a_schedule_with_a_missing_rollover_is_not_treated_as_zero_carry(cfg):
    result = swap_check(
        report(OBSERVED, positions=(held(),), swap="90"),
        schedule(days=("2026-09-28", "2026-09-30")),
    )
    assert result["checked"] == 0 and result["schedule_not_covering"] == [401]


@pytest.mark.parametrize("tolerance", ["-1", "nan", "inf"])
def test_tolerance_must_be_finite_and_non_negative(tolerance):
    with pytest.raises(ValueError, match="invalid_swap_tolerance"):
        swap_check(report(OBSERVED), schedule(), tolerance_jpy=tolerance)
