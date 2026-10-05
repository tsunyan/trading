"""Independent swap recalculation for held positions, reported and never enforced."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pandas as pd
import pytest
from test_live_account import report as account_report

from trading.account_reader import POSITIONS, HeldPosition, Observation
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


def observation(path, when):
    return Observation(path=path, query=(), response_at=when, received_at=when, sha256="a" * 64)


def report(when, *, first_assets=None, **values):
    """Two sweeps; the second sweep's positions read is at `when`."""
    first_assets = when - timedelta(seconds=2) if first_assets is None else first_assets
    reads = (
        observation("/v1/account/assets", first_assets),
        observation(POSITIONS, when - timedelta(seconds=1)),
        observation("/v1/account/assets", when - timedelta(seconds=1)),
        observation("/v1/account/assets", when - timedelta(seconds=1)),
        observation(POSITIONS, when),
        observation("/v1/account/assets", when),
    )
    return account_report(when, **values).model_copy(update={"observations": reads})


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


def test_a_collection_that_starts_before_the_rollover_uses_the_positions_read(cfg):
    rollover = pd.Timestamp(rollover_time(pd.Timestamp("2026-10-01").date())).to_pydatetime()
    # First assets read before the rollover; both positions reads already include it.
    result = swap_check(
        report(
            rollover + timedelta(seconds=30),
            first_assets=rollover - timedelta(seconds=1),
            positions=(held(swap="120"),),
        ),
        schedule(days=("2026-09-28", "2026-09-29", "2026-09-30", "2026-10-01")),
    )
    assert result["outside_tolerance"] == [] and result["checked"] == 1


def test_a_report_without_a_positions_read_is_refused():
    with pytest.raises(ValueError, match="positions_observation_required"):
        swap_check(account_report(OBSERVED), schedule())
