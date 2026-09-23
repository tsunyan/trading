import hashlib
from pathlib import Path

import numpy as np
import pandas as pd

from trading.config import Settings

SWAP_COLUMNS = [
    "timestamp",
    "symbol",
    "long_jpy_per_10k",
    "short_jpy_per_10k",
    "days",
]


def validate_swap_schedule(frame: pd.DataFrame, cfg: Settings) -> pd.DataFrame:
    """Validate timestamped, direction-specific swap credits for 10,000 units."""
    if cfg.market != "fx":
        raise ValueError("swap schedules apply to FX configurations only")
    missing = set(SWAP_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"missing swap columns: {sorted(missing)}")
    frame = frame[SWAP_COLUMNS].copy()
    if frame.empty:
        return frame
    if set(frame.symbol.astype(str)) != {cfg.symbol}:
        raise ValueError("swap symbol does not match configuration")
    times = [pd.Timestamp(value) for value in frame.timestamp]
    if any(pd.isna(value) or value.tzinfo is None for value in times):
        raise ValueError("swap timestamps must have explicit timezone offsets")
    frame["timestamp"] = pd.to_datetime(times, utc=True)
    if frame.timestamp.duplicated().any() or not frame.timestamp.is_monotonic_increasing:
        raise ValueError("swap timestamps must be unique and strictly increasing")
    for column in ("long_jpy_per_10k", "short_jpy_per_10k", "days"):
        frame[column] = pd.to_numeric(frame[column], errors="raise")
    if not np.isfinite(frame[["long_jpy_per_10k", "short_jpy_per_10k", "days"]].to_numpy()).all():
        raise ValueError("swap schedule contains non-finite values")
    if (frame.days <= 0).any() or (frame.days % 1 != 0).any():
        raise ValueError("swap days must be positive integers")
    frame["days"] = frame.days.astype(int)
    return frame.reset_index(drop=True)


def read_swap_schedule(path: Path, cfg: Settings) -> pd.DataFrame:
    frame = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path)
    return validate_swap_schedule(frame, cfg)


def _event_credits(
    schedule: pd.DataFrame | None,
    start: pd.Timestamp,
    end: pd.Timestamp,
    units: int,
) -> pd.Series:
    if schedule is None or schedule.empty or not units or end <= start:
        return pd.Series(dtype=float)
    events = schedule.loc[(schedule.timestamp > start) & (schedule.timestamp <= end)]
    column = "long_jpy_per_10k" if units > 0 else "short_jpy_per_10k"
    return abs(units) / 10_000 * events[column].astype(float)


def swap_credit_between(
    schedule: pd.DataFrame | None,
    start: pd.Timestamp,
    end: pd.Timestamp,
    units: int,
) -> float:
    """Return signed JPY carry for events in the half-open holding interval (start, end]."""
    return float(_event_credits(schedule, start, end, units).sum())


def swap_charges_between(
    schedule: pd.DataFrame | None,
    start: pd.Timestamp,
    end: pd.Timestamp,
    units: int,
) -> float:
    """Only the charges (negative carry) in (start, end].

    When bar data cannot say whether an event came before or after an intrabar extreme,
    a conservative risk check assumes charges came first and credits came after.
    """
    return float(_event_credits(schedule, start, end, units).clip(upper=0).sum())


def swap_fingerprint(schedule: pd.DataFrame | None) -> str | None:
    if schedule is None:
        return None
    return hashlib.sha256(schedule.to_csv(index=False).encode()).hexdigest()
