from pathlib import Path

import numpy as np
import pandas as pd

from trading.config import Settings

PRICE_COLUMNS = ["open", "high", "low", "close"]
SIDE_PRICE_COLUMNS = [f"{side}_{column}" for side in ("bid", "ask") for column in PRICE_COLUMNS]


def validate_bars(frame: pd.DataFrame, cfg: Settings) -> pd.DataFrame:
    required = {"timestamp", "symbol", "volume", *PRICE_COLUMNS}
    if not required.issubset(frame.columns):
        raise ValueError(f"missing columns: {sorted(required - set(frame.columns))}")
    if frame.empty:
        raise ValueError("no bars")
    frame = frame.copy()
    if set(frame.symbol.astype(str)) != {cfg.symbol}:
        raise ValueError("data symbol does not match configuration")
    times = [pd.Timestamp(value) for value in frame.timestamp]
    if any(pd.isna(value) or value.tzinfo is None for value in times):
        raise ValueError("timestamps must have explicit timezone offsets")
    frame["timestamp"] = pd.to_datetime(times, utc=True)
    if frame.timestamp.duplicated().any() or not frame.timestamp.is_monotonic_increasing:
        raise ValueError("timestamps must be unique and strictly increasing")
    if (frame.timestamp.diff().dropna().dt.total_seconds() < cfg.bar_seconds).any():
        raise ValueError("bars overlap or have the wrong interval")
    for col in [*PRICE_COLUMNS, "volume"]:
        frame[col] = pd.to_numeric(frame[col], errors="raise")
    if not np.isfinite(frame[[*PRICE_COLUMNS, "volume"]].to_numpy()).all():
        raise ValueError("bars contain non-finite values")
    if (frame[PRICE_COLUMNS] <= 0).any().any() or (frame.volume < 0).any():
        raise ValueError("prices must be positive and volume nonnegative")
    if (frame.high < frame[["open", "close", "low"]].max(axis=1)).any() or (
        frame.low > frame[["open", "close", "high"]].min(axis=1)
    ).any():
        raise ValueError("invalid OHLC envelope")
    present_side_columns = set(SIDE_PRICE_COLUMNS) & set(frame.columns)
    if present_side_columns and present_side_columns != set(SIDE_PRICE_COLUMNS):
        raise ValueError("BID/ASK OHLC columns must be supplied together")
    if present_side_columns:
        for column in SIDE_PRICE_COLUMNS:
            frame[column] = pd.to_numeric(frame[column], errors="raise")
        if not np.isfinite(frame[SIDE_PRICE_COLUMNS].to_numpy()).all():
            raise ValueError("BID/ASK OHLC contains non-finite values")
        if (frame[SIDE_PRICE_COLUMNS] <= 0).any().any():
            raise ValueError("BID/ASK OHLC prices must be positive")
        for column in PRICE_COLUMNS:
            if (frame[f"ask_{column}"] < frame[f"bid_{column}"]).any():
                raise ValueError("crossed BID/ASK OHLC")
    if "received_at" in frame.columns:
        received = [pd.Timestamp(value) for value in frame.received_at]
        if any(pd.isna(value) or value.tzinfo is None for value in received):
            raise ValueError("received_at must have explicit timezone offsets")
        frame["received_at"] = pd.to_datetime(received, utc=True)
    return frame.reset_index(drop=True)


def read_bars(path: Path, cfg: Settings) -> pd.DataFrame:
    frame = (
        pd.read_parquet(path)
        if path.suffix == ".parquet"
        else pd.read_csv(path, dtype={"symbol": str})
    )
    return validate_bars(frame, cfg)


def write_bars(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".parquet":
        frame.to_parquet(path, index=False)
    else:
        frame.to_csv(path, index=False)


def sample_bars(cfg: Settings, count: int = 240) -> pd.DataFrame:
    """Synthetic data for plumbing checks, never a performance benchmark."""
    scale = 150 if cfg.market == "fx" else 2500
    x = np.arange(count)
    closes = scale * (1 + 0.03 * np.sin(x / 12) + x * 0.00003)
    opens = np.r_[closes[0], closes[:-1]]
    times = (
        pd.date_range("2025-01-06", periods=count, freq="h", tz="UTC")
        if cfg.market == "fx"
        else pd.bdate_range("2025-01-06", periods=count, tz="Asia/Tokyo")
    )
    return validate_bars(
        pd.DataFrame(
            {
                "timestamp": times,
                "symbol": cfg.symbol,
                "open": opens,
                "high": np.maximum(opens, closes) * 1.002,
                "low": np.minimum(opens, closes) * 0.998,
                "close": closes,
                "volume": 0 if cfg.market == "fx" else 100000,
            }
        ),
        cfg,
    )
