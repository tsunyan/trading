import hashlib
import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from trading.config import Settings

PRICE_COLUMNS = ["open", "high", "low", "close"]
SIDE_PRICE_COLUMNS = [f"{side}_{column}" for side in ("bid", "ask") for column in PRICE_COLUMNS]
# Describe when and where a bar was collected, not what it says; they differ between fetches.
METADATA_COLUMNS = {"received_at", "source"}
LINEAGE_SCHEMA_VERSION = 1


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def lineage_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.lineage.json")


def publish_new_file(source: Path, target: Path) -> None:
    """Atomically publish a completed file without replacing an existing target."""
    try:
        os.link(source, target)
    except FileExistsError:
        raise
    except OSError as exc:
        raise OSError(
            f"cannot atomically publish {target}; its filesystem must support hard links: {exc}"
        ) from exc


def read_data_lineage(path: Path, *, artifact_sha256: str | None = None) -> dict | None:
    metadata = lineage_path(path)
    if not metadata.exists():
        return None
    try:
        payload = json.loads(metadata.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid data lineage {metadata}: {exc}") from exc
    if payload.get("schema_version") != LINEAGE_SCHEMA_VERSION:
        raise ValueError(f"unsupported data lineage schema in {metadata}")
    recorded = payload.get("artifact", {}).get("sha256")
    actual = artifact_sha256 or file_sha256(path)
    if recorded != actual:
        raise ValueError(f"data lineage {metadata} does not match {path}")
    return payload


def describe_data_artifact(
    path: Path,
    *,
    payload: dict | None = None,
    artifact_sha256: str | None = None,
) -> dict:
    payload = (
        read_data_lineage(path, artifact_sha256=artifact_sha256) if payload is None else payload
    )
    description = {
        "path": path.name,
        "sha256": artifact_sha256 or file_sha256(path),
    }
    if payload is not None:
        metadata = lineage_path(path)
        description.update(
            {
                "lineage_path": metadata.name,
                "lineage_sha256": file_sha256(metadata),
            }
        )
    return description


def prepare_data_lineage(
    path: Path,
    artifact_source: Path,
    record: dict,
) -> tuple[Path, Path, dict]:
    """Write a complete temporary lineage file for a staged data artifact."""
    reserved = {"schema_version", "created_at", "artifact"} & set(record)
    if reserved:
        raise ValueError(f"reserved data lineage fields: {sorted(reserved)}")
    target = lineage_path(path)
    payload = {
        "schema_version": LINEAGE_SCHEMA_VERSION,
        "created_at": datetime.now(UTC).isoformat(),
        "artifact": {
            "path": path.name,
            "sha256": file_sha256(artifact_source),
            "bytes": artifact_source.stat().st_size,
        },
        **record,
    }
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return target, temporary, payload


def write_data_lineage(path: Path, record: dict) -> tuple[Path, dict]:
    """Publish immutable lineage beside a completed data artifact."""
    target, temporary, payload = prepare_data_lineage(path, path, record)
    if target.exists():
        temporary.unlink(missing_ok=True)
        raise FileExistsError(f"{target} already exists; choose a new output path")
    try:
        publish_new_file(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return target, payload


def validate_bars(frame: pd.DataFrame, cfg: Settings) -> pd.DataFrame:
    required = {"timestamp", "symbol", "volume", *PRICE_COLUMNS}
    if not required.issubset(frame.columns):
        raise ValueError(f"missing columns: {sorted(required - set(frame.columns))}")
    if frame.empty:
        raise ValueError("no bars")
    lineage = frame.attrs.get("lineage")
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
        for side in ("bid", "ask"):
            high, low = frame[f"{side}_high"], frame[f"{side}_low"]
            others = frame[[f"{side}_open", f"{side}_close"]]
            if (high < others.max(axis=1)).any() or (low > others.min(axis=1)).any():
                raise ValueError(f"invalid {side.upper()} OHLC envelope")
        for column in PRICE_COLUMNS:
            if (frame[f"ask_{column}"] < frame[f"bid_{column}"]).any():
                raise ValueError("crossed BID/ASK OHLC")
            # The archived sides must substantiate the mid prices the strategy trades on.
            midpoint = (frame[f"bid_{column}"] + frame[f"ask_{column}"]) / 2
            if not np.isclose(frame[column], midpoint, rtol=1e-9, atol=1e-9).all():
                raise ValueError("mid OHLC does not match the BID/ASK midpoint")
    if "received_at" in frame.columns:
        received = [pd.Timestamp(value) for value in frame.received_at]
        if any(pd.isna(value) or value.tzinfo is None for value in received):
            raise ValueError("received_at must have explicit timezone offsets")
        frame["received_at"] = pd.to_datetime(received, utc=True)
    frame = frame.reset_index(drop=True)
    if lineage is not None:
        frame.attrs["lineage"] = lineage
    return frame


def read_bars(path: Path, cfg: Settings) -> pd.DataFrame:
    frame = (
        pd.read_parquet(path)
        if path.suffix == ".parquet"
        else pd.read_csv(path, dtype={"symbol": str})
    )
    frame = validate_bars(frame, cfg)
    metadata = lineage_path(path)
    artifact_sha256 = file_sha256(path) if metadata.exists() else None
    lineage = read_data_lineage(path, artifact_sha256=artifact_sha256)
    if lineage is not None:
        frame.attrs["lineage"] = lineage
        frame.attrs["artifact"] = describe_data_artifact(
            path,
            payload=lineage,
            artifact_sha256=artifact_sha256,
        )
    return frame


def write_bars(frame: pd.DataFrame, path: Path, *, overwrite: bool = False) -> None:
    """Refuses to replace an existing file unless told to; only caches may be rewritten."""
    if path.exists() and not overwrite:
        raise FileExistsError(f"{path} already exists; choose a new output path")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".parquet":
        frame.to_parquet(path, index=False)
    else:
        frame.to_csv(path, index=False)


def merge_bars(frames: list[pd.DataFrame], cfg: Settings) -> pd.DataFrame:
    """Join separately fetched bar files into one series.

    Overlapping bars must agree on every price column; the first input's copy (and its
    receipt time) is kept. Inputs must share one schema, so files saved without BID/ASK
    columns cannot be mixed with files that have them.
    """
    if not frames:
        raise ValueError("no bar files to merge")
    validated = [validate_bars(frame, cfg) for frame in frames]
    columns = set(validated[0].columns)
    if any(set(frame.columns) != columns for frame in validated[1:]):
        raise ValueError("bar files have different columns; re-fetch them in one format")
    combined = pd.concat(validated, ignore_index=True)
    kept = combined.drop_duplicates("timestamp", keep="first").set_index("timestamp")
    repeats = combined[combined.timestamp.duplicated(keep="first")].set_index("timestamp")
    content = sorted(columns - METADATA_COLUMNS - {"timestamp"})
    differs = (repeats[content].to_numpy() != kept.loc[repeats.index, content].to_numpy()).any(
        axis=1
    )
    if differs.any():
        first = repeats.index[differs][0].isoformat()
        raise ValueError(
            f"{int(differs.sum())} overlapping bars differ between inputs (first at {first})"
        )
    return validate_bars(kept.sort_index().reset_index(), cfg)


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
