"""Opt-in forward observation account. No private API, catch-up fills, or live orders."""

import argparse
import hashlib
import json
import math
import sqlite3
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pandas as pd

from trading.config import Settings, load_settings
from trading.data import validate_bars
from trading.gmo import GmoPublic, GmoSwapCalendar, trading_date
from trading.paper import paper_step
from trading.provenance import runtime_versions
from trading.swap import read_swap_schedule, validate_swap_schedule

CORE_FILES = ("observer.py", "paper.py", "strategy.py", "config.py", "gmo.py", "swap.py", "data.py")
ATTEMPT_SCHEMA = """
CREATE TABLE attempts (
 id INTEGER PRIMARY KEY AUTOINCREMENT, started_at TEXT NOT NULL,
 finished_at TEXT, status TEXT NOT NULL, detail_json TEXT NOT NULL
);
"""


def code_hashes() -> dict:
    return {
        name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
        for name in CORE_FILES
    }


def load_manifest(directory: Path) -> dict:
    return json.loads((directory / "manifest.json").read_text(encoding="utf-8"))


def initialize(directory: Path, cfg: Settings, now: datetime | None = None) -> dict:
    if cfg.market != "fx":
        raise ValueError("observer supports FX only")
    now = now or datetime.now(UTC)
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    directory.mkdir(parents=True, exist_ok=False)
    manifest = {
        "mode": "exploratory_forward_paper",
        "created_at": now.isoformat(),
        "config": cfg.model_dump(),
        "config_sha256": cfg.fingerprint,
        "core_sha256": code_hashes(),
        "runtime": runtime_versions(),
        "expected_interval_minutes": 15,
        "swap_enabled": True,
        "execution": "fresh public BID/ASK quote + configured slippage/commission",
        "note": "User-authorized observation, not strategy promotion or validated profitability.",
        "limitations": [
            "Only observed quotes can fill. Downtime is not replayed.",
            "Risk checked at observations, not continuously or intrabar.",
            "Paper spread-entry filter and observation timing differ from research backtests.",
        ],
    }
    manifest["observer_id"] = hashlib.sha256(
        json.dumps(manifest, sort_keys=True).encode()
    ).hexdigest()
    (directory / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (directory / "source").mkdir()
    for name in CORE_FILES:
        (directory / "source" / name).write_bytes(Path(__file__).with_name(name).read_bytes())
    with closing(sqlite3.connect(directory / "observations.sqlite")) as conn:
        conn.executescript(ATTEMPT_SCHEMA)
    return manifest


def collect_bars(api, cfg: Settings, directory: Path, now: datetime) -> pd.DataFrame:
    today = trading_date(now)
    # 7 calendar days is insufficient for 120 hourly bars around a holiday/weekend.
    days = max(21, math.ceil(cfg.warmup_bars / 24) * 2 + 7)
    cache = directory / "candles"
    cache.mkdir(exist_ok=True)
    frames = []
    for offset in range(days, -1, -1):
        day = today - timedelta(days=offset)
        path = cache / f"{day}.parquet"
        if path.exists():
            frame = pd.read_parquet(path)
        else:
            returned, frame = next(api.candle_days(cfg, day, day, now))
            if returned != day:
                raise ValueError("unexpected candle date")
            # Cache only finalized non-empty days. Empty weekdays could be a provider outage.
            if day < today and not frame.empty and frame.attrs.get("trading_date_complete"):
                frame.to_parquet(path, index=False)
        if not frame.empty:
            frames.append(frame)
    if not frames:
        raise ValueError("no completed candles available")
    return validate_bars(pd.concat(frames, ignore_index=True), cfg)


def collect_swaps(calendar, cfg, directory, manifest, now):
    path = directory / "swap.csv"
    end = trading_date(now) - timedelta(days=1)
    existing = read_swap_schedule(path, cfg) if path.exists() else None
    start = trading_date(datetime.fromisoformat(manifest["created_at"])) - timedelta(days=1)
    if existing is not None and not existing.empty:
        start = existing.timestamp.iloc[-1].tz_convert("Asia/Tokyo").date()
    if start <= end:
        appended = calendar.history(cfg, start, end, now)
        schedule = validate_swap_schedule(
            pd.concat([existing, appended], ignore_index=True)
            if existing is not None
            else appended,
            cfg,
        )
        temporary = directory / "swap.pending.csv"
        schedule.to_csv(temporary, index=False)
        temporary.replace(path)
    elif existing is not None:
        schedule = existing
    else:
        raise ValueError("swap history unavailable")
    return schedule


def observe(directory: Path, api=None, calendar=None, clock=None) -> dict:
    """One bounded attempt, serialized by a separate DB transaction; errors persist.

    A hard interruption rolls back the attempt transaction; the missing observation is
    visible as staleness. The paper DB independently protects against duplicate fills.
    """
    clock = clock or (lambda: datetime.now(UTC))
    manifest = load_manifest(directory)
    cfg = Settings.model_validate(manifest["config"])
    log = directory / "observations.sqlite"
    with closing(sqlite3.connect(log, timeout=1, isolation_level=None)) as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            return {"status": "busy", "message": str(exc)}
        # This transaction doubles as the lock. A killed process rolls it back.
        start = clock()
        attempt = conn.execute(
            "INSERT INTO attempts(started_at,status,detail_json) VALUES (?, 'running', '{}')",
            (start.isoformat(),),
        ).lastrowid
        try:
            if (
                cfg.fingerprint != manifest["config_sha256"]
                or code_hashes() != manifest["core_sha256"]
            ):
                raise ValueError("observation spec/code changed; do not reuse this account")
            if runtime_versions() != manifest["runtime"]:
                raise ValueError("observation runtime changed; review before continuing")
            if start < datetime.fromisoformat(manifest["created_at"]):
                raise ValueError("observation time precedes account creation")
            with httpx.Client(follow_redirects=False) as client:
                api = api or GmoPublic(client)
                calendar = calendar or GmoSwapCalendar(client)
                api.validate_rules(cfg)
                # Fast closed-market check; fetch a fresh quote again after candle collection.
                quote = api.quote(cfg.symbol)
                if quote.status != "OPEN":
                    result = {"status": "market_closed", "quote": quote.model_dump(mode="json")}
                else:
                    frame = collect_bars(api, cfg, directory, start)
                    schedule = collect_swaps(calendar, cfg, directory, manifest, clock())
                    quote = api.quote(cfg.symbol)
                    now = clock()
                    # Save exact inputs even when freshness/spread checks reject the observation.
                    inputs = directory / "inputs"
                    inputs.mkdir(exist_ok=True)
                    digest = hashlib.sha256(frame.to_csv(index=False).encode()).hexdigest()
                    artifact = inputs / f"{digest}.parquet"
                    if not artifact.exists():
                        frame.to_parquet(artifact, index=False)
                    event = paper_step(frame, quote, cfg, directory / "paper.sqlite", now, schedule)
                    result = {
                        "status": "ok",
                        "action": event["action"],
                        "input_sha256": digest,
                        "event": event,
                    }
        except Exception as exc:
            # Record failures, including unexpected provider schema errors; never retry a fill
            # inside this attempt. BaseException (interrupt/termination) is not swallowed.
            result = {"status": "error", "error": str(exc), "error_type": type(exc).__name__}
        conn.execute(
            "UPDATE attempts SET finished_at=?,status=?,detail_json=? WHERE id=?",
            (clock().isoformat(), result["status"], json.dumps(result), attempt),
        )
        conn.execute("COMMIT")
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    init = sub.add_parser("init")
    init.add_argument("--config", type=Path, required=True)
    init.add_argument("--directory", type=Path, required=True)
    step = sub.add_parser("step")
    step.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "init":
        result = initialize(args.directory, load_settings(args.config))
    else:
        result = observe(args.directory)
    print(json.dumps(result, ensure_ascii=True, indent=2))
    return int(result.get("status") == "error")


if __name__ == "__main__":
    raise SystemExit(main())
