import json
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from trading.config import Settings
from trading.data import validate_bars
from trading.gmo import Quote
from trading.strategy import drawdown_halt, entry_units, wants_long

SCHEMA = """
CREATE TABLE IF NOT EXISTS account (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    config_hash TEXT NOT NULL,
    state_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    observed_at TEXT NOT NULL,
    payload_json TEXT NOT NULL
);
"""


def paper_step(
    frame: pd.DataFrame,
    quote: Quote,
    cfg: Settings,
    database: Path,
    now: datetime | None = None,
) -> dict:
    """One forward observation; fills are local, at the freshly observed quote."""
    if cfg.market != "fx":
        raise ValueError("forward paper trading currently supports FX only")
    now = now or datetime.now(UTC)
    if now.tzinfo is None:
        raise ValueError("now must have a timezone")
    if quote.symbol != cfg.symbol or quote.status != "OPEN":
        raise ValueError("symbol mismatch or market not OPEN")
    age = (now - quote.timestamp).total_seconds()
    if not -cfg.max_future_quote_seconds <= age <= cfg.max_quote_age_seconds:
        raise ValueError("stale or future quote")
    frame = validate_bars(frame, cfg)
    close_times = frame.timestamp + pd.Timedelta(seconds=cfg.bar_seconds)
    completed = frame.loc[close_times <= pd.Timestamp(quote.timestamp)]
    if len(completed) < cfg.slow:
        raise ValueError("not enough completed bars for paper signal")
    signal_time = completed.timestamp.iloc[-1] + pd.Timedelta(seconds=cfg.bar_seconds)
    if (pd.Timestamp(quote.timestamp) - signal_time).total_seconds() > cfg.max_signal_age_seconds:
        raise ValueError("stale signal data")
    signal_id = signal_time.isoformat()
    database.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(database, timeout=10, isolation_level=None)) as connection:
        connection.executescript(SCHEMA)
        connection.execute("BEGIN IMMEDIATE")
        try:
            row = connection.execute(
                "SELECT config_hash, state_json FROM account WHERE id = 1",
            ).fetchone()
            if row and row[0] not in {
                cfg.fingerprint,
                *cfg.paper_tolerance_legacy_fingerprints,
            }:
                raise ValueError("paper database belongs to a different config; use a new database")
            state = (
                json.loads(row[1])
                if row
                else {
                    "cash": cfg.initial_cash,
                    "units": 0,
                    "peak": cfg.initial_cash,
                    "halted": False,
                    "last_signal": None,
                    "last_quote": None,
                    "commission": 0.0,
                }
            )
            if state["last_signal"] and pd.Timestamp(signal_id) < pd.Timestamp(
                state["last_signal"]
            ):
                raise ValueError("signal history moved backwards")
            if state["last_quote"]:
                previous = datetime.fromisoformat(state["last_quote"])
                if quote.timestamp < previous:
                    raise ValueError("quote time moved backwards")
                if quote.timestamp == previous:
                    connection.execute("ROLLBACK")
                    return {"mode": "paper", "action": "duplicate_quote", "state": state}
            equity = state["cash"] + state["units"] * quote.bid
            state["peak"] = max(state["peak"], equity)
            state["halted"] |= drawdown_halt(equity, state["peak"], cfg)
            want = wants_long(completed.close.tolist(), cfg) and not state["halted"]
            is_new = state["last_signal"] != signal_id
            units, price, fee = 0, 0.0, 0.0
            action = "hold" if is_new else "same_signal"
            if state["units"] and (state["halted"] or (is_new and not want)):
                price = quote.bid - cfg.slippage
                if price <= 0:
                    raise ValueError("slippage exceeds quote price")
                units = -state["units"]
                action = "sell"
            elif is_new and want and not state["units"]:
                if quote.ask - quote.bid > cfg.max_spread:
                    raise ValueError("spread exceeds entry limit")
                price = quote.ask + cfg.slippage
                units = entry_units(state["cash"], equity, price, cfg)
                action = "buy" if units else "quantity_below_minimum"
            if units:
                fee = abs(units) * price * cfg.commission_rate
                state["cash"] -= units * price + fee
                state["units"] += units
                state["commission"] += fee
            state["last_signal"] = signal_id
            state["last_quote"] = quote.timestamp.isoformat()
            state["equity"] = state["cash"] + state["units"] * quote.bid
            state["peak"] = max(state["peak"], state["equity"])
            state["halted"] |= drawdown_halt(state["equity"], state["peak"], cfg)
            event = {
                "mode": "paper",
                "symbol": cfg.symbol,
                "action": action,
                "signal_time": signal_id,
                "observed_at": now.isoformat(),
                "quote": quote.model_dump(mode="json"),
                "filled_units": units,
                "fill_price": price if units else None,
                "commission_jpy": fee,
                "state": state,
            }
            connection.execute(
                "INSERT INTO account VALUES (1, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET "
                "config_hash = excluded.config_hash, state_json = excluded.state_json",
                (cfg.fingerprint, json.dumps(state)),
            )
            connection.execute(
                "INSERT INTO events(observed_at, payload_json) VALUES (?, ?)",
                (now.isoformat(), json.dumps(event)),
            )
            connection.execute("COMMIT")
            return event
        except Exception:
            connection.execute("ROLLBACK")
            raise


def paper_status(database: Path) -> dict:
    """Read-only snapshot; state is None when the database holds no account yet."""
    with closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)) as conn:
        row = conn.execute("SELECT config_hash, state_json FROM account WHERE id = 1").fetchone()
        if not row:
            return {"mode": "paper", "state": None, "events": 0}
        return {
            "mode": "paper",
            "config_sha256": row[0],
            "state": json.loads(row[1]),
            "events": conn.execute("SELECT count(*) FROM events").fetchone()[0],
        }
