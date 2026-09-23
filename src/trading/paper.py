import hashlib
import json
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from trading.config import Settings
from trading.data import validate_bars
from trading.gmo import Quote
from trading.strategy import (
    drawdown_halt,
    entry_units,
    maintenance_margin_halt,
    margin_metrics,
    signal_direction,
)
from trading.swap import (
    swap_credit_between,
    swap_fingerprint,
    validate_swap_schedule,
)

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


def _mark_price(units: int, quote: Quote) -> float:
    return quote.bid if units >= 0 else quote.ask


def _account_fingerprint(cfg: Settings, swap_schedule: pd.DataFrame | None) -> str:
    schedule_hash = swap_fingerprint(swap_schedule)
    if schedule_hash is None:
        return cfg.fingerprint
    return hashlib.sha256(f"{cfg.fingerprint}:{schedule_hash}".encode()).hexdigest()


def paper_step(
    frame: pd.DataFrame,
    quote: Quote,
    cfg: Settings,
    database: Path,
    now: datetime | None = None,
    swap_schedule: pd.DataFrame | None = None,
) -> dict:
    """One forward observation; fills are local, at the freshly observed quote."""
    if cfg.market != "fx":
        raise ValueError("forward paper trading currently supports FX only")
    now = now or datetime.now(UTC)
    if now.tzinfo is None:
        raise ValueError("now must have a timezone")
    if quote.symbol != cfg.symbol or quote.status != "OPEN":
        raise ValueError("symbol mismatch or market not OPEN")
    if swap_schedule is not None:
        swap_schedule = validate_swap_schedule(swap_schedule, cfg)
    account_fingerprint = _account_fingerprint(cfg, swap_schedule)
    age = (now - quote.timestamp).total_seconds()
    if not -cfg.max_future_quote_seconds <= age <= cfg.max_quote_age_seconds:
        raise ValueError("stale or future quote")
    frame = validate_bars(frame, cfg)
    completion_boundary = min(pd.Timestamp(now), pd.Timestamp(quote.timestamp))
    close_times = frame.timestamp + pd.Timedelta(seconds=cfg.bar_seconds)
    completed = frame.loc[close_times <= completion_boundary]
    if len(completed) < cfg.warmup_bars:
        raise ValueError("not enough completed bars for paper signal")
    signal_time = completed.timestamp.iloc[-1] + pd.Timedelta(seconds=cfg.bar_seconds)
    latest_observation = max(pd.Timestamp(now), pd.Timestamp(quote.timestamp))
    if (latest_observation - signal_time).total_seconds() > cfg.max_signal_age_seconds:
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
            accepted_fingerprints = {account_fingerprint}
            if swap_schedule is None:
                accepted_fingerprints.update(cfg.paper_tolerance_legacy_fingerprints)
            if row and row[0] not in accepted_fingerprints:
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
                    "swap_pnl": 0.0,
                    "last_swap_check": None,
                    "position_opened_at": None,
                    "liquidation_reason": None,
                }
            )
            state.setdefault("liquidation_reason", None)
            state.setdefault("swap_pnl", 0.0)
            state.setdefault("last_swap_check", None)
            state.setdefault("position_opened_at", None)
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
            swap_credit = 0.0
            if state["units"] and swap_schedule is not None:
                start_values = [
                    pd.Timestamp(value)
                    for value in (state["last_swap_check"], state["position_opened_at"])
                    if value is not None
                ]
                swap_start = max(start_values) if start_values else completion_boundary
                swap_credit = swap_credit_between(
                    swap_schedule,
                    swap_start,
                    completion_boundary,
                    state["units"],
                )
                state["cash"] += swap_credit
                state["swap_pnl"] += swap_credit
            mark_price = _mark_price(state["units"], quote)
            equity = state["cash"] + state["units"] * mark_price
            state["peak"] = max(state["peak"], equity)
            if maintenance_margin_halt(state["units"], equity, mark_price, cfg):
                state["halted"] = True
                state["liquidation_reason"] = state["liquidation_reason"] or "maintenance_margin"
            elif drawdown_halt(equity, state["peak"], cfg):
                state["halted"] = True
                state["liquidation_reason"] = state["liquidation_reason"] or "drawdown"
            target = signal_direction(completed.close.tolist(), cfg) if not state["halted"] else 0
            current = 1 if state["units"] > 0 else -1 if state["units"] < 0 else 0
            is_new = state["last_signal"] != signal_id
            units, price, fee = 0, 0.0, 0.0
            action = "hold" if is_new else "same_signal"
            if state["units"] and (state["halted"] or (is_new and current != target)):
                price = quote.bid - cfg.slippage if state["units"] > 0 else quote.ask + cfg.slippage
                if price <= 0:
                    raise ValueError("slippage exceeds quote price")
                units = -state["units"]
                action = "sell" if units < 0 else "buy_to_cover"
            elif is_new and target and not state["units"]:
                if quote.ask - quote.bid > cfg.max_spread:
                    raise ValueError("spread exceeds entry limit")
                price = quote.ask + cfg.slippage if target > 0 else quote.bid - cfg.slippage
                if price <= 0:
                    raise ValueError("slippage exceeds quote price")
                size = entry_units(state["cash"], equity, price, cfg)
                units = size * target
                if units > 0:
                    action = "buy"
                elif units < 0:
                    action = "sell_short"
                else:
                    action = "quantity_below_minimum"
            if units:
                fee = abs(units) * price * cfg.commission_rate
                state["cash"] -= units * price + fee
                state["units"] += units
                state["commission"] += fee
                state["position_opened_at"] = (
                    quote.timestamp.isoformat() if state["units"] else None
                )
            state["last_signal"] = signal_id
            state["last_quote"] = quote.timestamp.isoformat()
            state["last_swap_check"] = completion_boundary.isoformat()
            mark_price = _mark_price(state["units"], quote)
            state["equity"] = state["cash"] + state["units"] * mark_price
            state["peak"] = max(state["peak"], state["equity"])
            if maintenance_margin_halt(state["units"], state["equity"], mark_price, cfg):
                state["halted"] = True
                state["liquidation_reason"] = state["liquidation_reason"] or "maintenance_margin"
            elif drawdown_halt(state["equity"], state["peak"], cfg):
                state["halted"] = True
                state["liquidation_reason"] = state["liquidation_reason"] or "drawdown"
            state.update(margin_metrics(state["units"], state["equity"], mark_price, cfg))
            event = {
                "mode": "paper",
                "symbol": cfg.symbol,
                "strategy": cfg.strategy,
                "action": action,
                "signal_time": signal_id,
                "observed_at": now.isoformat(),
                "quote": quote.model_dump(mode="json"),
                "filled_units": units,
                "fill_price": price if units else None,
                "commission_jpy": fee,
                "swap_credit_jpy": swap_credit,
                "state": state,
            }
            connection.execute(
                "INSERT INTO account VALUES (1, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET "
                "config_hash = excluded.config_hash, state_json = excluded.state_json",
                (account_fingerprint, json.dumps(state)),
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
    """Read-only snapshot. Requires an existing database; state is None until a step writes one."""
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
