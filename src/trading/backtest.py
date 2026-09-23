import hashlib
import importlib.metadata
import json
import math
from datetime import UTC, datetime
from pathlib import Path

import backtrader as bt
import pandas as pd

from trading.config import Settings
from trading.data import validate_bars
from trading.provenance import reproducibility_fields
from trading.strategy import (
    drawdown_halt,
    entry_units,
    maintenance_margin_halt,
    margin_metrics,
    signal_direction,
)
from trading.swap import swap_credit_between, swap_fingerprint, validate_swap_schedule

TRADE_COLUMNS = [
    "entry_timestamp",
    "exit_timestamp",
    "side",
    "units",
    "entry_price",
    "exit_price",
    "gross_pnl_jpy",
    "commission_jpy",
    "net_pnl_jpy",
]


class SwapCommissionInfo(bt.CommInfoBase):
    """Backtrader commission model with timestamped long/short FX carry."""

    def __init__(self, cfg: Settings, schedule: pd.DataFrame | None):
        super().__init__()
        self.swap_schedule = schedule

    @staticmethod
    def _utc(value) -> pd.Timestamp:
        timestamp = pd.Timestamp(value)
        return (
            timestamp.tz_localize("UTC")
            if timestamp.tzinfo is None
            else timestamp.tz_convert("UTC")
        )

    def get_credit_interest(self, data, pos, dt):
        if not pos.size or pos.datetime is None:
            return 0.0
        credit = swap_credit_between(
            self.swap_schedule,
            self._utc(pos.datetime),
            self._utc(dt),
            int(pos.size),
        )
        return -credit


def completed_trades(fills: pd.DataFrame) -> pd.DataFrame:
    """Pair signed long and short fills using FIFO realized-PnL accounting."""
    trades: list[dict] = []
    open_lots: list[dict] = []

    for fill in fills.itertuples(index=False):
        raw_units = float(fill.filled_units)
        price = float(fill.price)
        commission = float(fill.commission)
        if not math.isfinite(raw_units) or not raw_units.is_integer() or raw_units == 0:
            raise ValueError("filled units must be a finite non-zero integer")
        if not math.isfinite(price) or price <= 0:
            raise ValueError("fill price must be finite and positive")
        if not math.isfinite(commission) or commission < 0:
            raise ValueError("fill commission must be finite and non-negative")
        remaining = int(raw_units)
        commission_per_unit = commission / abs(remaining)

        while remaining and open_lots and (remaining > 0) != (open_lots[0]["units"] > 0):
            entry = open_lots[0]
            side = 1 if entry["units"] > 0 else -1
            matched_units = min(abs(remaining), abs(entry["units"]))
            matched_commission = matched_units * (
                entry["commission_per_unit"] + commission_per_unit
            )
            gross_pnl = matched_units * (price - entry["price"]) * side
            trades.append(
                {
                    "entry_timestamp": entry["timestamp"],
                    "exit_timestamp": fill.timestamp,
                    "side": "long" if side > 0 else "short",
                    "units": matched_units,
                    "entry_price": entry["price"],
                    "exit_price": price,
                    "gross_pnl_jpy": gross_pnl,
                    "commission_jpy": matched_commission,
                    "net_pnl_jpy": gross_pnl - matched_commission,
                }
            )
            entry["units"] -= side * matched_units
            remaining += side * matched_units
            if entry["units"] == 0:
                open_lots.pop(0)

        if remaining:
            open_lots.append(
                {
                    "timestamp": fill.timestamp,
                    "units": remaining,
                    "price": price,
                    "commission_per_unit": commission_per_unit,
                }
            )

    return pd.DataFrame(trades, columns=TRADE_COLUMNS)


def performance_metrics(trades: pd.DataFrame, equity: pd.DataFrame) -> dict:
    """Return only realized-trade metrics; open positions remain in account equity."""
    closed_trades = len(trades)
    winners = trades[trades.net_pnl_jpy > 0]
    losers = trades[trades.net_pnl_jpy < 0]
    gross_profit = float(winners.net_pnl_jpy.sum())
    gross_loss = float(-losers.net_pnl_jpy.sum())
    return {
        "closed_trades": closed_trades,
        "winning_trades": len(winners),
        "losing_trades": len(losers),
        "win_rate_pct": (len(winners) / closed_trades * 100) if closed_trades else None,
        "gross_profit_jpy": gross_profit,
        "gross_loss_jpy": gross_loss,
        "net_realized_pnl_jpy": float(trades.net_pnl_jpy.sum()),
        "average_trade_pnl_jpy": (float(trades.net_pnl_jpy.mean()) if closed_trades else None),
        "profit_factor": gross_profit / gross_loss if gross_loss else None,
        "exposure_pct": float((equity.units != 0).mean() * 100) if len(equity) else 0.0,
    }


class ResearchStrategy(bt.Strategy):
    """Directional research harness: decide on a closed bar, fill at the next open."""

    params = (("cfg", None), ("active_start", None))

    def __init__(self):
        self.cfg = self.p.cfg
        self.active_start = self.p.active_start
        self.closes = []
        self.equity_rows = []
        self.order_rows = []
        self.pending = None
        self.pending_reason = None
        self.peak = self.cfg.initial_cash
        self.halted = False
        self.liquidation_reason = None

    def timestamp(self):
        """Current bar time as UTC ISO; the feed index is tz-naive UTC."""
        return self.data.datetime.datetime(0).replace(tzinfo=UTC).isoformat()

    def notify_order(self, order):
        """Record every status transition, not just fills, and release the pending slot."""
        self.order_rows.append(
            {
                "timestamp": self.timestamp(),
                "order_id": order.ref,
                "status": order.getstatusname(),
                "requested_units": order.created.size,
                "filled_units": order.executed.size,
                "price": order.executed.price,
                "commission": order.executed.comm,
                "reason": self.pending_reason,
            }
        )
        if not order.alive():
            self.pending = None
            self.pending_reason = None

    def submit(self, order, reason: str):
        self.pending = order
        self.pending_reason = reason

    def next(self):
        """One decision per bar: at most one order in flight, and no re-entry once halted."""
        self.closes.append(float(self.data.close[0]))
        current = self.data.datetime.datetime(0).replace(tzinfo=UTC)
        if self.active_start is not None and current < self.active_start:
            return
        equity = self.broker.getvalue()
        self.peak = max(self.peak, equity)
        margin_halt = maintenance_margin_halt(
            self.position.size, equity, float(self.data.close[0]), self.cfg
        )
        drawdown = drawdown_halt(equity, self.peak, self.cfg)
        if margin_halt:
            self.halted = True
            self.liquidation_reason = self.liquidation_reason or "maintenance_margin"
        elif drawdown:
            self.halted = True
            self.liquidation_reason = self.liquidation_reason or "drawdown"
        margin = margin_metrics(self.position.size, equity, float(self.data.close[0]), self.cfg)
        swap_pnl = -float(self.broker.d_credit.get(self.data, 0.0))
        self.equity_rows.append(
            {
                "timestamp": self.timestamp(),
                "cash": self.broker.getcash(),
                "equity": equity,
                "units": self.position.size,
                "drawdown": 1 - equity / self.peak,
                "halted": self.halted,
                "swap_pnl": swap_pnl,
                **margin,
                "liquidation_reason": self.liquidation_reason,
            }
        )
        if self.pending:
            return
        target = signal_direction(self.closes, self.cfg) if not self.halted else 0
        current = 1 if self.position.size > 0 else -1 if self.position.size < 0 else 0
        if self.position and current != target:
            reason = self.liquidation_reason if self.halted else "signal_exit"
            self.submit(self.close(), reason)
        elif not self.position and target:
            adverse_cost = self.cfg.spread / 2 + self.cfg.slippage
            estimate = self.data.close[0] + adverse_cost * target
            size = entry_units(self.broker.getcash(), equity, estimate, self.cfg)
            if size:
                order = self.buy(size=size) if target > 0 else self.sell(size=size)
                self.submit(order, "signal_entry")


def run_backtest(
    frame: pd.DataFrame,
    cfg: Settings,
    *,
    active_start: datetime | pd.Timestamp | None = None,
    swap_schedule: pd.DataFrame | None = None,
) -> tuple[dict, pd.DataFrame, pd.DataFrame]:
    """Return (report, equity, orders). Re-validates the frame and strategy warm-up."""
    frame = validate_bars(frame, cfg)
    if swap_schedule is not None:
        swap_schedule = validate_swap_schedule(swap_schedule, cfg)
    if len(frame) < cfg.warmup_bars + 2:
        raise ValueError("not enough bars for warm-up and next-bar execution")
    active_start_utc = None
    if active_start is not None:
        active_start_utc = pd.Timestamp(active_start)
        if active_start_utc.tzinfo is None:
            raise ValueError("active_start must have an explicit timezone offset")
        active_start_utc = active_start_utc.tz_convert("UTC")
        matches = frame.index[frame.timestamp == active_start_utc].tolist()
        if len(matches) != 1 or matches[0] < cfg.warmup_bars:
            raise ValueError("active_start must match a bar after sufficient warm-up")
    feed = frame.set_index("timestamp")[["open", "high", "low", "close", "volume"]].copy()
    feed.index = feed.index.tz_convert("UTC").tz_localize(None)
    engine = bt.Cerebro(stdstats=False)
    engine.adddata(
        bt.feeds.PandasData(
            dataname=feed,
            timeframe=bt.TimeFrame.Minutes if cfg.market == "fx" else bt.TimeFrame.Days,
            compression=60 if cfg.market == "fx" else 1,
        )
    )
    engine.addstrategy(
        ResearchStrategy,
        cfg=cfg,
        active_start=active_start_utc.to_pydatetime() if active_start_utc is not None else None,
    )
    engine.broker.setcash(cfg.initial_cash)
    # Keep carry separate from execution commission in orders and completed trades.
    engine.broker.set_int2pnl(False)
    engine.broker.addcommissioninfo(
        SwapCommissionInfo(
            cfg,
            swap_schedule,
            commission=cfg.commission_rate,
            stocklike=True,
            percabs=True,
            leverage=cfg.max_leverage,
            interest=0.0,
            interest_long=True,
        )
    )
    # Allow adverse costs beyond the candle envelope rather than silently clipping them.
    engine.broker.set_slippage_fixed(
        cfg.spread / 2 + cfg.slippage,
        slip_open=True,
        slip_match=True,
        slip_out=True,
    )
    strategy = engine.run()[0]
    equity = pd.DataFrame(strategy.equity_rows)
    orders = pd.DataFrame(
        strategy.order_rows,
        columns=[
            "timestamp",
            "order_id",
            "status",
            "requested_units",
            "filled_units",
            "price",
            "commission",
            "reason",
        ],
    )
    fills = orders[orders.status == "Completed"]
    trades = completed_trades(fills)
    final_equity = float(engine.broker.getvalue())
    report = {
        "mode": "backtest",
        "symbol": cfg.symbol,
        "strategy": cfg.strategy,
        "strategy_parameters": cfg.strategy_parameters,
        "initial_cash_jpy": cfg.initial_cash,
        "final_equity_jpy": final_equity,
        "return_pct": (final_equity / cfg.initial_cash - 1) * 100,
        "max_drawdown_pct": float(equity.drawdown.max() * 100),
        "fills": len(fills),
        "commission_jpy": float(fills.commission.sum()),
        "swap_pnl_jpy": -float(engine.broker.d_credit.get(strategy.data, 0.0)),
        "open_units": int(strategy.position.size),
        "pending_orders": len(engine.broker.get_orders_open()),
        "halted": bool(strategy.halted),
        "liquidation_reason": strategy.liquidation_reason,
        "performance": performance_metrics(trades, equity),
        "limitations": [
            "JPY accounting supports supplied FX swap history; stock borrow fees are absent.",
            "Fixed spread/slippage assumptions; no order book, liquidity or price-limit model.",
            "Open positions marked to last close; final orders may be pending.",
            "Risk liquidation executes at the next open; it is not a guaranteed loss cap.",
        ],
    }
    if active_start_utc is not None:
        report["active_start"] = active_start_utc.isoformat()
    return report, equity, orders


def save_run(
    frame: pd.DataFrame,
    cfg: Settings,
    directory: Path,
    *,
    swap_schedule: pd.DataFrame | None = None,
) -> dict:
    """Archive bars, equity, orders, fills and trades. Refuses to overwrite a run."""
    report, equity, orders = run_backtest(frame, cfg, swap_schedule=swap_schedule)
    trades = completed_trades(orders[orders.status == "Completed"])
    directory.mkdir(parents=True, exist_ok=False)
    frame.to_parquet(directory / "bars.parquet", index=False)
    equity.to_csv(directory / "equity.csv", index=False)
    orders.to_csv(directory / "orders.csv", index=False)
    orders[orders.status == "Completed"].to_csv(directory / "fills.csv", index=False)
    trades.to_csv(directory / "trades.csv", index=False)
    if swap_schedule is not None:
        swap_schedule = validate_swap_schedule(swap_schedule, cfg)
        swap_schedule.to_csv(directory / "swap.csv", index=False)
        report["swap_sha256"] = swap_fingerprint(swap_schedule)
    report["data_sha256"] = hashlib.sha256(frame.to_csv(index=False).encode()).hexdigest()
    report["config_sha256"] = cfg.fingerprint
    report["created_at"] = datetime.now(UTC).isoformat()
    report["versions"] = {
        name: importlib.metadata.version(name) for name in ("backtrader", "pandas", "trading-lab")
    }
    report.update(
        reproducibility_fields(
            cfg,
            report["data_sha256"],
            report.get("swap_sha256"),
        )
    )
    (directory / "config.json").write_text(cfg.model_dump_json(indent=2), encoding="utf-8")
    (directory / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report
