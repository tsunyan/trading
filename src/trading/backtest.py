import hashlib
import importlib.metadata
import json
from datetime import UTC, datetime
from pathlib import Path

import backtrader as bt
import pandas as pd

from trading.config import Settings
from trading.data import validate_bars
from trading.strategy import drawdown_halt, entry_units, wants_long

TRADE_COLUMNS = [
    "entry_timestamp",
    "exit_timestamp",
    "units",
    "entry_price",
    "exit_price",
    "gross_pnl_jpy",
    "commission_jpy",
    "net_pnl_jpy",
]


def completed_trades(fills: pd.DataFrame) -> pd.DataFrame:
    """Pair long-only fills with FIFO accounting to expose realized performance."""
    trades: list[dict] = []
    open_lots: list[dict] = []

    for fill in fills.itertuples(index=False):
        units = int(fill.filled_units)
        if units > 0:
            open_lots.append(
                {
                    "timestamp": fill.timestamp,
                    "units": units,
                    "price": float(fill.price),
                    "commission_per_unit": float(fill.commission) / units,
                }
            )
            continue

        remaining = -units
        exit_commission_per_unit = float(fill.commission) / remaining
        while remaining and open_lots:
            entry = open_lots[0]
            matched_units = min(remaining, entry["units"])
            commission = matched_units * (entry["commission_per_unit"] + exit_commission_per_unit)
            gross_pnl = matched_units * (float(fill.price) - entry["price"])
            trades.append(
                {
                    "entry_timestamp": entry["timestamp"],
                    "exit_timestamp": fill.timestamp,
                    "units": matched_units,
                    "entry_price": entry["price"],
                    "exit_price": float(fill.price),
                    "gross_pnl_jpy": gross_pnl,
                    "commission_jpy": commission,
                    "net_pnl_jpy": gross_pnl - commission,
                }
            )
            entry["units"] -= matched_units
            remaining -= matched_units
            if entry["units"] == 0:
                open_lots.pop(0)

        if remaining:
            raise ValueError("sell fill exceeds the open long position")

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
    """Long-only research harness: decide on a closed bar, fill at the next open."""

    params = (("cfg", None),)

    def __init__(self):
        self.cfg = self.p.cfg
        self.closes = []
        self.equity_rows = []
        self.order_rows = []
        self.pending = None
        self.peak = self.cfg.initial_cash
        self.halted = False

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
            }
        )
        if not order.alive():
            self.pending = None

    def next(self):
        """One decision per bar: at most one order in flight, and no re-entry once halted."""
        self.closes.append(float(self.data.close[0]))
        equity = self.broker.getvalue()
        self.peak = max(self.peak, equity)
        self.halted |= drawdown_halt(equity, self.peak, self.cfg)
        self.equity_rows.append(
            {
                "timestamp": self.timestamp(),
                "cash": self.broker.getcash(),
                "equity": equity,
                "units": self.position.size,
                "drawdown": 1 - equity / self.peak,
                "halted": self.halted,
            }
        )
        if self.pending:
            return
        want = wants_long(self.closes, self.cfg) and not self.halted
        if self.position and not want:
            self.pending = self.close()
        elif not self.position and want:
            estimate = self.data.close[0] + self.cfg.spread / 2 + self.cfg.slippage
            size = entry_units(self.broker.getcash(), equity, estimate, self.cfg)
            if size:
                self.pending = self.buy(size=size)


def run_backtest(frame: pd.DataFrame, cfg: Settings) -> tuple[dict, pd.DataFrame, pd.DataFrame]:
    """Return (report, equity, orders). Re-validates the frame; needs slow + 2 bars."""
    frame = validate_bars(frame, cfg)
    if len(frame) < cfg.slow + 2:
        raise ValueError("not enough bars for warm-up and next-bar execution")
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
    engine.addstrategy(ResearchStrategy, cfg=cfg)
    engine.broker.setcash(cfg.initial_cash)
    engine.broker.setcommission(commission=cfg.commission_rate, stocklike=True, percabs=True)
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
        ],
    )
    fills = orders[orders.status == "Completed"]
    trades = completed_trades(fills)
    final_equity = float(engine.broker.getvalue())
    report = {
        "mode": "backtest",
        "symbol": cfg.symbol,
        "initial_cash_jpy": cfg.initial_cash,
        "final_equity_jpy": final_equity,
        "return_pct": (final_equity / cfg.initial_cash - 1) * 100,
        "max_drawdown_pct": float(equity.drawdown.max() * 100),
        "fills": len(fills),
        "commission_jpy": float(fills.commission.sum()),
        "open_units": int(strategy.position.size),
        "pending_orders": len(engine.broker.get_orders_open()),
        "halted": bool(strategy.halted),
        "performance": performance_metrics(trades, equity),
        "limitations": [
            "Long-only, unlevered JPY accounting; no FX swap or dividends/corporate actions.",
            "Fixed spread/slippage assumptions; no order book, liquidity or price-limit model.",
            "Open positions marked to last close; final orders may be pending.",
            "Drawdown stop executes at next available open; not a guaranteed loss cap.",
        ],
    }
    return report, equity, orders


def save_run(frame: pd.DataFrame, cfg: Settings, directory: Path) -> dict:
    """Archive bars, equity, orders, fills and trades. Refuses to overwrite a run."""
    report, equity, orders = run_backtest(frame, cfg)
    trades = completed_trades(orders[orders.status == "Completed"])
    directory.mkdir(parents=True, exist_ok=False)
    frame.to_parquet(directory / "bars.parquet", index=False)
    equity.to_csv(directory / "equity.csv", index=False)
    orders.to_csv(directory / "orders.csv", index=False)
    orders[orders.status == "Completed"].to_csv(directory / "fills.csv", index=False)
    trades.to_csv(directory / "trades.csv", index=False)
    report["data_sha256"] = hashlib.sha256(frame.to_csv(index=False).encode()).hexdigest()
    report["config_sha256"] = cfg.fingerprint
    report["created_at"] = datetime.now(UTC).isoformat()
    report["versions"] = {
        name: importlib.metadata.version(name) for name in ("backtrader", "pandas", "trading-lab")
    }
    (directory / "config.json").write_text(cfg.model_dump_json(indent=2), encoding="utf-8")
    (directory / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report
