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


class ResearchStrategy(bt.Strategy):
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
        return self.data.datetime.datetime(0).replace(tzinfo=UTC).isoformat()

    def notify_order(self, order):
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
        "limitations": [
            "Long-only, unlevered JPY accounting; no FX swap or dividends/corporate actions.",
            "Fixed spread/slippage assumptions; no order book, liquidity or price-limit model.",
            "Open positions marked to last close; final orders may be pending.",
            "Drawdown stop executes at next available open; not a guaranteed loss cap.",
        ],
    }
    return report, equity, orders


def save_run(frame: pd.DataFrame, cfg: Settings, directory: Path) -> dict:
    report, equity, orders = run_backtest(frame, cfg)
    directory.mkdir(parents=True, exist_ok=False)
    frame.to_parquet(directory / "bars.parquet", index=False)
    equity.to_csv(directory / "equity.csv", index=False)
    orders.to_csv(directory / "orders.csv", index=False)
    orders[orders.status == "Completed"].to_csv(directory / "fills.csv", index=False)
    report["data_sha256"] = hashlib.sha256(frame.to_csv(index=False).encode()).hexdigest()
    report["config_sha256"] = cfg.fingerprint
    report["created_at"] = datetime.now(UTC).isoformat()
    report["versions"] = {
        name: importlib.metadata.version(name) for name in ("backtrader", "pandas", "trading-lab")
    }
    (directory / "config.json").write_text(cfg.model_dump_json(indent=2), encoding="utf-8")
    (directory / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report
