import pandas as pd
import pytest

from trading.backtest import completed_trades, run_backtest, save_run
from trading.data import validate_bars
from trading.strategy import entry_units


def test_next_open_costs_and_cash_accounting(bars, cfg):
    report, equity, orders = run_backtest(bars, cfg)
    fills = orders[orders.status == "Completed"]
    assert fills.filled_units.tolist() == [1000, -1000]
    assert fills.price.tolist() == pytest.approx([154.02, 148.98])
    assert fills.timestamp.iloc[0].startswith("2025-01-06T03:00")
    expected_fee = 1000 * (154.02 + 148.98) * cfg.commission_rate
    assert report["commission_jpy"] == pytest.approx(expected_fee)
    assert report["final_equity_jpy"] == pytest.approx(
        cfg.initial_cash + 1000 * (148.98 - 154.02) - expected_fee,
    )
    assert report["open_units"] == 0
    assert equity.iloc[0].equity == cfg.initial_cash
    assert report["performance"] == {
        "closed_trades": 1,
        "winning_trades": 0,
        "losing_trades": 1,
        "win_rate_pct": 0.0,
        "gross_profit_jpy": 0.0,
        "gross_loss_jpy": pytest.approx(5343.0),
        "net_realized_pnl_jpy": pytest.approx(-5343.0),
        "average_trade_pnl_jpy": pytest.approx(-5343.0),
        "profit_factor": 0.0,
        "exposure_pct": pytest.approx(37.5),
    }


def test_future_prices_do_not_change_past_orders(bars, cfg):
    changed = bars.copy()
    for col in ["open", "high", "low", "close"]:
        changed.loc[6:, col] *= 2
    _, equity1, orders1 = run_backtest(bars, cfg)
    _, equity2, orders2 = run_backtest(changed, cfg)
    assert equity1.iloc[:6].equals(equity2.iloc[:6])
    fills1 = orders1[orders1.status == "Completed"].iloc[0]
    fills2 = orders2[orders2.status == "Completed"].iloc[0]
    assert fills1.price == fills2.price
    assert fills1.timestamp == fills2.timestamp


def test_drawdown_halts_future_entries(bars, cfg):
    cfg = cfg.model_copy(update={"max_drawdown": 0.001})
    report, _, _ = run_backtest(bars, cfg)
    assert report["halted"]
    assert report["open_units"] == 0


def test_short_fifo_accounts_for_profitable_and_losing_trades():
    fills = pd.DataFrame(
        [
            {"timestamp": "t1", "filled_units": -100, "price": 150, "commission": 10},
            {"timestamp": "t2", "filled_units": 40, "price": 145, "commission": 4},
            {"timestamp": "t3", "filled_units": 60, "price": 155, "commission": 6},
        ]
    )

    trades = completed_trades(fills)

    assert trades.side.tolist() == ["short", "short"]
    assert trades.units.tolist() == [40, 60]
    assert trades.gross_pnl_jpy.tolist() == pytest.approx([200, -300])
    assert trades.net_pnl_jpy.tolist() == pytest.approx([192, -312])


def test_backtest_enters_short_when_enabled(bars, cfg):
    cfg = cfg.model_copy(update={"allow_short": True})
    descending = bars.copy()
    descending.loc[:3, "open"] = [154.0, 153.0, 152.0, 150.0]
    descending.loc[:3, "close"] = [154.0, 153.0, 152.0, 150.0]
    descending.loc[:3, "high"] = [155.0, 154.0, 153.0, 151.0]
    descending.loc[:3, "low"] = [153.0, 152.0, 151.0, 149.0]

    _, _, orders = run_backtest(descending, cfg)

    fills = orders[orders.status == "Completed"]
    assert fills.filled_units.iloc[0] < 0
    assert fills.price.iloc[0] == pytest.approx(149.98)


def test_leverage_increases_sizing_without_scaling_pnl_afterward(cfg):
    unlevered_cfg = cfg.model_copy(update={"max_units": 10_000})
    unlevered = entry_units(1_000_000, 1_000_000, 150, unlevered_cfg)
    leveraged_cfg = unlevered_cfg.model_copy(update={"max_leverage": 2})
    leveraged = entry_units(1_000_000, 1_000_000, 150, leveraged_cfg)

    assert leveraged > unlevered
    assert leveraged <= unlevered * 2


def test_margin_breach_halts_and_liquidates_at_next_open(bars, cfg):
    cfg = cfg.model_copy(
        update={
            "allocation": 1,
            "max_units": 100_000,
            "max_leverage": 10,
            "maintenance_margin_ratio": 0.9,
            "max_drawdown": 0.9,
        }
    )

    report, equity, orders = run_backtest(bars, cfg)

    assert report["halted"]
    assert report["liquidation_reason"] == "maintenance_margin"
    assert report["open_units"] == 0
    assert (equity.effective_leverage > cfg.max_leverage).any()
    fills = orders[orders.status == "Completed"]
    assert fills.reason.iloc[-1] == "maintenance_margin"


def test_equity_lots(bars, cfg):
    cfg = cfg.model_copy(
        update={
            "market": "jp_equity",
            "bar_seconds": 86400,
            "lot_size": 100,
            "max_units": 1550,
            "symbol": "7203",
        }
    )
    bars = bars.copy()
    bars["symbol"] = cfg.symbol
    bars["timestamp"] = bars.timestamp.iloc[0] + (bars.timestamp - bars.timestamp.iloc[0]) * 24
    _, _, orders = run_backtest(bars, cfg)
    assert (orders.requested_units % 100 == 0).all()


@pytest.mark.parametrize("fault", ["duplicate", "unsorted", "nan", "ohlc", "symbol", "naive"])
def test_invalid_data_rejected(bars, cfg, fault):
    if fault == "duplicate":
        bars.loc[1, "timestamp"] = bars.timestamp.iloc[0]
    elif fault == "unsorted":
        bars = bars.iloc[::-1]
    elif fault == "nan":
        bars.loc[0, "close"] = float("nan")
    elif fault == "ohlc":
        bars.loc[0, "low"] = 1000
    elif fault == "symbol":
        bars.loc[0, "symbol"] = "EUR_USD"
    else:
        bars["timestamp"] = bars.timestamp.dt.tz_localize(None)
    with pytest.raises(ValueError):
        validate_bars(bars, cfg)


def test_report_does_not_overwrite_existing_run(bars, cfg, tmp_path):
    path = tmp_path / "result"
    report = save_run(bars, cfg, path)
    assert len(report["data_sha256"]) == 64
    assert (path / "fills.csv").exists()
    assert (path / "trades.csv").exists()
    with pytest.raises(FileExistsError):
        save_run(bars, cfg, path)
