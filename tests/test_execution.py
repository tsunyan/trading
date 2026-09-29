import numpy as np
import pandas as pd
import pytest

from trading.backtest import run_backtest, save_run
from trading.evaluation import buy_and_hold_benchmark, evaluate_strategy, save_evaluation
from trading.execution import ExecutionModel, execution_prices
from trading.strategy import entry_units

OBSERVED = ExecutionModel(mode="bid_ask")


def test_fixed_mode_rejects_ineffective_multiplier():
    with pytest.raises(ValueError, match="fixed execution"):
        ExecutionModel(observed_spread_multiplier=2)
    assert ExecutionModel().stressed(2).observed_spread_multiplier == 1
    assert OBSERVED.stressed(2).observed_spread_multiplier == 2


@pytest.mark.parametrize("execution", [ExecutionModel(), OBSERVED])
def test_benchmark_normalizes_index_and_rejects_unsorted_bars(bars, cfg, execution):
    frame = with_sides(bars)
    start = frame.timestamp.iloc[2]
    expected = buy_and_hold_benchmark(frame, cfg, start, execution=execution)
    frame.index = range(100, 100 + len(frame))
    assert buy_and_hold_benchmark(frame, cfg, start, execution=execution) == expected
    with pytest.raises(ValueError, match="strictly increasing"):
        buy_and_hold_benchmark(frame.iloc[::-1], cfg, start, execution=execution)


@pytest.mark.parametrize("short", [False, True])
def test_fixed_fills_use_precomputed_side_prices(cfg, short):
    cfg = cfg.model_copy(update={"allow_short": short, "max_drawdown": 0.9, "max_leverage": 2})
    frame = trending(cfg, short)
    report, equity, orders = run_backtest(frame, cfg)
    executed = fills(orders)
    size = executed.filled_units.iloc[0]
    prices = execution_prices(frame, cfg, ExecutionModel())
    entry = prices.sell_open.iloc[3] if short else prices.buy_open.iloc[3]
    assert executed.price.iloc[0] == pytest.approx(entry)
    assert (size < 0) == short
    marked = cfg.initial_cash + size * (frame.close.iloc[-1] - entry)
    marked -= abs(size) * entry * cfg.commission_rate
    assert report["final_equity_jpy"] == pytest.approx(marked)
    assert equity.equity.iloc[-1] == pytest.approx(marked)


def with_sides(frame, spreads=0.2):
    result = frame.copy()
    for field in ("open", "high", "low", "close"):
        result[f"bid_{field}"] = result[field] - np.asarray(spreads) / 2
        result[f"ask_{field}"] = result[field] + np.asarray(spreads) / 2
    return result


def trending(cfg, short=False, count=12):
    prices = 100.0 + np.arange(count) * (-1 if short else 1)
    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-06", periods=count, freq="h", tz="UTC"),
            "symbol": cfg.symbol,
            "open": prices,
            "high": prices + 0.5,
            "low": prices - 0.5,
            "close": prices,
            "volume": 0,
        }
    )


def fills(orders):
    return orders.loc[orders.status == "Completed"].reset_index(drop=True)


def test_bid_ask_requires_complete_quotes_and_valid_prices(bars, cfg):
    with pytest.raises(ValueError, match="requires complete"):
        run_backtest(bars, cfg, execution=OBSERVED)
    with pytest.raises(ValueError, match="non-positive"):
        run_backtest(with_sides(bars), cfg.model_copy(update={"slippage": 200}), execution=OBSERVED)
    for value in (0, -1, float("nan"), float("inf")):
        with pytest.raises(ValueError):
            ExecutionModel(mode="bid_ask", observed_spread_multiplier=value)


def test_spread_floor_stress_and_no_double_charge(bars, cfg):
    frame = with_sides(bars, [0.004, 0.4, 0.004, 0.4, 0.004, 0.4, 0.004, 0.4])
    prices = execution_prices(frame, cfg, OBSERVED)
    assert prices.buy_open.iloc[0] == pytest.approx(150 + cfg.spread / 2 + cfg.slippage)
    assert prices.sell_open.iloc[1] == pytest.approx(frame.bid_open.iloc[1] - cfg.slippage)
    stress_cfg = cfg.model_copy(update={"spread": cfg.spread * 2, "slippage": cfg.slippage * 2})
    stressed = execution_prices(frame, stress_cfg, OBSERVED.stressed(2))
    assert stressed.buy_open.iloc[1] == pytest.approx(151 + 0.4 + cfg.slippage * 2)
    assert stressed.buy_open.iloc[0] == pytest.approx(150 + cfg.spread + cfg.slippage * 2)


def test_observed_round_trip_prices_fees_and_state(bars, cfg):
    frame = with_sides(bars)
    report, equity, orders = run_backtest(frame, cfg, execution=OBSERVED)
    executed = fills(orders)
    assert executed.filled_units.tolist() == [1000, -1000]
    assert executed.price.tolist() == pytest.approx([154.11, 148.89])
    fees = 1000 * (154.11 + 148.89) * cfg.commission_rate
    expected = cfg.initial_cash + 1000 * (148.89 - 154.11) - fees
    assert report["commission_jpy"] == pytest.approx(fees)
    assert report["final_equity_jpy"] == pytest.approx(expected)
    assert report["liquidation_equity_jpy"] == pytest.approx(expected)
    assert equity.equity.iloc[3] == pytest.approx(
        cfg.initial_cash + 1000 * (153.9 - 154.11) - 154.11 * 1000 * cfg.commission_rate
    )


@pytest.mark.parametrize("short", [False, True])
def test_observed_marks_and_final_exit_reconcile_for_both_sides(cfg, short):
    cfg = cfg.model_copy(update={"allow_short": short, "max_drawdown": 0.9, "max_leverage": 2})
    frame = with_sides(trending(cfg, short), np.linspace(0.1, 0.6, 12))
    report, equity, orders = run_backtest(frame, cfg, execution=OBSERVED)
    executed = fills(orders)
    assert len(executed) == 1
    size = executed.filled_units.iloc[0]
    assert (size < 0) == short
    entry = (
        frame.bid_open.iloc[3] - cfg.slippage if short else frame.ask_open.iloc[3] + cfg.slippage
    )
    assert executed.price.iloc[0] == pytest.approx(entry)
    mark = frame.ask_close.iloc[-1] if short else frame.bid_close.iloc[-1]
    exit_price = mark + cfg.slippage if short else mark - cfg.slippage
    marked = cfg.initial_cash + size * (mark - entry) - abs(size) * entry * cfg.commission_rate
    liquidated = marked + size * (exit_price - mark) - abs(size) * exit_price * cfg.commission_rate
    assert report["final_equity_jpy"] == pytest.approx(marked)
    assert equity.equity.iloc[-1] == pytest.approx(marked)
    assert report["liquidation_equity_jpy"] == pytest.approx(liquidated)


def test_decision_side_price_sizes_order_without_next_bar_lookahead(bars, cfg):
    cfg = cfg.model_copy(update={"max_units": 10000, "max_drawdown": 0.9})
    frame = with_sides(bars, [0.2, 0.2, 20, 0.2, 0.2, 0.2, 0.2, 0.2])
    _, _, orders = run_backtest(frame, cfg, execution=OBSERVED)
    expected = entry_units(cfg.initial_cash, cfg.initial_cash, 162 + cfg.slippage, cfg)
    assert fills(orders).filled_units.iloc[0] == expected
    changed = with_sides(bars, [0.2, 0.2, 20, 10, 0.2, 0.2, 0.2, 0.2])
    _, _, changed_orders = run_backtest(changed, cfg, execution=OBSERVED)
    assert fills(changed_orders).filled_units.iloc[0] == expected
    assert fills(changed_orders).price.iloc[0] > fills(orders).price.iloc[0]


def test_observed_next_open_cash_rejection(bars, cfg):
    cfg = cfg.model_copy(
        update={
            "initial_cash": 1000,
            "allocation": 1,
            "min_units": 1,
            "commission_rate": 0,
            "slippage": 0,
            "spread": 0,
        }
    )
    frame = with_sides(bars, [0, 0, 0, 100, 0, 0, 0, 0])
    _, equity, orders = run_backtest(frame, cfg, execution=OBSERVED)
    first = orders[orders.order_id == orders.order_id.iloc[0]]
    assert "Margin" in first.status.to_list()
    assert "Completed" not in first.status.to_list()
    assert equity.units.iloc[3] == 0


def test_spread_mark_can_trigger_drawdown_and_next_open_liquidation(bars, cfg):
    cfg = cfg.model_copy(update={"max_drawdown": 0.008})
    frame = with_sides(bars, [0.02, 0.02, 0.02, 0.02, 20, 0.2, 0.02, 0.02])
    old, _, _ = run_backtest(frame, cfg)
    report, equity, orders = run_backtest(frame, cfg, execution=OBSERVED)
    assert not old["halted"]
    assert report["liquidation_reason"] == "drawdown"
    assert equity.halted.iloc[4]
    assert not equity.halted.iloc[3]
    executed = fills(orders)
    assert executed.timestamp.iloc[-1] == frame.timestamp.iloc[5].isoformat()
    assert executed.price.iloc[-1] == pytest.approx(frame.bid_open.iloc[5] - cfg.slippage)
    assert report["open_units"] == 0


@pytest.mark.parametrize("short", [False, True])
def test_side_extreme_triggers_margin_even_with_later_credit(cfg, short):
    cfg = cfg.model_copy(
        update={
            "allocation": 0.8,
            "max_leverage": 10,
            "max_units": 100000,
            "max_drawdown": 0.9,
            "allow_short": short,
        }
    )
    frame = with_sides(trending(cfg, short), [0.02] * 7 + [30] + [0.02] * 4)
    # Credit is intentionally large and after the unknown intrabar extreme.
    swaps = pd.DataFrame(
        {
            "timestamp": [frame.timestamp.iloc[7] + pd.Timedelta(minutes=30)],
            "symbol": cfg.symbol,
            "long_jpy_per_10k": 1000000,
            "short_jpy_per_10k": 1000000,
            "days": 1,
        }
    )
    report, equity, orders = run_backtest(frame, cfg, execution=OBSERVED, swap_schedule=swaps)
    assert report["liquidation_reason"] == "maintenance_margin"
    assert equity.halted.iloc[7]
    assert not equity.halted.iloc[6]
    assert fills(orders).timestamp.iloc[-1] == frame.timestamp.iloc[8].isoformat()


def test_future_quotes_do_not_change_past_path(bars, cfg):
    original = with_sides(bars)
    changed = with_sides(bars, [0.2] * 6 + [20] * 2)
    _, equity1, orders1 = run_backtest(original, cfg, execution=OBSERVED)
    _, equity2, orders2 = run_backtest(changed, cfg, execution=OBSERVED)
    pd.testing.assert_frame_equal(equity1.iloc[:6], equity2.iloc[:6])
    assert fills(orders1).price.iloc[0] == fills(orders2).price.iloc[0]


def test_benchmark_uses_same_side_entry_mark_and_exit(bars, cfg):
    frame = with_sides(bars)
    result = buy_and_hold_benchmark(frame, cfg, frame.timestamp.iloc[2], execution=OBSERVED)
    units = result["units"]
    entry = 154.11
    mark = 147.9
    exit_price = 147.89
    assert result["entry_price"] == pytest.approx(entry)
    marked = cfg.initial_cash + units * (mark - entry) - units * entry * cfg.commission_rate
    assert result["marked_final_equity_jpy"] == pytest.approx(marked)
    assert result["liquidation_final_equity_jpy"] == pytest.approx(
        marked + units * (exit_price - mark) - units * exit_price * cfg.commission_rate
    )


def test_benchmark_side_low_triggers_forced_exit(cfg):
    cfg = cfg.model_copy(update={"allocation": 0.8, "max_leverage": 10, "max_units": 100000})
    frame = with_sides(trending(cfg), [0.02] * 7 + [30] + [0.2] * 4)
    result = buy_and_hold_benchmark(frame, cfg, frame.timestamp.iloc[2], execution=OBSERVED)
    assert result["forced_exit_timestamp"] == frame.timestamp.iloc[8].isoformat()
    units, entry = result["units"], result["entry_price"]
    exit_price = frame.bid_open.iloc[8] - cfg.slippage
    assert result["liquidation_final_equity_jpy"] == pytest.approx(
        cfg.initial_cash
        + units * (exit_price - entry)
        - units * (entry + exit_price) * cfg.commission_rate
    )


def test_evaluation_stresses_observed_spreads_and_preserves_identity(cfg, tmp_path):
    frame = with_sides(trending(cfg, count=24), 0.4)
    report, _, orders, _ = evaluate_strategy(frame, cfg, execution=OBSERVED)
    baseline = fills(
        orders[(orders.scenario == "baseline") & (orders.evaluation_scope == "continuous")]
    )
    stressed = fills(
        orders[(orders.scenario == "stressed") & (orders.evaluation_scope == "continuous")]
    )
    assert baseline.price.iloc[0] == pytest.approx(104.21)
    assert stressed.price.iloc[0] == pytest.approx(104.42)
    assert report["scenarios"]["stressed"]["benchmarks"]["buy_and_hold"][
        "entry_price"
    ] == pytest.approx(104.42)
    saved = save_evaluation(frame, cfg, tmp_path / "observed", execution=OBSERVED)
    fixed = save_evaluation(frame, cfg, tmp_path / "fixed")
    assert saved["experiment_id"] != fixed["experiment_id"]
    assert saved["run_parameters"]["execution"] == OBSERVED.model_dump()
    assert saved["config_sha256"] == fixed["config_sha256"] == cfg.fingerprint
    run = save_run(frame, cfg, tmp_path / "single", execution=OBSERVED)
    assert run["run_parameters"]["execution"] == OBSERVED.model_dump()
