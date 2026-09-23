import json

import pandas as pd
import pytest

from trading.evaluation import (
    _evidence_gate,
    buy_and_hold_benchmark,
    chronological_folds,
    evaluate_strategy,
    exposure_matched_buy_hold,
    save_comparison,
    save_evaluation,
)


def evaluation_bars(cfg, count=80):
    pattern = [150.0, 151.0, 153.0, 155.0, 152.0, 149.0, 147.0, 150.0]
    prices = (pattern * (count // len(pattern) + 1))[:count]
    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=count, freq="h", tz="UTC"),
            "symbol": cfg.symbol,
            "open": prices,
            "high": [price + 1 for price in prices],
            "low": [price - 1 for price in prices],
            "close": prices,
            "volume": 0,
        }
    )


def test_chronological_folds_cover_each_evaluation_bar_once(cfg):
    bars = evaluation_bars(cfg)
    folds = chronological_folds(bars, cfg, 3)
    assert sum(fold["evaluation_bars"] for fold in folds) == len(bars) - cfg.slow
    assert all(fold["warmup_bars"] == cfg.slow for fold in folds)
    assert folds[0]["active_start"] == bars.timestamp.iloc[cfg.slow]
    assert folds[0]["active_end"] < folds[1]["active_start"]


def test_future_prices_do_not_change_an_earlier_evaluation_fold(cfg):
    bars = evaluation_bars(cfg)
    changed = bars.copy()
    for column in ["open", "high", "low", "close"]:
        changed.loc[40:, column] *= 1.5
    report1, _, _, _ = evaluate_strategy(bars, cfg, fold_count=3)
    report2, _, _, _ = evaluate_strategy(changed, cfg, fold_count=3)
    first1 = report1["scenarios"]["baseline"]["folds"][0]
    first2 = report2["scenarios"]["baseline"]["folds"][0]
    assert first1 == first2


def test_cost_stress_cannot_improve_equity(cfg):
    report, equity, orders, trades = evaluate_strategy(evaluation_bars(cfg), cfg, fold_count=3)
    baseline = report["scenarios"]["baseline"]["summary"]
    stressed = report["scenarios"]["stressed"]["summary"]
    assert stressed["compounded_return_pct"] <= baseline["compounded_return_pct"]
    assert "continuous" in report["scenarios"]["baseline"]
    assert report["scenarios"]["baseline"]["benchmarks"]["cash"]["return_pct"] == 0
    assert "buy_and_hold" in report["scenarios"]["baseline"]["benchmarks"]
    assert report["data_quality"]["gap_count"] == 0
    for table in (equity, orders, trades):
        assert "continuous" in set(table.evaluation_scope)
    assert report["verdict"]["status"] == "insufficient_evidence"


def test_evaluation_reports_unclassified_interval_gaps(cfg):
    bars = evaluation_bars(cfg).drop(index=40).reset_index(drop=True)

    report, _, _, _ = evaluate_strategy(bars, cfg, fold_count=3)

    assert report["data_quality"]["gap_count"] == 1
    assert report["data_quality"]["unobserved_bar_intervals"] == 1
    assert "unclassified" in report["data_quality"]["classification"]


MATCHED_EXCESS = {"baseline_matched_excess_pct": 1.0, "stressed_matched_excess_pct": 0.5}


def gate_inputs(*, stressed_drawdown=1.0, stressed_halted=False):
    frame = pd.DataFrame(
        {"timestamp": [pd.Timestamp("2025-01-01", tz="UTC"), pd.Timestamp("2026-01-01", tz="UTC")]}
    )
    fold_summary = {
        "folds": 3,
        "evaluation_bars": 2_000,
        "profitable_folds": 2,
        "worst_max_drawdown_pct": 1.0,
        "halted_folds": 0,
    }
    baseline = {
        "return_pct": 2.0,
        "max_drawdown_pct": 1.0,
        "halted": False,
        "performance": {"closed_trades": 30},
    }
    stressed = {
        "return_pct": 1.0,
        "max_drawdown_pct": stressed_drawdown,
        "halted": stressed_halted,
        "performance": {"closed_trades": 30},
    }
    return frame, fold_summary, baseline, stressed


def test_evidence_gate_accepts_only_research_candidate(cfg):
    frame, folds, baseline, stressed = gate_inputs()

    verdict = _evidence_gate(frame, cfg, folds, folds, baseline, stressed, **MATCHED_EXCESS)

    assert verdict["status"] == "candidate"
    assert "not a profit guarantee" in verdict["note"]


def test_evidence_gate_rejects_stressed_drawdown_and_halt(cfg):
    frame, folds, baseline, stressed = gate_inputs(
        stressed_drawdown=cfg.max_drawdown * 100 + 1,
        stressed_halted=True,
    )

    verdict = _evidence_gate(frame, cfg, folds, folds, baseline, stressed, **MATCHED_EXCESS)

    assert verdict["status"] == "rejected"
    assert "stressed_drawdown_limit_exceeded" in verdict["reason_codes"]
    assert "stressed_halt_present" in verdict["reason_codes"]


def test_evaluation_rejects_short_data_and_invalid_stress(cfg, bars):
    with pytest.raises(ValueError, match="not enough bars"):
        evaluate_strategy(bars, cfg, fold_count=3)
    with pytest.raises(ValueError, match="greater than 1"):
        evaluate_strategy(evaluation_bars(cfg), cfg, stress_multiplier=1)


def test_evaluation_artifacts_are_immutable(cfg, tmp_path):
    path = tmp_path / "evaluation"
    report = save_evaluation(evaluation_bars(cfg), cfg, path)
    assert report["mode"] == "chronological_evaluation"
    assert (path / "report.json").exists()
    assert (path / "equity.csv").exists()
    assert (path / "orders.csv").exists()
    assert (path / "trades.csv").exists()
    assert len(report["experiment_id"]) == 64
    assert len(report["code_sha256"]) == 64
    assert report["git_dirty"] in {True, False, None}
    with pytest.raises(FileExistsError):
        save_evaluation(evaluation_bars(cfg), cfg, path)


def test_fixed_candidate_comparison_saves_full_results(cfg, tmp_path):
    momentum = cfg.model_copy(
        update={
            "strategy": "momentum",
            "lookback": 2,
            "signal_threshold": 0.005,
            "allow_short": True,
        }
    )
    path = tmp_path / "comparison"

    report = save_comparison(evaluation_bars(cfg), [cfg, momentum], path)

    assert report["mode"] == "fixed_strategy_comparison"
    assert report["candidate_count"] == 2
    assert len(report["comparison_id"]) == 64
    assert len(report["data_sha256"]) == 64
    assert {row["strategy"] for row in report["candidates"]} == {"sma_cross", "momentum"}
    assert all(len(row["experiment_id"]) == 64 for row in report["candidates"])
    assert (path / "comparison.csv").exists()
    assert (path / "01-sma_cross" / "report.json").exists()
    assert (path / "02-momentum" / "report.json").exists()

    with pytest.raises(ValueError, match="unique"):
        save_comparison(evaluation_bars(cfg), [cfg, cfg], tmp_path / "duplicate")


def test_evidence_gate_requires_stressed_fold_consistency(cfg):
    frame, folds, baseline, stressed = gate_inputs()
    stressed_folds = {**folds, "profitable_folds": 1}

    verdict = _evidence_gate(
        frame, cfg, folds, stressed_folds, baseline, stressed, **MATCHED_EXCESS
    )

    assert verdict["status"] == "rejected"
    assert verdict["reason_codes"] == ["insufficient_stressed_profitable_folds"]


def test_evidence_gate_measures_calendar_days_from_evaluation_start(cfg):
    frame, folds, baseline, stressed = gate_inputs()

    verdict = _evidence_gate(
        frame,
        cfg,
        folds,
        folds,
        baseline,
        stressed,
        pd.Timestamp("2025-12-01", tz="UTC"),
        **MATCHED_EXCESS,
    )

    assert verdict["evidence_checks"]["calendar_days"]["actual"] == pytest.approx(31)
    assert verdict["reason_codes"] == ["calendar_days_below_minimum"]


def test_leveraged_buy_and_hold_is_liquidated_on_maintenance_margin(cfg):
    leveraged = cfg.model_copy(
        update={"allocation": 1.0, "max_leverage": 10.0, "max_units": 100_000}
    )
    prices = [100.0] * 5 + [94.0, 94.0, 90.0, 90.0, 90.0]
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=len(prices), freq="h", tz="UTC"),
            "open": prices,
            "high": prices,
            "low": prices,
            "close": prices,
        }
    )

    result = buy_and_hold_benchmark(frame, leveraged, frame.timestamp.iloc[1])

    assert result["liquidation_reason"] == "maintenance_margin"
    assert result["forced_exit_timestamp"] == frame.timestamp.iloc[6].isoformat()
    assert result["marked_final_equity_jpy"] == pytest.approx(
        result["liquidation_final_equity_jpy"]
    )


def test_experiment_id_distinguishes_evaluation_settings(cfg, tmp_path):
    bars = evaluation_bars(cfg)

    two = save_evaluation(bars, cfg, tmp_path / "two", fold_count=2)
    three = save_evaluation(bars, cfg, tmp_path / "three", fold_count=3)

    assert two["run_parameters"]["fold_count"] == 2
    assert two["experiment_id"] != three["experiment_id"]


def test_comparison_evaluates_candidates_over_one_shared_period(cfg, tmp_path):
    longer_sma = cfg.model_copy(update={"fast": 2, "slow": 6})
    momentum = cfg.model_copy(update={"strategy": "momentum", "lookback": 2})
    path = tmp_path / "comparison"

    report = save_comparison(evaluation_bars(cfg), [longer_sma, momentum], path)

    candidate_reports = [
        json.loads((path / row["candidate"] / "report.json").read_text(encoding="utf-8"))
        for row in report["candidates"]
    ]
    assert report["warmup_bars"] == 6
    assert {item["warmup_bars"] for item in candidate_reports} == {6}
    assert len({item["active_start"] for item in candidate_reports}) == 1

    with pytest.raises(ValueError, match="shorter"):
        evaluate_strategy(evaluation_bars(cfg), longer_sma, warmup_bars=3)


def test_folds_do_not_require_a_second_warm_up_in_the_active_period(cfg):
    folds = chronological_folds(evaluation_bars(cfg), cfg, 3, warmup_bars=40)

    assert [fold["evaluation_bars"] for fold in folds] == [14, 13, 13]
    assert all(len(fold["frame"]) == 40 + fold["evaluation_bars"] for fold in folds)


def test_benchmark_swap_accrues_through_each_candle_close(cfg):
    frame = evaluation_bars(cfg, count=6)
    inside_last_candle = frame.timestamp.iloc[-1] + pd.Timedelta(minutes=30)
    schedule = pd.DataFrame(
        {
            "timestamp": [inside_last_candle],
            "symbol": [cfg.symbol],
            "long_jpy_per_10k": [100.0],
            "short_jpy_per_10k": [-100.0],
            "days": [1],
        }
    )

    result = buy_and_hold_benchmark(frame, cfg, frame.timestamp.iloc[0], schedule)

    assert result["swap_pnl_jpy"] == pytest.approx(result["units"] / 10_000 * 100)


def test_failed_comparison_leaves_no_output_and_can_be_retried(cfg, tmp_path):
    momentum = cfg.model_copy(update={"strategy": "momentum", "lookback": 2})
    path = tmp_path / "comparison"

    with pytest.raises(ValueError, match="greater than 1"):
        save_comparison(evaluation_bars(cfg), [cfg, momentum], path, stress_multiplier=1)

    assert list(tmp_path.iterdir()) == []
    save_comparison(evaluation_bars(cfg), [cfg, momentum], path)
    assert (path / "report.json").exists()


def test_git_state_is_captured_once_before_comparison_artifacts(cfg, tmp_path, monkeypatch):
    from trading import evaluation

    calls = []

    def fake_git_state():
        calls.append(sorted(p.name for p in tmp_path.iterdir()))
        return {"git_commit": "abc", "git_dirty": False}

    monkeypatch.setattr(evaluation, "git_state", fake_git_state)
    momentum = cfg.model_copy(update={"strategy": "momentum", "lookback": 2})
    path = tmp_path / "comparison"

    save_comparison(evaluation_bars(cfg), [cfg, momentum], path)

    assert calls == [[]]
    for child in ("01-sma_cross", "02-momentum"):
        child_report = json.loads((path / child / "report.json").read_text(encoding="utf-8"))
        assert child_report["git_dirty"] is False


def test_buy_and_hold_margin_is_tested_at_the_candle_low(cfg):
    leveraged = cfg.model_copy(
        update={"allocation": 1.0, "max_leverage": 10.0, "max_units": 100_000}
    )
    prices = [100.0] * 8
    lows = list(prices)
    lows[4] = 93.0  # breaches maintenance intrabar, then recovers by the close
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=len(prices), freq="h", tz="UTC"),
            "open": prices,
            "high": prices,
            "low": lows,
            "close": prices,
        }
    )

    result = buy_and_hold_benchmark(frame, leveraged, frame.timestamp.iloc[1])

    assert result["liquidation_reason"] == "maintenance_margin"
    assert result["forced_exit_timestamp"] == frame.timestamp.iloc[5].isoformat()


def test_exposure_matched_buy_hold_scales_by_signed_exposure():
    equity = pd.DataFrame({"units": [0, 100, 100, -100], "gross_notional": [0.0, 1e3, 1e3, 1e3]})
    buy_hold = {"average_notional_jpy": 1_000.0, "marked_return_pct": 8.0}

    matched = exposure_matched_buy_hold(equity, buy_hold, strategy_return_pct=5.0)

    # Long 2 bars and short 1 bar out of 4: net a quarter of buy-and-hold's exposure.
    assert matched["exposure_ratio"] == pytest.approx(0.25)
    assert matched["return_pct"] == pytest.approx(2.0)
    assert matched["strategy_excess_return_pct"] == pytest.approx(3.0)


def test_evidence_gate_rejects_returns_explained_by_market_exposure(cfg):
    frame, folds, baseline, stressed = gate_inputs()

    verdict = _evidence_gate(
        frame,
        cfg,
        folds,
        folds,
        baseline,
        stressed,
        baseline_matched_excess_pct=0.4,
        stressed_matched_excess_pct=-0.1,
    )

    assert verdict["status"] == "rejected"
    assert verdict["reason_codes"] == ["stressed_not_above_exposure_matched_buy_hold"]


def test_evaluation_reports_exposure_matched_benchmark_per_scenario(cfg):
    report, _, _, _ = evaluate_strategy(evaluation_bars(cfg), cfg, fold_count=3)

    for scenario in ("baseline", "stressed"):
        benchmarks = report["scenarios"][scenario]["benchmarks"]
        matched = benchmarks["exposure_matched_buy_hold"]
        strategy_return = report["scenarios"][scenario]["continuous"]["return_pct"]
        assert benchmarks["buy_and_hold"]["average_notional_jpy"] > 0
        assert matched["return_pct"] == pytest.approx(
            matched["exposure_ratio"] * benchmarks["buy_and_hold"]["marked_return_pct"]
        )
        assert matched["strategy_excess_return_pct"] == pytest.approx(
            strategy_return - matched["return_pct"]
        )
    checks = report["verdict"]["performance_checks"]
    assert "baseline_beats_exposure_matched_buy_hold" in checks
    assert "stressed_beats_exposure_matched_buy_hold" in checks


def test_fold_consistency_uses_liquidation_value():
    from trading.evaluation import _summary

    def fold(marked, liquidated):
        return {
            "evaluation_bars": 10,
            "result": {
                "return_pct": marked,
                "liquidation_return_pct": liquidated,
                "max_drawdown_pct": 0.1,
                "fills": 1,
                "open_units": 100,
                "halted": False,
                "performance": {
                    "gross_profit_jpy": 0.0,
                    "gross_loss_jpy": 0.0,
                    "closed_trades": 0,
                    "net_realized_pnl_jpy": 0.0,
                },
            },
        }

    summary = _summary([fold(0.01, -0.02), fold(0.01, -0.02), fold(0.5, 0.4)])

    assert summary["profitable_folds"] == 1
    assert summary["fold_return_basis"] == "liquidation_value"


def test_benchmark_margin_breach_is_not_rescued_by_a_later_credit(cfg):
    leveraged = cfg.model_copy(
        update={"allocation": 1.0, "max_leverage": 10.0, "max_units": 100_000}
    )
    prices = [100.0] * 8
    lows = list(prices)
    lows[4] = 93.0
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=len(prices), freq="h", tz="UTC"),
            "open": prices,
            "high": prices,
            "low": lows,
            "close": prices,
        }
    )
    schedule = pd.DataFrame(
        {
            "timestamp": [frame.timestamp.iloc[4] + pd.Timedelta(minutes=30)],
            "symbol": [cfg.symbol],
            "long_jpy_per_10k": [1_000_000.0],
            "short_jpy_per_10k": [-1_000_000.0],
            "days": [1],
        }
    )

    result = buy_and_hold_benchmark(frame, leveraged, frame.timestamp.iloc[1], schedule)

    assert result["liquidation_reason"] == "maintenance_margin"
