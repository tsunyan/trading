import pandas as pd
import pytest

from trading.evaluation import (
    _evidence_gate,
    chronological_folds,
    evaluate_strategy,
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

    verdict = _evidence_gate(frame, cfg, folds, folds, baseline, stressed)

    assert verdict["status"] == "candidate"
    assert "not a profit guarantee" in verdict["note"]


def test_evidence_gate_rejects_stressed_drawdown_and_halt(cfg):
    frame, folds, baseline, stressed = gate_inputs(
        stressed_drawdown=cfg.max_drawdown * 100 + 1,
        stressed_halted=True,
    )

    verdict = _evidence_gate(frame, cfg, folds, folds, baseline, stressed)

    assert verdict["status"] == "rejected"
    assert "stressed_drawdown_limit_exceeded" in verdict["reason_codes"]
    assert "stressed_halt_present" in verdict["reason_codes"]


def test_evaluation_rejects_short_data_and_invalid_stress(cfg, bars):
    with pytest.raises(ValueError, match="not enough bars"):
        evaluate_strategy(bars, cfg, fold_count=2)
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
