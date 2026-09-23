import hashlib
import importlib.metadata
import json
import math
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from trading.backtest import completed_trades, run_backtest
from trading.config import Settings
from trading.data import validate_bars
from trading.provenance import reproducibility_fields
from trading.strategy import entry_units, maintenance_margin_halt
from trading.swap import swap_credit_between, swap_fingerprint, validate_swap_schedule

MIN_EVALUATION_BARS = {"fx": 2_000, "jp_equity": 500}
MIN_CALENDAR_DAYS = {"fx": 180.0, "jp_equity": 730.0}
MIN_CLOSED_TRADES = 30


def interval_gap_report(frame: pd.DataFrame, cfg: Settings) -> dict:
    """Describe discontinuities without guessing whether they are closures or outages."""
    intervals = frame.timestamp.diff().dropna().dt.total_seconds() / cfg.bar_seconds
    gaps = intervals[intervals > 1]
    return {
        "gap_count": int(len(gaps)),
        "unobserved_bar_intervals": int(sum(max(math.floor(value) - 1, 0) for value in gaps)),
        "largest_gap_seconds": float(gaps.max() * cfg.bar_seconds) if len(gaps) else 0.0,
        "classification": (
            "unclassified; scheduled closures and collection outages are not separated"
        ),
    }


def buy_and_hold_benchmark(
    frame: pd.DataFrame,
    cfg: Settings,
    active_start: pd.Timestamp,
    swap_schedule: pd.DataFrame | None = None,
) -> dict:
    """Buy once at the strategy's first executable open and report marked/liquidated value.

    Like the strategy account, the position is force-closed at the next open once the
    maintenance-margin rule trips, and the account stays flat afterwards.
    """
    active_matches = frame.index[frame.timestamp == active_start].tolist()
    if len(active_matches) != 1 or active_matches[0] + 1 >= len(frame):
        raise ValueError("benchmark active_start must leave a next bar")
    entry_index = active_matches[0] + 1
    adverse_cost = cfg.spread / 2 + cfg.slippage
    entry_price = float(frame.open.iloc[entry_index]) + adverse_cost
    units = entry_units(cfg.initial_cash, cfg.initial_cash, entry_price, cfg)
    entry_fee = units * entry_price * cfg.commission_rate
    entry_time = frame.timestamp.iloc[entry_index]
    path = frame.iloc[entry_index:].copy()
    swap_path = [
        swap_credit_between(swap_schedule, entry_time, timestamp, units)
        for timestamp in path.timestamp
    ]
    closes = path.close.astype(float)
    equities = (
        cfg.initial_cash
        - entry_fee
        + units * (closes - entry_price)
        + pd.Series(swap_path, index=path.index)
    )
    forced_exit = None
    for position in range(len(path) - 1):
        if maintenance_margin_halt(
            units, float(equities.iloc[position]), float(closes.iloc[position]), cfg
        ):
            forced_exit = position + 1
            break
    if forced_exit is None:
        exit_time = None
        total_swap = float(swap_path[-1]) if swap_path else 0.0
        exit_price = float(frame.close.iloc[-1]) - adverse_cost
    else:
        exit_time = path.timestamp.iloc[forced_exit]
        total_swap = float(swap_path[forced_exit])
        exit_price = float(path.open.iloc[forced_exit]) - adverse_cost
    exit_fee = units * exit_price * cfg.commission_rate
    liquidation_equity = (
        cfg.initial_cash + units * (exit_price - entry_price) - entry_fee - exit_fee + total_swap
    )
    if forced_exit is not None:
        equities.iloc[forced_exit:] = liquidation_equity
    peaks = equities.cummax().clip(lower=cfg.initial_cash)
    marked_equity = float(equities.iloc[-1])
    return {
        "entry_timestamp": entry_time.isoformat(),
        "entry_price": entry_price,
        "units": units,
        "gross_notional_jpy": units * entry_price,
        "initial_effective_leverage": units * entry_price / cfg.initial_cash,
        "entry_commission_jpy": entry_fee,
        "swap_pnl_jpy": total_swap,
        "forced_exit_timestamp": exit_time.isoformat() if exit_time is not None else None,
        "liquidation_reason": "maintenance_margin" if exit_time is not None else None,
        "marked_final_equity_jpy": marked_equity,
        "marked_return_pct": (marked_equity / cfg.initial_cash - 1) * 100,
        "liquidation_final_equity_jpy": liquidation_equity,
        "liquidation_return_pct": (liquidation_equity / cfg.initial_cash - 1) * 100,
        "max_drawdown_pct": float((1 - equities / peaks).max() * 100),
    }


def chronological_folds(
    frame: pd.DataFrame,
    cfg: Settings,
    fold_count: int,
    warmup_bars: int | None = None,
) -> list[dict]:
    """Split once in time; each independent fold receives only preceding warm-up bars."""
    frame = validate_bars(frame, cfg)
    if fold_count < 2:
        raise ValueError("fold_count must be at least 2")
    warmup_bars = _resolve_warmup(cfg, warmup_bars)
    # Each fold frame already carries its own warm-up; the active part needs one decision bar
    # and one bar to fill it.
    minimum_active_bars = 2
    evaluation_bars = len(frame) - warmup_bars
    if evaluation_bars < fold_count * minimum_active_bars:
        required = warmup_bars + fold_count * minimum_active_bars
        raise ValueError(f"not enough bars for {fold_count} folds; need at least {required}")

    base_size, extra = divmod(evaluation_bars, fold_count)
    cursor = warmup_bars
    folds = []
    for number in range(1, fold_count + 1):
        size = base_size + (1 if number <= extra else 0)
        stop = cursor + size
        folds.append(
            {
                "number": number,
                "warmup_bars": warmup_bars,
                "evaluation_bars": size,
                "active_start": frame.timestamp.iloc[cursor],
                "active_end": frame.timestamp.iloc[stop - 1],
                "frame": frame.iloc[cursor - warmup_bars : stop].reset_index(drop=True),
            }
        )
        cursor = stop
    return folds


def _resolve_warmup(cfg: Settings, warmup_bars: int | None) -> int:
    """A caller may extend warm-up to align candidates, never shorten the strategy's own."""
    if warmup_bars is None:
        return cfg.warmup_bars
    if warmup_bars < cfg.warmup_bars:
        raise ValueError("warmup_bars cannot be shorter than the strategy requires")
    return warmup_bars


def _cost_config(cfg: Settings, multiplier: float) -> Settings:
    values = cfg.model_dump()
    for field in ("commission_rate", "spread", "slippage"):
        values[field] *= multiplier
    return Settings.model_validate(values)


def _summary(folds: list[dict]) -> dict:
    reports = [fold["result"] for fold in folds]
    performances = [report["performance"] for report in reports]
    gross_profit = sum(item["gross_profit_jpy"] for item in performances)
    gross_loss = sum(item["gross_loss_jpy"] for item in performances)
    compounded = math.prod(1 + report["return_pct"] / 100 for report in reports) - 1
    return {
        "folds": len(folds),
        "evaluation_bars": sum(fold["evaluation_bars"] for fold in folds),
        "profitable_folds": sum(report["return_pct"] > 0 for report in reports),
        "compounded_return_pct": compounded * 100,
        "mean_fold_return_pct": sum(report["return_pct"] for report in reports) / len(reports),
        "worst_max_drawdown_pct": max(report["max_drawdown_pct"] for report in reports),
        "fills": sum(report["fills"] for report in reports),
        "closed_trades": sum(item["closed_trades"] for item in performances),
        "net_realized_pnl_jpy": sum(item["net_realized_pnl_jpy"] for item in performances),
        "gross_profit_jpy": gross_profit,
        "gross_loss_jpy": gross_loss,
        "profit_factor": gross_profit / gross_loss if gross_loss else None,
        "open_positions_at_fold_end": sum(report["open_units"] != 0 for report in reports),
        "halted_folds": sum(report["halted"] for report in reports),
    }


def _concat_tables(tables: list[pd.DataFrame]) -> pd.DataFrame:
    populated = [table for table in tables if not table.empty]
    return pd.concat(populated, ignore_index=True) if populated else tables[0].iloc[0:0].copy()


def _evidence_gate(
    frame: pd.DataFrame,
    cfg: Settings,
    baseline_folds: dict,
    stressed_folds: dict,
    baseline_continuous: dict,
    stressed_continuous: dict,
    active_start: pd.Timestamp | None = None,
) -> dict:
    # Warm-up bars never trade, so they do not count toward the evaluated span.
    evaluation_start = frame.timestamp.iloc[0] if active_start is None else active_start
    coverage_days = float((frame.timestamp.iloc[-1] - evaluation_start).total_seconds() / 86_400)
    required_profitable_folds = math.ceil(baseline_folds["folds"] * 2 / 3)
    baseline_performance = baseline_continuous["performance"]
    evidence_checks = {
        "evaluation_bars": {
            "actual": baseline_folds["evaluation_bars"],
            "minimum": MIN_EVALUATION_BARS[cfg.market],
            "passed": baseline_folds["evaluation_bars"] >= MIN_EVALUATION_BARS[cfg.market],
        },
        "calendar_days": {
            "actual": coverage_days,
            "minimum": MIN_CALENDAR_DAYS[cfg.market],
            "passed": coverage_days >= MIN_CALENDAR_DAYS[cfg.market],
        },
        "closed_trades": {
            "actual": baseline_performance["closed_trades"],
            "minimum": MIN_CLOSED_TRADES,
            "passed": baseline_performance["closed_trades"] >= MIN_CLOSED_TRADES,
        },
    }
    performance_checks = {
        "positive_baseline_continuous_return": baseline_continuous["return_pct"] > 0,
        "positive_stressed_continuous_return": stressed_continuous["return_pct"] > 0,
        "fold_consistency": baseline_folds["profitable_folds"] >= required_profitable_folds,
        "stressed_fold_consistency": (
            stressed_folds["profitable_folds"] >= required_profitable_folds
        ),
        "baseline_drawdown_within_limit": (
            baseline_continuous["max_drawdown_pct"] <= cfg.max_drawdown * 100
            and baseline_folds["worst_max_drawdown_pct"] <= cfg.max_drawdown * 100
        ),
        "stressed_drawdown_within_limit": (
            stressed_continuous["max_drawdown_pct"] <= cfg.max_drawdown * 100
            and stressed_folds["worst_max_drawdown_pct"] <= cfg.max_drawdown * 100
        ),
        "no_baseline_halt": (
            not baseline_continuous["halted"] and baseline_folds["halted_folds"] == 0
        ),
        "no_stressed_halt": (
            not stressed_continuous["halted"] and stressed_folds["halted_folds"] == 0
        ),
    }
    failed_evidence = [name for name, check in evidence_checks.items() if not check["passed"]]
    failed_performance = [name for name, passed in performance_checks.items() if not passed]
    if failed_evidence:
        status = "insufficient_evidence"
        reasons = [f"{name}_below_minimum" for name in failed_evidence]
    elif failed_performance:
        status = "rejected"
        reason_names = {
            "positive_baseline_continuous_return": "baseline_continuous_return_not_positive",
            "positive_stressed_continuous_return": "stressed_continuous_return_not_positive",
            "fold_consistency": "insufficient_profitable_folds",
            "stressed_fold_consistency": "insufficient_stressed_profitable_folds",
            "baseline_drawdown_within_limit": "baseline_drawdown_limit_exceeded",
            "stressed_drawdown_within_limit": "stressed_drawdown_limit_exceeded",
            "no_baseline_halt": "baseline_halt_present",
            "no_stressed_halt": "stressed_halt_present",
        }
        reasons = [reason_names[name] for name in failed_performance]
    else:
        status = "candidate"
        reasons = []
    return {
        "status": status,
        "reason_codes": reasons,
        "evidence_checks": evidence_checks,
        "performance_checks": performance_checks,
        "required_profitable_folds": required_profitable_folds,
        "note": "A candidate clears minimum research gates; it is not a profit guarantee.",
    }


def evaluate_strategy(
    frame: pd.DataFrame,
    cfg: Settings,
    *,
    fold_count: int = 3,
    stress_multiplier: float = 2.0,
    swap_schedule: pd.DataFrame | None = None,
    warmup_bars: int | None = None,
) -> tuple[dict, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Evaluate one fixed strategy across chronological folds and stressed costs.

    warmup_bars may exceed the strategy's own need so that compared candidates share one
    evaluated period; it may not be shorter.
    """
    frame = validate_bars(frame, cfg)
    if swap_schedule is not None:
        swap_schedule = validate_swap_schedule(swap_schedule, cfg)
    if not math.isfinite(stress_multiplier) or stress_multiplier <= 1:
        raise ValueError("stress_multiplier must be finite and greater than 1")
    warmup_bars = _resolve_warmup(cfg, warmup_bars)
    fold_specs = chronological_folds(frame, cfg, fold_count, warmup_bars)
    scenarios = {"baseline": cfg, "stressed": _cost_config(cfg, stress_multiplier)}
    scenario_reports = {}
    equity_tables = []
    order_tables = []
    trade_tables = []
    continuous_active_start = frame.timestamp.iloc[warmup_bars]

    for scenario, scenario_cfg in scenarios.items():
        cost_multiplier = 1.0 if scenario == "baseline" else stress_multiplier
        continuous_report, continuous_equity, continuous_orders = run_backtest(
            frame,
            scenario_cfg,
            active_start=continuous_active_start,
            swap_schedule=swap_schedule,
        )
        buy_hold = buy_and_hold_benchmark(
            frame,
            scenario_cfg,
            continuous_active_start,
            swap_schedule,
        )
        continuous_trades = completed_trades(
            continuous_orders[continuous_orders.status == "Completed"]
        )
        continuous_metadata = {
            "scenario": scenario,
            "cost_multiplier": cost_multiplier,
            "evaluation_scope": "continuous",
            "fold": pd.NA,
        }
        for table, destination in (
            (continuous_equity, equity_tables),
            (continuous_orders, order_tables),
            (continuous_trades, trade_tables),
        ):
            table = table.copy()
            for column, value in reversed(continuous_metadata.items()):
                table.insert(0, column, value)
            destination.append(table)

        fold_reports = []
        for spec in fold_specs:
            report, equity, orders = run_backtest(
                spec["frame"],
                scenario_cfg,
                active_start=spec["active_start"],
                swap_schedule=swap_schedule,
            )
            trades = completed_trades(orders[orders.status == "Completed"])
            metadata = {
                "scenario": scenario,
                "cost_multiplier": cost_multiplier,
                "evaluation_scope": "independent_fold",
                "fold": spec["number"],
            }
            for table, destination in (
                (equity, equity_tables),
                (orders, order_tables),
                (trades, trade_tables),
            ):
                table = table.copy()
                for column, value in reversed(metadata.items()):
                    table.insert(0, column, value)
                destination.append(table)
            fold_reports.append(
                {
                    "fold": spec["number"],
                    "warmup_bars": spec["warmup_bars"],
                    "evaluation_bars": spec["evaluation_bars"],
                    "active_start": spec["active_start"].isoformat(),
                    "active_end": spec["active_end"].isoformat(),
                    "result": report,
                }
            )
        scenario_reports[scenario] = {
            "cost_multiplier": cost_multiplier,
            "continuous": continuous_report,
            "benchmarks": {
                "cash": {
                    "final_equity_jpy": cfg.initial_cash,
                    "return_pct": 0.0,
                    "max_drawdown_pct": 0.0,
                },
                "buy_and_hold": buy_hold,
                "strategy_excess_return_vs_buy_hold_pct": (
                    continuous_report["return_pct"] - buy_hold["marked_return_pct"]
                ),
            },
            "summary": _summary(fold_reports),
            "folds": fold_reports,
        }

    report = {
        "mode": "chronological_evaluation",
        "symbol": cfg.symbol,
        "strategy": cfg.strategy,
        "strategy_parameters": cfg.strategy_parameters,
        "data_start": frame.timestamp.iloc[0].isoformat(),
        "data_end": frame.timestamp.iloc[-1].isoformat(),
        "bars": len(frame),
        "warmup_bars": warmup_bars,
        "active_start": continuous_active_start.isoformat(),
        "fold_count": fold_count,
        "stress_multiplier": stress_multiplier,
        "data_quality": interval_gap_report(frame, cfg),
        "scenarios": scenario_reports,
        "verdict": _evidence_gate(
            frame,
            cfg,
            scenario_reports["baseline"]["summary"],
            scenario_reports["stressed"]["summary"],
            scenario_reports["baseline"]["continuous"],
            scenario_reports["stressed"]["continuous"],
            continuous_active_start,
        ),
        "limitations": [
            "The strategy and parameters are fixed; this is not parameter optimization.",
            (
                "Independent folds reset cash, positions, and risk state; continuous results "
                "represent one uninterrupted account path."
            ),
            "Open positions are marked to the final close and are not force-liquidated.",
            "Minimum evidence gates reduce weak claims but do not prove future profitability.",
        ],
    }
    return (
        report,
        _concat_tables(equity_tables),
        _concat_tables(order_tables),
        _concat_tables(trade_tables),
    )


def save_evaluation(
    frame: pd.DataFrame,
    cfg: Settings,
    directory: Path,
    *,
    fold_count: int = 3,
    stress_multiplier: float = 2.0,
    swap_schedule: pd.DataFrame | None = None,
    warmup_bars: int | None = None,
) -> dict:
    """Save an immutable chronological evaluation and its per-fold audit tables."""
    frame = validate_bars(frame, cfg)
    report, equity, orders, trades = evaluate_strategy(
        frame,
        cfg,
        fold_count=fold_count,
        stress_multiplier=stress_multiplier,
        swap_schedule=swap_schedule,
        warmup_bars=warmup_bars,
    )
    directory.mkdir(parents=True, exist_ok=False)
    frame.to_parquet(directory / "bars.parquet", index=False)
    equity.to_csv(directory / "equity.csv", index=False)
    orders.to_csv(directory / "orders.csv", index=False)
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
            {
                "mode": "chronological_evaluation",
                "fold_count": fold_count,
                "stress_multiplier": stress_multiplier,
                "warmup_bars": report["warmup_bars"],
            },
        )
    )
    (directory / "config.json").write_text(cfg.model_dump_json(indent=2), encoding="utf-8")
    (directory / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def save_comparison(
    frame: pd.DataFrame,
    candidates: list[Settings],
    directory: Path,
    *,
    fold_count: int = 3,
    stress_multiplier: float = 2.0,
    swap_schedule: pd.DataFrame | None = None,
) -> dict:
    """Evaluate fixed candidate configs and save every full audit trail plus a summary."""
    if len(candidates) < 2:
        raise ValueError("comparison requires at least two candidate configs")
    fingerprints = [candidate.fingerprint for candidate in candidates]
    if len(set(fingerprints)) != len(fingerprints):
        raise ValueError("comparison candidate configs must be unique")
    comparable_fields = ("market", "symbol", "bar_seconds", "initial_cash")
    reference = tuple(getattr(candidates[0], field) for field in comparable_fields)
    if any(
        tuple(getattr(candidate, field) for field in comparable_fields) != reference
        for candidate in candidates[1:]
    ):
        raise ValueError("comparison candidates must share market, symbol, interval, and cash")

    # Every candidate starts trading on the same bar so returns cover the same period.
    common_warmup = max(candidate.warmup_bars for candidate in candidates)
    directory.mkdir(parents=True, exist_ok=False)
    rows = []
    for number, candidate in enumerate(candidates, start=1):
        name = f"{number:02d}-{candidate.strategy}"
        report = save_evaluation(
            frame,
            candidate,
            directory / name,
            fold_count=fold_count,
            stress_multiplier=stress_multiplier,
            swap_schedule=swap_schedule,
            warmup_bars=common_warmup,
        )
        baseline = report["scenarios"]["baseline"]
        stressed = report["scenarios"]["stressed"]
        rows.append(
            {
                "candidate": name,
                "strategy": candidate.strategy,
                "strategy_parameters": candidate.strategy_parameters,
                "config_sha256": candidate.fingerprint,
                "experiment_id": report["experiment_id"],
                "baseline_return_pct": baseline["continuous"]["return_pct"],
                "stressed_return_pct": stressed["continuous"]["return_pct"],
                "baseline_max_drawdown_pct": baseline["continuous"]["max_drawdown_pct"],
                "stressed_max_drawdown_pct": stressed["continuous"]["max_drawdown_pct"],
                "closed_trades": baseline["continuous"]["performance"]["closed_trades"],
                "profit_factor": baseline["continuous"]["performance"]["profit_factor"],
                "excess_return_vs_buy_hold_pct": baseline["benchmarks"][
                    "strategy_excess_return_vs_buy_hold_pct"
                ],
                "verdict": report["verdict"]["status"],
                "reason_codes": report["verdict"]["reason_codes"],
            }
        )

    ranking = sorted(rows, key=lambda row: row["stressed_return_pct"], reverse=True)
    for rank, row in enumerate(ranking, start=1):
        row["diagnostic_rank_by_stressed_return"] = rank
    report = {
        "mode": "fixed_strategy_comparison",
        "symbol": candidates[0].symbol,
        "candidate_count": len(rows),
        "warmup_bars": common_warmup,
        "fold_count": fold_count,
        "stress_multiplier": stress_multiplier,
        "candidates": rows,
        "comparison_id": hashlib.sha256(
            ":".join(row["experiment_id"] for row in rows).encode()
        ).hexdigest(),
        "data_sha256": json.loads(
            (directory / rows[0]["candidate"] / "report.json").read_text(encoding="utf-8")
        )["data_sha256"],
        "note": (
            "Ranking is descriptive on already-viewed data; it is not strategy approval or "
            "an unused holdout result. A candidate status never authorizes live trading."
        ),
    }
    table = pd.DataFrame(rows)
    table["strategy_parameters"] = table.strategy_parameters.map(json.dumps)
    table["reason_codes"] = table.reason_codes.map(json.dumps)
    table.to_csv(directory / "comparison.csv", index=False)
    (directory / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report
