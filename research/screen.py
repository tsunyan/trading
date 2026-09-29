"""Run a preregistered, finite research batch using the existing trading engine.

Usage: uv run python research/screen.py --plan research/20260929-plan.json
All hypotheses and exact configs are recorded before any strategy result is computed.
No live or paper account is touched. Existing run directories are never overwritten.
"""

import argparse
import hashlib
import json
import math
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from trading.cli import swap_covers_bars
from trading.config import Settings
from trading.data import read_bars
from trading.evaluation import save_evaluation
from trading.execution import ExecutionModel, execution_prices
from trading.ledger import add_hypothesis, record_run, summary
from trading.provenance import git_state, source_sha256
from trading.swap import read_swap_schedule


def daily_pnl(equity: pd.DataFrame, initial_cash: float, final_value: float) -> np.ndarray:
    """Preserve all P&L, including opening costs and final liquidation costs."""
    values = equity.equity.astype(float).copy()
    values.iloc[-1] = final_value
    delta = values.diff()
    delta.iloc[0] = values.iloc[0] - initial_cash
    # Equity is marked at candle close; stored timestamps label hourly candle opens.
    closes = pd.to_datetime(equity.timestamp, utc=True) + pd.Timedelta(hours=1)
    daily = pd.Series(delta.to_numpy(), index=closes).resample("D").sum()
    return daily.to_numpy()


def block_pvalue(pnl: np.ndarray, block: int, samples: int, seed: int) -> float:
    """One-sided circular block bootstrap of the centered null mean (cash benchmark).

    Dependence within blocks is retained. The result remains an approximate in-sample
    diagnostic; it is not an OOS probability of profit or an alpha test.
    """
    pnl = np.asarray(pnl, dtype=float)
    if pnl.ndim != 1 or len(pnl) < block or block < 1 or samples < 1:
        raise ValueError("invalid bootstrap dimensions")
    if not np.isfinite(pnl).all():
        raise ValueError("non-finite P&L")
    observed = float(pnl.mean())
    if observed <= 0:
        return 1.0
    centered = pnl - observed
    rng = np.random.default_rng(seed)
    exceedances = 0
    blocks = math.ceil(len(pnl) / block)
    # Bounded working set rather than allocating samples * history in one array.
    for offset in range(0, samples, 200):
        size = min(200, samples - offset)
        starts = rng.integers(0, len(pnl), size=(size, blocks))
        indices = (starts[..., None] + np.arange(block)) % len(pnl)
        indices = indices.reshape(size, -1)[:, : len(pnl)]
        exceedances += int((centered[indices].mean(axis=1) >= observed).sum())
    return (exceedances + 1) / (samples + 1)


def evaluate_one(job: dict) -> dict:
    cfg = Settings.model_validate(job["config"])
    frame = read_bars(Path(job["data"]), cfg)
    swaps = read_swap_schedule(Path(job["swap_data"]), cfg)
    swap_covers_bars(swaps, frame, cfg)
    report = save_evaluation(
        frame,
        cfg,
        Path(job["output"]),
        fold_count=job["folds"],
        stress_multiplier=job["stress_multiplier"],
        swap_schedule=swaps,
        warmup_bars=job["warmup_bars"],
        git=job["git"],
        execution=ExecutionModel.model_validate(job.get("execution", {})),
    )
    return {"name": job["name"], "hypothesis": job["hypothesis"], "report": report}


def compact_result(job: dict, report: dict, trials: int, bootstrap: dict) -> dict:
    scenarios = report["scenarios"]
    equity = pd.read_csv(
        Path(job["output"]) / "equity.csv",
        usecols=["timestamp", "equity", "scenario", "evaluation_scope"],
    )
    equity = equity[
        (equity.scenario == "stressed") & (equity.evaluation_scope == "continuous")
    ].reset_index(drop=True)
    stressed = scenarios["stressed"]["continuous"]
    pnl = daily_pnl(equity, job["config"]["initial_cash"], stressed["liquidation_equity_jpy"])
    if not np.isclose(
        pnl.sum(), stressed["liquidation_equity_jpy"] - job["config"]["initial_cash"], atol=1e-6
    ):
        raise ValueError("daily P&L does not reconcile")
    row = {
        "name": job["name"],
        "hypothesis": job["hypothesis"],
        "primary": job["primary"],
        "output": job["output"],
        "execution": report["execution"],
        "verdict": report["verdict"],
        "data_quality": {k: v for k, v in report["data_quality"].items() if k != "gaps"},
        "scenarios": {},
        "bootstrap_cash_pvalues": {},
    }
    for name, scenario in scenarios.items():
        continuous = scenario["continuous"]
        days = (pd.Timestamp(report["data_end"]) - pd.Timestamp(report["active_start"])).days
        row["scenarios"][name] = {
            **{
                key: continuous[key]
                for key in (
                    "return_pct",
                    "liquidation_return_pct",
                    "max_drawdown_pct",
                    "halted",
                    "commission_jpy",
                    "swap_pnl_jpy",
                    "open_units",
                    "pending_orders",
                )
            },
            "annualized_liquidation_return_pct": (
                (1 + continuous["liquidation_return_pct"] / 100) ** (365.25 / days) - 1
            )
            * 100,
            "closed_trades": continuous["performance"]["closed_trades"],
            "excess_vs_matched_buy_hold_pct": scenario["benchmarks"]["exposure_matched_buy_hold"][
                "strategy_excess_return_pct"
            ],
            "fold_returns_pct": [f["result"]["liquidation_return_pct"] for f in scenario["folds"]],
        }
    for block in bootstrap["block_calendar_days"]:
        pvalue = block_pvalue(pnl, block, bootstrap["samples"], bootstrap["seed"])
        row["bootstrap_cash_pvalues"][str(block)] = {
            "raw": pvalue,
            "bonferroni": min(1.0, pvalue * trials),
        }
    row["statistical_screen_passed"] = all(
        value["bonferroni"] < bootstrap["alpha"] for value in row["bootstrap_cash_pvalues"].values()
    )
    return row


def run(plan_path: Path, ledger: Path, workers: int) -> dict:
    raw_plan = plan_path.read_bytes()
    plan = json.loads(raw_plan)
    plan_hash = hashlib.sha256(raw_plan).hexdigest()
    output = Path("runs") / plan["batch_id"]
    if output.exists():
        raise FileExistsError(f"{output} already exists; never overwrite research")
    existing = summary(ledger)
    known = {h["hypothesis_id"] for h in existing["hypotheses"]}
    overlap = known & {h["id"] for h in plan["hypotheses"]}
    if overlap:
        raise ValueError(f"hypotheses already registered: {overlap}")
    previous_trials = sum(
        len(h["entries"]) for h in existing["hypotheses"] if h["hypothesis_id"] != "H000-plumbing"
    )
    jobs = []
    git = git_state()
    for hypothesis in plan["hypotheses"]:
        for variant in hypothesis["variants"]:
            config = {
                **plan["base_config"],
                **{k: v for k, v in variant.items() if k not in ("name", "execution")},
            }
            cfg = Settings.model_validate(config)
            execution = ExecutionModel.model_validate(variant.get("execution", {}))
            if cfg.warmup_bars > plan["warmup_bars"]:
                raise ValueError("common warmup is too short")
            jobs.append(
                {
                    "name": variant["name"],
                    "hypothesis": hypothesis["id"],
                    "primary": variant["name"] == hypothesis["primary"],
                    "config": cfg.model_dump(),
                    "execution": execution.model_dump(),
                    "output": str(output / variant["name"]),
                    "git": git,
                    **{
                        k: plan[k]
                        for k in (
                            "data",
                            "swap_data",
                            "folds",
                            "stress_multiplier",
                            "warmup_bars",
                        )
                    },
                }
            )
    # Validate input completeness before registration or lengthy evaluation.
    cfg = Settings.model_validate(jobs[0]["config"])
    frame = read_bars(Path(plan["data"]), cfg)
    swaps = read_swap_schedule(Path(plan["swap_data"]), cfg)
    swap_covers_bars(swaps, frame, cfg)
    for job in jobs:
        execution_prices(
            frame,
            Settings.model_validate(job["config"]),
            ExecutionModel.model_validate(job["execution"]),
        )
    manifest = {
        "registered_at": datetime.now(UTC).isoformat(),
        "plan": plan,
        "plan_sha256": plan_hash,
        "code_sha256": source_sha256(),
        "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "input_file_sha256": {
            key: hashlib.sha256(Path(plan[key]).read_bytes()).hexdigest()
            for key in ("data", "swap_data")
        },
        "previous_research_trials": previous_trials,
        "batch_trials": len(jobs),
        "bonferroni_trials": previous_trials + len(jobs),
        "jobs": jobs,
    }
    output.mkdir(parents=True, exist_ok=False)
    # Keep the exact orchestration code for future batches as well as its digest.
    (output / "runner.py").write_bytes(Path(__file__).read_bytes())
    shutil.copytree(
        Path(__file__).resolve().parents[1] / "src" / "trading",
        output / "source" / "trading",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    (output / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    for hypothesis in plan["hypotheses"]:
        description = {
            **hypothesis,
            "plan_sha256": plan_hash,
            "acceptance": plan["acceptance"],
            "base_config": plan["base_config"],
            "manifest": str(output / "manifest.json"),
        }
        add_hypothesis(ledger, hypothesis["id"], json.dumps(description, ensure_ascii=False))
    print(
        f"Registered {len(plan['hypotheses'])} hypotheses / {len(jobs)} variants; "
        f"multiple-testing divisor={manifest['bonferroni_trials']}",
        flush=True,
    )
    rows = []
    with ProcessPoolExecutor(max_workers=workers) as pool:
        pending = {pool.submit(evaluate_one, job): job for job in jobs}
        for future in as_completed(pending):
            job = pending[future]
            result = future.result()
            entries = record_run(
                ledger,
                Path(job["output"]),
                job["hypothesis"],
                f"Preregistered {len(jobs)}-variant research screen; plan {plan_hash}",
            )
            row = compact_result(
                job, result["report"], manifest["bonferroni_trials"], plan["bootstrap"]
            )
            row["ledger_entries"] = entries
            rows.append(row)
            (Path(job["output"]) / "screen.json").write_text(
                json.dumps(row, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            baseline = row["scenarios"]["baseline"]["liquidation_return_pct"]
            stressed = row["scenarios"]["stressed"]["liquidation_return_pct"]
            print(
                f"{job['name']}: {row['verdict']['status']} "
                f"baseline={baseline:+.3f}% stressed={stressed:+.3f}%",
                flush=True,
            )
    rows.sort(key=lambda row: row["name"])
    result = {"manifest": str(output / "manifest.json"), "results": rows}
    (output / "summary.json").write_text(
        json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--ledger", type=Path, default=Path("runs/ledger.sqlite"))
    parser.add_argument("--workers", type=int, default=3)
    args = parser.parse_args()
    run(args.plan, args.ledger, args.workers)
