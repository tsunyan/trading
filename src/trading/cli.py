import argparse
import json
import sqlite3
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
import pandas as pd

from trading.backtest import save_run
from trading.config import load_settings
from trading.data import merge_bars, read_bars, sample_bars, write_bars
from trading.evaluation import interval_gap_report, save_comparison, save_evaluation
from trading.gmo import GmoPublic, GmoSwapCalendar, trading_date
from trading.ledger import (
    DECISIONS,
    add_hypothesis,
    decide,
    freeze_hypothesis,
    record_run,
    require_hypothesis,
    summary,
)
from trading.paper import paper_status, paper_step
from trading.swap import read_swap_schedule, require_swap_coverage, validate_swap_schedule

LEDGER = Path("runs/ledger.sqlite")
# Commands whose results are research trials; each must be counted against a hypothesis.
TRIAL_COMMANDS = ("backtest", "evaluate", "compare")


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description="JPY backtests and local FX paper trading")
    sub = root.add_subparsers(dest="command", required=True)
    for command in ("sample", "backtest", "evaluate", "fetch-fx", "fetch-swap", "paper-step"):
        item = sub.add_parser(command)
        item.add_argument("--config", type=Path, required=True)
        if command in {"backtest", "evaluate", "paper-step"}:
            item.add_argument("--swap-data", type=Path)
        if command in {"sample", "fetch-fx", "fetch-swap"}:
            item.add_argument("--output", type=Path, required=True)
        if command in {"fetch-fx", "fetch-swap"}:
            item.add_argument("--start", type=date.fromisoformat, required=True)
            item.add_argument("--end", type=date.fromisoformat, required=True)
        elif command in {"backtest", "evaluate"}:
            item.add_argument("--data", type=Path, required=True)
            item.add_argument("--output", type=Path, required=True)
            if command == "evaluate":
                item.add_argument("--folds", type=int, default=3)
                item.add_argument("--stress-multiplier", type=float, default=2.0)
        elif command == "paper-step":
            item.add_argument("--database", type=Path, default=Path("runs/paper.sqlite"))
            item.add_argument("--cache", type=Path, default=Path("data/fx_latest.parquet"))
    status = sub.add_parser("paper-status")
    status.add_argument("--database", type=Path, default=Path("runs/paper.sqlite"))
    compare = sub.add_parser("compare")
    compare.add_argument("--config", action="append", type=Path, required=True)
    compare.add_argument("--data", type=Path, required=True)
    compare.add_argument("--output", type=Path, required=True)
    compare.add_argument("--swap-data", type=Path)
    compare.add_argument("--folds", type=int, default=3)
    compare.add_argument("--stress-multiplier", type=float, default=2.0)
    merge = sub.add_parser("merge-bars")
    merge.add_argument("--config", type=Path, required=True)
    merge.add_argument("--input", action="append", type=Path, required=True)
    merge.add_argument("--output", type=Path, required=True)
    for command in TRIAL_COMMANDS:
        item = sub.choices[command]
        item.add_argument("--hypothesis", required=True)
        item.add_argument("--purpose", required=True)
        item.add_argument("--ledger", type=Path, default=LEDGER)
    ledger = sub.add_parser("ledger").add_subparsers(dest="ledger_command", required=True)
    hypothesis = ledger.add_parser("add-hypothesis")
    hypothesis.add_argument("--id", required=True)
    hypothesis.add_argument("--description", required=True)
    freeze = ledger.add_parser("freeze")
    freeze.add_argument("--id", required=True)
    freeze.add_argument("--entry", type=int, required=True)
    imported = ledger.add_parser("import")
    imported.add_argument("--run", type=Path, required=True)
    imported.add_argument("--hypothesis", required=True)
    imported.add_argument("--purpose", required=True)
    decision = ledger.add_parser("decide")
    decision.add_argument("--entry", type=int, required=True)
    decision.add_argument("--decision", choices=DECISIONS, required=True)
    decision.add_argument("--reason", required=True)
    ledger.add_parser("list").add_argument("--hypothesis")
    for item in ledger.choices.values():
        item.add_argument("--database", type=Path, default=LEDGER)
    return root


def run_ledger(args) -> dict:
    if args.ledger_command == "add-hypothesis":
        return add_hypothesis(args.database, args.id, args.description)
    if args.ledger_command == "freeze":
        return freeze_hypothesis(args.database, args.id, args.entry)
    if args.ledger_command == "import":
        entries = record_run(args.database, args.run, args.hypothesis, args.purpose, imported=True)
        return {"run": str(args.run), "ledger_entries": entries}
    if args.ledger_command == "decide":
        return decide(args.database, args.entry, args.decision, args.reason)
    return summary(args.database, args.hypothesis)


def execute(args) -> dict:
    if args.command == "ledger":
        return run_ledger(args)
    if args.command not in TRIAL_COMMANDS:
        return run_command(args)
    # Refuse before running, so no trial can happen without a place to record it.
    require_hypothesis(args.ledger, args.hypothesis)
    report = run_command(args)
    try:
        entries = record_run(args.ledger, args.output, args.hypothesis, args.purpose)
    except (ValueError, OSError, sqlite3.Error) as exc:
        raise ValueError(
            f"run saved to {args.output} but not recorded ({exc}); "
            "record it with `trading ledger import`"
        ) from exc
    return {**report, "ledger_entries": entries}


def swap_covers_bars(schedule, frame, cfg) -> None:
    """Research runs may not treat an unfetched rollover as zero carry."""
    start = frame.timestamp.iloc[0]
    require_swap_coverage(
        schedule, start, frame.timestamp.iloc[-1] + pd.Timedelta(seconds=cfg.bar_seconds)
    )


def run_command(args) -> dict:
    if args.command == "paper-status":
        return paper_status(args.database)
    if args.command == "compare":
        candidates = [load_settings(path) for path in args.config]
        frame = read_bars(args.data, candidates[0])
        swap_schedule = (
            read_swap_schedule(args.swap_data, candidates[0]) if args.swap_data else None
        )
        swap_covers_bars(swap_schedule, frame, candidates[0])
        return save_comparison(
            frame,
            candidates,
            args.output,
            fold_count=args.folds,
            stress_multiplier=args.stress_multiplier,
            swap_schedule=swap_schedule,
        )
    if args.command == "merge-bars":
        cfg = load_settings(args.config)
        inputs = [read_bars(path, cfg) for path in args.input]
        frame = merge_bars(inputs, cfg)
        write_bars(frame, args.output)
        return {
            "output": str(args.output),
            "inputs": [str(path) for path in args.input],
            "bars": len(frame),
            "overlapping_bars": sum(len(item) for item in inputs) - len(frame),
            "data_start": frame.timestamp.iloc[0].isoformat(),
            "data_end": frame.timestamp.iloc[-1].isoformat(),
            "data_quality": interval_gap_report(frame, cfg),
        }
    cfg = load_settings(args.config)
    swap_schedule = (
        read_swap_schedule(args.swap_data, cfg) if getattr(args, "swap_data", None) else None
    )
    if args.command == "sample":
        write_bars(sample_bars(cfg), args.output)
        return {"output": str(args.output), "synthetic": True}
    if args.command in {"backtest", "evaluate"}:
        frame = read_bars(args.data, cfg)
        swap_covers_bars(swap_schedule, frame, cfg)
    if args.command == "backtest":
        return save_run(
            frame,
            cfg,
            args.output,
            swap_schedule=swap_schedule,
        )
    if args.command == "evaluate":
        return save_evaluation(
            frame,
            cfg,
            args.output,
            fold_count=args.folds,
            stress_multiplier=args.stress_multiplier,
            swap_schedule=swap_schedule,
        )
    with httpx.Client(follow_redirects=False) as client:
        api = GmoPublic(client)
        if args.command == "fetch-fx":
            if args.output.exists():
                raise FileExistsError(f"{args.output} already exists; choose a new output path")
            frame = api.candles(cfg, args.start, args.end)
            write_bars(frame, args.output)
            return {"output": str(args.output), "bars": len(frame), "source": "GMO public API"}
        if args.command == "fetch-swap":
            if args.output.exists():
                raise FileExistsError(f"{args.output} already exists; choose a new output path")
            frame = GmoSwapCalendar(client).history(cfg, args.start, args.end)
            validate_swap_schedule(frame, cfg)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            # Exclusive create: an earlier download is evidence, not a cache.
            with args.output.open("x", encoding="utf-8", newline="") as handle:
                frame.to_csv(handle, index=False)
            return {
                "output": str(args.output),
                "events": len(frame),
                "days": int(frame.days.sum()),
                "source": "GMO swap calendar",
            }
        if cfg.market != "fx":
            raise ValueError("paper-step currently supports FX only")
        api.validate_rules(cfg)
        now = datetime.now(UTC)
        today = trading_date(now)
        frame = api.candles(cfg, today - timedelta(days=7), today, now)
        write_bars(frame, args.cache, overwrite=True)
        quote = api.quote(cfg.symbol)
        return paper_step(frame, quote, cfg, args.database, swap_schedule=swap_schedule)


def main() -> int:
    # Ledger text is Japanese; keep piped output UTF-8 instead of the Windows code page.
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
    args = parser().parse_args()
    try:
        result = execute(args)
    except (ValueError, OSError, httpx.HTTPError, sqlite3.Error, KeyError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
