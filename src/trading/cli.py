import argparse
import json
import sqlite3
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

from trading.backtest import save_run
from trading.config import load_settings
from trading.data import read_bars, sample_bars, write_bars
from trading.evaluation import save_comparison, save_evaluation
from trading.gmo import GmoPublic
from trading.paper import paper_status, paper_step
from trading.swap import read_swap_schedule


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description="JPY backtests and local FX paper trading")
    sub = root.add_subparsers(dest="command", required=True)
    for command in ("sample", "backtest", "evaluate", "fetch-fx", "paper-step"):
        item = sub.add_parser(command)
        item.add_argument("--config", type=Path, required=True)
        if command in {"backtest", "evaluate", "paper-step"}:
            item.add_argument("--swap-data", type=Path)
        if command in {"sample", "fetch-fx"}:
            item.add_argument("--output", type=Path, required=True)
        if command == "fetch-fx":
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
    return root


def execute(args) -> dict:
    if args.command == "paper-status":
        return paper_status(args.database)
    if args.command == "compare":
        candidates = [load_settings(path) for path in args.config]
        frame = read_bars(args.data, candidates[0])
        swap_schedule = (
            read_swap_schedule(args.swap_data, candidates[0]) if args.swap_data else None
        )
        return save_comparison(
            frame,
            candidates,
            args.output,
            fold_count=args.folds,
            stress_multiplier=args.stress_multiplier,
            swap_schedule=swap_schedule,
        )
    cfg = load_settings(args.config)
    swap_schedule = (
        read_swap_schedule(args.swap_data, cfg) if getattr(args, "swap_data", None) else None
    )
    if args.command == "sample":
        write_bars(sample_bars(cfg), args.output)
        return {"output": str(args.output), "synthetic": True}
    if args.command == "backtest":
        return save_run(
            read_bars(args.data, cfg),
            cfg,
            args.output,
            swap_schedule=swap_schedule,
        )
    if args.command == "evaluate":
        return save_evaluation(
            read_bars(args.data, cfg),
            cfg,
            args.output,
            fold_count=args.folds,
            stress_multiplier=args.stress_multiplier,
            swap_schedule=swap_schedule,
        )
    with httpx.Client(follow_redirects=False) as client:
        api = GmoPublic(client)
        if args.command == "fetch-fx":
            frame = api.candles(cfg, args.start, args.end)
            write_bars(frame, args.output)
            return {"output": str(args.output), "bars": len(frame), "source": "GMO public API"}
        if cfg.market != "fx":
            raise ValueError("paper-step currently supports FX only")
        api.validate_rules(cfg)
        now = datetime.now(UTC)
        # GMO trading dates roll over at 06:00 JST, not at UTC or local midnight.
        trading_date = (now.astimezone(ZoneInfo("Asia/Tokyo")) - timedelta(hours=6)).date()
        frame = api.candles(cfg, trading_date - timedelta(days=7), trading_date, now)
        write_bars(frame, args.cache)
        quote = api.quote(cfg.symbol)
        return paper_step(frame, quote, cfg, args.database, swap_schedule=swap_schedule)


def main() -> int:
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
