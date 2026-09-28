import argparse
import json
import sqlite3
import sys
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import httpx
import pandas as pd

from trading.backtest import save_run
from trading.config import load_settings
from trading.data import (
    describe_data_artifact,
    lineage_path,
    merge_bars,
    prepare_data_lineage,
    publish_new_file,
    read_bars,
    sample_bars,
    write_bars,
)
from trading.evaluation import interval_gap_report, save_comparison, save_evaluation
from trading.gmo import PUBLIC_URL, GmoPublic, GmoSwapCalendar, trading_date
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
from trading.provenance import runtime_versions, source_sha256
from trading.swap import read_swap_schedule, require_swap_coverage, validate_swap_schedule

LEDGER = Path("runs/ledger.sqlite")
# Commands whose results are research trials; each must be counted against a hypothesis.
TRIAL_COMMANDS = ("backtest", "evaluate", "compare")
FETCH_CHECKPOINT_VERSION = 1


def _commit_new_file(frame: pd.DataFrame, target: Path, temporary: Path) -> None:
    """Write completely before publishing `target`, and never replace evidence."""
    temporary = temporary.with_name(f"{temporary.stem}.{uuid.uuid4().hex}{temporary.suffix}")
    write_bars(frame, temporary, overwrite=True)
    try:
        publish_new_file(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)


def _publish_data_artifact(
    frame: pd.DataFrame,
    output: Path,
    temporary: Path,
    lineage_record: dict,
) -> tuple[Path, dict]:
    """Stage data and lineage completely, then publish both or roll back both."""
    metadata = lineage_path(output)
    if output.exists() or metadata.exists():
        existing = output if output.exists() else metadata
        raise FileExistsError(f"{existing} already exists; choose a new output path")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = temporary.with_name(f"{temporary.stem}.{uuid.uuid4().hex}{temporary.suffix}")
    write_bars(frame, temporary, overwrite=True)
    metadata, lineage_temporary, payload = prepare_data_lineage(
        output,
        temporary,
        lineage_record,
    )
    output_published = False
    lineage_published = False
    try:
        publish_new_file(temporary, output)
        output_published = True
        publish_new_file(lineage_temporary, metadata)
        lineage_published = True
    except BaseException:
        if lineage_published:
            metadata.unlink(missing_ok=True)
        if output_published:
            output.unlink(missing_ok=True)
        raise
    finally:
        temporary.unlink(missing_ok=True)
        lineage_temporary.unlink(missing_ok=True)
    return metadata, payload


def _require_trading_date(frame: pd.DataFrame, expected: date) -> None:
    actual = {trading_date(value.to_pydatetime()) for value in frame.timestamp}
    if actual != {expected}:
        raise ValueError(
            f"fetch checkpoint for {expected} contains bars from "
            f"{sorted(day.isoformat() for day in actual)}"
        )


def _fetch_fx(api: GmoPublic, cfg, start: date, end: date, output: Path) -> dict:
    """Fetch date-sized parts so a later invocation can resume after interruption."""
    if output.exists():
        raise FileExistsError(f"{output} already exists; choose a new output path")
    if lineage_path(output).exists():
        raise FileExistsError(f"{lineage_path(output)} already exists; choose a new output path")
    checkpoint = output.with_name(f"{output.name}.fetch-fx")
    manifest = checkpoint / "request.json"
    request = {
        "version": FETCH_CHECKPOINT_VERSION,
        "market": cfg.market,
        "symbol": cfg.symbol,
        "bar_seconds": cfg.bar_seconds,
        "start": start.isoformat(),
        "end": end.isoformat(),
    }
    if manifest.exists():
        try:
            saved_request = json.loads(manifest.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"invalid fetch checkpoint {manifest}: {exc}") from exc
        if saved_request != request:
            raise ValueError(
                f"fetch checkpoint {checkpoint} belongs to a different request; "
                "use the original arguments or a new output path"
            )
    else:
        if checkpoint.exists() and any(checkpoint.iterdir()):
            raise ValueError(f"fetch checkpoint {checkpoint} has no request metadata")
        checkpoint.mkdir(parents=True, exist_ok=True)
        with manifest.open("x", encoding="utf-8") as handle:
            json.dump(request, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")

    frames = []
    empty_dates = []
    resumed_dates = 0
    fetched_dates = 0
    for offset in range((end - start).days + 1):
        day = start + timedelta(days=offset)
        part = checkpoint / f"{day.isoformat()}.parquet"
        empty = checkpoint / f"{day.isoformat()}.empty"
        if part.exists() and empty.exists():
            raise ValueError(f"fetch checkpoint has conflicting records for {day}")
        if part.exists():
            frame = read_bars(part, cfg)
            _require_trading_date(frame, day)
            frames.append(frame)
            resumed_dates += 1
            continue
        if empty.exists():
            empty_dates.append(day.isoformat())
            resumed_dates += 1
            continue
        [(returned_day, frame)] = api.candle_days(cfg, day, day)
        if returned_day != day:
            raise ValueError(f"GMO collector returned {returned_day} while fetching {day}")
        if frame.empty:
            reason = frame.attrs.get("empty_reason")
            if reason == "provider_empty":
                empty.touch(exist_ok=False)
                empty_dates.append(day.isoformat())
            elif reason in {"incomplete_only", "current_trading_date"}:
                raise ValueError(
                    f"no completed candles are available for {day}; "
                    "retry after the trading date has closed"
                )
            else:
                raise ValueError(f"GMO collector returned an unexplained empty frame for {day}")
        else:
            if frame.attrs.get("trading_date_complete") is False:
                raise ValueError(
                    f"trading date {day} is still in progress; retry after its 06:00 JST rollover"
                )
            _require_trading_date(frame, day)
            temporary = checkpoint / f".{day.isoformat()}.tmp.parquet"
            _commit_new_file(frame, part, temporary)
            frames.append(frame)
        fetched_dates += 1

    if not frames:
        raise ValueError("no candles returned for requested trading dates")
    frame = merge_bars(frames, cfg)
    suffix = output.suffix
    temporary = output.with_name(f".{output.stem}.fetch-fx.tmp{suffix}")

    collection = {
        "provider": "GMO Coin",
        "endpoint": f"{PUBLIC_URL}/klines",
        "market": cfg.market,
        "symbol": cfg.symbol,
        "bar_seconds": cfg.bar_seconds,
        "requested_trading_dates": {"start": start.isoformat(), "end": end.isoformat()},
        "empty_trading_dates": sorted(empty_dates),
    }
    frame.attrs["lineage"] = {"collection": collection}
    data_quality = interval_gap_report(frame, cfg)
    lineage_record = {
        "operation": "fetch-fx",
        "config_sha256": cfg.fingerprint,
        "code_sha256": source_sha256(),
        "runtime": runtime_versions(),
        "inputs": [],
        "collection": collection,
        "transformation": {
            "raw_columns": [
                f"{side}_{column}"
                for side in ("bid", "ask")
                for column in ("open", "high", "low", "close")
            ],
            "derived_columns": {
                column: f"(bid_{column} + ask_{column}) / 2"
                for column in ("open", "high", "low", "close")
            },
            "filters": ["discard bars whose end time is after collection time"],
        },
        "summary": {
            "rows": len(frame),
            "data_start": frame.timestamp.iloc[0].isoformat(),
            "data_end": frame.timestamp.iloc[-1].isoformat(),
            "fetched_dates": fetched_dates,
            "resumed_dates": resumed_dates,
            "data_quality": data_quality,
        },
    }
    lineage, _ = _publish_data_artifact(
        frame,
        output,
        temporary,
        lineage_record,
    )

    # Remove only the files owned by this exact, validated checkpoint.
    for day in (start + timedelta(days=offset) for offset in range((end - start).days + 1)):
        (checkpoint / f"{day.isoformat()}.parquet").unlink(missing_ok=True)
        (checkpoint / f"{day.isoformat()}.empty").unlink(missing_ok=True)
        (checkpoint / f".{day.isoformat()}.tmp.parquet").unlink(missing_ok=True)
    manifest.unlink(missing_ok=True)
    try:
        checkpoint.rmdir()
    except OSError:
        pass
    return {
        "output": str(output),
        "bars": len(frame),
        "source": "GMO public API",
        "fetched_dates": fetched_dates,
        "resumed_dates": resumed_dates,
        "lineage": str(lineage),
        "data_quality": data_quality,
    }


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
        input_artifacts = [
            item.attrs.get("artifact") or describe_data_artifact(path)
            for path, item in zip(args.input, inputs, strict=True)
        ]
        reported_empty_dates = {
            day
            for item in inputs
            for day in (item.attrs.get("lineage") or {})
            .get("collection", {})
            .get("empty_trading_dates", [])
        }
        frame = merge_bars(inputs, cfg)
        present_dates = {
            trading_date(timestamp.to_pydatetime()).isoformat() for timestamp in frame.timestamp
        }
        superseded_empty_dates = sorted(reported_empty_dates & present_dates)
        empty_dates = sorted(reported_empty_dates - present_dates)
        collection = {
            "provider": "derived from input artifacts",
            "market": cfg.market,
            "symbol": cfg.symbol,
            "bar_seconds": cfg.bar_seconds,
            "empty_trading_dates": empty_dates,
            "superseded_empty_trading_dates": superseded_empty_dates,
        }
        frame.attrs["lineage"] = {"collection": collection}
        data_quality = interval_gap_report(frame, cfg)
        overlapping = sum(len(item) for item in inputs) - len(frame)
        lineage_record = {
            "operation": "merge-bars",
            "config_sha256": cfg.fingerprint,
            "code_sha256": source_sha256(),
            "runtime": runtime_versions(),
            "inputs": input_artifacts,
            "collection": collection,
            "transformation": {
                "ordering": "timestamp ascending",
                "overlap_policy": "content must match; keep the first input copy",
                "overlapping_bars": overlapping,
            },
            "summary": {
                "rows": len(frame),
                "data_start": frame.timestamp.iloc[0].isoformat(),
                "data_end": frame.timestamp.iloc[-1].isoformat(),
                "data_quality": data_quality,
            },
        }
        suffix = args.output.suffix
        temporary = args.output.with_name(f".{args.output.stem}.merge-bars.tmp{suffix}")
        lineage, _ = _publish_data_artifact(
            frame,
            args.output,
            temporary,
            lineage_record,
        )
        return {
            "output": str(args.output),
            "inputs": [str(path) for path in args.input],
            "bars": len(frame),
            "overlapping_bars": overlapping,
            "data_start": frame.timestamp.iloc[0].isoformat(),
            "data_end": frame.timestamp.iloc[-1].isoformat(),
            "data_quality": data_quality,
            "lineage": str(lineage),
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
            api.validate_candle_range(cfg, args.start, args.end)
            return _fetch_fx(api, cfg, args.start, args.end, args.output)
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
