"""Repeated retained-history measurements using only temporary synthetic stores."""

import argparse
import json
import platform
import sqlite3
import statistics
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from benchmark_journal_archive import measure as journal_measure
from benchmark_operations_archive import measure as monitor_measure


def samples(operation, size, count):
    """Keep every raw sample, including failures; do not average errors away."""
    results = []
    for index in range(count):
        started = datetime.now(UTC)
        stamp = time.perf_counter()
        print(
            json.dumps({"size": size, "sample": index + 1, "phase": "started"}),
            file=sys.stderr,
            flush=True,
        )
        try:
            result = operation(size)
        except Exception as error:
            results.append(
                {
                    "sample": index + 1,
                    "started_at": started.isoformat(),
                    "ok": False,
                    "error_type": type(error).__name__,
                }
            )
        else:
            results.append(
                {
                    "sample": index + 1,
                    "started_at": started.isoformat(),
                    "ok": True,
                    "measurement": result,
                }
            )
        results[-1]["total_seconds"] = time.perf_counter() - stamp
        print(
            json.dumps(
                {
                    "size": size,
                    "sample": index + 1,
                    "phase": "finished",
                    "ok": results[-1]["ok"],
                    "total_seconds": results[-1]["total_seconds"],
                }
            ),
            file=sys.stderr,
            flush=True,
        )
    successful = [item["measurement"] for item in results if item["ok"]]
    times = {
        key: {"min": min(values), "median": statistics.median(values), "max": max(values)}
        for key in (successful[0] if successful else {})
        if key.endswith("_seconds")
        for values in ([item[key] for item in successful],)
    }
    return {
        "size": size,
        "successful_samples": len(successful),
        "failed_samples": len(results) - len(successful),
        "timings_seconds": times,
        "samples": results,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--segments", nargs="+", type=int, default=[500, 2500, 5000])
    parser.add_argument("--records", type=int, default=1024)
    parser.add_argument("--alerts", nargs="+", type=int, default=[10_000, 50_000, 100_000])
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    if (
        not 1 <= args.samples <= 5
        or not 4 <= args.records <= 20_000
        or args.records % 2
        or any(not 1 <= size <= 10_000 for size in args.segments)
        or any(not 1000 <= size <= 100_000 for size in args.alerts)
        or args.samples * args.records * sum(args.segments) > 30_000_000
        or args.samples * sum(args.alerts) > 1_000_000
        or len(set(args.segments)) != len(args.segments)
        or len(set(args.alerts)) != len(args.alerts)
    ):
        parser.error("bounded, unique sizes and 1..5 samples required")
    if args.output is not None and args.output.exists():
        parser.error("output already exists; choose a new artifact path")
    started = datetime.now(UTC)
    result = {
        "measured_at": started.isoformat(),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "sqlite": sqlite3.sqlite_version,
        "records_per_segment": args.records,
        "samples_per_size": args.samples,
        "fixture": "Existing validated BEGIN/HEARTBEAT/ACK/END archive and submitted-alert "
        "builders; independent temporary stores for each sample; fixed 2026-10-04 data",
        "limitations": "Warm local I/O; no OS cache eviction or real broker traffic. "
        "Samples include full retained-byte checks, reopen and concurrent writes, "
        "but not the complete live dispatch or watchdog path. No production latency "
        "guarantee; summary timings describe successful samples only; errors retained.",
        "journal": [
            samples(lambda size: journal_measure(size, args.records), size, args.samples)
            for size in args.segments
        ],
        "monitor": [samples(monitor_measure, size, args.samples) for size in args.alerts],
    }
    result["finished_at"] = datetime.now(UTC).isoformat()
    body = json.dumps(result, indent=2) + "\n"
    if args.output is None:
        print(body, end="")
    else:
        with args.output.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(body)
        print(json.dumps({"artifact": str(args.output), "finished_at": result["finished_at"]}))
    return int(any(item["failed_samples"] for item in result["journal"] + result["monitor"]))


if __name__ == "__main__":
    raise SystemExit(main())
