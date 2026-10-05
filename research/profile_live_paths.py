"""Profile only measured synthetic watchdog/dispatch stages; profiler time is not latency."""

import argparse
import cProfile
import hashlib
import json
import platform
import pstats
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import benchmark_live_paths as benchmark


def profile(segments, alerts):
    stages = []
    original = benchmark.measured
    root = Path(__file__).resolve().parents[1]

    def measured(operation):
        profiler = cProfile.Profile()
        with profiler:
            outcome = original(operation)
        stats = pstats.Stats(profiler)
        functions = []
        for (filename, line, name), (primitive, calls, own, cumulative, _) in stats.stats.items():
            path = Path(filename)
            source = path.relative_to(root).as_posix() if path.is_relative_to(root) else filename
            functions.append(
                {
                    "source": source,
                    "line": line,
                    "function": name,
                    "primitive_calls": primitive,
                    "calls": calls,
                    "own_seconds": own,
                    "cumulative_seconds": cumulative,
                }
            )
        functions.sort(key=lambda row: (-row["own_seconds"], row["source"], row["line"]))
        stages.append(
            {
                "stage": "watchdog" if not stages else "dispatch",
                "profiled_elapsed_seconds": outcome[0],
                "error_type": outcome[2],
                "total_calls": stats.total_calls,
                "primitive_calls": stats.prim_calls,
                "total_own_seconds": stats.total_tt,
                "functions": functions,
            }
        )
        return outcome

    with patch.object(benchmark, "measured", measured):
        result = benchmark.measure(segments, alerts, "frozen")
    if len(stages) != 2:
        raise RuntimeError("profile_stage_count_invalid")
    for stage in stages:
        if sum(row["calls"] for row in stage["functions"]) != stage["total_calls"]:
            raise RuntimeError("profile_call_count_invalid")
    return {"measurement": result, "stages": stages}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--segments", type=int, default=500)
    parser.add_argument("--alerts", type=int, default=50_000)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if not 1 <= args.segments <= 5000 or not 1000 <= args.alerts <= 100_000:
        parser.error("bounded synthetic sizes required")
    if args.output.exists():
        parser.error("output already exists; choose a new artifact path")
    paths = [Path(__file__), Path(benchmark.__file__)]
    result = {
        "format": "synthetic-live-path-profile-v1",
        "started_at": datetime.now(UTC).isoformat(),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "sqlite": sqlite3.sqlite_version,
        "script_sha256": {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths
        },
        "clock_mode": "frozen",
        "fixture": "Registered synthetic stores, memory credentials, MockTransport; "
        "socket.socket and ctypes.WinDLL forbidden by benchmark.measure",
        "limitations": "cProfile overhead; frozen time; warm local I/O; single sample; "
        "setup excluded from profiles; no real broker, vault, toast, or tasks; "
        "cumulative function times overlap and must not be added",
        **profile(args.segments, args.alerts),
        "finished_at": datetime.now(UTC).isoformat(),
    }
    with args.output.open("x", encoding="utf-8", newline="\n") as output:
        json.dump(result, output, indent=2)
        output.write("\n")
    print(json.dumps({"artifact": str(args.output), "stages": len(result["stages"])}))


if __name__ == "__main__":
    main()
