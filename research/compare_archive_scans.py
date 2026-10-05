"""Pair old/current archive verification on one synthetic read-only SQLite snapshot."""

import argparse
import ast
import copy
import ctypes
import hashlib
import json
import platform
import socket
import sqlite3
import statistics
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import benchmark_operations_archive as benchmark

from trading import private_operations as operations

BASELINE = "daf0d23"
CHANGED_METHODS = {"_verify_delivery", "_verify_delivery_values", "_verify_archives"}


def _class(tree):
    return next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "PrivateOperations"
    )


def _baseline():
    command = ["git", "show", f"{BASELINE}:src/trading/private_operations.py"]
    raw = subprocess.check_output(command)
    current = Path(operations.__file__).read_bytes()
    old_tree, current_tree = ast.parse(raw), ast.parse(current)
    nodes = {node.name: node for node in _class(old_tree).body if isinstance(node, ast.FunctionDef)}
    for tree in (old_tree, current_tree):
        owner = _class(tree)
        owner.body = [
            node
            for node in owner.body
            if not isinstance(node, ast.FunctionDef) or node.name not in CHANGED_METHODS
        ]
    if ast.dump(old_tree, include_attributes=False) != ast.dump(
        current_tree, include_attributes=False
    ):
        raise RuntimeError("comparison_dependencies_changed")
    namespace = dict(operations.__dict__)
    for name in ("_verify_delivery", "_verify_archives"):
        node = copy.deepcopy(nodes[name])
        node.decorator_list = []
        unit = ast.Module(body=[node], type_ignores=[])
        exec(compile(unit, f"{BASELINE}:src/trading/private_operations.py", "exec"), namespace)
    legacy = type(
        "BaselineArchiveScan",
        (operations.PrivateOperations,),
        {
            "_verify_archives": namespace["_verify_archives"],
            "_verify_delivery": staticmethod(namespace["_verify_delivery"]),
        },
    )
    return legacy, {
        "baseline_commit": subprocess.check_output(["git", "rev-parse", BASELINE]).decode().strip(),
        "baseline_source_sha256": hashlib.sha256(raw).hexdigest(),
        "current_source_sha256": hashlib.sha256(current).hexdigest(),
        "dependencies_ast_equal": True,
    }


def compare(alerts, pairs):
    legacy_type, provenance = _baseline()
    original_status = operations.PrivateOperations.status
    evidence = {}

    def status(monitor, *args, **kwargs):
        if not evidence:
            legacy = object.__new__(legacy_type)
            legacy.__dict__.update(monitor.__dict__)
            before = hashlib.sha256(monitor.path.read_bytes()).hexdigest()
            trials = []
            with monitor._store() as conn:
                conn.execute("PRAGMA query_only=ON")
                state = monitor._verify(conn)[0]
                versions = {
                    "baseline": lambda: legacy._verify_archives(conn, state),
                    "current": lambda: monitor._verify_archives(conn, state),
                }
                expected = versions["baseline"]()
                if versions["current"]() != expected:
                    raise RuntimeError("comparison_warmup_result_changed")
                for pair in range(pairs):
                    order = ("baseline", "current") if pair % 2 == 0 else ("current", "baseline")
                    for version in order:
                        started = time.perf_counter()
                        actual = versions[version]()
                        elapsed = time.perf_counter() - started
                        if actual != expected:
                            raise RuntimeError("comparison_result_changed")
                        trials.append({"pair": pair + 1, "version": version, "seconds": elapsed})
            after = hashlib.sha256(monitor.path.read_bytes()).hexdigest()
            if before != after:
                raise RuntimeError("comparison_source_changed")
            summaries = {}
            for version in versions:
                values = [trial["seconds"] for trial in trials if trial["version"] == version]
                summaries[version] = {
                    "min": min(values),
                    "median": statistics.median(values),
                    "max": max(values),
                }
            evidence.update(
                {
                    "source_sha256_before": before,
                    "source_sha256_after": after,
                    "state_sha256": operations._hash(operations._body(state)),
                    "alerts": state.alert_count,
                    "archived_alerts": state.archived_alerts,
                    "returned_latest": expected.isoformat(),
                    "snapshot_query_only": True,
                    "trials": trials,
                    "timings_seconds": summaries,
                }
            )
        return original_status(monitor, *args, **kwargs)

    forbidden_attempts = []

    def forbidden(*args, **kwargs):
        forbidden_attempts.append(True)
        raise RuntimeError("real_boundary_forbidden")

    with (
        patch.object(operations.PrivateOperations, "status", status),
        patch.object(socket, "socket", forbidden),
        patch.object(ctypes, "WinDLL", forbidden, create=True),
    ):
        fixture = benchmark.measure(alerts)
    if forbidden_attempts:
        raise RuntimeError("real_boundary_attempted")
    if not evidence:
        raise RuntimeError("comparison_snapshot_missing")
    return {
        **provenance,
        **evidence,
        "fixture_checks": {
            key: value for key, value in fixture.items() if not key.endswith("_seconds")
        },
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--alerts", type=int, default=50_000)
    parser.add_argument("--pairs", type=int, default=8)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if not 1000 <= args.alerts <= 100_000 or not 2 <= args.pairs <= 16:
        parser.error("bounded alerts and paired samples required")
    if args.output.exists():
        parser.error("output already exists; choose a new artifact path")
    result = {
        "format": "synthetic-archive-scan-comparison-v1",
        "started_at": datetime.now(UTC).isoformat(),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "sqlite": sqlite3.sqlite_version,
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "limitations": "One warm synthetic store; alternating order; one SQLite read snapshot; "
        "pure archive verifier only, not dispatch/deadline/freshness validation; "
        "no production capacity guarantee. Unchanged dependencies checked by AST; "
        "historical verifier methods compiled from the fixed local Git baseline.",
        **compare(args.alerts, args.pairs),
        "finished_at": datetime.now(UTC).isoformat(),
    }
    with args.output.open("x", encoding="utf-8", newline="\n") as output:
        json.dump(result, output, indent=2)
        output.write("\n")
    print(json.dumps({"artifact": str(args.output), "timings_seconds": result["timings_seconds"]}))


if __name__ == "__main__":
    main()
