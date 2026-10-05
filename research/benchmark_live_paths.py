"""Measure registered watchdog/dispatch paths in isolated synthetic test workspaces."""

import argparse
import ctypes
import json
import platform
import socket
import sqlite3
import sys
import tempfile
import time
from contextlib import ExitStack
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from benchmark_journal_archive import build
from benchmark_retained_history import samples

from trading.order_runtime import OrderRuntimeError
from trading.private_operations import Alert, PrivateOperations, _body, _hash
from trading.segmented_journal import SegmentedEventJournal


class TrialClock:
    """Freeze setup time; optionally include real elapsed time during measurements."""

    def __init__(self):
        self._wall = datetime(2026, 10, 4, tzinfo=UTC)
        self._mono = 0.0
        self.started = None

    def elapsed(self):
        return 0.0 if self.started is None else time.perf_counter() - self.started

    @property
    def wall(self):
        return self._wall + timedelta(seconds=self.elapsed())

    @wall.setter
    def wall(self, value):
        if self.started is not None:
            raise RuntimeError("measurement_clock_already_started")
        self._wall = value

    @property
    def mono(self):
        return self._mono + self.elapsed()

    def advance(self, seconds):
        if self.started is None:
            self._wall += timedelta(seconds=seconds)
            self._mono += seconds
        else:
            time.sleep(seconds)


def seed_alerts(monitor, records, now):
    """Bulk-stage fully validated sent test alerts, retaining the real monitor binding."""
    body = _body(Alert(kind="private_notification_test", created_at=now))
    digest = _hash(body)
    with monitor._store(write=True) as conn:
        state = monitor._verify(conn)[0]
        if state.alert_count:
            raise RuntimeError("synthetic_monitor_must_be_empty")
        state = state.model_copy(update={"alert_count": records})
        for _ in range(records):
            monitor._verify_delivery(
                {
                    "attempts": 1,
                    "last_attempt_at": now.isoformat(),
                    "submitted_at": now.isoformat(),
                    "acknowledged_at": None,
                    "resolved_at": None,
                    "error": None,
                },
                monitor._parse_alert(body, state).created_at,
            )
        conn.executemany(
            "INSERT INTO alerts(id,body,digest,last_attempt_at,attempts,submitted_at) "
            "VALUES(?,?,?,?,1,?)",
            [(i, body, digest, now.isoformat(), now.isoformat()) for i in range(1, records + 1)],
        )
        while state.alert_count - state.archived_alerts > 1000:
            state = monitor._archive_closed(conn, state, now)
        monitor._write(conn, state)
        conn.commit()


def measured(operation):
    started = time.perf_counter()
    try:
        result = operation()
    except OrderRuntimeError as error:
        return time.perf_counter() - started, None, type(error).__name__
    return time.perf_counter() - started, result, None


def measure(segments, alerts, clock_mode):
    # Reuse the exercised composition fixtures. The dev dependency group is required.
    tests = Path(__file__).resolve().parents[1] / "tests"
    sys.path.insert(0, str(tests))
    try:
        from test_account_guard import account, quote
        from test_live_operations import setup as setup_operations
        from test_live_operations import unbound
        from test_order_runtime import runtime, stored, transport
    finally:
        sys.path.remove(str(tests))

    forbidden_attempts = []

    def forbidden(*args, **kwargs):
        forbidden_attempts.append(True)
        raise RuntimeError("real_boundary_forbidden")

    with tempfile.TemporaryDirectory(prefix="trading-live-path-bench-") as temporary:
        with ExitStack() as stack:
            stack.enter_context(patch.object(ctypes, "WinDLL", forbidden, create=True))
            stack.enter_context(patch.object(socket, "socket", forbidden))
            stack.enter_context(patch("test_private_sync.Clock", TrialClock))
            setup = setup_operations.__wrapped__(unbound.__wrapped__(Path(temporary)))
            values, live, monitor, order, _ = next(setup)
            stack.callback(setup.close)
            clock, workspace = values[0], values[5]
            workspace.journal = build(
                workspace.journal.path.parent, segments, 1024, existing=workspace.journal
            )
            workspace.control.update(
                workspace.control.snapshot()["owner"], journal=workspace.journal
            )
            seed_alerts(monitor, alerts, clock.wall)
            history = workspace.journal.check_history()
            implementation_sha256 = live[3].activation_context()["implementation_sha256"]
            backend, vault, reference = stored((values, live, monitor, order, []))
            live[3].update_account(account(clock.wall), quote(clock.wall), now=clock.wall)
            workspace.control.update(workspace.control.snapshot()["owner"], success=True)
            assert monitor.watchdog(send=lambda *a: None)["conditions"] == []

            counts = {"journal_checks": 0, "monitor_archive_checks": 0}
            original_journal = SegmentedEventJournal.check_history
            original_monitor = PrivateOperations._verify_archives

            def counted_journal(self, *args, **kwargs):
                counts["journal_checks"] += 1
                return original_journal(self, *args, **kwargs)

            def counted_monitor(self, *args, **kwargs):
                counts["monitor_archive_checks"] += 1
                return original_monitor(self, *args, **kwargs)

            stack.enter_context(
                patch.object(SegmentedEventJournal, "check_history", counted_journal)
            )
            stack.enter_context(
                patch.object(PrivateOperations, "_verify_archives", counted_monitor)
            )
            if clock_mode == "elapsed":
                clock.started = time.perf_counter()

            watchdog_seconds, watched, watchdog_error = measured(
                lambda: monitor.watchdog(send=lambda *a: None)
            )
            watchdog_counts = dict(counts)
            calls = []
            dispatch_stage = "runtime_init"

            def dispatch():
                nonlocal dispatch_stage
                runner = runtime((values, live, monitor, order, []))
                dispatch_stage = "review_context"
                current = quote(clock.wall)
                context = runner.context(order.client_id, quote=current)
                dispatch_stage = "dispatch"
                return runner.dispatch(
                    order.client_id,
                    expected_sha256=context["checkpoint_sha256"],
                    credential_reference=reference,
                    quote=current,
                    order_permission_confirmed=True,
                    vault=vault,
                    transport=transport(calls, live),
                )

            dispatch_seconds, receipt, dispatch_error = measured(dispatch)
            dispatch_counts = {key: counts[key] - watchdog_counts[key] for key in counts}
            if len(calls) > 1 or (receipt is not None and len(calls) != 1):
                raise RuntimeError("synthetic_dispatch_invariant_failed")
            if values[3].reads:
                raise RuntimeError("unexpected_read_credential_access")
            if forbidden_attempts:
                raise RuntimeError("real_boundary_attempted")
            if clock_mode == "frozen" and (dispatch_error is not None or receipt is None):
                raise RuntimeError("frozen_fixture_dispatch_refused")
            return {
                "segments": segments,
                "journal_records": history["archived_records"],
                "journal_body_bytes": history["archived_bytes"],
                "alerts": alerts,
                "clock_mode": clock_mode,
                "implementation_sha256": implementation_sha256,
                "watchdog_seconds": watchdog_seconds,
                "watchdog_error_type": watchdog_error,
                "watchdog_conditions": watched["conditions"] if watched else None,
                "watchdog_scan_counts": watchdog_counts,
                "dispatch_seconds": dispatch_seconds,
                "dispatch_error_type": dispatch_error,
                "dispatch_last_stage": dispatch_stage,
                "dispatch_scan_counts": dispatch_counts,
                "mock_http_requests": len(calls),
                "mock_order_accepted": receipt is not None,
                "memory_order_credential_reads": len(backend.reads),
                "real_network_or_native_credentials": False,
                "freshness_limits": {
                    "sync_seconds": 30,
                    "watchdog_seconds": 40,
                    "account_quote_seconds": 60,
                },
            }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--segments", nargs="+", type=int, default=[500, 2500, 5000])
    parser.add_argument("--alerts", nargs="+", type=int, default=[10_000, 50_000, 100_000])
    parser.add_argument(
        "--clock-mode", nargs="+", choices=("frozen", "elapsed"), default=["frozen", "elapsed"]
    )
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if (
        len(args.segments) != len(args.alerts)
        or not 1 <= args.samples <= 3
        or any(not 1 <= n <= 5000 for n in args.segments)
        or any(not 1000 <= n <= 100_000 for n in args.alerts)
        or len(set(args.segments)) != len(args.segments)
        or len(set(args.clock_mode)) != len(args.clock_mode)
        or sum(args.segments) * 1024 * args.samples * len(args.clock_mode) > 50_000_000
    ):
        parser.error("bounded paired sizes and unique clock modes required")
    if args.output.exists():
        parser.error("output already exists; choose a new artifact path")
    result = {
        "started_at": datetime.now(UTC).isoformat(),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "sqlite": sqlite3.sqlite_version,
        "fixture": "Test composition fixtures; retained BEGIN/HEARTBEAT/ACK/END and sent test "
        "alerts; real local stores, bindings and checks; memory credentials and MockTransport",
        "limitations": "Warm local synthetic paths. Frozen mode excludes elapsed time from "
        "freshness checks; elapsed mode includes it but no concurrent sync refresh. Real broker, "
        "Windows vault, cash/fill history and OS scheduling are not measured. "
        "No production deadline guarantee.",
        "measurements": [
            {
                "clock_mode": mode,
                "alerts": alerts,
                **samples(
                    lambda size, alerts=alerts, mode=mode: measure(size, alerts, mode),
                    segments,
                    args.samples,
                ),
            }
            for segments, alerts in zip(args.segments, args.alerts, strict=True)
            for mode in args.clock_mode
        ],
    }
    result["finished_at"] = datetime.now(UTC).isoformat()
    with args.output.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"artifact": str(args.output), "finished_at": result["finished_at"]}))
    return int(any(item["failed_samples"] for item in result["measurements"]))


if __name__ == "__main__":
    raise SystemExit(main())
