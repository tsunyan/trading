"""Temporary synthetic monitor stores; no account, credentials, HTTP, tasks or desktop toast."""

import argparse
import json
import platform
import sqlite3
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from trading.private_operations import Alert, PrivateOperations, _body, _hash

NOW = datetime(2026, 10, 4, tzinfo=UTC)
VIEW = {
    "control": {
        "instance": "a" * 32,
        "phase": "READY",
        "revision": 0,
        "generation": 0,
        "sync_successes": 0,
    },
    "plan_sha256": "b" * 64,
    "reads": {"stopped": False, "reopen_required": False},
    "cash": {"halted": False},
    "journal": {"unacknowledged_records": [], "session_open": False, "rejected_frames": 0},
}


def timed(operation):
    start = time.perf_counter()
    result = operation()
    return time.perf_counter() - start, result


def measure(records):
    with tempfile.TemporaryDirectory(prefix="trading-monitor-bench-") as temporary:
        directory = Path(temporary)
        workspace = SimpleNamespace(
            status=lambda: VIEW,
            plan=SimpleNamespace(
                supervisor=SimpleNamespace(sync_interval_seconds=15, sync_timeout_seconds=35)
            ),
        )
        with patch("trading.private_operations.PrivateSyncWorkspace", return_value=workspace):
            monitor = PrivateOperations.create(directory, clock=lambda: NOW)
        body = _body(Alert(kind="private_notification_test", created_at=NOW))
        digest = _hash(body)
        with monitor._store(write=True) as conn:
            state = monitor._verify(conn)[0].model_copy(update={"alert_count": records})
            # Fully check generated rows, then bulk-stage them to avoid quadratic public appends.
            for _identity in range(1, records + 1):
                row = {
                    "attempts": 1,
                    "last_attempt_at": NOW.isoformat(),
                    "submitted_at": NOW.isoformat(),
                    "acknowledged_at": None,
                    "resolved_at": None,
                    "error": None,
                }
                monitor._verify_delivery(row, monitor._parse_alert(body, state).created_at)
            conn.executemany(
                "INSERT INTO alerts(id,body,digest,last_attempt_at,attempts,submitted_at) "
                "VALUES(?,?,?,?,1,?)",
                [
                    (i, body, digest, NOW.isoformat(), NOW.isoformat())
                    for i in range(1, records + 1)
                ],
            )
            while state.alert_count - state.archived_alerts > 1000:
                state = monitor._archive_closed(conn, state, NOW)
            monitor._write(conn, state)
            conn.commit()
        monitor._observe = lambda _: (VIEW, False)
        status_seconds, status = timed(monitor.status)
        open_seconds, _ = timed(lambda: PrivateOperations(directory, clock=lambda: NOW))
        full_seconds, audit = timed(monitor.audit_history)
        check_seconds, checked = timed(monitor.check)
        append_seconds, appended = timed(monitor.test_notification)
        notify_seconds, sent = timed(lambda: monitor.notify(send=lambda *a: None))
        assert checked["conditions"] == [] and audit["alerts"] == records
        assert appended["alert_count"] == records + 1 and sent["submitted"] == 1
        return {
            "alerts": records,
            "active_alerts": status["active_alerts"],
            "archive_count": status["archive_count"],
            "sqlite_bytes": monitor.path.stat().st_size,
            "status_seconds": status_seconds,
            "open_seconds": open_seconds,
            "full_audit_seconds": full_seconds,
            "check_seconds": check_seconds,
            "append_seconds": append_seconds,
            "notify_seconds": notify_seconds,
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--alerts", nargs="+", type=int, default=[10_000, 50_000])
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if any(not 1000 <= count <= 100_000 for count in args.alerts):
        parser.error("synthetic sizes must be between 1000 and 100000")
    result = {
        "date": NOW.date().isoformat(),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "sqlite": sqlite3.sqlite_version,
        "fixture": "Fully checked submitted test alerts, 1000 hot rows; synthetic READY view",
        "limitations": "One warm local sample per size. Archive bytes and delivery metadata "
        "still grow with history. Monitor-only timing, no production guarantee.",
        "measurements": [measure(count) for count in args.alerts],
    }
    output = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.write_text(output, encoding="utf-8", newline="\n")
    else:
        print(output, end="")


if __name__ == "__main__":
    main()
