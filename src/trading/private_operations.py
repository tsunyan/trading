"""Local Private-sync watchdog and durable Windows notification outbox. No credentials or HTTP."""

import argparse
import hashlib
import json
import os
import sqlite3
import subprocess
import time
import uuid
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import AwareDatetime, Field

from trading.broker_contracts import Contract
from trading.paper_runner import _process_lock
from trading.private_sync import PrivateSyncWorkspace
from trading.storage_init import new_storage_directory
from trading.stream_control import StreamControlError
from trading.windows_notify import send_toast

MAX_ALERTS = 10_000
CONDITIONS = frozenset(
    {
        "private_sync_stopped",
        "private_sync_owner_missing",
        "private_sync_stale",
        "private_reads_blocked",
        "private_cash_halted",
        "private_journal_unresolved",
        "private_sync_unavailable",
    }
)
KINDS = CONDITIONS | {"private_notification_test", "private_condition_cleared"}
SCHEMA = """
CREATE TABLE monitor (id INTEGER PRIMARY KEY CHECK(id=1),body TEXT NOT NULL,digest TEXT NOT NULL);
CREATE TABLE conditions (kind TEXT PRIMARY KEY);
CREATE TABLE alerts (
 id INTEGER PRIMARY KEY,body TEXT NOT NULL,digest TEXT NOT NULL,
 acknowledged_at TEXT,resolved_at TEXT,last_attempt_at TEXT,attempts INTEGER NOT NULL DEFAULT 0,
 submitted_at TEXT,error TEXT
);
"""


class OperationsError(ValueError):
    """Fixed local reason codes only."""


class MonitorState(Contract):
    version: Literal[1] = 1
    instance: str = Field(pattern=r"^[a-f0-9]{32}$")
    workspace: str = Field(min_length=1, max_length=2048)
    control_instance: str = Field(pattern=r"^[a-f0-9]{32}$")
    plan_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    lock_device: str = Field(pattern=r"^[0-9]+$")
    lock_inode: str = Field(pattern=r"^[1-9][0-9]*$")
    created_at: AwareDatetime
    last_check_at: AwareDatetime | None = None
    last_progress_at: AwareDatetime | None = None
    generation: int | None = Field(default=None, strict=True, ge=0)
    sync_successes: int = Field(default=0, strict=True, ge=0)
    stale_seconds: int = Field(strict=True, ge=30, le=86400)
    retry_seconds: int = Field(strict=True, ge=1, le=86400)
    alert_count: int = Field(default=0, strict=True, ge=0, le=MAX_ALERTS)


class Alert(Contract):
    kind: str
    created_at: AwareDatetime
    revision: int | None = Field(default=None, strict=True, ge=0)
    condition: str | None = None


def _body(model):
    return json.dumps(model.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def _hash(body):
    return hashlib.sha256(body.encode()).hexdigest()


def _now(clock):
    try:
        stamp = clock()
        if not isinstance(stamp, datetime) or stamp.utcoffset() is None:
            raise ValueError
        return stamp.astimezone(UTC)
    except Exception:
        raise OperationsError("operations_clock_invalid") from None


class PrivateOperations:
    """Independent diagnostics. Nothing here recovers, reconnects, books or sends orders."""

    def __init__(self, directory, *, clock=lambda: datetime.now(UTC), monotonic=time.monotonic):
        self.directory = Path(directory).resolve()
        self.storage = self.directory / "private-operations"
        self.path = self.storage / "operations.sqlite"
        self.lock_path = self.storage / "operations.lock"
        self.clock, self.monotonic = clock, monotonic
        self._instance = None
        with self._store() as conn:
            self._instance = self._verify(conn)[0].instance

    @classmethod
    def create(cls, directory, *, stale_seconds=120, retry_seconds=300, **clocks):
        directory = Path(directory).resolve()
        workspace = PrivateSyncWorkspace(directory)
        view = workspace.status()
        minimum = (
            workspace.plan.supervisor.sync_interval_seconds
            + workspace.plan.supervisor.sync_timeout_seconds
        )
        if type(stale_seconds) is not int or stale_seconds < minimum:
            raise OperationsError("operations_stale_threshold_invalid")
        storage = directory / "private-operations"
        with new_storage_directory(
            storage, ("operations.sqlite-journal", "operations.sqlite", "operations.lock")
        ):
            with (storage / "operations.lock").open("xb") as lock:
                lock.write(b"\0")
                lock.flush()
                os.fsync(lock.fileno())
                identity = os.fstat(lock.fileno())
            state = MonitorState(
                instance=uuid.uuid4().hex,
                workspace=str(directory),
                control_instance=view["control"]["instance"],
                plan_sha256=view["plan_sha256"],
                lock_device=str(identity.st_dev),
                lock_inode=str(identity.st_ino),
                created_at=_now(clocks.get("clock", lambda: datetime.now(UTC))),
                stale_seconds=stale_seconds,
                retry_seconds=retry_seconds,
            )
            with closing(sqlite3.connect(storage / "operations.sqlite")) as conn:
                conn.execute("PRAGMA synchronous=FULL")
                conn.executescript(SCHEMA)
                body = _body(state)
                conn.execute("INSERT INTO monitor VALUES(1,?,?)", (body, _hash(body)))
                conn.commit()
            return cls(directory, **clocks)

    @contextmanager
    def _store(self, *, write=False):
        try:
            with closing(
                sqlite3.connect(self.path.as_uri() + "?mode=rw", uri=True, timeout=1)
            ) as conn:
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA synchronous=FULL")
                conn.execute("BEGIN IMMEDIATE" if write else "BEGIN")
                yield conn
        except sqlite3.Error:
            raise OperationsError("operations_storage_unavailable") from None

    def _verify(self, conn):
        try:
            rows = conn.execute("SELECT * FROM monitor LIMIT 2").fetchall()
            if len(rows) != 1 or rows[0]["id"] != 1 or len(rows[0]["body"].encode()) > 4096:
                raise ValueError
            raw = rows[0]["body"]
            state = MonitorState.model_validate_json(raw)
            if (
                _body(state) != raw
                or _hash(raw) != rows[0]["digest"]
                or state.workspace != str(self.directory)
                or (self._instance is not None and state.instance != self._instance)
                or (state.last_check_at is not None and state.last_check_at < state.created_at)
                or (
                    state.last_progress_at is not None
                    and (
                        state.last_check_at is None or state.last_progress_at > state.last_check_at
                    )
                )
            ):
                raise ValueError
            conditions = {row[0] for row in conn.execute("SELECT kind FROM conditions LIMIT 8")}
            if not conditions <= CONDITIONS:
                raise ValueError
            rows = conn.execute(
                "SELECT * FROM alerts ORDER BY id LIMIT ?", (MAX_ALERTS + 1,)
            ).fetchall()
            if len(rows) != state.alert_count:
                raise ValueError
            for index, row in enumerate(rows, 1):
                raw = row["body"]
                if row["id"] != index or len(raw.encode()) > 512 or _hash(raw) != row["digest"]:
                    raise ValueError
                alert = Alert.model_validate_json(raw)
                if (
                    _body(alert) != raw
                    or alert.kind not in KINDS
                    or alert.created_at < state.created_at
                ):
                    raise ValueError
                if (alert.kind == "private_condition_cleared") != (alert.condition in CONDITIONS):
                    raise ValueError
                if type(row["attempts"]) is not int or not 0 <= row["attempts"] < 2**63:
                    raise ValueError
                if bool(row["attempts"]) != (row["last_attempt_at"] is not None):
                    raise ValueError
                for name in ("acknowledged_at", "resolved_at", "last_attempt_at", "submitted_at"):
                    if row[name] is not None:
                        stamp = datetime.fromisoformat(row[name])
                        if stamp.utcoffset() is None or stamp < alert.created_at:
                            raise ValueError
                if row["submitted_at"] is not None and row["last_attempt_at"] is None:
                    raise ValueError
                if row["error"] not in {None, "notification_failed"}:
                    raise ValueError
            return state, conditions, rows
        except (ValueError, TypeError, KeyError):
            raise OperationsError("operations_integrity_failed") from None

    @contextmanager
    def _owned(self):
        with self._store() as conn:
            state, _, _ = self._verify(conn)

        def check_lock():
            try:
                identity = self.lock_path.stat()
                if (str(identity.st_dev), str(identity.st_ino)) != (
                    state.lock_device,
                    state.lock_inode,
                ):
                    raise ValueError
            except (OSError, ValueError):
                raise OperationsError("operations_lock_changed") from None

        check_lock()
        with _process_lock(self.lock_path) as owned:
            check_lock()
            if not owned:
                raise OperationsError("operations_busy")
            with self._store(write=True) as conn:
                try:
                    current, conditions, rows = self._verify(conn)
                    now = _now(self.clock)
                    latest = current.last_check_at or current.created_at
                    for row in rows:
                        latest = max(latest, Alert.model_validate_json(row["body"]).created_at)
                        for name in (
                            "acknowledged_at",
                            "resolved_at",
                            "last_attempt_at",
                            "submitted_at",
                        ):
                            if row[name] is not None:
                                latest = max(latest, datetime.fromisoformat(row[name]))
                    if now < latest:
                        raise OperationsError("operations_clock_invalid")
                    yield conn, current, conditions, rows, now
                    conn.commit()
                except BaseException:
                    conn.rollback()
                    raise

    @staticmethod
    def _write(conn, state):
        body = _body(state)
        conn.execute("UPDATE monitor SET body=?,digest=? WHERE id=1", (body, _hash(body)))

    def _alert(self, conn, state, kind, now, *, revision=None, condition=None):
        if state.alert_count >= MAX_ALERTS:
            raise OperationsError("operations_alert_capacity")
        alert = Alert(kind=kind, created_at=now, revision=revision, condition=condition)
        body = _body(alert)
        updated = state.model_copy(update={"alert_count": state.alert_count + 1})
        conn.execute(
            "INSERT INTO alerts(id,body,digest) VALUES(?,?,?)",
            (updated.alert_count, body, _hash(body)),
        )
        return updated

    def _observe(self, state):
        # Reload around legitimate rotation/concurrent updates. A failed sample
        # never clears conditions or guesses state from filenames or old views.
        for attempt in range(3):
            try:
                workspace = PrivateSyncWorkspace(self.directory)
                try:
                    with workspace.control.ownership():
                        view, owner_present = workspace.status(), False
                except StreamControlError as error:
                    if str(error) != "stream_owner_busy":
                        raise
                    view, owner_present = workspace.status(), True
                if (view["control"]["instance"], view["plan_sha256"]) != (
                    state.control_instance,
                    state.plan_sha256,
                ):
                    raise ValueError
                if view["control"]["revision"] != workspace.control.snapshot()["revision"]:
                    raise ValueError
                if (
                    state.generation is not None
                    and view["control"]["generation"] < state.generation
                ) or view["control"]["sync_successes"] < state.sync_successes:
                    raise ValueError
                return view, owner_present
            except (ValueError, OSError):
                if attempt < 2:
                    time.sleep(0.05)
        return None, None

    def check(self):
        with self._owned() as (conn, state, previous, _, now):
            view, owner = self._observe(state)
            active, revision = set(), None
            if view is None:
                active = previous | {"private_sync_unavailable"}
            else:
                control, reads, journal = view["control"], view["reads"], view["journal"]
                revision = control["revision"]
                if control["phase"] == "STOPPED":
                    active.add("private_sync_stopped")
                if control["phase"] == "RUNNING":
                    if not owner:
                        active.add("private_sync_owner_missing")
                    if state.generation != control["generation"] or state.last_progress_at is None:
                        state = state.model_copy(update={"last_progress_at": now})
                    elif control["sync_successes"] > state.sync_successes:
                        state = state.model_copy(update={"last_progress_at": now})
                    if (now - state.last_progress_at).total_seconds() >= state.stale_seconds:
                        active.add("private_sync_stale")
                else:
                    state = state.model_copy(update={"last_progress_at": None})
                state = state.model_copy(
                    update={
                        "generation": control["generation"],
                        "sync_successes": control["sync_successes"],
                    }
                )
                if reads["stopped"] or reads["reopen_required"]:
                    active.add("private_reads_blocked")
                if view["cash"]["halted"]:
                    active.add("private_cash_halted")
                if (control["phase"] != "RUNNING" or not owner) and (
                    journal["unacknowledged_records"]
                    or journal["session_open"]
                    or journal["rejected_frames"]
                ):
                    active.add("private_journal_unresolved")
            for kind in sorted(active - previous):
                state = self._alert(conn, state, kind, now, revision=revision)
                conn.execute("INSERT INTO conditions VALUES(?)", (kind,))
            for kind in sorted(previous - active):
                conn.execute("DELETE FROM conditions WHERE kind=?", (kind,))
                for row in conn.execute("SELECT id,body FROM alerts WHERE resolved_at IS NULL"):
                    if Alert.model_validate_json(row["body"]).kind == kind:
                        conn.execute(
                            "UPDATE alerts SET resolved_at=? WHERE id=?",
                            (now.isoformat(), row["id"]),
                        )
                state = self._alert(
                    conn, state, "private_condition_cleared", now, revision=revision, condition=kind
                )
            state = state.model_copy(update={"last_check_at": now})
            self._write(conn, state)
        return self.status()

    def status(self):
        with self._store() as conn:
            state, conditions, rows = self._verify(conn)
        return {
            "monitor_instance": state.instance,
            "control_instance": state.control_instance,
            "plan_sha256": state.plan_sha256,
            "last_check_at": state.last_check_at,
            "last_progress_at": state.last_progress_at,
            "stale_seconds": state.stale_seconds,
            "retry_seconds": state.retry_seconds,
            "conditions": sorted(conditions),
            "alert_count": state.alert_count,
            "unacknowledged_alerts": sum(r["acknowledged_at"] is None for r in rows),
            "notifications_waiting_submission": sum(
                r["acknowledged_at"] is None
                and r["resolved_at"] is None
                and r["submitted_at"] is None
                for r in rows
            ),
            "complete": False,
            "live_enabled": False,
        }

    def alerts(self):
        with self._store() as conn:
            _, _, rows = self._verify(conn)
        return [
            {
                "id": r["id"],
                **Alert.model_validate_json(r["body"]).model_dump(mode="json"),
                **{
                    k: r[k]
                    for k in ("acknowledged_at", "resolved_at", "submitted_at", "attempts", "error")
                },
            }
            for r in rows
            if r["acknowledged_at"] is None
        ][-100:]

    def acknowledge(self, alert_id):
        if type(alert_id) is not int or alert_id <= 0:
            raise OperationsError("operations_alert_id_invalid")
        with self._owned() as (conn, _, _, _, now):
            if not conn.execute(
                "UPDATE alerts SET acknowledged_at=? WHERE id=? AND acknowledged_at IS NULL",
                (now.isoformat(), alert_id),
            ).rowcount:
                raise OperationsError("operations_alert_not_pending")
        return self.status()

    def test_notification(self):
        with self._owned() as (conn, state, _, _, now):
            state = self._alert(conn, state, "private_notification_test", now)
            self._write(conn, state)
        return self.status()

    def notify(self, *, send=send_toast):
        submitted, failed = 0, 0
        with self._owned() as (conn, state, _, rows, now):
            started = self.monotonic()
            for row in rows:
                if any(
                    row[k] is not None for k in ("acknowledged_at", "resolved_at", "submitted_at")
                ):
                    continue
                if row["last_attempt_at"] is not None:
                    delay = (now - datetime.fromisoformat(row["last_attempt_at"])).total_seconds()
                    if delay < state.retry_seconds:
                        continue
                if submitted + failed >= 10 or self.monotonic() - started >= 25:
                    break
                conn.execute(
                    "UPDATE alerts SET last_attempt_at=?,attempts=attempts+1 WHERE id=?",
                    (now.isoformat(), row["id"]),
                )
                conn.commit()  # Persist an attempt before the native boundary.
                alert = Alert.model_validate_json(row["body"])
                try:
                    send({"id": row["id"], "kind": alert.kind}, state.control_instance)
                    submitted_at, error = now.isoformat(), None
                    submitted += 1
                except (OSError, subprocess.SubprocessError):
                    submitted_at, error = None, "notification_failed"
                    failed += 1
                conn.execute(
                    "UPDATE alerts SET submitted_at=?,error=? WHERE id=?",
                    (submitted_at, error, row["id"]),
                )
                conn.commit()
        return {"submitted": submitted, "failed": failed, "complete": False, "live_enabled": False}


class OperationsParser(argparse.ArgumentParser):
    def error(self, message):
        self.exit(2, "Invalid private operations arguments.\n")


def main(argv=None):
    parser = OperationsParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=("init", "check", "watchdog", "status", "alerts", "ack", "notify", "test"),
    )
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--stale-seconds", type=int, default=120)
    parser.add_argument("--retry-seconds", type=int, default=300)
    parser.add_argument("--alert-id", type=int)
    args = parser.parse_args(argv)
    try:
        monitor = (
            PrivateOperations.create(
                args.directory, stale_seconds=args.stale_seconds, retry_seconds=args.retry_seconds
            )
            if args.command == "init"
            else PrivateOperations(args.directory)
        )
        if args.command in {"init", "status"}:
            result = monitor.status()
        elif args.command == "alerts":
            result = {"alerts": monitor.alerts()}
        elif args.command == "ack":
            result = monitor.acknowledge(args.alert_id)
        elif args.command == "notify":
            result = monitor.notify()
        elif args.command == "test":
            monitor.test_notification()
            result = monitor.notify()
        else:
            result = monitor.check()
            if args.command == "watchdog":
                result["notifications"] = monitor.notify()
        print(json.dumps({"ok": True, **result}, default=str))
        return int(
            bool(result.get("conditions"))
            or bool(result.get("failed"))
            or bool(result.get("notifications", {}).get("failed"))
        )
    except Exception:
        print(json.dumps({"ok": False, "reason": "private_operations_failed"}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
