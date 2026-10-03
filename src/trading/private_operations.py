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
ARCHIVE_BATCH = 1000
ZERO = "0" * 64
ARCHIVE_SCHEMA = (
    "CREATE TABLE alert_archives(id INTEGER PRIMARY KEY,body TEXT NOT NULL,"
    "digest TEXT NOT NULL,payload BLOB NOT NULL)",
    "CREATE TABLE alert_archive_index(id INTEGER PRIMARY KEY,archive INTEGER NOT NULL,"
    "kind TEXT NOT NULL,created_at TEXT NOT NULL,digest TEXT NOT NULL)",
    "CREATE INDEX alert_archive_members ON alert_archive_index(archive,id)",
    "CREATE INDEX IF NOT EXISTS active_alert_rows ON alerts(id) WHERE body<>''",
    "CREATE INDEX IF NOT EXISTS unread_alert_rows ON alerts(id) WHERE acknowledged_at IS NULL",
)
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
    alert_count: int = Field(default=0, strict=True, ge=0, lt=2**63)
    archive_count: int = Field(default=0, strict=True, ge=0, lt=2**63)
    archived_alerts: int = Field(default=0, strict=True, ge=0, lt=2**63)
    archive_head: str = Field(default=ZERO, pattern=r"^[a-f0-9]{64}$")


class Alert(Contract):
    kind: str
    created_at: AwareDatetime
    revision: int | None = Field(default=None, strict=True, ge=0)
    condition: str | None = None


class AlertArchive(Contract):
    monitor: str = Field(pattern=r"^[a-f0-9]{32}$")
    index: int = Field(strict=True, gt=0, lt=2**63)
    previous: str = Field(pattern=r"^[a-f0-9]{64}$")
    records: int = Field(strict=True, gt=0, le=ARCHIVE_BATCH)
    bytes: int = Field(strict=True, gt=0, le=700_000)
    payload_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    index_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    sealed_at: AwareDatetime


def _body(model):
    data = model.model_dump(mode="json")
    if isinstance(model, MonitorState):
        # Preserve exact canonical bytes of pre-archive monitor records on read.
        for name, default in (("archive_count", 0), ("archived_alerts", 0), ("archive_head", ZERO)):
            if data[name] == default:
                data.pop(name)
    return _json(data)


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


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
                for statement in ARCHIVE_SCHEMA:
                    conn.execute(statement)
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
            count, minimum, maximum = conn.execute(
                "SELECT COUNT(*),COALESCE(MIN(id),1),COALESCE(MAX(id),0) FROM alerts"
            ).fetchone()
            if (count, minimum, maximum) != (state.alert_count, 1, state.alert_count):
                raise ValueError
            rows = conn.execute(
                "SELECT * FROM alerts WHERE body<>'' ORDER BY id LIMIT ?", (MAX_ALERTS + 1,)
            ).fetchall()
            if len(rows) > MAX_ALERTS or len(rows) + state.archived_alerts != state.alert_count:
                raise ValueError
            latest = state.last_check_at or state.created_at
            for row in rows:
                raw = row["body"]
                if type(raw) is not str or len(raw.encode()) > 512 or _hash(raw) != row["digest"]:
                    raise ValueError
                alert = self._parse_alert(raw, state)
                latest = max(latest, self._verify_delivery(row, alert.created_at))
            latest = max(latest, self._verify_archives(conn, state))
            return state, conditions, rows, latest
        except (ValueError, TypeError, KeyError):
            raise OperationsError("operations_integrity_failed") from None

    @staticmethod
    def _parse_alert(raw, state):
        alert = Alert.model_validate_json(raw)
        if (
            _body(alert) != raw
            or alert.kind not in KINDS
            or alert.created_at < state.created_at
            or (alert.kind == "private_condition_cleared") != (alert.condition in CONDITIONS)
        ):
            raise ValueError
        return alert

    @staticmethod
    def _verify_delivery(row, created_at):
        if type(row["attempts"]) is not int or not 0 <= row["attempts"] < 2**63:
            raise ValueError
        if bool(row["attempts"]) != (row["last_attempt_at"] is not None):
            raise ValueError
        latest = created_at
        for name in ("acknowledged_at", "resolved_at", "last_attempt_at", "submitted_at"):
            if row[name] is not None:
                stamp = datetime.fromisoformat(row[name])
                if stamp.utcoffset() is None or stamp < created_at:
                    raise ValueError
                latest = max(latest, stamp)
        if row["submitted_at"] is not None and row["last_attempt_at"] is None:
            raise ValueError
        if row["error"] not in {None, "notification_failed"}:
            raise ValueError
        return latest

    def _archive_payload(self, conn, index):
        row = conn.execute(
            "SELECT body,digest,length(payload),typeof(payload) FROM alert_archives WHERE id=?",
            (index,),
        ).fetchone()
        if (
            row is None
            or type(row[0]) is not str
            or len(row[0].encode()) > 1024
            or _hash(row[0]) != row[1]
        ):
            raise ValueError
        descriptor = AlertArchive.model_validate_json(row[0])
        if (
            _body(descriptor) != row[0]
            or descriptor.index != index
            or tuple(row[2:]) != (descriptor.bytes, "blob")
        ):
            raise ValueError
        payload = conn.execute(
            "SELECT payload FROM alert_archives WHERE id=?", (index,)
        ).fetchone()[0]
        if hashlib.sha256(payload).hexdigest() != descriptor.payload_sha256:
            raise ValueError
        return descriptor, row[1], payload

    def _verify_archives(self, conn, state, *, full=False):
        present = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' "
            "AND name IN ('alert_archives','alert_archive_index')"
        ).fetchone()[0]
        if (
            present == 0
            and state.archive_count == state.archived_alerts == 0
            and state.archive_head == ZERO
        ):
            return state.created_at
        if present != 2:
            raise ValueError
        if conn.execute("SELECT COUNT(*) FROM alert_archives").fetchone()[0] != state.archive_count:
            raise ValueError
        head, total, latest = ZERO, 0, state.created_at
        previous_seal = state.created_at
        for expected, (index,) in enumerate(
            conn.execute("SELECT id FROM alert_archives ORDER BY id"), 1
        ):
            descriptor, digest, payload = self._archive_payload(conn, index)
            if (
                index != expected
                or descriptor.monitor != state.instance
                or descriptor.previous != head
                or descriptor.sealed_at < previous_seal
            ):
                raise ValueError
            members = conn.execute(
                "SELECT i.id,i.kind,i.created_at,i.digest AS original_digest,a.* "
                "FROM alert_archive_index i "
                "JOIN alerts a ON a.id=i.id WHERE i.archive=? ORDER BY i.id",
                (index,),
            ).fetchall()
            identities = [[r[0], r[1], r[2], r[3]] for r in members]
            if (
                len(members) != descriptor.records
                or _hash(_json(identities)) != descriptor.index_sha256
            ):
                raise ValueError
            original = json.loads(payload) if full else None
            if full and (
                type(original) is not list
                or len(original) != descriptor.records
                or _json(original).encode() != payload
            ):
                raise ValueError
            latest = max(latest, descriptor.sealed_at)
            previous_seal = descriptor.sealed_at
            for offset, row in enumerate(members):
                created = datetime.fromisoformat(row[2])
                if (
                    not 1 <= row[0] <= state.alert_count
                    or row[1] not in KINDS
                    or created.utcoffset() is None
                    or created < state.created_at
                    or row[3] != row["digest"]
                    or row["body"] != ""
                    or (row[1] in CONDITIONS and row["resolved_at"] is None)
                    or not any(
                        row[k] is not None
                        for k in ("acknowledged_at", "resolved_at", "submitted_at")
                    )
                ):
                    raise ValueError
                latest = max(latest, self._verify_delivery(row, created))
                if full:
                    item = original[offset]
                    if (
                        type(item) is not list
                        or len(item) != 3
                        or (item[0], item[2]) != (row[0], row[3])
                    ):
                        raise ValueError
                    alert = self._parse_alert(item[1], state)
                    if _hash(item[1]) != item[2] or (alert.kind, alert.created_at.isoformat()) != (
                        row[1],
                        row[2],
                    ):
                        raise ValueError
            head, total = digest, total + descriptor.records
        if (
            head != state.archive_head
            or total != state.archived_alerts
            or conn.execute("SELECT COUNT(*) FROM alert_archive_index").fetchone()[0] != total
        ):
            raise ValueError
        return latest

    @contextmanager
    def _owned(self):
        with self._store() as conn:
            state, _, _, _ = self._verify(conn)

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
                    current, conditions, rows, latest = self._verify(conn)
                    now = _now(self.clock)
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
        if state.alert_count >= 2**63 - 1:
            raise OperationsError("operations_alert_id_capacity")
        if state.alert_count - state.archived_alerts >= MAX_ALERTS:
            state = self._archive_closed(conn, state, now)
        alert = Alert(kind=kind, created_at=now, revision=revision, condition=condition)
        body = _body(alert)
        updated = state.model_copy(update={"alert_count": state.alert_count + 1})
        conn.execute(
            "INSERT INTO alerts(id,body,digest) VALUES(?,?,?)",
            (updated.alert_count, body, _hash(body)),
        )
        return updated

    def _archive_closed(self, conn, state, now):
        selected, identities = [], []
        for row in conn.execute("SELECT * FROM alerts WHERE body<>'' ORDER BY id"):
            alert = self._parse_alert(row["body"], state)
            if (alert.kind in CONDITIONS and row["resolved_at"] is None) or not any(
                row[k] is not None for k in ("acknowledged_at", "resolved_at", "submitted_at")
            ):
                continue
            selected.append([row["id"], row["body"], row["digest"]])
            identities.append([row["id"], alert.kind, alert.created_at.isoformat(), row["digest"]])
            if len(selected) >= ARCHIVE_BATCH:
                break
        if not selected:
            raise OperationsError("operations_alert_capacity")
        present = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' "
            "AND name IN ('alert_archives','alert_archive_index')"
        ).fetchone()[0]
        if present == 0:
            for statement in ARCHIVE_SCHEMA:
                conn.execute(statement)  # No executescript: preserve the surrounding transaction.
        payload = _json(selected).encode()
        descriptor = AlertArchive(
            monitor=state.instance,
            index=state.archive_count + 1,
            previous=state.archive_head,
            records=len(selected),
            bytes=len(payload),
            payload_sha256=hashlib.sha256(payload).hexdigest(),
            index_sha256=_hash(_json(identities)),
            sealed_at=now,
        )
        body = _body(descriptor)
        updated = state.model_copy(
            update={
                "archive_count": descriptor.index,
                "archived_alerts": state.archived_alerts + len(selected),
                "archive_head": _hash(body),
            }
        )
        conn.execute(
            "INSERT INTO alert_archives VALUES(?,?,?,?)",
            (descriptor.index, body, _hash(body), payload),
        )
        conn.executemany(
            "INSERT INTO alert_archive_index VALUES(?,?,?,?,?)",
            [(r[0], descriptor.index, *r[1:]) for r in identities],
        )
        conn.executemany("UPDATE alerts SET body='' WHERE id=?", [(r[0],) for r in selected])
        try:
            self._verify_archives(conn, updated)
        except (ValueError, TypeError, KeyError):
            raise OperationsError("operations_integrity_failed") from None
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
                for row in conn.execute(
                    "SELECT id,body FROM alerts WHERE body<>'' AND resolved_at IS NULL"
                ):
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
            state, conditions, rows, _ = self._verify(conn)
            unread, waiting = conn.execute(
                "SELECT COUNT(*) FILTER (WHERE acknowledged_at IS NULL),"
                "COUNT(*) FILTER (WHERE acknowledged_at IS NULL AND resolved_at IS NULL "
                "AND submitted_at IS NULL) FROM alerts"
            ).fetchone()
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
            "active_alerts": len(rows),
            "archived_alerts": state.archived_alerts,
            "archive_count": state.archive_count,
            "unacknowledged_alerts": unread,
            "notifications_waiting_submission": waiting,
            "complete": False,
            "live_enabled": False,
        }

    def alerts(self):
        with self._store() as conn:
            self._verify(conn)
            rows = [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM alerts WHERE acknowledged_at IS NULL ORDER BY id DESC LIMIT 100"
                )
            ]
            return self._alert_views(conn, list(reversed(rows)))

    def _alert_views(self, conn, rows):
        try:
            archives = {}
            for row in rows:
                if row["body"] == "":
                    index = conn.execute(
                        "SELECT archive FROM alert_archive_index WHERE id=?", (row["id"],)
                    ).fetchone()[0]
                    if index not in archives:
                        _, _, payload = self._archive_payload(conn, index)
                        archives[index] = {item[0]: item for item in json.loads(payload)}
                    item = archives[index][row["id"]]
                    if item[2] != row["digest"] or _hash(item[1]) != item[2]:
                        raise OperationsError("operations_integrity_failed")
                    row["body"] = item[1]
            return [
                {
                    "id": r["id"],
                    **Alert.model_validate_json(r["body"]).model_dump(mode="json"),
                    **{
                        k: r[k]
                        for k in (
                            "acknowledged_at",
                            "resolved_at",
                            "submitted_at",
                            "attempts",
                            "error",
                        )
                    },
                }
                for r in rows
            ]
        except (ValueError, TypeError, KeyError, IndexError, AttributeError):
            raise OperationsError("operations_integrity_failed") from None

    def history(self, *, after_id=0, limit=100):
        """Paginate retained alerts, including acknowledged rows; no notification or recovery."""
        if (
            type(after_id) is not int
            or not 0 <= after_id < 2**63
            or type(limit) is not int
            or not 1 <= limit <= 100
        ):
            raise OperationsError("operations_history_page_invalid")
        with self._store() as conn:
            self._verify(conn)
            rows = [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM alerts WHERE id>? ORDER BY id LIMIT ?", (after_id, limit)
                )
            ]
            return {
                "alerts": self._alert_views(conn, rows),
                "next_alert_id": rows[-1]["id"] if rows else after_id,
                "complete": False,
                "live_enabled": False,
            }

    def audit_history(self):
        """Explicit semantic audit of every retained alert body and current delivery state."""
        try:
            with self._store() as conn:
                state, _, _, _ = self._verify(conn)
                self._verify_archives(conn, state, full=True)
                return {
                    "alerts": state.alert_count,
                    "archived_alerts": state.archived_alerts,
                    "archive_count": state.archive_count,
                    "complete": False,
                    "live_enabled": False,
                }
        except (ValueError, TypeError, KeyError):
            raise OperationsError("operations_integrity_failed") from None

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

    def watchdog(self, *, send=send_toast):
        """A full pending outbox must still drain; a failed sample never clears conditions."""
        try:
            result = self.check()
        except OperationsError as error:
            if str(error) != "operations_alert_capacity":
                raise
            result = self.status()
            result.update(check_failed=True, reason="operations_alert_capacity")
        result["notifications"] = self.notify(send=send)
        return result


class OperationsParser(argparse.ArgumentParser):
    def error(self, message):
        self.exit(2, "Invalid private operations arguments.\n")


def main(argv=None):
    parser = OperationsParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=(
            "init",
            "check",
            "watchdog",
            "status",
            "alerts",
            "ack",
            "notify",
            "test",
            "audit",
            "history",
        ),
    )
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--stale-seconds", type=int, default=120)
    parser.add_argument("--retry-seconds", type=int, default=300)
    parser.add_argument("--alert-id", type=int)
    parser.add_argument("--after-alert-id", type=int, default=0)
    parser.add_argument("--limit", type=int, default=100)
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
        elif args.command == "audit":
            result = monitor.audit_history()
        elif args.command == "history":
            result = monitor.history(after_id=args.after_alert_id, limit=args.limit)
        elif args.command == "ack":
            result = monitor.acknowledge(args.alert_id)
        elif args.command == "notify":
            result = monitor.notify()
        elif args.command == "test":
            monitor.test_notification()
            result = monitor.notify()
        elif args.command == "watchdog":
            result = monitor.watchdog()
        else:
            result = monitor.check()
        print(json.dumps({"ok": not result.get("check_failed", False), **result}, default=str))
        return int(
            bool(result.get("conditions"))
            or bool(result.get("check_failed"))
            or bool(result.get("failed"))
            or bool(result.get("notifications", {}).get("failed"))
        )
    except Exception:
        print(json.dumps({"ok": False, "reason": "private_operations_failed"}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
