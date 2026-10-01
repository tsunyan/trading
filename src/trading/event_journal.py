"""Durable local event capture, not a broker ledger or a resumable live session."""

import hashlib
import json
import re
import sqlite3
import time
import uuid
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import AwareDatetime, Field, TypeAdapter

from trading.account_events import MAX_FRAME_BYTES, EventError, parse_event
from trading.broker_contracts import Contract
from trading.storage_init import new_storage_directory
from trading.wire_validation import clock_skew

ZERO = "0" * 64
MAX_BYTES = 32_000_000
BUSY_TIMEOUT_SECONDS = 1
BUSY_ATTEMPTS = 3
TIME = TypeAdapter(AwareDatetime)
SCHEMA = """
CREATE TABLE journal (
 id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL,
 instance TEXT NOT NULL, scope TEXT NOT NULL, max_records INTEGER NOT NULL,
 count INTEGER NOT NULL, bytes INTEGER NOT NULL, head TEXT NOT NULL
);
CREATE TABLE records (id INTEGER PRIMARY KEY, body TEXT NOT NULL, digest TEXT NOT NULL);
"""


class JournalError(ValueError):
    """Only fixed reason codes; no account values or OS error text."""


class _JournalBusy(JournalError):
    """SQLite rolled the transaction back, so nothing from it was committed."""


class Entry(Contract):
    kind: Literal["BEGIN", "EVENT", "HEARTBEAT", "ACK", "END", "REJECTED", "FAULT"]
    epoch: int = Field(strict=True, gt=0)
    session: str = Field(pattern=r"^[a-f0-9]{32}$")
    at: AwareDatetime
    monotonic_ns: int = Field(strict=True, ge=0, lt=2**63)
    sequence: int | None = Field(default=None, strict=True, gt=0, lt=2**63)
    payload: str | None = Field(default=None, max_length=MAX_FRAME_BYTES)
    target: int | None = Field(default=None, strict=True, gt=0)
    reason: Literal["frame_rejected", "sequence_gap", "delivery_failed", "clock_invalid"] | None = (
        None
    )
    rejected_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    clock_skew_ms: int | None = Field(default=None, strict=True, ge=0, le=1000)


def _entry_body(entry):
    # Preserve canonical bytes of pre-tolerance records for existing journals.
    data = entry.model_dump(mode="json")
    if entry.clock_skew_ms is None:
        data.pop("clock_skew_ms")
    return _canonical(data)


def _canonical(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    )


def _digest(instance, scope, index, previous, body):
    data = _canonical([instance, scope, index, previous, body])
    return hashlib.sha256(data.encode()).hexdigest()


class EventJournal:
    """Append-only via this API. Hashes detect inconsistency, NOT hostile DB edits.

    start_session(expected_head=...) is explicit fencing, not a claim that an old
    receiver has died. It closes the old logical epoch and refuses all its writes.
    Reopening this object never starts or resumes a capture session.
    """

    def __init__(self, directory: Path, scope: str):
        if not isinstance(scope, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", scope):
            raise JournalError("invalid_journal_scope")
        self.path = Path(directory).resolve() / "event-journal.sqlite"
        self.scope, self._instance, self._failed = scope, None, False
        self._wait = time.sleep
        meta, _, _ = self._read()
        self._instance = meta["instance"]

    @classmethod
    def create(cls, directory: Path, scope: str, *, max_records=10_000):
        if not isinstance(scope, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", scope):
            raise JournalError("invalid_journal_scope")
        if type(max_records) is not int or not 4 <= max_records <= 20_000:
            raise JournalError("invalid_journal_capacity")
        directory = Path(directory).resolve()
        with new_storage_directory(
            directory, ("event-journal.sqlite-journal", "event-journal.sqlite")
        ):
            try:
                with closing(sqlite3.connect(directory / "event-journal.sqlite")) as conn:
                    conn.execute("PRAGMA synchronous=FULL")
                    conn.executescript(SCHEMA)
                    conn.execute(
                        "INSERT INTO journal VALUES(1,1,?,?,?,0,0,?)",
                        (uuid.uuid4().hex, scope, max_records, ZERO),
                    )
                    conn.commit()
            except (OSError, sqlite3.Error):
                raise JournalError("journal_initialization_failed") from None
            return cls(directory, scope)

    @contextmanager
    def _transaction(self, *, write=False):
        if self._failed:
            raise JournalError("journal_failed_closed")
        try:
            with closing(
                sqlite3.connect(
                    self.path.as_uri() + ("?mode=rw" if write else "?mode=ro"),
                    uri=True,
                    timeout=BUSY_TIMEOUT_SECONDS,
                )
            ) as conn:
                conn.row_factory = sqlite3.Row
                if write:
                    conn.execute("PRAGMA synchronous=FULL")
                conn.execute("BEGIN IMMEDIATE" if write else "BEGIN")
                try:
                    yield conn
                    conn.commit()
                except BaseException:
                    conn.rollback()
                    raise
        except (OSError, sqlite3.Error) as error:
            if (
                isinstance(error, sqlite3.Error)
                and (getattr(error, "sqlite_errorcode", 0) & 0xFF) == sqlite3.SQLITE_BUSY
            ):
                raise _JournalBusy("journal_busy") from None
            self._failed = True
            raise JournalError("journal_storage_failed") from None

    def _retry_busy(self, operation):
        # A busy transaction was rolled back, so a retry cannot duplicate a record.
        for attempt in range(BUSY_ATTEMPTS):
            try:
                return operation()
            except _JournalBusy:
                if attempt == BUSY_ATTEMPTS - 1:
                    raise
                self._wait(0.05)

    def _read(self):
        def snapshot():
            with self._transaction() as conn:
                return self._snapshot(conn)

        # Hold the shared lock only while copying rows. Full verification of a
        # large journal can outlast a writer's busy timeout at COMMIT.
        return self._check(*self._retry_busy(snapshot))

    def _verify(self, conn):
        """Verify inside a write transaction, which must see the same state it appends to."""
        return self._check(*self._snapshot(conn))

    def _snapshot(self, conn):
        """One consistent copy of the metadata and records; checked by _check()."""
        try:
            metas = conn.execute("SELECT * FROM journal LIMIT 2").fetchall()
            if len(metas) != 1:
                raise ValueError
            meta = dict(metas[0])
            if (
                meta["id"] != 1
                or meta["version"] != 1
                or meta["scope"] != self.scope
                or not isinstance(meta["instance"], str)
                or not re.fullmatch(r"[a-f0-9]{32}", meta["instance"])
                or (self._instance is not None and meta["instance"] != self._instance)
                or type(meta["max_records"]) is not int
                or not 4 <= meta["max_records"] <= 20_000
                or type(meta["count"]) is not int
                or not 0 <= meta["count"] <= meta["max_records"]
                or type(meta["bytes"]) is not int
                or not 0 <= meta["bytes"] <= MAX_BYTES
            ):
                raise ValueError
            # Inspect lengths before fetching potentially corrupted large bodies.
            lengths = conn.execute(
                "SELECT COUNT(*),COALESCE(SUM(length(CAST(body AS BLOB))),0),"
                "COALESCE(MAX(length(CAST(body AS BLOB))),0) FROM records"
            ).fetchone()
            if tuple(lengths[:2]) != (meta["count"], meta["bytes"]) or lengths[2] > 110_000:
                raise ValueError
            rows = [
                tuple(row) for row in conn.execute("SELECT id,body,digest FROM records ORDER BY id")
            ]
            return meta, rows
        except (ValueError, KeyError, TypeError, OverflowError):
            self._failed = True
            raise JournalError("journal_integrity_failed") from None

    def _check(self, meta, rows):
        """Bounded full verification, including lifecycle and delivery receipts."""
        try:
            state = {
                "epoch": 0,
                "session": None,
                "active": False,
                "sequence": 0,
                "pending": None,
                "at": None,
                "mono": None,
                "unacknowledged": [],
                "rejected": 0,
                "events": 0,
            }
            previous, entries = ZERO, []
            for index, (row_id, body, digest) in enumerate(rows, 1):
                if row_id != index or not isinstance(body, str):
                    raise ValueError
                if digest != _digest(meta["instance"], self.scope, index, previous, body):
                    raise ValueError
                entry = Entry.model_validate_json(body)
                if _entry_body(entry) != body:
                    raise ValueError
                self._reduce(state, entry, index)
                entries.append(entry)
                previous = digest
            if previous != meta["head"]:
                raise ValueError
            return meta, entries, state
        except (ValueError, KeyError, TypeError, OverflowError, RecursionError):
            self._failed = True
            raise JournalError("journal_integrity_failed") from None

    @staticmethod
    def _reduce(state, entry, index):
        if entry.kind != "BEGIN" and entry.clock_skew_ms is not None:
            raise ValueError
        populated = {
            name
            for name in ("sequence", "payload", "target", "reason", "rejected_sha256")
            if getattr(entry, name) is not None
        }
        fields = {
            "BEGIN": set(),
            "EVENT": {"sequence", "payload"},
            "HEARTBEAT": set(),
            "ACK": {"target"},
            "END": set(),
            "FAULT": {"reason"},
        }
        if entry.kind == "REJECTED":
            if "reason" not in populated or populated - {"reason", "rejected_sha256"}:
                raise ValueError
            if entry.reason not in {"frame_rejected", "sequence_gap"}:
                raise ValueError
        elif populated != fields[entry.kind]:
            raise ValueError
        if state["at"] is not None and entry.at < state["at"]:
            raise ValueError
        if entry.kind == "BEGIN":
            if entry.epoch != state["epoch"] + 1 or entry.session == state["session"]:
                raise ValueError
            state.update(
                epoch=entry.epoch,
                session=entry.session,
                active=True,
                sequence=0,
                pending=None,
                clock_skew_ms=entry.clock_skew_ms or 0,
            )
        else:
            if (
                not state["active"]
                or entry.epoch != state["epoch"]
                or entry.session != state["session"]
                or entry.monotonic_ns < state["mono"]
            ):
                raise ValueError
            if (
                entry.kind in {"EVENT", "HEARTBEAT", "END", "REJECTED"}
                and state["pending"] is not None
            ):
                raise ValueError
            if entry.kind == "EVENT":
                if entry.sequence != state["sequence"] + 1:
                    raise ValueError
                parse_event(
                    entry.payload.encode("utf-8"),
                    entry.at,
                    clock_skew_ms=state["clock_skew_ms"],
                )
                state["sequence"] = entry.sequence
                state["events"] += 1
            if entry.kind in {"EVENT", "HEARTBEAT"}:
                state["pending"] = index
                state["unacknowledged"].append(index)
            elif entry.kind == "ACK":
                if state["pending"] != entry.target:
                    raise ValueError
                state["unacknowledged"].remove(entry.target)
                state["pending"] = None
            elif entry.kind in {"END", "REJECTED", "FAULT"}:
                if entry.kind == "FAULT" and entry.reason not in {
                    "delivery_failed",
                    "clock_invalid",
                }:
                    raise ValueError
                state["active"] = False
                state["rejected"] += entry.kind == "REJECTED"
        state.update(at=entry.at, mono=entry.monotonic_ns)

    def _append(self, conn, meta, entry):
        body = _entry_body(entry)
        size = len(body.encode())
        if meta["count"] >= meta["max_records"] or meta["bytes"] + size > MAX_BYTES:
            raise JournalError("journal_capacity_exceeded")
        index = meta["count"] + 1
        digest = _digest(meta["instance"], self.scope, index, meta["head"], body)
        conn.execute("INSERT INTO records VALUES(?,?,?)", (index, body, digest))
        conn.execute(
            "UPDATE journal SET count=?,bytes=?,head=? WHERE id=1",
            (index, meta["bytes"] + size, digest),
        )
        return index

    @staticmethod
    def _stamp(at, monotonic_ns):
        try:
            at = TIME.validate_python(at).astimezone(UTC)
            if type(monotonic_ns) is not int or not 0 <= monotonic_ns < 2**63:
                raise ValueError
            return at, monotonic_ns
        except (ValueError, TypeError, OverflowError):
            raise JournalError("invalid_capture_clock") from None

    @staticmethod
    def _current(state, session):
        if not state["active"] or state["session"] != session:
            raise JournalError("capture_session_fenced")

    def inspect(self):
        meta, _, state = self._read()
        return {
            "instance": meta["instance"],
            "scope": self.scope,
            "head": meta["head"],
            "records": meta["count"],
            "bytes": meta["bytes"],
            "epoch": state["epoch"],
            "session_open": state["active"],
            "captured_events": state["events"],
            "rejected_frames": state["rejected"],
            "unacknowledged_records": tuple(state["unacknowledged"]),
            "history_gap_unproven": True,
            "resync_required": True,
            "complete": False,
            "live_enabled": False,
        }

    def start_session(self, *, expected_head, at: datetime, monotonic_ns: int, clock_skew_ms=0):
        clock_skew(clock_skew_ms)
        at, mono = self._stamp(at, monotonic_ns)

        def attempt():
            with self._transaction(write=True) as conn:
                meta, _, state = self._verify(conn)
                if expected_head != meta["head"]:
                    raise JournalError("journal_head_changed")
                if state["at"] is not None and at < state["at"]:
                    raise JournalError("invalid_capture_clock")
                session = uuid.uuid4().hex
                entry = Entry(
                    kind="BEGIN",
                    epoch=state["epoch"] + 1,
                    session=session,
                    at=at,
                    monotonic_ns=mono,
                    clock_skew_ms=clock_skew_ms or None,
                )
                self._append(conn, meta, entry)
            return session

        return self._retry_busy(attempt)

    def current(self, session):
        meta, _, state = self._read()
        self._current(state, session)
        if state["pending"] is not None:
            raise JournalError("capture_delivery_unresolved")
        return {
            "epoch": state["epoch"],
            "head": meta["head"],
            "unacknowledged_records": tuple(state["unacknowledged"]),
        }

    def record(self, session, kind, *, at, monotonic_ns, sequence=None, payload=None):
        if not isinstance(kind, str) or kind not in {"EVENT", "HEARTBEAT", "END"}:
            raise JournalError("invalid_capture_kind")
        at, mono = self._stamp(at, monotonic_ns)

        def attempt():
            error = None
            with self._transaction(write=True) as conn:
                meta, _, state = self._verify(conn)
                self._current(state, session)
                if state["pending"] is not None:
                    raise JournalError("capture_delivery_unresolved")
                fields = dict(epoch=state["epoch"], session=session, at=at, monotonic_ns=mono)
                if at < state["at"] or mono < state["mono"]:
                    fields.update(at=state["at"], monotonic_ns=state["mono"])
                    entry = Entry(kind="FAULT", reason="clock_invalid", **fields)
                    error = "invalid_capture_clock"
                elif kind == "EVENT":
                    digest = (
                        hashlib.sha256(payload).hexdigest()
                        if type(payload) is bytes and len(payload) <= MAX_FRAME_BYTES
                        else None
                    )
                    if type(sequence) is not int or sequence != state["sequence"] + 1:
                        error = "sequence_gap"
                    else:
                        try:
                            parse_event(payload, at, clock_skew_ms=state["clock_skew_ms"])
                        except EventError:
                            error = "frame_rejected"
                    if error:
                        entry = Entry(
                            kind="REJECTED", reason=error, rejected_sha256=digest, **fields
                        )
                    else:
                        entry = Entry(
                            kind=kind, sequence=sequence, payload=payload.decode("utf-8"), **fields
                        )
                else:
                    if payload is not None or sequence is not None:
                        raise JournalError("invalid_capture_fields")
                    entry = Entry(kind=kind, **fields)
                index = self._append(conn, meta, entry)
            return index, error

        index, error = self._retry_busy(attempt)
        if error:
            raise JournalError(error)
        return index

    def acknowledge(self, session, record_id):
        def attempt():
            with self._transaction(write=True) as conn:
                meta, _, state = self._verify(conn)
                self._current(state, session)
                if type(record_id) is not int or state["pending"] != record_id:
                    raise JournalError("capture_receipt_mismatch")
                entry = Entry(
                    kind="ACK",
                    epoch=state["epoch"],
                    session=session,
                    at=state["at"],
                    monotonic_ns=state["mono"],
                    target=record_id,
                )
                self._append(conn, meta, entry)

        self._retry_busy(attempt)

    def fail_delivery(self, session):
        def attempt():
            with self._transaction(write=True) as conn:
                meta, _, state = self._verify(conn)
                self._current(state, session)
                entry = Entry(
                    kind="FAULT",
                    reason="delivery_failed",
                    epoch=state["epoch"],
                    session=session,
                    at=state["at"],
                    monotonic_ns=state["mono"],
                )
                self._append(conn, meta, entry)

        self._retry_busy(attempt)

    def replay(self):
        """Reconstruct recorded, acknowledged input only; never return a live monitor."""
        from trading.account_sync import AccountSyncMonitor, SyncError

        meta, entries, state = self._read()
        clock = [datetime(1970, 1, 1, tzinfo=UTC), 0]
        monitor = None
        session = None
        outcomes = []
        for index, entry in enumerate(entries, 1):
            clock[:] = [entry.at, entry.monotonic_ns]
            error = None
            if entry.kind == "BEGIN":
                monitor = AccountSyncMonitor(
                    clock=lambda: clock[0],
                    monotonic=lambda: clock[1] / 1e9,
                    clock_skew_ms=entry.clock_skew_ms or 0,
                )
                session = monitor.start_session()
            elif entry.kind in {"EVENT", "HEARTBEAT"}:
                if index in state["unacknowledged"]:
                    monitor.disconnect(session)
                    error = "delivery_outcome_unknown"
                else:
                    try:
                        if entry.kind == "EVENT":
                            monitor.ingest(session, entry.sequence, entry.payload.encode("utf-8"))
                        else:
                            monitor.heartbeat(session)
                    except SyncError:
                        error = "historical_monitor_rejected"
            elif entry.kind in {"END", "REJECTED", "FAULT"}:
                monitor.disconnect(session)
            if entry.kind != "ACK":
                outcomes.append(
                    {
                        "record": index,
                        "epoch": entry.epoch,
                        "kind": entry.kind,
                        "error": error,
                        "phase": monitor.status()["phase"],
                    }
                )
        return {
            "historical_only": True,
            "records": meta["count"],
            "head": meta["head"],
            "outcomes": outcomes,
            "unacknowledged_records": tuple(state["unacknowledged"]),
            "resync_required": True,
            "complete": False,
            "live_enabled": False,
        }
