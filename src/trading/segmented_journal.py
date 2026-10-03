"""Bounded active journals with atomically retained, linked SQLite archives."""

import hashlib
import re
import sqlite3
import uuid
from contextlib import closing, contextmanager
from pathlib import Path

from pydantic import AwareDatetime, Field

from trading.account_events import parse_event
from trading.broker_contracts import Contract
from trading.event_journal import (
    MAX_BYTES,
    SCHEMA,
    ZERO,
    Entry,
    EventJournal,
    JournalError,
    _canonical,
)
from trading.execution_cash_book import ExecutionCashBook
from trading.storage_init import new_storage_directory

MAX_SEGMENTS = 10_000
ARCHIVE_SCHEMA = """
CREATE TABLE series (
 id INTEGER PRIMARY KEY CHECK(id=1), instance TEXT NOT NULL,
 origin TEXT NOT NULL, count INTEGER NOT NULL, head TEXT NOT NULL
);
CREATE TABLE segments (id INTEGER PRIMARY KEY, body TEXT NOT NULL, digest TEXT NOT NULL);
CREATE TABLE archived_records (
 segment INTEGER NOT NULL, id INTEGER NOT NULL, body TEXT NOT NULL, digest TEXT NOT NULL,
 PRIMARY KEY(segment,id)
);
"""


class Segment(Contract):
    series: str = Field(pattern=r"^[a-f0-9]{32}$")
    index: int = Field(strict=True, gt=0, le=MAX_SEGMENTS)
    previous: str = Field(pattern=r"^[a-f0-9]{64}$")
    instance: str = Field(pattern=r"^[a-f0-9]{32}$")
    next_instance: str = Field(pattern=r"^[a-f0-9]{32}$")
    scope: str = Field(pattern=r"^[A-Za-z0-9_-]{1,64}$")
    records: int = Field(strict=True, ge=2, le=20_000)
    bytes: int = Field(strict=True, ge=1, le=MAX_BYTES)
    max_records: int = Field(strict=True, ge=4, le=20_000)
    head: str = Field(pattern=r"^[a-f0-9]{64}$")
    started_at: AwareDatetime
    ended_at: AwareDatetime


def _segment_digest(series, index, previous, body):
    return hashlib.sha256(_canonical([series, index, previous, body]).encode()).hexdigest()


class SegmentedEventJournal(EventJournal):
    """Version 2: active records stay bounded; all archived records stay in this DB.

    A rotation only accepts a cleanly ended segment without any unknown delivery.
    It copies its records and starts an empty, linked segment in one transaction.
    The old journal object is fenced by its instance, including after reopening it
    as a standalone version-1 journal. This is not broker continuity evidence.

    Archived bodies are fully audited on open and explicit audit_history(), and
    before rotation. Active operations check the active segment and the constant-
    size last archive anchor. They do not rescan all archived bodies per event.
    """

    _version = 2

    def __init__(self, directory, scope):
        self._series_instance = None
        super().__init__(directory, scope)
        self.audit_history()

    @classmethod
    def create(cls, directory, scope, *, max_records=1024):
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
                    conn.executescript(SCHEMA + ARCHIVE_SCHEMA)
                    origin = uuid.uuid4().hex
                    conn.execute(
                        "INSERT INTO journal VALUES(1,2,?,?,?,0,0,?)",
                        (origin, scope, max_records, ZERO),
                    )
                    conn.execute(
                        "INSERT INTO series VALUES(1,?,?,0,?)", (uuid.uuid4().hex, origin, ZERO)
                    )
                    conn.commit()
            except (OSError, sqlite3.Error):
                raise JournalError("journal_initialization_failed") from None
            return cls(directory, scope)

    def _anchor(self, conn, meta):
        try:
            rows = conn.execute("SELECT * FROM series LIMIT 2").fetchall()
            if len(rows) != 1:
                raise ValueError
            series = dict(rows[0])
            if (
                series["id"] != 1
                or not re.fullmatch(r"[a-f0-9]{32}", series["instance"])
                or not re.fullmatch(r"[a-f0-9]{32}", series["origin"])
                or not re.fullmatch(r"[a-f0-9]{64}", series["head"])
                or type(series["count"]) is not int
                or not 0 <= series["count"] <= MAX_SEGMENTS
                or (
                    self._series_instance is not None
                    and series["instance"] != self._series_instance
                )
            ):
                raise ValueError
            # Only the immutable last anchor is needed for each active operation.
            last = conn.execute(
                "SELECT id,length(CAST(body AS BLOB)),digest FROM segments ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if series["count"] == 0:
                if (
                    last is not None
                    or series["head"] != ZERO
                    or meta["instance"] != series["origin"]
                ):
                    raise ValueError
            else:
                if last is None or last[0] != series["count"] or not 1 <= last[1] <= 2048:
                    raise ValueError
                body = conn.execute("SELECT body FROM segments WHERE id=?", (last[0],)).fetchone()[
                    0
                ]
                segment = self._descriptor(series, last[0], body, last[2])
                if last[2] != series["head"] or segment.next_instance != meta["instance"]:
                    raise ValueError
                meta["_archive_at"] = segment.ended_at
            return series
        except (ValueError, TypeError, KeyError, OverflowError):
            self._failed = True
            raise JournalError("journal_archive_integrity_failed") from None

    def _descriptor(self, series, index, body, digest):
        segment = Segment.model_validate_json(body)
        if (
            _canonical(segment.model_dump(mode="json")) != body
            or segment.index != index
            or segment.series != series["instance"]
            or segment.scope != self.scope
            or segment.records > segment.max_records
            or segment.instance == segment.next_instance
            or segment.ended_at < segment.started_at
            or _segment_digest(series["instance"], index, segment.previous, body) != digest
        ):
            raise ValueError
        return segment

    def _snapshot(self, conn):
        meta, records = super()._snapshot(conn)
        series = self._anchor(conn, meta)
        self._series_instance = series["instance"]
        meta["_archived_segments"] = series["count"]
        meta["_series_head"] = series["head"]
        return meta, records

    def _audit(self, conn, meta):
        series = self._anchor(conn, meta)
        previous, instance, total_records, total_bytes = ZERO, series["origin"], 0, 0
        prior_end = None
        try:
            count, oversized = conn.execute(
                "SELECT COUNT(*),COALESCE(MAX(length(CAST(body AS BLOB))),0) FROM segments"
            ).fetchone()
            if count != series["count"] or oversized > 2048:
                raise ValueError
            for expected, (index, body, digest) in enumerate(
                conn.execute("SELECT * FROM segments ORDER BY id"), 1
            ):
                segment = self._descriptor(series, index, body, digest)
                if index != expected:
                    raise ValueError
                if segment.previous != previous or segment.instance != instance:
                    raise ValueError
                lengths = conn.execute(
                    "SELECT COUNT(*),COALESCE(SUM(length(CAST(body AS BLOB))),0),"
                    "COALESCE(MAX(length(CAST(body AS BLOB))),0) FROM archived_records "
                    "WHERE segment=?",
                    (index,),
                ).fetchone()
                if tuple(lengths[:2]) != (segment.records, segment.bytes) or lengths[2] > 110_000:
                    raise ValueError
                records = [
                    tuple(r)
                    for r in conn.execute(
                        "SELECT id,body,digest FROM archived_records WHERE segment=? ORDER BY id",
                        (index,),
                    )
                ]
                archived_meta = {
                    "instance": segment.instance,
                    "head": segment.head,
                    "_archive_at": prior_end,
                }
                _, entries, state = self._check(archived_meta, records)
                if (
                    state["active"]
                    or state["unacknowledged"]
                    or entries[-1].kind != "END"
                    or entries[0].at != segment.started_at
                    or entries[-1].at != segment.ended_at
                ):
                    raise ValueError
                previous, instance = digest, segment.next_instance
                prior_end = segment.ended_at
                total_records += segment.records
                total_bytes += segment.bytes
            # No orphan archive rows, truncated descriptor chain, or spliced active segment.
            actual_count = conn.execute("SELECT COUNT(*) FROM archived_records").fetchone()[0]
            if (
                actual_count != total_records
                or previous != series["head"]
                or instance != meta["instance"]
            ):
                raise ValueError
            return {
                "series": series["instance"],
                "archive_head": series["head"],
                "archived_segments": series["count"],
                "archived_records": total_records,
                "archived_bytes": total_bytes,
                "active_instance": meta["instance"],
                "active_records": meta["count"],
                "active_head": meta["head"],
                "history_gap_unproven": True,
                "resync_required": True,
                "complete": False,
                "live_enabled": False,
            }
        except (ValueError, TypeError, KeyError, OverflowError, RecursionError):
            self._failed = True
            raise JournalError("journal_archive_integrity_failed") from None

    def audit_history(self):
        """Audit all retained bodies. Call outside the receive loop; no state resumes."""

        def attempt():
            with self._transaction() as conn:
                meta, _, _ = self._verify(conn)
                return self._audit(conn, meta)

        return self._retry_busy(attempt)

    def replay_archive(self, index):
        """Replay one audited archive diagnostically, without restoring a session."""
        if type(index) is not int or not 1 <= index <= MAX_SEGMENTS:
            raise JournalError("invalid_archive_index")

        def attempt():
            with self._transaction() as conn:
                meta, _, _ = self._verify(conn)
                self._audit(conn, meta)
                row = conn.execute("SELECT body FROM segments WHERE id=?", (index,)).fetchone()
                if row is None:
                    raise JournalError("archive_not_found")
                segment = Segment.model_validate_json(row[0])
                records = [
                    tuple(r)
                    for r in conn.execute(
                        "SELECT id,body,digest FROM archived_records WHERE segment=? ORDER BY id",
                        (index,),
                    )
                ]
            return self._check(
                {"instance": segment.instance, "head": segment.head, "count": segment.records},
                records,
            )

        result = self._replay_snapshot(*self._retry_busy(attempt))
        return {**result, "archive_index": index, "series": self._series_instance}

    def rotate(self, *, expected_head):
        """Retain a clean ended segment atomically. Return a new, unstarted handle."""

        def attempt():
            with self._transaction(write=True) as conn:
                meta, entries, state = self._verify(conn)
                if expected_head != meta["head"]:
                    raise JournalError("journal_head_changed")
                if state["unacknowledged"]:
                    raise JournalError("capture_delivery_unresolved")
                if state["active"] or not entries or entries[-1].kind != "END":
                    raise JournalError("journal_clean_end_required")
                self._audit(conn, meta)
                index = meta["_archived_segments"] + 1
                if index > MAX_SEGMENTS:
                    raise JournalError("journal_archive_capacity_exceeded")
                next_instance = uuid.uuid4().hex
                segment = Segment(
                    series=self._series_instance,
                    index=index,
                    previous=meta["_series_head"],
                    instance=meta["instance"],
                    next_instance=next_instance,
                    scope=self.scope,
                    records=meta["count"],
                    bytes=meta["bytes"],
                    max_records=meta["max_records"],
                    head=meta["head"],
                    started_at=entries[0].at,
                    ended_at=entries[-1].at,
                )
                body = _canonical(segment.model_dump(mode="json"))
                digest = _segment_digest(self._series_instance, index, segment.previous, body)
                conn.execute("INSERT INTO segments VALUES(?,?,?)", (index, body, digest))
                conn.execute(
                    "INSERT INTO archived_records SELECT ?,id,body,digest FROM records", (index,)
                )
                conn.execute("DELETE FROM records")
                conn.execute("UPDATE series SET count=?,head=? WHERE id=1", (index, digest))
                conn.execute(
                    "UPDATE journal SET instance=?,count=0,bytes=0,head=? WHERE id=1",
                    (next_instance, ZERO),
                )

        self._retry_busy(attempt)
        # The old handle intentionally stays bound to its retired instance.
        return type(self)(self.path.parent, self.scope)

    def capacity(self):
        """Constant-size scheduling hint; record/current still perform full verification."""

        def attempt():
            with self._transaction() as conn:
                rows = conn.execute("SELECT * FROM journal LIMIT 2").fetchall()
                if len(rows) != 1:
                    raise JournalError("journal_integrity_failed")
                meta = dict(rows[0])
                if (
                    meta["version"] != 2
                    or meta["scope"] != self.scope
                    or meta["instance"] != self._instance
                    or type(meta["count"]) is not int
                    or type(meta["bytes"]) is not int
                    or type(meta["max_records"]) is not int
                    or not 4 <= meta["max_records"] <= 20_000
                    or not 0 <= meta["count"] <= meta["max_records"]
                    or not 0 <= meta["bytes"] <= MAX_BYTES
                ):
                    raise JournalError("journal_integrity_failed")
                self._anchor(conn, meta)
                return {
                    "max_records": meta["max_records"],
                    "records_remaining": meta["max_records"] - meta["count"],
                    "bytes_remaining": MAX_BYTES - meta["bytes"],
                }

        return self._retry_busy(attempt)

    def _recovery_events(self, conn, expected_head):
        meta, entries, state = self._verify(conn)
        if expected_head != meta["head"]:
            raise JournalError("journal_head_changed")
        self._audit(conn, meta)
        if state["unacknowledged"]:
            raise JournalError("capture_delivery_unresolved")
        if entries and not state["active"] and entries[-1].kind != "END":
            raise JournalError("recovery_fault_requires_review")
        events, skew = [], 0
        for entry in entries:
            if entry.kind == "BEGIN":
                skew = entry.clock_skew_ms or 0
            elif entry.kind == "EVENT":
                event = parse_event(entry.payload.encode(), entry.at, clock_skew_ms=skew)
                if event.channel == "executionEvents":
                    events.append(event)
                    if len(events) > 2000:
                        raise JournalError("recovery_execution_capacity")
        return tuple(events)

    def recovery_events(self, *, expected_head):
        """Read bounded current-segment notices, preserving every duplicate variant.

        The caller must hold StreamControl OS ownership and check its stopped state.
        This does not retire a session, acknowledge delivery or repair history gaps.
        """

        def attempt():
            with self._transaction() as conn:
                return self._recovery_events(conn, expected_head)

        return self._retry_busy(attempt)

    @contextmanager
    def guard_recovery_events(self, *, expected_head):
        """Fence journal changes through a separate, idempotent cash transaction.

        StreamControl ownership is also required. Cash and journal do not commit
        atomically; no journal write or stop clearance is performed here.
        """
        with self._transaction(write=True) as conn:
            yield self._recovery_events(conn, expected_head)

    def retire_for_recovery(self, *, expected_head, cash_book, at, monotonic_ns):
        """Explicit orphan retirement ONLY under the supervisor's checked OS ownership.

        Never clear unknown ACKs or infer that a received execution was booked.
        A clean END (or an empty segment) needs no additional journal write.
        This API alone does not prove that a former process is absent.
        """
        if not isinstance(cash_book, ExecutionCashBook) or cash_book.scope != self.scope:
            raise JournalError("recovery_cash_book_required")
        at, mono = self._stamp(at, monotonic_ns)

        def attempt():
            with self._transaction(write=True) as conn:
                meta, entries, state = self._verify(conn)
                if expected_head != meta["head"]:
                    raise JournalError("journal_head_changed")
                self._audit(conn, meta)
                if state["unacknowledged"]:
                    raise JournalError("capture_delivery_unresolved")
                if cash_book.snapshot()["halted"]:
                    raise JournalError("recovery_cash_book_halted")
                unique, variants, skew = {}, [], 0
                for entry in entries:
                    if entry.kind == "BEGIN":
                        skew = entry.clock_skew_ms or 0
                    elif entry.kind == "EVENT":
                        event = parse_event(entry.payload.encode(), entry.at, clock_skew_ms=skew)
                        if event.channel == "executionEvents":
                            if event.entity_id in unique:
                                variants.append(event)
                            else:
                                unique[event.entity_id] = event
                receipt = cash_book.match_booked_events(tuple(unique.values()))
                if set(receipt["booked_execution_ids"]) != set(unique):
                    raise JournalError("recovery_execution_not_booked")
                for event in variants:
                    if cash_book.match_booked_events((event,))["booked_execution_ids"] != (
                        event.entity_id,
                    ):
                        raise JournalError("recovery_execution_not_booked")
                if not entries:
                    return meta["head"]
                if not state["active"]:
                    if entries[-1].kind != "END":
                        raise JournalError("recovery_fault_requires_review")
                    return meta["head"]
                if at < state["at"]:
                    raise JournalError("invalid_capture_clock")
                self._append(
                    conn,
                    meta,
                    # Monotonic clocks reset with the process. Keep the old epoch's
                    # maximum until its END; new epochs get a new monotonic origin.
                    Entry(
                        kind="END",
                        epoch=state["epoch"],
                        session=state["session"],
                        at=at,
                        monotonic_ns=max(mono, state["mono"]),
                    ),
                )
                return conn.execute("SELECT head FROM journal").fetchone()[0]

        return self._retry_busy(attempt)
