"""Persistent supervisor stops and stable OS ownership; never restores account proof."""

import hashlib
import os
import sqlite3
import stat
import time
import uuid
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from trading.broker_contracts import Contract
from trading.event_journal import _canonical
from trading.execution_cash_book import ExecutionCashBook
from trading.segmented_journal import SegmentedEventJournal
from trading.storage_init import new_storage_directory

REASONS = {
    "created",
    "running",
    "closed",
    "recovery_approved",
    "stream_failed",
    "sync_failed",
    "sync_deadline",
    "rollover_failed",
    "clock_invalid",
    "worker_not_joined",
    "startup_failed",
}
SCHEMA = """
CREATE TABLE control (id INTEGER PRIMARY KEY CHECK(id=1), body TEXT NOT NULL, digest TEXT NOT NULL);
CREATE TABLE transitions (
 id INTEGER PRIMARY KEY, revision INTEGER NOT NULL, kind TEXT NOT NULL,
 wall_ns INTEGER NOT NULL, state_digest TEXT NOT NULL
);
"""


class StreamControlError(ValueError):
    """Fixed local reason codes only."""


class ControlState(Contract):
    version: Literal[1] = 1
    instance: str = Field(pattern=r"^[a-f0-9]{32}$")
    scope: str = Field(pattern=r"^[A-Za-z0-9_-]{1,64}$")
    series: str = Field(pattern=r"^[a-f0-9]{32}$")
    cash_instance: str = Field(pattern=r"^[a-f0-9]{32}$")
    journal_path: str = Field(max_length=2048)
    cash_path: str = Field(max_length=2048)
    journal_instance: str = Field(pattern=r"^[a-f0-9]{32}$")
    journal_head: str = Field(pattern=r"^[a-f0-9]{64}$")
    owner_device: str = Field(pattern=r"^[0-9]+$")
    owner_inode: str = Field(pattern=r"^[1-9][0-9]*$")
    phase: Literal["READY", "RUNNING", "STOPPED"] = "READY"
    owner: str | None = Field(default=None, pattern=r"^[a-f0-9]{32}$")
    generation: int = Field(default=0, strict=True, ge=0)
    revision: int = Field(default=0, strict=True, ge=0)
    wall_ns: int = Field(strict=True, gt=0, lt=2**63)
    reason: str = "created"
    cleanup_unknown: bool = Field(default=False, strict=True)
    rotations: int = Field(default=0, strict=True, ge=0)
    sync_successes: int = Field(default=0, strict=True, ge=0)
    sync_retries: int = Field(default=0, strict=True, ge=0)
    last_sync_wall_ns: int | None = Field(default=None, strict=True, gt=0, lt=2**63)

    @model_validator(mode="after")
    def coherent(self):
        if self.reason not in REASONS:
            raise ValueError("invalid reason")
        if self.phase == "READY" and (self.owner is not None or self.cleanup_unknown):
            raise ValueError("ready ownership invalid")
        if self.phase != "READY" and self.owner is None:
            raise ValueError("missing owner")
        if self.phase == "RUNNING" and not self.cleanup_unknown:
            raise ValueError("remote ownership must remain uncertain until clean close")
        return self


def _body(state):
    body = _canonical(state.model_dump())
    return body, hashlib.sha256(body.encode()).hexdigest()


class StreamControl:
    """A RUNNING record left by process death never expires or resumes by itself.

    Ownership lasts through receiver cleanup and REST worker exit. Replaced or
    missing lock files are rejected, not recreated. Recovery requires explicit
    expected state and journal head, absent OS ownership, known ACKs, and receipts.
    This is one local control domain, not a lock across machines or other apps.
    """

    def __init__(self, directory, *, wall_ns=time.time_ns):
        self.path = Path(directory).resolve() / "stream-control.sqlite"
        self.lock_path = self.path.parent / "stream-owner.lock"
        self._wall = wall_ns
        self._instance = None
        self._owned = False
        self._handle = None
        self._failed = False
        with self._transaction() as conn:
            state = self._state(conn)
        self._instance = state.instance

    @classmethod
    def create(cls, directory, journal, cash_book, **clocks):
        if not isinstance(journal, SegmentedEventJournal) or not isinstance(
            cash_book, ExecutionCashBook
        ):
            raise StreamControlError("explicit_supervisor_stores_required")
        if journal.scope != cash_book.scope:
            raise StreamControlError("supervisor_scope_mismatch")
        view, audit, cash = journal.inspect(), journal.audit_history(), cash_book.snapshot()
        if view["records"] or audit["archived_segments"] or cash["halted"]:
            raise StreamControlError("fresh_supervisor_journal_required")
        directory = Path(directory).resolve()
        with new_storage_directory(
            directory,
            ("stream-control.sqlite-journal", "stream-control.sqlite", "stream-owner.lock"),
        ):
            try:
                instance = uuid.uuid4().hex
                with (directory / "stream-owner.lock").open("xb") as handle:
                    handle.write(instance.encode())
                    handle.flush()
                    os.fsync(handle.fileno())
                    identity = os.fstat(handle.fileno())
                state = ControlState(
                    instance=instance,
                    scope=journal.scope,
                    series=audit["series"],
                    cash_instance=cash["instance"],
                    journal_path=str(journal.path),
                    cash_path=str(cash_book.path),
                    journal_instance=view["instance"],
                    journal_head=view["head"],
                    owner_device=str(identity.st_dev),
                    owner_inode=str(identity.st_ino),
                    wall_ns=clocks.get("wall_ns", time.time_ns)(),
                )
                body, digest = _body(state)
                with closing(sqlite3.connect(directory / "stream-control.sqlite")) as conn:
                    conn.execute("PRAGMA synchronous=FULL")
                    conn.executescript(SCHEMA)
                    conn.execute("INSERT INTO control VALUES(1,?,?)", (body, digest))
                    conn.execute(
                        "INSERT INTO transitions VALUES(1,0,'CREATED',?,?)", (state.wall_ns, digest)
                    )
                    conn.commit()
            except (OSError, sqlite3.Error, ValueError):
                raise StreamControlError("stream_control_initialization_failed") from None
            return cls(directory, **clocks)

    @contextmanager
    def _transaction(self):
        if self._failed:
            raise StreamControlError("stream_control_failed_closed")
        try:
            with closing(
                sqlite3.connect(self.path.as_uri() + "?mode=rw", uri=True, timeout=1)
            ) as conn:
                conn.execute("PRAGMA synchronous=FULL")
                conn.execute("BEGIN IMMEDIATE")
                try:
                    yield conn
                    conn.commit()
                except BaseException:
                    conn.rollback()
                    raise
        except (OSError, sqlite3.Error):
            self._failed = True
            raise StreamControlError("stream_control_storage_failed") from None

    def _state(self, conn):
        try:
            rows = conn.execute(
                "SELECT id,length(CAST(body AS BLOB)),digest FROM control LIMIT 2"
            ).fetchall()
            if len(rows) != 1 or rows[0][0] != 1 or not 1 <= rows[0][1] <= 8192:
                raise ValueError
            body = conn.execute("SELECT body FROM control WHERE id=1").fetchone()[0]
            state = ControlState.model_validate_json(body)
            if _body(state) != (body, rows[0][2]) or (
                self._instance is not None and self._instance != state.instance
            ):
                raise ValueError
            self._verify_lock(state)
            return state
        except (ValueError, TypeError, KeyError, OSError):
            self._failed = True
            raise StreamControlError("stream_control_integrity_failed") from None

    def _verify_lock(self, state, handle=None, *, read_contents=True):
        identity = self.lock_path.lstat()
        candidate = os.fstat(handle.fileno()) if handle is not None else identity
        if (
            not stat.S_ISREG(identity.st_mode)
            or not stat.S_ISREG(candidate.st_mode)
            or candidate.st_size != 32
            or candidate.st_ino <= 0
            or (str(candidate.st_dev), str(candidate.st_ino))
            != (state.owner_device, state.owner_inode)
            or (identity.st_dev, identity.st_ino) != (candidate.st_dev, candidate.st_ino)
        ):
            raise StreamControlError("stream_owner_file_changed")
        if handle is not None and read_contents:
            handle.seek(0)
            if handle.read(33) != state.instance.encode():
                raise StreamControlError("stream_owner_file_changed")

    @contextmanager
    def ownership(self):
        if self._owned:
            raise StreamControlError("stream_owner_busy")
        with self._transaction() as conn:
            state = self._state(conn)
        try:
            handle = self.lock_path.open("r+b", buffering=0)
        except OSError:
            raise StreamControlError("stream_owner_unavailable") from None
        with handle:
            self._verify_lock(state, handle, read_contents=False)
            handle.seek(0)
            try:
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                elif os.name == "posix":
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                else:
                    raise StreamControlError("stream_owner_platform_unsupported")
            except OSError:
                raise StreamControlError("stream_owner_busy") from None
            self._owned, self._handle = True, handle
            try:
                self._verify_lock(state, handle)
                yield
            finally:
                self._owned, self._handle = False, None
                if os.name == "nt":
                    try:
                        handle.seek(0)
                        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                    except OSError:
                        pass

    def snapshot(self):
        with self._transaction() as conn:
            return {**self._state(conn).model_dump(), "complete": False, "live_enabled": False}

    def check_binding(self, journal, cash_book):
        state = self.snapshot()
        if (
            not isinstance(journal, SegmentedEventJournal)
            or not isinstance(cash_book, ExecutionCashBook)
            or str(journal.path) != state["journal_path"]
            or str(cash_book.path) != state["cash_path"]
            or journal.scope != state["scope"]
            or cash_book.scope != state["scope"]
            or journal.archive_identity()["series"] != state["series"]
            or cash_book.snapshot()["instance"] != state["cash_instance"]
        ):
            raise StreamControlError("stream_control_binding_mismatch")

    def _write(self, conn, state, kind=None, **changes):
        if not self._owned:
            raise StreamControlError("stream_owner_required")
        self._verify_lock(state, self._handle)
        try:
            now = self._wall()
        except Exception:
            now = None
        if type(now) is not int or not 0 < now < 2**63 or now < state.wall_ns:
            if changes.get("phase") != "STOPPED":
                raise StreamControlError("stream_control_clock_invalid")
            now = state.wall_ns
        data = {**state.model_dump(), **changes, "revision": state.revision + 1, "wall_ns": now}
        updated = ControlState.model_validate(data)
        body, digest = _body(updated)
        if kind is not None:
            count = conn.execute("SELECT COUNT(*) FROM transitions").fetchone()[0]
            if count >= 10_000:
                raise StreamControlError("stream_control_capacity_exceeded")
            conn.execute(
                "INSERT INTO transitions(revision,kind,wall_ns,state_digest) VALUES(?,?,?,?)",
                (updated.revision, kind, now, digest),
            )
        conn.execute("UPDATE control SET body=?,digest=? WHERE id=1", (body, digest))
        return updated

    def begin(self, journal, *, expected_revision, expected_head):
        if type(expected_revision) is not int or expected_revision < 0:
            raise StreamControlError("invalid_stream_revision")
        view = journal.inspect()
        with self._transaction() as conn:
            state = self._state(conn)
            if state.phase != "READY":
                raise StreamControlError("stream_recovery_required")
            if state.revision != expected_revision:
                raise StreamControlError("stream_control_state_changed")
            if (state.journal_instance, state.journal_head) != (
                view["instance"],
                view["head"],
            ) or expected_head != view["head"]:
                raise StreamControlError("stream_journal_checkpoint_changed")
            if view["session_open"] or view["unacknowledged_records"]:
                raise StreamControlError("stream_journal_unresolved")
            return self._write(
                conn,
                state,
                "BEGIN",
                phase="RUNNING",
                owner=uuid.uuid4().hex,
                generation=state.generation + 1,
                reason="running",
                cleanup_unknown=True,
            ).model_dump()

    def update(self, owner, *, success=False, retry=False, journal=None):
        view = journal.inspect() if journal is not None else None
        with self._transaction() as conn:
            state = self._state(conn)
            if state.phase != "RUNNING" or state.owner != owner:
                raise StreamControlError("stream_control_owner_fenced")
            changes = {
                "sync_successes": state.sync_successes + int(success),
                "sync_retries": state.sync_retries + int(retry),
            }
            if success:
                changes["last_sync_wall_ns"] = self._wall()
            if view is not None:
                changes.update(
                    journal_instance=view["instance"],
                    journal_head=view["head"],
                    rotations=state.rotations + 1,
                )
            self._write(conn, state, "ROTATE" if view is not None else None, **changes)

    def finish(self, owner, journal, *, reason="closed", cleanup_unknown=False):
        if (
            reason not in REASONS - {"created", "running", "recovery_approved"}
            or type(cleanup_unknown) is not bool
        ):
            raise StreamControlError("invalid_stream_stop_reason")
        view = journal.inspect()
        clean = (
            reason == "closed"
            and not cleanup_unknown
            and not view["session_open"]
            and not view["unacknowledged_records"]
        )
        with self._transaction() as conn:
            state = self._state(conn)
            if state.owner != owner or state.phase not in {"RUNNING", "STOPPED"}:
                raise StreamControlError("stream_control_owner_fenced")
            if state.phase == "STOPPED":
                clean = False
            self._write(
                conn,
                state,
                "FINISH" if clean else "STOP",
                phase="READY" if clean else "STOPPED",
                owner=None if clean else owner,
                reason=reason if state.phase != "STOPPED" else state.reason,
                cleanup_unknown=cleanup_unknown,
                journal_instance=view["instance"],
                journal_head=view["head"],
            )

    def review_delivery_uncertainty(
        self,
        journal,
        cash_book,
        *,
        expected_revision,
        expected_head,
        expected_reason,
        at=None,
        monotonic_ns=0,
    ):
        """Record the operator's review of unknown delivery while no capture can run.

        Recovery afterwards stays strict: every stored execution, including formerly
        unknown ones, must be matched by GET and booked first (reconcile-stopped).
        """
        if type(expected_revision) is not int or expected_revision < 0:
            raise StreamControlError("invalid_stream_revision")
        with self.ownership():
            self.check_binding(journal, cash_book)
            with self._transaction() as conn:
                state = self._state(conn)
                if state.phase == "READY":
                    raise StreamControlError("stream_recovery_not_required")
                if state.revision != expected_revision or state.reason != expected_reason:
                    raise StreamControlError("stream_control_state_changed")
            head = journal.review_delivery_uncertainty(
                expected_head=expected_head,
                at=datetime.now(UTC) if at is None else at,
                monotonic_ns=monotonic_ns,
            )
            with self._transaction() as conn:
                current = self._state(conn)
                if current.revision != state.revision:
                    raise StreamControlError("stream_control_state_changed")
                updated = self._write(conn, current, "REVIEW", journal_head=head)
            return {**updated.model_dump(), "complete": False, "live_enabled": False}

    def recover(
        self,
        journal,
        cash_book,
        *,
        expected_revision,
        expected_head,
        expected_reason,
        acknowledge_token_uncertainty=False,
        at=None,
        monotonic_ns=0,
    ):
        if type(expected_revision) is not int or expected_revision < 0:
            raise StreamControlError("invalid_stream_revision")
        if type(acknowledge_token_uncertainty) is not bool:
            raise StreamControlError("invalid_recovery_acknowledgment")
        with self.ownership():
            self.check_binding(journal, cash_book)
            with self._transaction() as conn:
                state = self._state(conn)
                if state.phase == "READY":
                    raise StreamControlError("stream_recovery_not_required")
                if state.revision != expected_revision or state.reason != expected_reason:
                    raise StreamControlError("stream_control_state_changed")
                if state.cleanup_unknown and not acknowledge_token_uncertainty:
                    raise StreamControlError("stream_token_uncertainty_requires_acknowledgment")
            head = journal.retire_for_recovery(
                expected_head=expected_head,
                cash_book=cash_book,
                at=datetime.now(UTC) if at is None else at,
                monotonic_ns=monotonic_ns,
            )
            if journal.inspect()["records"]:
                journal = journal.rotate(expected_head=head)
            view = journal.inspect()
            with self._transaction() as conn:
                current = self._state(conn)
                if current.revision != state.revision:
                    raise StreamControlError("stream_control_state_changed")
                self._write(
                    conn,
                    current,
                    "RECOVER",
                    phase="READY",
                    owner=None,
                    reason="recovery_approved",
                    generation=current.generation + 1,
                    cleanup_unknown=False,
                    journal_instance=view["instance"],
                    journal_head=view["head"],
                )
            return journal
