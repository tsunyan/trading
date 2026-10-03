"""Durable local Private POST/stream coordination; no keys, HTTP or trade permission."""

import hashlib
import json
import os
import sqlite3
import stat
import time
import uuid
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from trading.broker_contracts import Contract
from trading.private_stream_token import PrivateStreamLimiter, StreamError
from trading.read_control import PersistentReadLimiter
from trading.storage_init import new_storage_directory

SCHEMA = """
CREATE TABLE control (id INTEGER PRIMARY KEY CHECK(id=1),body TEXT NOT NULL,digest TEXT NOT NULL);
CREATE TABLE events (revision INTEGER PRIMARY KEY,kind TEXT NOT NULL,wall_ns INTEGER NOT NULL,
                     digest TEXT NOT NULL);
"""
REASONS = {"created", "completed", "operator_stop", "operation_unknown", "clock_invalid"}


class PostControlError(StreamError):
    """Fixed local codes only; no request, response or credential objects."""


class PostState(Contract):
    version: Literal[1] = 1
    instance: str = Field(pattern=r"^[a-f0-9]{32}$")
    scope: str = Field(pattern=r"^[A-Za-z0-9_-]{1,64}$")
    read_instance: str = Field(pattern=r"^[a-f0-9]{32}$")
    read_path: str = Field(max_length=2048)
    owner_device: str = Field(pattern=r"^[0-9]+$")
    owner_inode: str = Field(pattern=r"^[1-9][0-9]*$")
    phase: Literal["READY", "IN_FLIGHT", "STOPPED"] = "READY"
    claim: str | None = Field(default=None, pattern=r"^[a-f0-9]{32}$")
    operation: Literal["private_stream", "order", "close_order", "cancel"] | None = None
    request_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    wall_ns: int = Field(strict=True, ge=0, lt=2**63)
    revision: int = Field(default=0, strict=True, ge=0)
    reason: str = "created"

    @model_validator(mode="after")
    def coherent(self):
        if self.reason not in REASONS or not Path(self.read_path).is_absolute():
            raise ValueError("invalid_post_state")
        if (self.claim is None) != (self.operation is None):
            raise ValueError("invalid_post_claim")
        if self.phase == "READY" and self.claim is not None:
            raise ValueError("ready_post_claim")
        if self.phase == "IN_FLIGHT" and self.claim is None:
            raise ValueError("missing_post_claim")
        if self.operation in {"order", "close_order", "cancel"} and self.request_sha256 is None:
            raise ValueError("missing_post_request_digest")
        if self.operation in {None, "private_stream"} and self.request_sha256 is not None:
            raise ValueError("unexpected_post_request_digest")
        return self


def _encode(state):
    body = json.dumps(state.model_dump(), sort_keys=True, separators=(",", ":"))
    return body, hashlib.sha256(body.encode()).hexdigest()


class PersistentPostLimiter(PrivateStreamLimiter):
    """One bound local domain across processes, keys, tokens and future order clients.

    Stable OS ownership covers the committed claim, unconditional 1.1s wait,
    operation and durable completion. Death or exception leaves a blocked claim;
    no lease timeout, implicit recreation, reset or GET-only orphan resolution.
    Scope/binding is a local declaration, not broker account identity.
    """

    def __init__(
        self, directory, reads, *, wall_ns=time.time_ns, monotonic=time.monotonic, sleep=time.sleep
    ):
        if not isinstance(reads, PersistentReadLimiter):
            raise PostControlError("explicit_post_read_control_required")
        super().__init__(monotonic=monotonic, sleep=sleep)
        self.path = Path(directory).resolve() / "post-control.sqlite"
        self.lock_path = self.path.parent / "post-owner.lock"
        self.reads, self._wall = reads, wall_ns
        self._instance = None
        self._failed = False
        self._active_claim = None
        with self._transaction() as conn:
            state = self._state(conn)
        self._instance = state.instance
        self._binding(state)

    @classmethod
    def create(cls, directory, reads, **clocks):
        if not isinstance(reads, PersistentReadLimiter):
            raise PostControlError("explicit_post_read_control_required")
        view = reads.status()
        if view["blocked"]:
            raise PostControlError("post_dependencies_blocked")
        if reads.post_binding() is not None:
            raise PostControlError("post_control_already_bound")
        directory = Path(directory).resolve()
        instance = uuid.uuid4().hex
        with new_storage_directory(
            directory, ("post-control.sqlite-journal", "post-control.sqlite", "post-owner.lock")
        ):
            with (directory / "post-owner.lock").open("xb") as handle:
                handle.write(instance.encode())
                handle.flush()
                os.fsync(handle.fileno())
                identity = os.fstat(handle.fileno())
            state = PostState(
                instance=instance,
                scope=reads.scope,
                read_instance=view["instance_id"],
                read_path=str(reads.path),
                owner_device=str(identity.st_dev),
                owner_inode=str(identity.st_ino),
                wall_ns=0,
            )
            body, digest = _encode(state)
            with closing(sqlite3.connect(directory / "post-control.sqlite")) as conn:
                conn.execute("PRAGMA synchronous=FULL")
                conn.executescript(SCHEMA)
                conn.execute("INSERT INTO control VALUES(1,?,?)", (body, digest))
                conn.execute("INSERT INTO events VALUES(0,'CREATED',0,?)", (digest,))
                conn.commit()
        # A crash between store creation and binding retains that unbound store.
        # Never adopt it automatically or silently recreate a bound domain.
        reads.bind_post(instance, directory)
        return cls(directory, reads, **clocks)

    @contextmanager
    def _transaction(self):
        if self._failed:
            raise PostControlError("post_control_failed_closed")
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
            raise PostControlError("post_control_storage_failed") from None

    def _state(self, conn):
        try:
            rows = conn.execute("SELECT id,body,digest FROM control LIMIT 2").fetchall()
            if len(rows) != 1 or rows[0][0] != 1:
                raise ValueError
            _, body, digest = rows[0]
            state = PostState.model_validate_json(body)
            if _encode(state) != (body, digest) or self._instance not in {None, state.instance}:
                raise ValueError
            last = conn.execute(
                "SELECT revision,digest FROM events ORDER BY revision DESC LIMIT 1"
            ).fetchone()
            if last != (state.revision, digest):
                raise ValueError
            return state
        except (ValueError, TypeError):
            self._failed = True
            raise PostControlError("post_control_integrity_failed") from None

    def _binding(self, state):
        if (
            self.reads.status()["instance_id"] != state.read_instance
            or self.reads.scope != state.scope
            or str(self.reads.path) != state.read_path
            or self.reads.post_binding()
            != {"instance": state.instance, "path": str(self.path.parent)}
        ):
            self._failed = True
            raise PostControlError("post_control_binding_mismatch")

    def _verify_owner(self, state, handle=None):
        try:
            path = self.lock_path.lstat()
            actual = os.fstat(handle.fileno()) if handle is not None else path
            valid = (
                stat.S_ISREG(path.st_mode)
                and path.st_ino > 0
                and path.st_size == 32
                and (str(path.st_dev), str(path.st_ino)) == (state.owner_device, state.owner_inode)
                and (actual.st_dev, actual.st_ino) == (path.st_dev, path.st_ino)
            )
        except OSError:
            valid = False
        if not valid:
            self._failed = True
            raise PostControlError("post_owner_file_changed")

    @contextmanager
    def _ownership(self):
        with self._transaction() as conn:
            state = self._state(conn)
        try:
            handle = self.lock_path.open("r+b", buffering=0)
        except OSError:
            self._failed = True
            raise PostControlError("post_owner_unavailable") from None
        with handle:
            self._verify_owner(state, handle)
            try:
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                elif os.name == "posix":
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                else:
                    raise PostControlError("post_owner_platform_unsupported")
            except OSError:
                raise PostControlError("post_owner_busy") from None
            try:
                self._verify_owner(state, handle)
                handle.seek(0)
                if handle.read(33) != state.instance.encode():
                    self._failed = True
                    raise PostControlError("post_owner_file_changed")
                yield handle
            finally:
                if os.name == "nt":
                    try:
                        handle.seek(0)
                        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                    except OSError:
                        pass

    def _now(self, state):
        try:
            now = self._wall()
            if type(now) is not int or not 0 < now < 2**63 or now < state.wall_ns:
                raise ValueError
            return now
        except Exception:
            raise PostControlError("post_control_clock_invalid") from None

    def _write(self, conn, state, kind, **changes):
        data = {**state.model_dump(), **changes, "revision": state.revision + 1}
        updated = PostState.model_validate(data)
        body, digest = _encode(updated)
        conn.execute("UPDATE control SET body=?,digest=? WHERE id=1", (body, digest))
        conn.execute(
            "INSERT INTO events VALUES(?,?,?,?)", (updated.revision, kind, updated.wall_ns, digest)
        )
        return updated

    def stop(self, reason="operator_stop"):
        if reason not in {"operator_stop", "operation_unknown", "clock_invalid"}:
            raise PostControlError("invalid_post_stop_reason")
        self._stopped = True
        with self._transaction() as conn:
            state = self._state(conn)
            if state.phase == "STOPPED" and reason != "clock_invalid":
                return
            self._write(conn, state, "STOPPED", phase="STOPPED", reason=reason)

    def check(self):
        try:
            if self._stopped:
                raise PostControlError("private_posts_stopped")
            now = super().check()
            with self._transaction() as conn:
                state = self._state(conn)
            self._binding(state)
            self._verify_owner(state)
            self._now(state)
            if state.phase == "STOPPED":
                raise PostControlError("private_posts_stopped")
            if state.phase == "IN_FLIGHT" and state.claim != self._active_claim:
                try:
                    with self._ownership():
                        # The owner may have committed completion between the
                        # first read and this nonblocking probe. Never clear an
                        # orphan or infer completion from elapsed time.
                        with self._transaction() as conn:
                            current = self._state(conn)
                        self._binding(current)
                        self._now(current)
                        if current.phase == "STOPPED":
                            raise PostControlError("private_posts_stopped")
                        if current.phase != "READY":
                            raise PostControlError("post_claim_unresolved")
                except PostControlError as error:
                    if str(error) != "post_owner_busy":
                        raise
                    # A live OS owner is valid for a non-sending check. New
                    # operations still require exclusive ownership and READY.
            return now
        except PostControlError as error:
            if str(error) == "post_control_clock_invalid":
                self.stop("clock_invalid")
            raise
        except StreamError:
            self.stop("clock_invalid")
            raise PostControlError("post_control_clock_invalid") from None

    def snapshot(self):
        with self._transaction() as conn:
            state = self._state(conn)
        self._binding(state)
        self._verify_owner(state)
        return {
            **state.model_dump(),
            "blocked": state.phase != "READY",
            "live_enabled": False,
            "complete": False,
        }

    @contextmanager
    def operation(self, kind, *, request_sha256=None):
        # Validate metadata before acquiring ownership or consuming a claim.
        PostState(
            instance="0" * 32,
            scope="validation",
            read_instance="0" * 32,
            read_path=str(self.reads.path),
            owner_device="0",
            owner_inode="1",
            wall_ns=0,
            phase="IN_FLIGHT",
            claim="0" * 32,
            operation=kind,
            request_sha256=request_sha256,
        )
        with self._ownership() as handle:
            self.check()
            with self._transaction() as conn:
                state = self._state(conn)
                if state.phase != "READY":
                    raise PostControlError("post_claim_unresolved")
                token = uuid.uuid4().hex
                self._write(
                    conn,
                    state,
                    "CLAIMED",
                    phase="IN_FLIGHT",
                    claim=token,
                    operation=kind,
                    request_sha256=request_sha256,
                    wall_ns=self._now(state),
                )
            self._active_claim = token
            try:
                before = self.check()
                self._sleep(1.1)
                after = self.check()
                # A native timer can return just before its monotonic deadline.
                # Wait out that small remainder; never weaken the minimum interval
                # or hide a materially incomplete/injected wait.
                for _ in range(2):
                    remaining = before + 1.1 - after
                    if not 1e-9 < remaining <= 0.02:
                        break
                    self._sleep(max(remaining, 0.01))
                    after = self.check()
                if not 1.1 - 1e-9 <= after - before <= 30:
                    raise PostControlError("post_control_clock_invalid")
                with self._transaction() as conn:
                    state = self._state(conn)
                    self._verify_owner(state, handle)
                    if state.claim != token or state.phase != "IN_FLIGHT":
                        raise PostControlError("post_claim_unresolved")
                yield token
                with self._transaction() as conn:
                    state = self._state(conn)
                    self._verify_owner(state, handle)
                    if state.claim != token:
                        raise PostControlError("post_claim_unresolved")
                    self._write(
                        conn,
                        state,
                        "COMPLETED",
                        claim=None,
                        operation=None,
                        request_sha256=None,
                        phase="STOPPED" if state.phase == "STOPPED" else "READY",
                        reason=state.reason if state.phase == "STOPPED" else "completed",
                        wall_ns=self._now(state),
                    )
            except BaseException as error:
                self.stop(
                    "clock_invalid"
                    if isinstance(error, PostControlError)
                    and str(error) == "post_control_clock_invalid"
                    else "operation_unknown"
                )
                raise
            finally:
                self._active_claim = None

    @contextmanager
    def slot(self):
        with self.operation("private_stream"):
            yield


def main(argv=None):
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("init", "status", "stop"))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--read-control-directory", type=Path, required=True)
    parser.add_argument("--scope", required=True)
    args = parser.parse_args(argv)
    try:
        reads = PersistentReadLimiter(args.read_control_directory, args.scope)
        control = (
            PersistentPostLimiter.create(args.directory, reads)
            if args.command == "init"
            else PersistentPostLimiter(args.directory, reads)
        )
        if args.command == "stop":
            control.stop()
        print(json.dumps(control.snapshot(), ensure_ascii=False))
    except (ValueError, OSError, sqlite3.Error):
        parser.exit(2, "private_post_control_failed\n")


if __name__ == "__main__":
    main()
