"""Durable local Private POST/stream coordination; no keys, HTTP or trade permission."""

import hashlib
import json
import os
import sqlite3
import stat
import threading
import time
import uuid
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from trading.broker_contracts import Contract
from trading.private_stream_token import PrivateStreamLimiter, StreamBusyError, StreamError
from trading.read_control import PersistentReadLimiter
from trading.storage_init import new_storage_directory

SCHEMA = """
CREATE TABLE control (id INTEGER PRIMARY KEY CHECK(id=1),body TEXT NOT NULL,digest TEXT NOT NULL);
CREATE TABLE events (revision INTEGER PRIMARY KEY,kind TEXT NOT NULL,wall_ns INTEGER NOT NULL,
                     digest TEXT NOT NULL);
"""
REASONS = {
    "created",
    "completed",
    "operator_stop",
    "operation_unknown",
    "order_cleanup_failed",
    "clock_invalid",
    "token_failed",
    "token_recovered",
}
TOKEN_OPERATIONS = {"token_acquire", "token_renew", "token_delete"}
# 60-minute broker expiry plus bounded pacing, request duration and clock skew.
TOKEN_QUIET_SECONDS = 3660
TOKEN_CONFIRMATIONS = frozenset({"token-only", "old-clients-closed", "expiry-waited"})


class PostControlError(StreamError):
    """Fixed local codes only; no request, response or credential objects."""


class PostBusyError(PostControlError, StreamBusyError):
    pass


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
    operation: (
        Literal[
            "private_stream",
            "token_acquire",
            "token_renew",
            "token_delete",
            "order",
            "close_order",
            "cancel",
        ]
        | None
    ) = None
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
        if (
            self.operation not in {"order", "close_order", "cancel"}
            and self.request_sha256 is not None
        ):
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
        self._generation = None
        self._failed = False
        self._active_claim = None
        self._active_thread = None
        with self._transaction() as conn:
            state = self._state(conn)
            self._generation = self._epoch(conn)
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

    def _epoch(self, conn):
        return conn.execute(
            "SELECT COALESCE(MAX(revision),-1) FROM events "
            "WHERE kind IN ('TOKEN_RECOVERED','TRADE_RESOLVED')"
        ).fetchone()[0]

    def _state(self, conn):
        if self._generation is not None and self._generation != self._epoch(conn):
            raise PostControlError("new_post_control_required")
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
            self._resolution_history(conn, state)
            return state
        except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
            self._failed = True
            raise PostControlError("post_control_integrity_failed") from None

    def _resolution_history(self, conn, state):
        markers = conn.execute(
            "SELECT revision,digest FROM events WHERE kind='TRADE_RESOLVED' ORDER BY revision"
        ).fetchall()
        present = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='trade_resolutions'"
        ).fetchone()
        if not present and not markers:
            return []
        if not present:
            raise ValueError("missing_trade_resolution_history")
        rows = conn.execute(
            "SELECT revision,body,digest FROM trade_resolutions ORDER BY revision"
        ).fetchall()
        if len(rows) != len(markers):
            raise ValueError("invalid_trade_resolution_history")
        results = []
        for (revision, body, digest), marker in zip(rows, markers, strict=True):
            item = json.loads(body)
            before = PostState.model_validate_json(json.dumps(item["post_before"]))
            after = PostState.model_validate_json(json.dumps(item["post_after"]))
            reference = item["reference"]
            expected = before.model_copy(
                update={
                    "revision": before.revision + 1,
                    "phase": "STOPPED",
                    "claim": None,
                    "operation": None,
                    "request_sha256": None,
                    "wall_ns": after.wall_ns,
                    "reason": before.reason if before.phase == "STOPPED" else "operation_unknown",
                }
            )
            if (
                set(item) != {"post_before", "post_after", "reference"}
                or json.dumps(item, sort_keys=True, separators=(",", ":")) != body
                or hashlib.sha256(body.encode()).hexdigest() != digest
                or marker != (revision, _encode(after)[1])
                or after != expected
                or after.revision != revision
                or before.instance != state.instance
                or conn.execute(
                    "SELECT digest FROM events WHERE revision=?", (before.revision,)
                ).fetchone()
                != (_encode(before)[1],)
                or before.phase not in {"IN_FLIGHT", "STOPPED"}
                or before.operation not in {"order", "close_order", "cancel"}
                or before.claim is None
                or after.wall_ns < before.wall_ns
                or set(reference)
                != {"live_instance", "live_path", "client_id", "prepared_id", "prepared_sha256"}
                or not isinstance(reference["live_instance"], str)
                or len(reference["live_instance"]) != 32
                or any(c not in "0123456789abcdef" for c in reference["live_instance"])
                or not isinstance(reference["live_path"], str)
                or str(Path(reference["live_path"]).resolve()) != reference["live_path"]
                or not isinstance(reference["client_id"], str)
                or not 1 <= len(reference["client_id"]) <= 36
                or not reference["client_id"].isascii()
                or not reference["client_id"].isalnum()
                or type(reference["prepared_id"]) is not int
                or reference["prepared_id"] <= 0
                or not isinstance(reference["prepared_sha256"], str)
                or len(reference["prepared_sha256"]) != 64
                or any(c not in "0123456789abcdef" for c in reference["prepared_sha256"])
            ):
                raise ValueError("invalid_trade_resolution_history")
            results.append(item)
        return results

    def trade_resolutions(self):
        """Durable references for auditing resolved claims; never a restart permission."""
        with self._transaction() as conn:
            state = self._state(conn)
            return self._resolution_history(conn, state)

    def _resolve_trade(self, *, expected, reference, owner, validate):
        """Journal coordinator holds the OS owner and a validated live transaction.

        The prior live preparation commit is the durable authorization. This POST
        commit clears only the claim, records both states, and fences old objects.
        """
        with self._transaction() as conn:
            state = self._state(conn)
            self._binding(state)
            self._verify_owner(state, owner)
            if (
                state.model_dump() != expected
                or state.phase not in {"IN_FLIGHT", "STOPPED"}
                or state.claim is None
                or state.operation not in {"order", "close_order", "cancel"}
                or self._execution_binding(conn)
                != {"instance": reference["live_instance"], "path": reference["live_path"]}
            ):
                raise PostControlError("post_trade_resolution_checkpoint_changed")
            validate()
            updated = self._write(
                conn,
                state,
                "TRADE_RESOLVED",
                phase="STOPPED",
                claim=None,
                operation=None,
                request_sha256=None,
                wall_ns=self._now(state),
                reason=state.reason if state.phase == "STOPPED" else "operation_unknown",
            )
            item = {
                "post_before": state.model_dump(),
                "post_after": updated.model_dump(),
                "reference": reference,
            }
            body = json.dumps(item, sort_keys=True, separators=(",", ":"))
            conn.execute(
                "CREATE TABLE IF NOT EXISTS trade_resolutions (revision INTEGER PRIMARY KEY,"
                "body TEXT NOT NULL,digest TEXT NOT NULL)"
            )
            conn.execute(
                "INSERT INTO trade_resolutions VALUES(?,?,?)",
                (updated.revision, body, hashlib.sha256(body.encode()).hexdigest()),
            )
            self._resolution_history(conn, updated)
            validate()
        self._stopped = True
        return updated.model_dump()

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
    def _ownership(self, *, wait_seconds=0):
        with self._transaction() as conn:
            state = self._state(conn)
        try:
            handle = self.lock_path.open("r+b", buffering=0)
        except OSError:
            self._failed = True
            raise PostControlError("post_owner_unavailable") from None
        with handle:
            self._verify_owner(state, handle)
            started = self.check() if wait_seconds else None
            while True:
                try:
                    if os.name == "nt":
                        import msvcrt

                        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    elif os.name == "posix":
                        import fcntl

                        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    else:
                        raise PostControlError("post_owner_platform_unsupported")
                    break
                except OSError:
                    if not wait_seconds:
                        raise PostBusyError("post_owner_busy") from None
                    elapsed = self.check() - started
                    if elapsed >= wait_seconds - 1e-9:
                        raise PostBusyError("post_owner_busy") from None
                    self._sleep(min(0.05, wait_seconds - elapsed))
                    # An injected/nonadvancing wait must not spin indefinitely.
                    if self.check() - started <= elapsed:
                        self.stop("clock_invalid")
                        raise PostControlError("post_control_clock_invalid") from None
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
        if reason not in {
            "operator_stop",
            "operation_unknown",
            "clock_invalid",
            "token_failed",
            "order_cleanup_failed",
        }:
            raise PostControlError("invalid_post_stop_reason")
        self._stopped = True
        with self._transaction() as conn:
            state = self._state(conn)
            if state.phase == "STOPPED":
                if state.reason == "clock_invalid" or reason not in {
                    "operator_stop",
                    "clock_invalid",
                }:
                    return
                if state.reason == reason:
                    return
            self._write(conn, state, "STOPPED", phase="STOPPED", reason=reason)

    def fail_token(self, error=None):
        if not isinstance(error, StreamBusyError):
            self.stop(
                "clock_invalid" if str(error) == "stream_token_clock_invalid" else "token_failed"
            )

    def recover_token(self, *, expected_revision, expected_reason, expected_claim, confirmations):
        """Only expired token uncertainty; never resolve trade or legacy claims."""
        if frozenset(confirmations) != TOKEN_CONFIRMATIONS:
            raise PostControlError("explicit_token_recovery_confirmations_required")
        with self._ownership():
            with self._transaction() as conn:
                state = self._state(conn)
                self._binding(state)
                self._execution_binding(conn)
                now = self._now(state)
                if (
                    state.revision != expected_revision
                    or state.reason != expected_reason
                    or state.claim != expected_claim
                ):
                    raise PostControlError("post_recovery_checkpoint_changed")
                eligible = (
                    state.claim is not None
                    and state.operation in TOKEN_OPERATIONS
                    and state.phase in {"IN_FLIGHT", "STOPPED"}
                    and state.reason not in {"operator_stop", "clock_invalid"}
                ) or (
                    state.phase == "STOPPED"
                    and state.claim is None
                    and state.reason == "token_failed"
                )
                if not eligible:
                    raise PostControlError("post_token_recovery_refused")
                if now < state.wall_ns + TOKEN_QUIET_SECONDS * 1_000_000_000:
                    raise PostControlError("post_token_expiry_wait_required")
                self._write(
                    conn,
                    state,
                    "TOKEN_RECOVERED",
                    phase="READY",
                    claim=None,
                    operation=None,
                    request_sha256=None,
                    reason="token_recovered",
                    wall_ns=now,
                )
        # Even this operator handle cannot send after recovery. Reopen it and
        # create fresh token clients; other old handles fail the epoch check.
        self._stopped = True

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

    def owns_operation(self):
        """Only the thread holding this object's actual OS lease may dispatch."""
        return self._active_claim is not None and self._active_thread == threading.get_ident()

    def require_operation(self, kind, request_sha256):
        self.check()
        state = self.snapshot()
        if (
            not self.owns_operation()
            or state["phase"] != "IN_FLIGHT"
            or state["claim"] != self._active_claim
            or state["operation"] != kind
            or state["request_sha256"] != request_sha256
        ):
            raise PostControlError("explicit_owned_post_operation_required")
        return state["claim"]

    def execution_binding(self):
        with self._transaction() as conn:
            self._state(conn)
            return self._execution_binding(conn)

    def _execution_binding(self, conn):
        markers = conn.execute(
            "SELECT revision FROM events WHERE kind='EXECUTION_BOUND'"
        ).fetchall()
        present = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='execution_binding'"
        ).fetchone()
        if present is None and not markers:
            return None
        try:
            if present is None or len(markers) != 1:
                raise ValueError
            rows = conn.execute("SELECT id,body,digest FROM execution_binding LIMIT 2").fetchall()
            if len(rows) != 1 or rows[0][0] != 1:
                raise ValueError
            body, digest = rows[0][1:]
            binding = json.loads(body)
            if (
                set(binding) != {"instance", "path"}
                or not isinstance(binding["instance"], str)
                or len(binding["instance"]) != 32
                or any(c not in "0123456789abcdef" for c in binding["instance"])
                or not isinstance(binding["path"], str)
                or not Path(binding["path"]).is_absolute()
                or str(Path(binding["path"]).resolve()) != binding["path"]
                or json.dumps(binding, sort_keys=True, separators=(",", ":")) != body
                or hashlib.sha256(body.encode()).hexdigest() != digest
            ):
                raise ValueError
            return binding
        except (ValueError, TypeError, KeyError):
            self._failed = True
            raise PostControlError("execution_binding_integrity_failed") from None

    def bind_execution(self, instance, directory):
        binding = {"instance": instance, "path": str(Path(directory).resolve())}
        if (
            not isinstance(instance, str)
            or len(instance) != 32
            or any(c not in "0123456789abcdef" for c in instance)
        ):
            raise PostControlError("invalid_execution_binding")
        with self._ownership():
            self.check()
            with self._transaction() as conn:
                state = self._state(conn)
                if state.phase != "READY" or self.reads.status()["blocked"]:
                    raise PostControlError("execution_binding_dependencies_blocked")
                current = self._execution_binding(conn)
                if current is not None:
                    if current != binding:
                        raise PostControlError("execution_journal_already_bound")
                    return
                body = json.dumps(binding, sort_keys=True, separators=(",", ":"))
                conn.execute(
                    "CREATE TABLE execution_binding (id INTEGER PRIMARY KEY CHECK(id=1),"
                    "body TEXT NOT NULL,digest TEXT NOT NULL)"
                )
                conn.execute(
                    "INSERT INTO execution_binding VALUES(1,?,?)",
                    (body, hashlib.sha256(body.encode()).hexdigest()),
                )
                self._write(conn, state, "EXECUTION_BOUND", wall_ns=self._now(state))

    @contextmanager
    def operation(self, kind, *, request_sha256=None, _wait_seconds=0):
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
        with self._ownership(wait_seconds=_wait_seconds) as handle:
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
            self._active_thread = threading.get_ident()
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
                self._active_thread = None

    @contextmanager
    def slot(self):
        with self.operation("private_stream"):
            yield

    @contextmanager
    def token_slot(self, method):
        kinds = {"POST": "token_acquire", "PUT": "token_renew", "DELETE": "token_delete"}
        if method not in kinds:
            raise PostControlError("invalid_token_method")
        with self.operation(kinds[method], _wait_seconds=3):
            yield


def main(argv=None):
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("init", "status", "stop", "recover-token"))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--read-control-directory", type=Path, required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--expected-revision", type=int)
    parser.add_argument("--expected-reason")
    parser.add_argument("--expected-claim")
    parser.add_argument("--confirm", action="append", default=[])
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
        if args.command == "recover-token":
            control.recover_token(
                expected_revision=args.expected_revision,
                expected_reason=args.expected_reason,
                expected_claim=args.expected_claim,
                confirmations=args.confirm,
            )
            control = PersistentPostLimiter(args.directory, reads)
        print(json.dumps(control.snapshot(), ensure_ascii=False))
    except (ValueError, OSError, sqlite3.Error):
        parser.exit(2, "private_post_control_failed\n")


if __name__ == "__main__":
    main()
