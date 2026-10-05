"""Local durable GET coordination. No keys, network calls, or stale-lease takeover."""

import argparse
import hashlib
import json
import os
import re
import sqlite3
import stat
import sys
import time
import uuid
from contextlib import closing, contextmanager, nullcontext
from pathlib import Path

from trading.private_read import AccountReadLimiter, PrivateReadError
from trading.storage_init import new_storage_directory

SCHEMA = """
CREATE TABLE control (
    id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL,
    instance_id TEXT NOT NULL, scope TEXT NOT NULL,
    stopped INTEGER NOT NULL CHECK(stopped IN (0,1)), reason TEXT,
    in_flight TEXT, last_wall_ns INTEGER NOT NULL
);
CREATE TABLE events (
    id INTEGER PRIMARY KEY AUTOINCREMENT, wall_ns INTEGER NOT NULL,
    kind TEXT NOT NULL, token TEXT
);
CREATE TABLE owner_file (
    id INTEGER PRIMARY KEY CHECK(id=1), device TEXT NOT NULL, inode TEXT NOT NULL
);
"""
REASONS = {"client_stop", "operator_stop", "clock_invalid", "interrupted", "claim_mismatch"}
RECOVERY_CHECKS = frozenset({"cause", "permissions", "wait", "clock", "workers-paused"})
RECOVERY_TTL_NS = 300_000_000_000
OWNER_UPGRADE_MAX_BYTES = 64_000
ORPHAN_CHECKS = frozenset({"cause", "clock", "workers-paused", "get-only"})


class _ControlBusy(PrivateReadError):
    """A rolled-back transaction may be retried; this is not corruption."""


class PersistentReadLimiter(AccountReadLimiter):
    """Use the SAME local directory/scope in every process for one account.

    A durable committed claim serializes requests, without holding a DB transaction
    over network I/O. A crashed claim NEVER expires automatically. The scope is a
    local operator label, NOT verified broker identity. Do not use network filesystems.
    """

    def __init__(
        self,
        directory: Path,
        scope: str,
        *,
        wall_ns=time.time_ns,
        monotonic_ns=time.monotonic_ns,
        sleep=time.sleep,
    ):
        if not isinstance(scope, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", scope):
            raise PrivateReadError("invalid_control_scope")
        super().__init__()
        self.path = Path(directory).resolve() / "read-control.sqlite"
        self.scope = scope
        self._wall_ns = wall_ns
        self._mono_ns = monotonic_ns
        self._wait = sleep
        self._failed = False
        self._instance = None
        self._owner_identity = None
        with self._transaction() as conn:
            row = self._state(conn)
            self._instance = row["instance_id"]
            # Additive index also accelerates databases created by older versions.
            conn.execute("CREATE INDEX IF NOT EXISTS events_kind_id ON events(kind,id)")
            self._generation = self._current_generation(conn)

    @classmethod
    def create(cls, directory: Path, scope: str, **clocks):
        if not isinstance(scope, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", scope):
            raise PrivateReadError("invalid_control_scope")
        directory = Path(directory).resolve()
        # Explicit initialization only. Existing directories are never overwritten.
        with new_storage_directory(
            directory, ("read-control.sqlite-journal", "read-control.sqlite", "read-owner.lock")
        ):
            path = directory / "read-control.sqlite"
            try:
                instance = uuid.uuid4().hex
                # Never recreate this file on reopen: its filesystem identity is the
                # lock domain. Copying/restoring a directory requires separate review.
                with (directory / "read-owner.lock").open("xb") as owner:
                    owner.write(instance.encode("ascii"))
                    owner.flush()
                    os.fsync(owner.fileno())
                    identity = os.fstat(owner.fileno())
                with closing(sqlite3.connect(path)) as conn:
                    conn.execute("PRAGMA synchronous=FULL")
                    conn.executescript(SCHEMA)
                    conn.execute(
                        "INSERT INTO control VALUES (1,3,?,?,0,NULL,NULL,0)", (instance, scope)
                    )
                    conn.execute(
                        "INSERT INTO owner_file VALUES (1,?,?)",
                        (str(identity.st_dev), str(identity.st_ino)),
                    )
                    conn.execute("INSERT INTO events(wall_ns,kind) VALUES (0,'CREATED')")
                    conn.commit()
            except (sqlite3.Error, OSError):
                raise PrivateReadError("control_initialization_failed") from None
            return cls(directory, scope, **clocks)

    @contextmanager
    def _transaction(self):
        if self._failed:
            raise PrivateReadError("control_storage_failed")
        try:
            # mode=rw is essential: missing DB must never become a fresh account.
            with closing(
                sqlite3.connect(self.path.as_uri() + "?mode=rw", uri=True, timeout=1)
            ) as conn:
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA synchronous=FULL")
                conn.execute("BEGIN IMMEDIATE")
                try:
                    yield conn
                    conn.commit()
                except BaseException:
                    conn.rollback()
                    raise
        except (sqlite3.Error, OSError) as error:
            if (
                isinstance(error, sqlite3.Error)
                and (getattr(error, "sqlite_errorcode", 0) & 0xFF) == sqlite3.SQLITE_BUSY
            ):
                raise _ControlBusy("control_storage_busy") from None
            self._failed = True
            raise PrivateReadError("control_storage_failed") from None

    def _state(self, conn):
        rows = conn.execute("SELECT * FROM control").fetchall()
        row = dict(rows[0]) if len(rows) == 1 else {}
        try:
            valid = (
                row["id"] == 1
                and row["version"] in {1, 2, 3}
                and (self._owner_identity is None or row["version"] == 3)
                and row["scope"] == self.scope
                and isinstance(row["instance_id"], str)
                and re.fullmatch(r"[a-f0-9]{32}", row["instance_id"])
                and (self._instance is None or row["instance_id"] == self._instance)
                and type(row["stopped"]) is int
                and row["stopped"] in {0, 1}
                and (
                    (row["stopped"] == 0 and row["reason"] is None)
                    or (row["stopped"] == 1 and row["reason"] in REASONS)
                )
                and (
                    row["in_flight"] is None
                    or isinstance(row["in_flight"], str)
                    and re.fullmatch(r"[a-f0-9]{32}", row["in_flight"])
                )
                and type(row["last_wall_ns"]) is int
                and row["last_wall_ns"] >= 0
            )
        except (KeyError, TypeError):
            valid = False
        if not valid:
            self._failed = True
            raise PrivateReadError("invalid_control_identity_or_state")
        if row["version"] == 3:
            owners = conn.execute("SELECT id,device,inode FROM owner_file").fetchall()
            if len(owners) != 1 or owners[0]["id"] != 1:
                raise PrivateReadError("invalid_control_owner")
            identity = (owners[0]["device"], owners[0]["inode"])
            if self._owner_identity is not None and identity != self._owner_identity:
                raise PrivateReadError("invalid_control_owner")
            self._owner_identity = identity
            self._verify_owner_path()
        return row

    def _verify_owner_path(self, descriptor=None):
        try:
            path_stat = (self.path.parent / "read-owner.lock").lstat()
            candidate = path_stat if descriptor is None else os.fstat(descriptor)
            valid = (
                stat.S_ISREG(path_stat.st_mode)
                and stat.S_ISREG(candidate.st_mode)
                and candidate.st_ino > 0
                and (str(candidate.st_dev), str(candidate.st_ino)) == self._owner_identity
                and (path_stat.st_dev, path_stat.st_ino) == (candidate.st_dev, candidate.st_ino)
                and candidate.st_size == 32
            )
        except OSError:
            valid = False
        if not valid:
            self._failed = True
            raise PrivateReadError("control_owner_file_changed")

    @contextmanager
    def _owner_lock(self, *, required=False):
        with self._transaction() as conn:
            version = self._state(conn)["version"]
        if version < 3:
            if required:
                raise PrivateReadError("legacy_claim_not_resolvable")
            yield
            return
        try:
            owner = (self.path.parent / "read-owner.lock").open("r+b", buffering=0)
        except OSError:
            self._failed = True
            raise PrivateReadError("control_owner_unavailable") from None
        # Closing the descriptor releases the OS lock even if cleanup raises.
        # Process death also releases it; elapsed time and PIDs are never evidence.
        with owner:
            self._verify_owner_path(owner.fileno())
            try:
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(owner.fileno(), msvcrt.LK_NBLCK, 1)
                elif os.name == "posix":
                    import fcntl

                    fcntl.flock(owner.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                else:
                    raise PrivateReadError("control_owner_platform_unsupported")
            except OSError:
                raise PrivateReadError("read_claim_unresolved") from None
            try:
                self._verify_owner_path(owner.fileno())
                try:
                    contents = owner.read(33)
                except OSError:
                    contents = None
                if contents != self._instance.encode("ascii"):
                    self._failed = True
                    raise PrivateReadError("control_owner_file_changed")
                yield
            finally:
                if os.name == "nt":
                    # Windows may release a closed handle's byte lock late, which
                    # would refuse the next slot(). Unlock the same byte first;
                    # read() moved the position that msvcrt.locking() uses.
                    try:
                        owner.seek(0)
                        msvcrt.locking(owner.fileno(), msvcrt.LK_UNLCK, 1)
                    except OSError:
                        pass  # close() still releases it; never mask the original error.

    @staticmethod
    def _event(conn, now, kind, token=None):
        conn.execute("INSERT INTO events(wall_ns,kind,token) VALUES (?,?,?)", (now, kind, token))

    def _stamp(self, conn, row):
        try:
            now = self._wall_ns()
            valid = type(now) is int and 0 < now < 2**63 and now >= row["last_wall_ns"]
        except Exception:
            valid = False
        if not valid:
            self._halt(conn, row, "clock_invalid")
            return None
        conn.execute("UPDATE control SET last_wall_ns=? WHERE id=1", (now,))
        return now

    def _halt(self, conn, row, reason):
        if not row["stopped"] or reason == "claim_mismatch":
            conn.execute("UPDATE control SET stopped=1,reason=? WHERE id=1", (reason,))
        # Every new stop invalidates a pending recovery, even if already stopped.
        self._event(conn, row["last_wall_ns"], "STOP_" + reason.upper())

    def stop(self, reason="client_stop"):
        if reason not in REASONS:
            raise PrivateReadError("invalid_stop_reason")
        # Do not wait for a network slot: stop can be recorded while a GET runs.
        for attempt in range(3):
            try:
                with self._transaction() as conn:
                    self._halt(conn, self._state(conn), reason)
                return
            except _ControlBusy:
                if attempt == 2:
                    # An unpersisted stop must not allow this client to send again
                    # or release an in-flight claim as though stopping succeeded.
                    self._failed = True
                    raise
                self._wait(0.05)

    def _stream_binding(self, conn, *, allow_legacy=False):
        receipts = conn.execute(
            "SELECT token FROM events WHERE kind='STREAM_BOUND' LIMIT 2"
        ).fetchall()
        present = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='stream_binding'"
        ).fetchone()
        if present is None:
            if receipts:
                self._failed = True
                raise PrivateReadError("stream_binding_integrity_failed")
            return None
        rows = conn.execute("SELECT id,supervisor_id FROM stream_binding LIMIT 2").fetchall()
        if (
            len(rows) != 1
            or rows[0][0] != 1
            or not isinstance(rows[0][1], str)
            or re.fullmatch(r"[a-f0-9]{32}", rows[0][1]) is None
            or len(receipts) > 1
            or (receipts and receipts[0]["token"] != rows[0][1])
        ):
            self._failed = True
            raise PrivateReadError("stream_binding_integrity_failed")
        if not receipts and not allow_legacy:
            raise PrivateReadError("stream_binding_history_confirmation_required")
        return rows[0][1]

    def stream_binding(self):
        """Read the permanent local supervisor association; never creates it."""
        with self._transaction() as conn:
            self._state(conn)
            return self._stream_binding(conn)

    def bind_stream(self, supervisor_id, *, legacy_binding_confirmed=False):
        """One supervisor per GET domain. No reset/rebind, even after clean close.

        An additive table keeps existing GET controls compatible. Association is
        explicit, local, and not proof of broker identity or a cross-PC lock.
        """
        if (
            not isinstance(supervisor_id, str)
            or not re.fullmatch(r"[a-f0-9]{32}", supervisor_id)
            or type(legacy_binding_confirmed) is not bool
        ):
            raise PrivateReadError("invalid_stream_binding")
        owner = self._owner_lock(required=True) if legacy_binding_confirmed else nullcontext()
        with owner, self._transaction() as conn:
            state = self._state(conn)
            if (state["stopped"] and not legacy_binding_confirmed) or state["in_flight"]:
                raise PrivateReadError("stream_binding_control_blocked")
            bound = self._stream_binding(conn, allow_legacy=legacy_binding_confirmed)
            if bound is not None:
                if bound != supervisor_id:
                    raise PrivateReadError("stream_supervisor_already_bound")
                if (
                    legacy_binding_confirmed
                    and conn.execute("SELECT 1 FROM events WHERE kind='STREAM_BOUND'").fetchone()
                    is None
                ):
                    self._event(conn, state["last_wall_ns"], "STREAM_BOUND", supervisor_id)
                return
            if legacy_binding_confirmed:
                raise PrivateReadError("legacy_stream_binding_required")
            conn.execute(
                "CREATE TABLE stream_binding ("
                "id INTEGER PRIMARY KEY CHECK(id=1),supervisor_id TEXT NOT NULL)"
            )
            conn.execute("INSERT INTO stream_binding VALUES(1,?)", (supervisor_id,))
            self._event(conn, state["last_wall_ns"], "STREAM_BOUND", supervisor_id)

    def status(self):
        with self._transaction() as conn:
            row = self._state(conn)
            count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            generation = self._current_generation(conn)
            upgrade_pending = self._owner_upgrade_pending(conn)
        reopen_required = generation != self._generation
        return {
            "scope": self.scope,
            "instance_id": row["instance_id"],
            "stopped": bool(row["stopped"]),
            "reason": row["reason"],
            "in_flight": row["in_flight"] is not None,
            "blocked": bool(
                row["stopped"] or row["in_flight"] or reopen_required or upgrade_pending
            ),
            "reopen_required": reopen_required,
            "generation": generation,
            "version": row["version"],
            "orphan_resolution_supported": row["version"] == 3,
            "owner_upgrade_incomplete": upgrade_pending,
            "events": count,
            "live_orders_enabled": False,
        }

    def post_binding(self):
        """Read one permanent POST domain; never create or repair it implicitly."""
        with self._transaction() as conn:
            self._state(conn)
            return self._post_binding(conn)

    def _post_binding(self, conn):
        receipts = conn.execute(
            "SELECT token FROM events WHERE kind='POST_BOUND' LIMIT 2"
        ).fetchall()
        present = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='post_binding'"
        ).fetchone()
        if present is None:
            if receipts:
                self._failed = True
                raise PrivateReadError("post_binding_integrity_failed")
            return None
        rows = conn.execute("SELECT id,instance,path FROM post_binding LIMIT 2").fetchall()
        if (
            len(rows) != 1
            or rows[0][0] != 1
            or not isinstance(rows[0][1], str)
            or not re.fullmatch(r"[a-f0-9]{32}", rows[0][1])
            or not isinstance(rows[0][2], str)
            or not Path(rows[0][2]).is_absolute()
            or str(Path(rows[0][2]).resolve()) != rows[0][2]
            or len(receipts) != 1
            or receipts[0]["token"]
            != hashlib.sha256((rows[0][1] + ":" + rows[0][2]).encode()).hexdigest()
        ):
            self._failed = True
            raise PrivateReadError("post_binding_integrity_failed")
        return {"instance": rows[0][1], "path": rows[0][2]}

    def bind_post(self, instance, path):
        """One POST domain per GET control, without rebinding after stop or deletion."""
        if (
            not isinstance(instance, str)
            or not re.fullmatch(r"[a-f0-9]{32}", instance)
            or not isinstance(path, Path)
            or not path.is_absolute()
        ):
            raise PrivateReadError("invalid_post_binding")
        expected = {"instance": instance, "path": str(path.resolve())}
        with self._transaction() as conn:
            state = self._state(conn)
            if state["stopped"] or state["in_flight"]:
                raise PrivateReadError("post_binding_control_blocked")
            current = self._post_binding(conn)
            if current is not None:
                if current != expected:
                    raise PrivateReadError("post_control_already_bound")
                return
            conn.execute(
                "CREATE TABLE post_binding (id INTEGER PRIMARY KEY CHECK(id=1),"
                "instance TEXT NOT NULL,path TEXT NOT NULL)"
            )
            conn.execute("INSERT INTO post_binding VALUES(1,?,?)", (instance, expected["path"]))
            self._event(
                conn,
                state["last_wall_ns"],
                "POST_BOUND",
                hashlib.sha256((instance + ":" + expected["path"]).encode()).hexdigest(),
            )

    def _claim(self):
        token = uuid.uuid4().hex
        error = None
        with self._transaction() as conn:
            row = self._state(conn)
            if self._current_generation(conn) != self._generation:
                error = "read_control_reopen_required"
            elif self._owner_upgrade_pending(conn):
                error = "read_owner_upgrade_incomplete"
            elif row["stopped"]:
                error = "account_reads_stopped"
            elif row["in_flight"]:
                error = "read_claim_unresolved"
            else:
                now = self._stamp(conn, row)
                if now is None:
                    error = "control_clock_invalid"
                else:
                    conn.execute("UPDATE control SET in_flight=? WHERE id=1", (token,))
                    self._event(conn, now, "CLAIMED", token)
        if error:
            raise PrivateReadError(error)
        return token

    @staticmethod
    def _current_generation(conn):
        return conn.execute(
            "SELECT COALESCE(MAX(id),0) FROM events WHERE kind IN "
            "('RECOVERY_APPROVED','OWNER_UPGRADE_STARTED','OWNER_UPGRADED')"
        ).fetchone()[0]

    def _owner_upgrade_pending(self, conn):
        """A committed upgrade intent fences reads until its filesystem/DB commit finishes."""
        receipts = conn.execute(
            "SELECT kind,token FROM events WHERE kind IN "
            "('OWNER_UPGRADE_STARTED','OWNER_UPGRADED') ORDER BY id LIMIT 3"
        ).fetchall()
        table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='read_owner_upgrade'"
        ).fetchone()
        if table is None and not receipts:
            return False
        try:
            if table is None:
                raise ValueError
            rows = conn.execute(
                "SELECT id,typeof(body) AS body_type,length(CAST(body AS BLOB)) AS body_bytes,"
                "typeof(digest) AS digest_type,length(CAST(digest AS BLOB)) AS digest_bytes "
                "FROM read_owner_upgrade LIMIT 2"
            ).fetchall()
            if (
                len(rows) != 1
                or rows[0]["id"] != 1
                or rows[0]["body_type"] != "text"
                or not 1 <= rows[0]["body_bytes"] <= OWNER_UPGRADE_MAX_BYTES
                or rows[0]["digest_type"] != "text"
                or rows[0]["digest_bytes"] != 64
            ):
                raise ValueError
            body, digest = conn.execute(
                "SELECT body,digest FROM read_owner_upgrade WHERE id=1"
            ).fetchone()
            plan = json.loads(body)
            source = plan["source"]["control"]
            state = self._state(conn)
            if (
                hashlib.sha256(body.encode()).hexdigest() != digest
                or plan["source"]["directory"] != str(self.path.parent)
                or source["version"] not in {1, 2}
                or source["instance_id"] != state["instance_id"]
                or source["scope"] != state["scope"]
                or [(r["kind"], r["token"]) for r in receipts]
                not in (
                    [("OWNER_UPGRADE_STARTED", digest)],
                    [("OWNER_UPGRADE_STARTED", digest), ("OWNER_UPGRADED", digest)],
                )
                or state["version"] != (3 if len(receipts) == 2 else source["version"])
            ):
                raise ValueError
        except (ValueError, KeyError, TypeError, AttributeError):
            raise PrivateReadError("read_owner_upgrade_integrity_failed") from None
        return len(receipts) == 1

    def _recovery_state(self, conn):
        row = self._state(conn)
        if self._current_generation(conn) != self._generation:
            raise PrivateReadError("read_control_reopen_required")
        if self._owner_upgrade_pending(conn):
            raise PrivateReadError("read_owner_upgrade_incomplete")
        if not row["stopped"] or row["in_flight"] or row["reason"] == "claim_mismatch":
            raise PrivateReadError("control_not_recoverable")
        return row

    def _recovery_revision(self, conn):
        last = conn.execute("SELECT * FROM events ORDER BY id DESC LIMIT 1").fetchone()
        if not last:
            raise PrivateReadError("missing_control_audit")
        state = {"control": self._state(conn), "last_event": dict(last)}
        return hashlib.sha256(json.dumps(state, sort_keys=True).encode()).hexdigest()

    def prepare_recovery(self):
        """Persist a state-bound proposal; do not clear the stop."""
        return self._prepare_resolution("recovery")

    def approve_recovery(self, proposal, revision, *, confirmations):
        """Clear an idle stop; old clients stay fenced until explicitly reopened."""
        return self._approve_resolution("recovery", proposal, revision, confirmations)

    def _orphan_state(self, conn):
        row = self._state(conn)
        if self._current_generation(conn) != self._generation:
            raise PrivateReadError("read_control_reopen_required")
        if (
            row["version"] != 3
            or not row["stopped"]
            or not row["in_flight"]
            or row["reason"] == "claim_mismatch"
        ):
            raise PrivateReadError("claim_not_resolvable")
        last_claim = conn.execute(
            "SELECT kind,token FROM events "
            "WHERE kind IN ('CLAIMED','COMPLETED','FAILED','ORPHAN_RESOLVED') "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if (
            not last_claim
            or last_claim["kind"] != "CLAIMED"
            or last_claim["token"] != row["in_flight"]
        ):
            raise PrivateReadError("claim_not_resolvable")
        return row

    def prepare_orphan_resolution(self):
        """Propose clearing a dead GET claim, never the stop."""
        return self._prepare_resolution("orphan")

    def approve_orphan_resolution(self, proposal, revision, *, confirmations):
        """Reacquire the owner lock and clear only the proposed claim."""
        return self._approve_resolution("orphan", proposal, revision, confirmations)

    def _resolution_context(self, kind):
        return self._owner_lock(required=True) if kind == "orphan" else nullcontext()

    def _resolution_state(self, conn, kind):
        return self._orphan_state(conn) if kind == "orphan" else self._recovery_state(conn)

    def _prepare_resolution(self, kind):
        result = None
        checks = ORPHAN_CHECKS if kind == "orphan" else RECOVERY_CHECKS
        with self._resolution_context(kind), self._transaction() as conn:
            row = self._resolution_state(conn, kind)
            now = self._stamp(conn, row)
            if now is not None:
                proposal = uuid.uuid4().hex
                self._event(conn, now, kind.upper() + "_PROPOSED", proposal)
                result = {
                    "proposal": proposal,
                    "revision": self._recovery_revision(conn),
                    "scope": self.scope,
                    "instance_id": self._instance,
                    "expires_at_ns": now + RECOVERY_TTL_NS,
                    "required_confirmations": sorted(checks),
                    "live_orders_enabled": False,
                }
                if kind == "orphan":
                    result["claim"] = row["in_flight"]
                else:
                    result["reason"] = row["reason"]
        if result is None:
            raise PrivateReadError("control_clock_invalid")
        return result

    def _approve_resolution(self, kind, proposal, revision, confirmations):
        checks = ORPHAN_CHECKS if kind == "orphan" else RECOVERY_CHECKS
        if (
            not isinstance(proposal, str)
            or not re.fullmatch(r"[a-f0-9]{32}", proposal)
            or not isinstance(revision, str)
            or not re.fullmatch(r"[a-f0-9]{64}", revision)
            or type(confirmations) not in {set, frozenset}
            or confirmations != checks
        ):
            raise PrivateReadError(kind + "_confirmation_required")
        error = None
        with self._resolution_context(kind), self._transaction() as conn:
            row = self._resolution_state(conn, kind)
            last = conn.execute("SELECT * FROM events ORDER BY id DESC LIMIT 1").fetchone()
            if (
                not last
                or last["kind"] != kind.upper() + "_PROPOSED"
                or last["token"] != proposal
                or self._recovery_revision(conn) != revision
            ):
                raise PrivateReadError(kind + "_proposal_changed")
            now = self._stamp(conn, row)
            if now is None:
                error = "control_clock_invalid"
            elif not 0 <= now - last["wall_ns"] < RECOVERY_TTL_NS:
                self._event(conn, now, kind.upper() + "_EXPIRED", proposal)
                error = kind + "_proposal_expired"
            else:
                self._event(conn, now, kind.upper() + "_CHECKS_CONFIRMED", proposal)
                if kind == "orphan":
                    conn.execute("UPDATE control SET in_flight=NULL WHERE id=1")
                    self._event(conn, now, "ORPHAN_RESOLVED", row["in_flight"])
                else:
                    # Old v1 binaries must fail closed after recovery. Do not
                    # update self._generation: this object must remain fenced.
                    conn.execute(
                        "UPDATE control SET version=MAX(version,2),stopped=0,reason=NULL WHERE id=1"
                    )
                    self._event(conn, now, "RECOVERY_APPROVED", proposal)
        if error:
            raise PrivateReadError(error)
        if kind == "orphan":
            return {"resolved": True, "stopped": True, "live_orders_enabled": False}
        return {"recovered": True, "reopen_required": True, "live_orders_enabled": False}

    def _check_claim(self, token):
        error = None
        with self._transaction() as conn:
            row = self._state(conn)
            if row["in_flight"] != token:
                self._halt(conn, row, "claim_mismatch")
                error = "read_claim_mismatch"
            elif row["stopped"]:
                error = "account_reads_stopped"
            elif self._stamp(conn, row) is None:
                error = "control_clock_invalid"
        if error:
            raise PrivateReadError(error)

    def _finish(self, token, outcome):
        # Retry only the local completion transaction, never the HTTP request.
        # The owner lock remains held until the claim is durably released.
        for attempt in range(3):
            try:
                return self._finish_once(token, outcome)
            except _ControlBusy:
                if attempt == 2:
                    raise
                self._wait(0.05)

    def _finish_once(self, token, outcome):
        error = None
        with self._transaction() as conn:
            row = self._state(conn)
            if row["in_flight"] != token:
                self._halt(conn, row, "claim_mismatch")
                error = "read_claim_mismatch"
            else:
                now = self._stamp(conn, row)
                if now is None:
                    now = row["last_wall_ns"]
                    error = "control_clock_invalid"
                conn.execute("UPDATE control SET in_flight=NULL WHERE id=1")
                self._event(conn, now, outcome, token)
        if error:
            raise PrivateReadError(error)

    @contextmanager
    def slot(self):
        with self._owner_lock(), self._claimed_slot():
            yield

    @contextmanager
    def _claimed_slot(self):
        token = self._claim()  # Committed BEFORE any wait or network operation.
        outcome = "FAILED"
        try:
            # Always wait after acquiring ownership: a forward wall-clock jump
            # or process restart cannot bypass minimum inter-request spacing.
            try:
                before = self._mono_ns()
                self._wait(0.25)
                after = self._mono_ns()
            except Exception:
                self.stop("clock_invalid")
                raise PrivateReadError("control_wait_invalid") from None
            if (
                type(before) is not int
                or type(after) is not int
                or not 250_000_000 <= after - before <= 5_000_000_000
            ):
                self.stop("clock_invalid")
                raise PrivateReadError("control_wait_invalid")
            self._check_claim(token)
            yield
            outcome = "COMPLETED"
        except BaseException as exc:
            if not isinstance(exc, Exception):
                self.stop("interrupted")
            raise
        finally:
            self._finish(token, outcome)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=(
            "init",
            "status",
            "stop",
            "prepare-recovery",
            "approve-recovery",
            "prepare-orphan",
            "approve-orphan",
        ),
    )
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--proposal")
    parser.add_argument("--revision")
    for check in sorted(RECOVERY_CHECKS | ORPHAN_CHECKS):
        parser.add_argument("--confirm-" + check, action="store_true")
    args = parser.parse_args(argv)
    try:
        control = (
            PersistentReadLimiter.create(args.directory, args.scope)
            if args.command == "init"
            else PersistentReadLimiter(args.directory, args.scope)
        )
        if args.command == "stop":
            control.stop("operator_stop")
        if args.command == "prepare-recovery":
            result = control.prepare_recovery()
        elif args.command == "prepare-orphan":
            result = control.prepare_orphan_resolution()
        elif args.command == "approve-orphan":
            if not sys.stdin.isatty():
                raise PrivateReadError("interactive_orphan_resolution_required")
            confirmations = {
                check
                for check in ORPHAN_CHECKS
                if getattr(args, "confirm_" + check.replace("-", "_"))
            }
            if confirmations != ORPHAN_CHECKS:
                raise PrivateReadError("orphan_confirmation_required")
            if input("Clear dead GET claim only by typing CLEAR GET CLAIM: ") != "CLEAR GET CLAIM":
                raise PrivateReadError("orphan_not_confirmed")
            result = control.approve_orphan_resolution(
                args.proposal, args.revision, confirmations=confirmations
            )
        elif args.command == "approve-recovery":
            if not sys.stdin.isatty():
                raise PrivateReadError("interactive_recovery_required")
            confirmations = {
                check
                for check in RECOVERY_CHECKS
                if getattr(args, "confirm_" + check.replace("-", "_"))
            }
            if confirmations != RECOVERY_CHECKS:
                raise PrivateReadError("recovery_confirmation_required")
            if input("Confirm GET-only recovery by typing RESUME READS: ") != "RESUME READS":
                raise PrivateReadError("recovery_not_confirmed")
            result = control.approve_recovery(
                args.proposal, args.revision, confirmations=confirmations
            )
        else:
            result = control.status()
        print(json.dumps(result))
    except (PrivateReadError, OSError, EOFError):
        parser.exit(2, "Read control failed; check scope, state and directory.\n")


if __name__ == "__main__":
    main()
