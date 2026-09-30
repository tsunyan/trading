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
from contextlib import closing, contextmanager
from pathlib import Path

from trading.private_read import AccountReadLimiter, PrivateReadError

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
ORPHAN_CHECKS = frozenset({"cause", "clock", "workers-paused", "get-only"})


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
            self._generation = self._current_generation(conn)

    @classmethod
    def create(cls, directory: Path, scope: str, **clocks):
        if not isinstance(scope, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", scope):
            raise PrivateReadError("invalid_control_scope")
        directory = Path(directory).resolve()
        # Explicit initialization only. Existing directories are never overwritten.
        directory.mkdir(parents=True, exist_ok=False)
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
        except (sqlite3.Error, OSError):
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
            self._verify_owner_path(owner.fileno())
            try:
                contents = owner.read(33)
            except OSError:
                contents = None
            if contents != self._instance.encode("ascii"):
                self._failed = True
                raise PrivateReadError("control_owner_file_changed")
            yield

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
        with self._transaction() as conn:
            self._halt(conn, self._state(conn), reason)

    def status(self):
        with self._transaction() as conn:
            row = self._state(conn)
            count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            generation = self._current_generation(conn)
        reopen_required = generation != self._generation
        return {
            "scope": self.scope,
            "instance_id": row["instance_id"],
            "stopped": bool(row["stopped"]),
            "reason": row["reason"],
            "in_flight": row["in_flight"] is not None,
            "blocked": bool(row["stopped"] or row["in_flight"] or reopen_required),
            "reopen_required": reopen_required,
            "generation": generation,
            "version": row["version"],
            "orphan_resolution_supported": row["version"] == 3,
            "events": count,
            "live_orders_enabled": False,
        }

    def _claim(self):
        token = uuid.uuid4().hex
        error = None
        with self._transaction() as conn:
            row = self._state(conn)
            if self._current_generation(conn) != self._generation:
                error = "read_control_reopen_required"
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
            "SELECT COALESCE(MAX(id),0) FROM events WHERE kind='RECOVERY_APPROVED'"
        ).fetchone()[0]

    def _recovery_state(self, conn):
        row = self._state(conn)
        if self._current_generation(conn) != self._generation:
            raise PrivateReadError("read_control_reopen_required")
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
        """Persist a single-use, state-bound proposal; does NOT clear a stop."""
        result = None
        with self._transaction() as conn:
            row = self._recovery_state(conn)
            now = self._stamp(conn, row)
            if now is not None:
                proposal = uuid.uuid4().hex
                self._event(conn, now, "RECOVERY_PROPOSED", proposal)
                result = {
                    "proposal": proposal,
                    "revision": self._recovery_revision(conn),
                    "scope": self.scope,
                    "instance_id": self._instance,
                    "reason": row["reason"],
                    "expires_at_ns": now + RECOVERY_TTL_NS,
                    "required_confirmations": sorted(RECOVERY_CHECKS),
                    "live_orders_enabled": False,
                }
        if result is None:
            raise PrivateReadError("control_clock_invalid")
        return result

    def approve_recovery(self, proposal, revision, *, confirmations):
        """Operator attestations only, NOT broker identity or two-person authorization.

        This atomically clears only a stopped, idle GET control. Any old client
        remains fenced by its captured generation; reopen explicitly after approval.
        """
        if (
            not isinstance(proposal, str)
            or not re.fullmatch(r"[a-f0-9]{32}", proposal)
            or not isinstance(revision, str)
            or not re.fullmatch(r"[a-f0-9]{64}", revision)
            or type(confirmations) not in {set, frozenset}
            or confirmations != RECOVERY_CHECKS
        ):
            raise PrivateReadError("recovery_confirmation_required")
        error = None
        with self._transaction() as conn:
            row = self._recovery_state(conn)
            last = conn.execute("SELECT * FROM events ORDER BY id DESC LIMIT 1").fetchone()
            if (
                not last
                or last["kind"] != "RECOVERY_PROPOSED"
                or last["token"] != proposal
                or self._recovery_revision(conn) != revision
            ):
                raise PrivateReadError("recovery_proposal_changed")
            now = self._stamp(conn, row)
            if now is None:
                error = "control_clock_invalid"
            elif not 0 <= now - last["wall_ns"] < RECOVERY_TTL_NS:
                self._event(conn, now, "RECOVERY_EXPIRED", proposal)
                error = "recovery_proposal_expired"
            else:
                self._event(conn, now, "RECOVERY_CHECKS_CONFIRMED", proposal)
                # Pre-recovery binaries only understand v1 and must fail closed
                # after an operator-approved recovery rather than bypass fencing.
                conn.execute(
                    "UPDATE control SET version=MAX(version,2),stopped=0,reason=NULL WHERE id=1"
                )
                self._event(conn, now, "RECOVERY_APPROVED", proposal)
        if error:
            raise PrivateReadError(error)
        # Do not update self._generation: this instance must not send after reset.
        return {"recovered": True, "reopen_required": True, "live_orders_enabled": False}

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
        """Propose clearing a dead GET claim, never a stop or order journal claim."""
        result = None
        with self._owner_lock(required=True), self._transaction() as conn:
            row = self._orphan_state(conn)
            now = self._stamp(conn, row)
            if now is not None:
                proposal = uuid.uuid4().hex
                self._event(conn, now, "ORPHAN_PROPOSED", proposal)
                result = {
                    "proposal": proposal,
                    "revision": self._recovery_revision(conn),
                    "scope": self.scope,
                    "instance_id": self._instance,
                    "claim": row["in_flight"],
                    "expires_at_ns": now + RECOVERY_TTL_NS,
                    "required_confirmations": sorted(ORPHAN_CHECKS),
                    "live_orders_enabled": False,
                }
        if result is None:
            raise PrivateReadError("control_clock_invalid")
        return result

    def approve_orphan_resolution(self, proposal, revision, *, confirmations):
        """Reacquire the OS lock and atomically clear only the proposed GET claim.

        The stop stays latched. A separate recovery and client reopen are required.
        """
        if (
            not isinstance(proposal, str)
            or not re.fullmatch(r"[a-f0-9]{32}", proposal)
            or not isinstance(revision, str)
            or not re.fullmatch(r"[a-f0-9]{64}", revision)
            or type(confirmations) not in {set, frozenset}
            or confirmations != ORPHAN_CHECKS
        ):
            raise PrivateReadError("orphan_confirmation_required")
        error = None
        with self._owner_lock(required=True), self._transaction() as conn:
            row = self._orphan_state(conn)
            last = conn.execute("SELECT * FROM events ORDER BY id DESC LIMIT 1").fetchone()
            if (
                not last
                or last["kind"] != "ORPHAN_PROPOSED"
                or last["token"] != proposal
                or self._recovery_revision(conn) != revision
            ):
                raise PrivateReadError("orphan_proposal_changed")
            now = self._stamp(conn, row)
            if now is None:
                error = "control_clock_invalid"
            elif not 0 <= now - last["wall_ns"] < RECOVERY_TTL_NS:
                self._event(conn, now, "ORPHAN_EXPIRED", proposal)
                error = "orphan_proposal_expired"
            else:
                self._event(conn, now, "ORPHAN_CHECKS_CONFIRMED", proposal)
                conn.execute("UPDATE control SET in_flight=NULL WHERE id=1")
                self._event(conn, now, "ORPHAN_RESOLVED", row["in_flight"])
        if error:
            raise PrivateReadError(error)
        return {"resolved": True, "stopped": True, "live_orders_enabled": False}

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
