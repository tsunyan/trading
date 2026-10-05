"""Explicit stopped, idle GET protocol upgrade. No keys, HTTP or claim resolution."""

import argparse
import hashlib
import json
import os
import re
import stat
import sys
import uuid
from pathlib import Path

from trading.private_read import PrivateReadError
from trading.read_control import OWNER_UPGRADE_MAX_BYTES, RECOVERY_TTL_NS, PersistentReadLimiter

UPGRADE_CHECKS = frozenset(
    {"cause", "clock", "workers-paused", "old-clients-closed", "get-only", "preserve-stops"}
)
COMPLETION_CHECKS = frozenset(
    {"finish-recorded-upgrade", "old-clients-closed", "get-only", "preserve-stops"}
)


def encoded(value):
    body = json.dumps(value, sort_keys=True, separators=(",", ":"))
    if len(body.encode("utf-8")) > OWNER_UPGRADE_MAX_BYTES:
        raise PrivateReadError("read_owner_upgrade_payload_too_large")
    return body


class ReadOwnerUpgrade:
    def __init__(self, directory, scope, **clocks):
        self.reads = PersistentReadLimiter(directory, scope, **clocks)
        self.owner_path = self.reads.path.parent / "read-owner.lock"

    def context(self):
        result = self.reads.status()
        with self.reads._transaction() as conn:
            pending = self.reads._owner_upgrade_pending(conn)
            result["intent_sha256"] = (
                conn.execute("SELECT digest FROM read_owner_upgrade WHERE id=1").fetchone()[0]
                if pending
                else None
            )
        return {
            **result,
            "owner_artifact_present": os.path.lexists(self.owner_path),
            "network_used": False,
        }

    def _source(self, conn):
        return {
            "directory": str(self.reads.path.parent),
            "control": self.reads._state(conn),
            "stream": self.reads._stream_binding(conn, allow_legacy=True),
            "post": self.reads._post_binding(conn),
        }

    def _legacy(self, conn):
        source = self._source(conn)
        state = source["control"]
        if self.reads._owner_upgrade_pending(conn):
            raise PrivateReadError("read_owner_upgrade_incomplete")
        if self.reads._current_generation(conn) != self.reads._generation:
            raise PrivateReadError("read_control_reopen_required")
        if (
            state["version"] not in {1, 2}
            or not state["stopped"]
            or state["in_flight"] is not None
            or state["reason"] == "claim_mismatch"
        ):
            raise PrivateReadError("read_owner_upgrade_requires_idle_legacy_stop")
        table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='owner_file'"
        ).fetchone()
        if table is not None or os.path.lexists(self.owner_path):
            raise PrivateReadError("read_owner_upgrade_existing_owner_artifact")
        token = None
        for kind, value in conn.execute(
            "SELECT kind,token FROM events WHERE kind IN "
            "('CLAIMED','COMPLETED','FAILED','ORPHAN_RESOLVED') ORDER BY id"
        ):
            if kind == "CLAIMED":
                if (
                    token is not None
                    or not isinstance(value, str)
                    or not re.fullmatch(r"[a-f0-9]{32}", value)
                ):
                    raise PrivateReadError("read_owner_upgrade_claim_history_required")
                token = value
            else:
                if token is None or value != token:
                    raise PrivateReadError("read_owner_upgrade_claim_history_required")
                token = None
        if token is not None:
            raise PrivateReadError("read_owner_upgrade_claim_history_required")
        return source

    def prepare(self):
        result = None
        with self.reads._transaction() as conn:
            source = self._legacy(conn)
            now = self.reads._stamp(conn, source["control"])
            if now is not None:
                payload = {"proposal": uuid.uuid4().hex, "source": self._source(conn)}
                self.reads._event(conn, now, "OWNER_UPGRADE_PROPOSED", encoded(payload))
                result = {
                    "proposal": payload["proposal"],
                    "revision": self.reads._recovery_revision(conn),
                    "source_version": source["control"]["version"],
                    "stopped": True,
                    "expires_wall_ns": now + RECOVERY_TTL_NS,
                    "upgraded": False,
                }
        if result is None:
            raise PrivateReadError("control_clock_invalid")
        return result

    def approve(self, proposal, revision, *, confirmations):
        if (
            not isinstance(proposal, str)
            or not re.fullmatch(r"[a-f0-9]{32}", proposal)
            or not isinstance(revision, str)
            or not re.fullmatch(r"[a-f0-9]{64}", revision)
            or type(confirmations) not in {set, frozenset}
            or confirmations != UPGRADE_CHECKS
        ):
            raise PrivateReadError("read_owner_upgrade_confirmations_required")
        error = None
        installation = None
        with self.reads._transaction() as conn:
            source = self._legacy(conn)
            last = conn.execute("SELECT * FROM events ORDER BY id DESC LIMIT 1").fetchone()
            if last is None or last["kind"] != "OWNER_UPGRADE_PROPOSED":
                raise PrivateReadError("read_owner_upgrade_proposal_changed")
            try:
                payload = json.loads(last["token"])
            except (ValueError, TypeError):
                raise PrivateReadError("read_owner_upgrade_proposal_changed") from None
            if (
                payload != {"proposal": proposal, "source": source}
                or self.reads._recovery_revision(conn) != revision
            ):
                raise PrivateReadError("read_owner_upgrade_proposal_changed")
            now = self.reads._stamp(conn, source["control"])
            if now is None:
                error = "control_clock_invalid"
            elif not 0 <= now - last["wall_ns"] < RECOVERY_TTL_NS:
                self.reads._event(conn, now, "OWNER_UPGRADE_EXPIRED", proposal)
                error = "read_owner_upgrade_proposal_expired"
            else:
                plan = {
                    "proposal": proposal,
                    "source": self._source(conn),
                    "confirmations": sorted(confirmations),
                }
                body = encoded(plan)
                digest = hashlib.sha256(body.encode()).hexdigest()
                conn.execute(
                    "CREATE TABLE read_owner_upgrade (id INTEGER PRIMARY KEY CHECK(id=1),"
                    "body TEXT NOT NULL,digest TEXT NOT NULL)"
                )
                conn.execute("INSERT INTO read_owner_upgrade VALUES(1,?,?)", (body, digest))
                self.reads._event(conn, now, "OWNER_UPGRADE_STARTED", digest)
                started_id = conn.execute("SELECT MAX(id) FROM events").fetchone()[0]
                installation = (plan, body, digest, started_id, now)
        if error is not None:
            raise PrivateReadError(error)
        # Commit the fence before any filesystem mutation; a partial installation
        # never resumes legacy reads or adopts an unrecorded lock file.
        self._install(*installation)
        return {
            "upgraded": True,
            "version": 3,
            "stopped": True,
            "reopen_required": True,
            "claim_resolved": False,
            "live_orders_enabled": False,
        }

    def complete(self, expected_sha256, *, confirmations):
        """Finish only the committed decision whose owner file has not been created."""
        if (
            not isinstance(expected_sha256, str)
            or not re.fullmatch(r"[a-f0-9]{64}", expected_sha256)
            or type(confirmations) not in {set, frozenset}
            or confirmations != COMPLETION_CHECKS
        ):
            raise PrivateReadError("read_owner_upgrade_completion_checks_required")
        with self.reads._transaction() as conn:
            if not self.reads._owner_upgrade_pending(conn):
                raise PrivateReadError("read_owner_upgrade_not_incomplete")
            body, digest = conn.execute(
                "SELECT body,digest FROM read_owner_upgrade WHERE id=1"
            ).fetchone()
            plan = json.loads(body)
            started = conn.execute(
                "SELECT id,wall_ns FROM events WHERE kind='OWNER_UPGRADE_STARTED'"
            ).fetchone()
            if (
                expected_sha256 != digest
                or plan.get("confirmations") != sorted(UPGRADE_CHECKS)
                or self._source(conn) != plan["source"]
                or conn.execute("SELECT MAX(id) FROM events").fetchone()[0] != started["id"]
            ):
                raise PrivateReadError("read_owner_upgrade_checkpoint_changed")
            if os.path.lexists(self.owner_path):
                raise PrivateReadError("read_owner_upgrade_unrecorded_owner_artifact")
        # Approval is already committed. Its expired TTL cannot undo that decision;
        # repeat the installation only if there is no unrecorded filesystem artifact.
        self._install(plan, body, digest, started["id"], started["wall_ns"])
        return {
            "upgraded": True,
            "version": 3,
            "stopped": True,
            "completed_recorded_upgrade": True,
            "reopen_required": True,
            "claim_resolved": False,
            "live_orders_enabled": False,
        }

    def _install(self, plan, body, digest, started_id, now):
        with self.owner_path.open("xb", buffering=0) as handle:
            handle.write(plan["source"]["control"]["instance_id"].encode("ascii"))
            os.fsync(handle.fileno())
            identity = os.fstat(handle.fileno())
            with self.reads._transaction() as conn:
                record = conn.execute(
                    "SELECT body,digest FROM read_owner_upgrade WHERE id=1"
                ).fetchone()
                current = self.owner_path.lstat()
                if (
                    self._source(conn) != plan["source"]
                    or conn.execute("SELECT MAX(id) FROM events").fetchone()[0] != started_id
                    or record is None
                    or tuple(record) != (body, digest)
                    or not self.reads._owner_upgrade_pending(conn)
                    or not stat.S_ISREG(current.st_mode)
                    or current.st_size != 32
                    or (current.st_dev, current.st_ino) != (identity.st_dev, identity.st_ino)
                    or identity.st_ino <= 0
                ):
                    raise PrivateReadError("read_owner_upgrade_checkpoint_changed")
                conn.execute(
                    "CREATE TABLE owner_file (id INTEGER PRIMARY KEY CHECK(id=1),"
                    "device TEXT NOT NULL,inode TEXT NOT NULL)"
                )
                conn.execute(
                    "INSERT INTO owner_file VALUES(1,?,?)",
                    (str(identity.st_dev), str(identity.st_ino)),
                )
                conn.execute("UPDATE control SET version=3 WHERE id=1")
                self.reads._event(conn, now, "OWNER_UPGRADED", digest)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("status", "prepare", "approve", "complete"))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--proposal")
    parser.add_argument("--revision")
    parser.add_argument("--intent-sha256")
    for item in sorted(UPGRADE_CHECKS | COMPLETION_CHECKS):
        parser.add_argument("--confirm-" + item, action="store_true")
    args = parser.parse_args(argv)
    try:
        upgrade = ReadOwnerUpgrade(args.directory, args.scope)
        if args.command == "status":
            result = upgrade.context()
        elif args.command == "prepare":
            result = upgrade.prepare()
        else:
            if not sys.stdin.isatty():
                raise PrivateReadError("interactive_read_owner_upgrade_required")
            checks = COMPLETION_CHECKS if args.command == "complete" else UPGRADE_CHECKS
            confirmations = {
                item for item in checks if getattr(args, "confirm_" + item.replace("-", "_"))
            }
            if confirmations != checks:
                raise PrivateReadError("read_owner_upgrade_confirmations_required")
            phrase = (
                "FINISH RECORDED GET UPGRADE"
                if args.command == "complete"
                else "UPGRADE IDLE GET OWNER"
            )
            if input(f"Type {phrase}: ") != phrase:
                raise PrivateReadError("read_owner_upgrade_not_confirmed")
            result = (
                upgrade.complete(args.intent_sha256, confirmations=confirmations)
                if args.command == "complete"
                else upgrade.approve(args.proposal, args.revision, confirmations=confirmations)
            )
        print(json.dumps(result))
    except (PrivateReadError, OSError, ValueError, EOFError) as error:
        reason = (
            str(error) if re.fullmatch(r"[a-z_]{1,64}", str(error)) else "read_owner_upgrade_failed"
        )
        parser.exit(2, reason + "\n")


if __name__ == "__main__":
    main()
