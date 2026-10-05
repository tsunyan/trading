"""Inspect a bounded temporary copy of stream metadata without acquiring ownership."""

import argparse
import hashlib
import json
import re
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path

from trading.read_control_evidence import (
    EvidenceError,
    _owner,
    _rows,
    _snapshot,
    canonical,
    publish,
)
from trading.stream_control import ControlState, _body


def _inspect(conn, scope):
    conn.execute("PRAGMA query_only=ON")
    if conn.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
        raise EvidenceError("stream_evidence_copy_invalid")
    rows = _rows(conn, "control", "id,body,digest")
    if rows is None or len(rows) != 1 or rows[0]["id"] != 1:
        raise EvidenceError("stream_evidence_control_missing")
    row = rows[0]
    if not isinstance(row["body"], str):
        raise EvidenceError("stream_evidence_control_shape_invalid")
    try:
        state = ControlState.model_validate_json(row["body"])
    except ValueError:
        raise EvidenceError("stream_evidence_control_shape_invalid") from None
    if state.scope != scope:
        raise EvidenceError("stream_evidence_scope_mismatch")
    body_sha256 = hashlib.sha256(row["body"].encode()).hexdigest()
    valid_digest = isinstance(row["digest"], str) and re.fullmatch(r"[a-f0-9]{64}", row["digest"])
    control = {
        "state_unverified": state.model_dump(),
        "body_sha256": body_sha256,
        "digest": row["digest"] if valid_digest else None,
        "digest_matches_body": bool(valid_digest and body_sha256 == row["digest"]),
        "body_matches_canonical_state": row["body"] == _body(state)[0],
    }
    transitions = _rows(
        conn,
        "transitions",
        "id,revision,kind,wall_ns,state_digest",
        order="ORDER BY id DESC",
        limit=20,
    )
    if transitions is None:
        raise EvidenceError("stream_evidence_audit_missing")
    for transition in transitions:
        if (
            any(type(transition[key]) is not int for key in ("id", "revision", "wall_ns"))
            or not isinstance(transition["kind"], str)
            or not re.fullmatch(r"[A-Z_]{1,64}", transition["kind"])
            or not isinstance(transition["state_digest"], str)
            or not re.fullmatch(r"[a-f0-9]{64}", transition["state_digest"])
        ):
            raise EvidenceError("stream_evidence_audit_shape_invalid")
    return state, control, transitions


def capture(directory, scope):
    if not isinstance(scope, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", scope):
        raise EvidenceError("stream_evidence_scope_invalid")
    try:
        directory = Path(directory).resolve()
        with _snapshot(directory / "stream-control.sqlite") as (copy, source):
            with closing(sqlite3.connect(copy.as_uri() + "?mode=ro&immutable=1", uri=True)) as conn:
                state, control, transitions = _inspect(conn, scope)
            owner = _owner(directory / "stream-owner.lock", state.instance)
            owner["identity_matches_control"] = bool(
                owner["status"] == "sampled"
                and (owner["device"], owner["inode"]) == (state.owner_device, state.owner_inode)
            )
            payload = {
                "format": "stream-control-evidence-v1",
                "captured_at": datetime.now(UTC).isoformat(),
                "source": source,
                "control_unverified": control,
                "audit_tail_unverified": transitions,
                "table_row_limit": 2,
                "audit_tail_limit": 20,
                "owner_sample": owner,
                "scope_is_broker_identity": False,
                "owner_absence_established": False,
                "recovery_authorized": False,
                "history_complete": False,
                "cross_store_atomic_snapshot": False,
                "bound_stores_opened": False,
                "source_opened_with_sqlite": False,
                "network_used": False,
            }
        return {
            "payload": payload,
            "sha256": hashlib.sha256(canonical(payload).encode()).hexdigest(),
        }
    except EvidenceError as error:
        # Shared copy helpers use GET diagnostics; this CLI exposes its own fixed namespace.
        raise EvidenceError(str(error).replace("read_evidence_", "stream_evidence_", 1)) from None
    except (sqlite3.Error, OSError, TypeError, UnicodeError):
        raise EvidenceError("stream_evidence_capture_failed") from None


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.output is not None and args.output.resolve().is_relative_to(
            args.directory.resolve()
        ):
            raise EvidenceError("stream_evidence_output_outside_source_required")
        evidence = capture(args.directory, args.scope)
        body = json.dumps(evidence, indent=2) + "\n"
        if args.output is None:
            print(body, end="")
        else:
            publish(args.output, body)
            print(json.dumps({"artifact": str(args.output), "sha256": evidence["sha256"]}))
    except (EvidenceError, OSError) as error:
        reason = str(error) if isinstance(error, EvidenceError) else "stream_evidence_output_failed"
        parser.exit(2, reason + "\n")


if __name__ == "__main__":
    main()
