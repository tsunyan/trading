"""Copy and inspect bounded GET metadata without opening the source with SQLite."""

import argparse
import hashlib
import json
import os
import re
import sqlite3
import stat
import tempfile
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path

MAX_DB_BYTES = 256 * 1024 * 1024
MAX_FIELD_BYTES = 64_000
TABLES = {
    "control": "id,version,instance_id,scope,stopped,reason,in_flight,last_wall_ns",
    "owner_file": "id,device,inode",
    "post_binding": "id,instance,path",
    "stream_binding": "id,supervisor_id",
}


class EvidenceError(ValueError):
    """Fixed diagnostic errors, with no recovery capability."""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _identity(value):
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns


def _sidecars(path):
    return any(os.path.lexists(str(path) + suffix) for suffix in ("-journal", "-wal", "-shm"))


def _stream(source, target=None):
    digest, total = hashlib.sha256(), 0
    while block := source.read(1024 * 1024):
        total += len(block)
        if total > MAX_DB_BYTES:
            raise EvidenceError("read_evidence_source_too_large")
        digest.update(block)
        if target is not None:
            target.write(block)
    return digest.hexdigest(), total


@contextmanager
def _snapshot(path):
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_ino <= 0:
        raise EvidenceError("read_evidence_source_not_regular")
    if not 20 <= before.st_size <= MAX_DB_BYTES:
        raise EvidenceError("read_evidence_source_size_invalid")
    if _sidecars(path):
        raise EvidenceError("read_evidence_source_sidecars_present")
    with (
        path.open("rb") as source,
        tempfile.TemporaryDirectory(prefix="trading-read-evidence-") as temporary,
    ):
        if _identity(os.fstat(source.fileno())) != _identity(before):
            raise EvidenceError("read_evidence_source_changed")
        header = source.read(20)
        if header[:16] != b"SQLite format 3\0" or header[18:20] != b"\x01\x01":
            raise EvidenceError("read_evidence_rollback_database_required")
        source.seek(0)
        copy = Path(temporary) / "snapshot.sqlite"
        with copy.open("xb") as output:
            digest, size = _stream(source, output)
        yield (
            copy,
            {
                "path": str(path),
                "sha256": digest,
                "bytes": size,
                "device": str(before.st_dev),
                "inode": str(before.st_ino),
            },
        )
        source.seek(0)
        if (
            _stream(source) != (digest, size)
            or _identity(os.fstat(source.fileno())) != _identity(before)
            or _identity(path.lstat()) != _identity(before)
            or _sidecars(path)
        ):
            raise EvidenceError("read_evidence_source_changed")


def _rows(conn, table, columns, *, order="", limit=2):
    # Every SQL identifier comes from this module's fixed calls, never CLI input.
    if (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        is None
    ):
        return None
    fields = columns.split(",")
    lengths = ",".join(f"length(CAST({field} AS BLOB))" for field in fields)
    sizes = conn.execute(f"SELECT {lengths} FROM {table} {order} LIMIT {limit}").fetchall()
    if any(size is not None and size > MAX_FIELD_BYTES for row in sizes for size in row):
        raise EvidenceError("read_evidence_field_too_large")
    values = conn.execute(f"SELECT {columns} FROM {table} {order} LIMIT {limit}").fetchall()
    if any(value is not None and type(value) not in {str, int} for row in values for value in row):
        raise EvidenceError("read_evidence_field_type_invalid")
    return [dict(zip(fields, row, strict=True)) for row in values]


def _metadata_shapes(tables):
    # Missing/empty optional tables remain useful evidence for legacy or damaged stores.
    # Reject malformed values rather than exposing arbitrary strings as identifiers.
    for name in ("owner_file", "post_binding", "stream_binding"):
        rows = tables[name]
        if rows is None or not rows:
            continue
        if len(rows) != 1 or type(rows[0]["id"]) is not int or rows[0]["id"] != 1:
            raise EvidenceError("read_evidence_metadata_shape_invalid")
        row = rows[0]
        if name == "owner_file":
            valid = all(
                isinstance(row[key], str) and re.fullmatch(r"[0-9]+", row[key])
                for key in ("device", "inode")
            )
        elif name == "post_binding":
            valid = (
                isinstance(row["instance"], str)
                and re.fullmatch(r"[a-f0-9]{32}", row["instance"])
                and isinstance(row["path"], str)
                and "\0" not in row["path"]
                and Path(row["path"]).is_absolute()
            )
        else:
            valid = isinstance(row["supervisor_id"], str) and re.fullmatch(
                r"[a-f0-9]{32}", row["supervisor_id"]
            )
        if not valid:
            raise EvidenceError("read_evidence_metadata_shape_invalid")


def _owner(path, instance):
    result = {"path": str(path), "owner_absence_established": False}
    try:
        before = path.lstat()
    except FileNotFoundError:
        return {**result, "status": "missing"}
    if not stat.S_ISREG(before.st_mode) or before.st_ino <= 0 or before.st_size != 32:
        return {**result, "status": "unsupported_file"}
    with path.open("rb") as source:
        body = source.read(33)
        if (
            _identity(os.fstat(source.fileno())) != _identity(before)
            or _identity(path.lstat()) != _identity(before)
            or len(body) != 32
        ):
            raise EvidenceError("read_evidence_owner_changed")
    return {
        **result,
        "status": "sampled",
        "device": str(before.st_dev),
        "inode": str(before.st_ino),
        "sha256": hashlib.sha256(body).hexdigest(),
        "contents_match_instance": body == instance.encode("ascii"),
    }


def capture(directory, scope):
    if not isinstance(scope, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", scope):
        raise EvidenceError("read_evidence_scope_invalid")
    directory = Path(directory).resolve()
    try:
        with _snapshot(directory / "read-control.sqlite") as (copy, source):
            with closing(sqlite3.connect(copy.as_uri() + "?mode=ro&immutable=1", uri=True)) as conn:
                conn.execute("PRAGMA query_only=ON")
                if conn.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                    raise EvidenceError("read_evidence_copy_invalid")
                tables = {name: _rows(conn, name, fields) for name, fields in TABLES.items()}
                _metadata_shapes(tables)
                control = tables["control"]
                if control is None or len(control) != 1 or control[0]["id"] != 1:
                    raise EvidenceError("read_evidence_control_missing")
                control = control[0]
                if control["scope"] != scope:
                    raise EvidenceError("read_evidence_scope_mismatch")
                if not isinstance(control["instance_id"], str) or not re.fullmatch(
                    r"[a-f0-9]{32}", control["instance_id"]
                ):
                    raise EvidenceError("read_evidence_instance_invalid")
                if (
                    any(
                        type(control[key]) is not int
                        for key in ("version", "stopped", "last_wall_ns")
                    )
                    or control["version"] not in {1, 2, 3}
                    or control["stopped"] not in {0, 1}
                    or control["reason"]
                    not in {
                        None,
                        "client_stop",
                        "operator_stop",
                        "clock_invalid",
                        "interrupted",
                        "claim_mismatch",
                    }
                    or control["in_flight"] is not None
                    and (
                        not isinstance(control["in_flight"], str)
                        or not re.fullmatch(r"[a-f0-9]{32}", control["in_flight"])
                    )
                ):
                    raise EvidenceError("read_evidence_control_shape_invalid")
                events = _rows(
                    conn, "events", "id,wall_ns,kind,token", order="ORDER BY id DESC", limit=20
                )
                if events is None:
                    raise EvidenceError("read_evidence_audit_missing")
                for event in events:
                    if any(type(event[key]) is not int for key in ("id", "wall_ns")):
                        raise EvidenceError("read_evidence_audit_shape_invalid")
                    token = event.pop("token")
                    if token is not None and not isinstance(token, str):
                        # An integer token would otherwise look the same as NULL.
                        raise EvidenceError("read_evidence_audit_shape_invalid")
                    event["token_sha256"] = (
                        hashlib.sha256(token.encode()).hexdigest()
                        if isinstance(token, str)
                        else None
                    )
                    event["token_identity"] = (
                        token
                        if isinstance(token, str)
                        and re.fullmatch(r"[a-f0-9]{32}|[a-f0-9]{64}", token)
                        else None
                    )
                    if not isinstance(event["kind"], str) or not re.fullmatch(
                        r"[A-Z_]{1,64}", event["kind"]
                    ):
                        raise EvidenceError("read_evidence_audit_kind_invalid")
                plan = _rows(conn, "read_owner_upgrade", "id,body,digest")
                if plan is not None:
                    for row in plan:
                        if type(row["id"]) is not int:
                            raise EvidenceError("read_evidence_upgrade_shape_invalid")
                        body = row.pop("body")
                        row["body_sha256"] = (
                            hashlib.sha256(body.encode()).hexdigest()
                            if isinstance(body, str)
                            else None
                        )
                        valid_digest = isinstance(row["digest"], str) and re.fullmatch(
                            r"[a-f0-9]{64}", row["digest"]
                        )
                        row["digest_matches_body"] = bool(
                            isinstance(body, str)
                            and valid_digest
                            and row["body_sha256"] == row["digest"]
                        )
                        if not valid_digest:
                            row["digest"] = None
                payload = {
                    "format": "read-control-evidence-v1",
                    "captured_at": datetime.now(UTC).isoformat(),
                    "source": source,
                    "tables_unverified": tables,
                    "audit_tail_unverified": events,
                    "table_row_limit": 2,
                    "audit_tail_limit": 20,
                    "history_complete": False,
                    "upgrade_intent_unverified": plan,
                    "owner_sample": _owner(directory / "read-owner.lock", control["instance_id"]),
                    "scope_is_broker_identity": False,
                    "owner_absence_established": False,
                    "recovery_authorized": False,
                    "cross_store_atomic_snapshot": False,
                    "source_opened_with_sqlite": False,
                    "network_used": False,
                }
        return {
            "payload": payload,
            "sha256": hashlib.sha256(canonical(payload).encode()).hexdigest(),
        }
    except (sqlite3.Error, OSError, TypeError, UnicodeError):
        raise EvidenceError("read_evidence_capture_failed") from None


def publish(path, body):
    """Install a fully written artifact; a visible final path is never a partial file."""
    path = Path(path)
    handle, temporary = tempfile.mkstemp(dir=path.parent, prefix=".evidence-", suffix=".tmp")
    try:
        with os.fdopen(handle, "wb") as output:
            output.write(body.encode("utf-8"))
            output.flush()
            os.fsync(output.fileno())
        os.link(temporary, path)  # Fails instead of replacing an existing artifact.
    finally:
        Path(temporary).unlink(missing_ok=True)


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
            raise EvidenceError("read_evidence_output_outside_source_required")
        evidence = capture(args.directory, args.scope)
        body = json.dumps(evidence, indent=2) + "\n"
        if args.output is None:
            print(body, end="")
        else:
            publish(args.output, body)
            print(json.dumps({"artifact": str(args.output), "sha256": evidence["sha256"]}))
    except (EvidenceError, OSError) as error:
        reason = str(error) if isinstance(error, EvidenceError) else "read_evidence_output_failed"
        parser.exit(2, reason + "\n")


if __name__ == "__main__":
    main()
