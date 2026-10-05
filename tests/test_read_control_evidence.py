"""Offline GET evidence never changes the original or claims ownership/recovery."""

import ctypes
import hashlib
import json
import socket
import sqlite3
from contextlib import closing

import pytest
from test_read_owner_upgrade import legacy

from trading import read_control_evidence as evidence
from trading.read_control import PersistentReadLimiter


@pytest.fixture(autouse=True)
def no_native_or_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("native credential or network boundary attempted")

    monkeypatch.setattr(ctypes, "WinDLL", forbidden, raising=False)
    monkeypatch.setattr(socket, "socket", forbidden)


def test_copy_is_read_only_and_records_missing_owner_without_establishing_absence(
    tmp_path, monkeypatch
):
    reads = PersistentReadLimiter.create(tmp_path / "reads", "synthetic")
    reads.stop()
    (reads.path.parent / "read-owner.lock").unlink()
    before = reads.path.read_bytes()
    original = sqlite3.connect
    connections = []

    def connect(path, *args, **kwargs):
        connections.append(path)
        assert str(reads.path) not in str(path)
        assert "mode=ro&immutable=1" in str(path)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(evidence.sqlite3, "connect", connect)
    result = evidence.capture(reads.path.parent, "synthetic")
    payload = result["payload"]
    assert connections and reads.path.read_bytes() == before
    assert payload["owner_sample"]["status"] == "missing"
    assert payload["tables_unverified"]["owner_file"]
    assert not payload["owner_absence_established"] and not payload["recovery_authorized"]
    assert not (reads.path.parent / "read-owner.lock").exists()
    assert payload["source"]["sha256"] == hashlib.sha256(before).hexdigest()
    assert result["sha256"] == hashlib.sha256(evidence.canonical(payload).encode()).hexdigest()
    assert sorted(p.name for p in reads.path.parent.iterdir()) == ["read-control.sqlite"]


def test_legacy_unfinished_claim_is_reported_and_arbitrary_event_payload_is_hashed(tmp_path):
    _, upgrade = legacy(tmp_path)
    token = "a" * 32
    secret = "arbitrary-sensitive-event-body"
    with closing(sqlite3.connect(upgrade.reads.path)) as conn, conn:
        conn.execute("UPDATE control SET in_flight=?", (token,))
        conn.execute("INSERT INTO events(wall_ns,kind,token) VALUES(1,'CLAIMED',?)", (token,))
        conn.execute("INSERT INTO events(wall_ns,kind,token) VALUES(1,'UNKNOWN',?)", (secret,))
    before = upgrade.reads.path.read_bytes()
    result = evidence.capture(upgrade.reads.path.parent, "synthetic")
    assert result["payload"]["tables_unverified"]["control"][0]["in_flight"] == token
    assert secret not in json.dumps(result)
    assert (
        result["payload"]["audit_tail_unverified"][0]["token_sha256"]
        == hashlib.sha256(secret.encode()).hexdigest()
    )
    assert upgrade.reads.path.read_bytes() == before
    assert not result["payload"]["owner_absence_established"]


@pytest.mark.parametrize("sidecar", ["-journal", "-wal", "-shm"])
def test_sidecar_requires_separate_quiescent_capture_and_is_not_deleted(tmp_path, sidecar):
    reads = PersistentReadLimiter.create(tmp_path / "reads", "synthetic")
    artifact = reads.path.with_name(reads.path.name + sidecar)
    artifact.write_bytes(b"preserve")
    before = reads.path.read_bytes()
    with pytest.raises(evidence.EvidenceError, match="sidecars_present"):
        evidence.capture(reads.path.parent, "synthetic")
    assert reads.path.read_bytes() == before and artifact.read_bytes() == b"preserve"


def test_wal_header_refuses_even_after_sidecars_are_closed(tmp_path):
    reads = PersistentReadLimiter.create(tmp_path / "reads", "synthetic")
    with closing(sqlite3.connect(reads.path)) as conn:
        assert conn.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
    assert not reads.path.with_name(reads.path.name + "-wal").exists()
    before = reads.path.read_bytes()
    with pytest.raises(evidence.EvidenceError, match="rollback_database_required"):
        evidence.capture(reads.path.parent, "synthetic")
    assert reads.path.read_bytes() == before


def test_concurrent_source_change_refuses_instead_of_issuing_stale_evidence(tmp_path, monkeypatch):
    reads = PersistentReadLimiter.create(tmp_path / "reads", "synthetic")
    original = evidence._owner

    def changed(path, instance):
        reads.stop()
        return original(path, instance)

    monkeypatch.setattr(evidence, "_owner", changed)
    with pytest.raises(evidence.EvidenceError, match="source_changed"):
        evidence.capture(reads.path.parent, "synthetic")
    assert reads.status()["stopped"]


def test_large_field_and_source_are_bounded_before_fetching_metadata(tmp_path, monkeypatch):
    reads = PersistentReadLimiter.create(tmp_path / "reads", "synthetic")
    with closing(sqlite3.connect(reads.path)) as conn, conn:
        conn.execute(
            "INSERT INTO events(wall_ns,kind,token) VALUES(1,'UNKNOWN',?)", ("界" * 30_000,)
        )
    with pytest.raises(evidence.EvidenceError, match="field_too_large"):
        evidence.capture(reads.path.parent, "synthetic")
    monkeypatch.setattr(evidence, "MAX_DB_BYTES", 64)
    with pytest.raises(evidence.EvidenceError, match="source_size_invalid"):
        evidence.capture(reads.path.parent, "synthetic")


def test_cli_preserves_existing_artifacts_and_cannot_create_a_missing_owner_in_source(
    tmp_path, capsys
):
    _, upgrade = legacy(tmp_path)
    args = ["--directory", str(upgrade.reads.path.parent), "--scope", "synthetic"]
    before = upgrade.reads.path.read_bytes()
    with pytest.raises(SystemExit) as refused:
        evidence.main([*args, "--output", str(upgrade.owner_path)])
    assert refused.value.code == 2
    assert "output_outside_source_required" in capsys.readouterr().err
    assert not upgrade.owner_path.exists()
    output = tmp_path / "evidence.json"
    evidence.main([*args, "--output", str(output)])
    saved = output.read_bytes()
    with pytest.raises(SystemExit) as refused:
        evidence.main([*args, "--output", str(output)])
    assert refused.value.code == 2 and output.read_bytes() == saved
    assert upgrade.reads.path.read_bytes() == before


def test_wrong_scope_missing_database_and_broken_schema_fail_without_creating_stores(tmp_path):
    absent = tmp_path / "missing"
    with pytest.raises(evidence.EvidenceError):
        evidence.capture(absent, "synthetic")
    assert not absent.exists()
    reads = PersistentReadLimiter.create(tmp_path / "reads", "synthetic")
    with pytest.raises(evidence.EvidenceError, match="scope_mismatch"):
        evidence.capture(reads.path.parent, "other")
    with closing(sqlite3.connect(reads.path)) as conn, conn:
        conn.execute("DROP TABLE control")
    with pytest.raises(evidence.EvidenceError, match="control_missing"):
        evidence.capture(reads.path.parent, "synthetic")


@pytest.mark.parametrize(
    "body,digest,matched",
    [
        (None, None, False),
        (None, "a" * 64, False),
        ("body", None, False),
        ("body", "invalid", False),
        ("body", hashlib.sha256(b"body").hexdigest(), True),
    ],
)
def test_upgrade_digest_match_requires_actual_text_and_valid_digest(
    tmp_path, body, digest, matched
):
    reads = PersistentReadLimiter.create(tmp_path / "reads", "synthetic")
    with closing(sqlite3.connect(reads.path)) as conn, conn:
        conn.execute(
            "CREATE TABLE read_owner_upgrade(id INTEGER PRIMARY KEY,body TEXT,digest TEXT)"
        )
        conn.execute("INSERT INTO read_owner_upgrade VALUES(1,?,?)", (body, digest))
    before = reads.path.read_bytes()
    row = evidence.capture(reads.path.parent, "synthetic")["payload"]["upgrade_intent_unverified"][
        0
    ]
    assert row["digest_matches_body"] is matched
    assert reads.path.read_bytes() == before


def metadata_store(tmp_path):
    reads = PersistentReadLimiter.create(tmp_path / "reads", "synthetic")
    bound_path = tmp_path / "absent-post"
    reads.bind_post("a" * 32, bound_path)
    reads.bind_stream("b" * 32)
    reads.stop()
    return reads, bound_path


def test_valid_bindings_are_sampled_without_opening_or_resolving_bound_path(tmp_path, monkeypatch):
    reads, bound_path = metadata_store(tmp_path)
    before = reads.path.read_bytes()
    original_resolve = type(bound_path).resolve
    original_stat = type(bound_path).stat

    def resolve(path, *args, **kwargs):
        assert path != bound_path, "bound path must not be resolved"
        return original_resolve(path, *args, **kwargs)

    def stat(path, *args, **kwargs):
        assert path != bound_path, "bound path must not be probed"
        return original_stat(path, *args, **kwargs)

    with monkeypatch.context() as guarded:
        guarded.setattr(type(bound_path), "resolve", resolve)
        guarded.setattr(type(bound_path), "stat", stat)
        result = evidence.capture(reads.path.parent, "synthetic")
    tables = result["payload"]["tables_unverified"]
    assert tables["post_binding"] == [{"id": 1, "instance": "a" * 32, "path": str(bound_path)}]
    assert tables["stream_binding"] == [{"id": 1, "supervisor_id": "b" * 32}]
    assert reads.path.read_bytes() == before and not bound_path.exists()
    assert not result["payload"]["recovery_authorized"]


@pytest.mark.parametrize(
    "table,column,value",
    [
        ("owner_file", "device", "synthetic-sensitive-metadata"),
        ("owner_file", "inode", "synthetic-sensitive-metadata"),
        ("owner_file", "device", "１２"),
        ("owner_file", "inode", "-1"),
        ("owner_file", "inode", None),
        ("post_binding", "instance", "synthetic-sensitive-metadata"),
        ("post_binding", "instance", "A" * 32),
        ("post_binding", "instance", "a" * 31),
        ("post_binding", "instance", "a" * 33),
        ("post_binding", "instance", None),
        ("post_binding", "path", "synthetic-sensitive-metadata"),
        ("post_binding", "path", None),
        ("post_binding", "path", "D:/synthetic-sensitive-metadata\0"),
        ("stream_binding", "supervisor_id", "synthetic-sensitive-metadata"),
        ("stream_binding", "supervisor_id", "B" * 32),
        ("stream_binding", "supervisor_id", "b" * 31),
        ("stream_binding", "supervisor_id", "b" * 33),
        ("stream_binding", "supervisor_id", 123),
    ],
)
def test_malformed_optional_metadata_is_refused_without_echo_or_source_changes(
    tmp_path, capsys, table, column, value
):
    reads, bound_path = metadata_store(tmp_path)
    # Remove NOT NULL constraints to represent damaged/older storage too.
    with closing(sqlite3.connect(reads.path)) as conn, conn:
        columns = evidence.TABLES[table]
        conn.execute(f"CREATE TABLE malformed AS SELECT {columns} FROM {table}")
        conn.execute(f"DROP TABLE {table}")
        conn.execute(f"ALTER TABLE malformed RENAME TO {table}")
        conn.execute(f"UPDATE {table} SET {column}=?", (value,))
    before = reads.path.read_bytes()
    owner_before = (reads.path.parent / "read-owner.lock").read_bytes()
    output = tmp_path / "evidence.json"
    with pytest.raises(SystemExit) as refused:
        evidence.main(
            ["--directory", str(reads.path.parent), "--scope", "synthetic", "--output", str(output)]
        )
    printed = capsys.readouterr()
    assert refused.value.code == 2
    assert printed.out == "" and printed.err == "read_evidence_metadata_shape_invalid\n"
    assert "synthetic-sensitive-metadata" not in printed.err
    assert not output.exists() and not bound_path.exists()
    assert reads.path.read_bytes() == before
    assert (reads.path.parent / "read-owner.lock").read_bytes() == owner_before


@pytest.mark.parametrize("table", ["owner_file", "post_binding", "stream_binding"])
@pytest.mark.parametrize("shape", ["missing", "empty", "wrong_id", "text_id", "duplicate"])
def test_optional_singleton_shapes_preserve_missing_and_empty_metadata(tmp_path, table, shape):
    reads, _ = metadata_store(tmp_path)
    with closing(sqlite3.connect(reads.path)) as conn, conn:
        columns = evidence.TABLES[table]
        conn.execute(f"CREATE TABLE malformed AS SELECT {columns} FROM {table}")
        conn.execute(f"DROP TABLE {table}")
        conn.execute(f"ALTER TABLE malformed RENAME TO {table}")
        if shape == "missing":
            conn.execute(f"DROP TABLE {table}")
        elif shape == "empty":
            conn.execute(f"DELETE FROM {table}")
        elif shape == "duplicate":
            conn.execute(f"INSERT INTO {table} SELECT * FROM {table}")
        else:
            conn.execute(
                f"UPDATE {table} SET id=?",
                (2 if shape == "wrong_id" else "synthetic-sensitive-metadata",),
            )
    before = reads.path.read_bytes()
    if shape in {"missing", "empty"}:
        sampled = evidence.capture(reads.path.parent, "synthetic")["payload"]["tables_unverified"]
        assert sampled[table] == (None if shape == "missing" else [])
    else:
        with pytest.raises(evidence.EvidenceError, match="^read_evidence_metadata_shape_invalid$"):
            evidence.capture(reads.path.parent, "synthetic")
    assert reads.path.read_bytes() == before


@pytest.mark.parametrize("version", [1, 2])
def test_legacy_versions_without_optional_tables_remain_sampleable(tmp_path, version):
    _, upgrade = legacy(tmp_path)
    with closing(sqlite3.connect(upgrade.reads.path)) as conn, conn:
        conn.execute("UPDATE control SET version=?", (version,))
        for table in ("owner_file", "post_binding", "stream_binding"):
            conn.execute(f"DROP TABLE IF EXISTS {table}")
    before = upgrade.reads.path.read_bytes()
    result = evidence.capture(upgrade.reads.path.parent, "synthetic")
    assert result["payload"]["tables_unverified"]["control"][0]["version"] == version
    assert all(
        result["payload"]["tables_unverified"][table] is None
        for table in ("owner_file", "post_binding", "stream_binding")
    )
    assert upgrade.reads.path.read_bytes() == before and not upgrade.owner_path.exists()


@pytest.mark.parametrize("column", ["id", "wall_ns"])
def test_audit_numeric_fields_cannot_expose_arbitrary_text(tmp_path, column):
    reads = PersistentReadLimiter.create(tmp_path / "reads", "synthetic")
    with closing(sqlite3.connect(reads.path)) as conn, conn:
        conn.execute("CREATE TABLE malformed AS SELECT * FROM events")
        conn.execute("DROP TABLE events")
        conn.execute("ALTER TABLE malformed RENAME TO events")
        conn.execute(f"UPDATE events SET {column}='synthetic-sensitive-metadata'")
    before = reads.path.read_bytes()
    with pytest.raises(evidence.EvidenceError, match="^read_evidence_audit_shape_invalid$"):
        evidence.capture(reads.path.parent, "synthetic")
    assert reads.path.read_bytes() == before


def test_upgrade_row_id_cannot_expose_arbitrary_text(tmp_path):
    reads = PersistentReadLimiter.create(tmp_path / "reads", "synthetic")
    with closing(sqlite3.connect(reads.path)) as conn, conn:
        conn.execute("CREATE TABLE read_owner_upgrade(id TEXT,body TEXT,digest TEXT)")
        conn.execute(
            "INSERT INTO read_owner_upgrade VALUES('synthetic-sensitive-metadata',NULL,NULL)"
        )
    before = reads.path.read_bytes()
    with pytest.raises(evidence.EvidenceError, match="^read_evidence_upgrade_shape_invalid$"):
        evidence.capture(reads.path.parent, "synthetic")
    assert reads.path.read_bytes() == before
