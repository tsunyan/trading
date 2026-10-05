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
