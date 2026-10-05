"""Offline stream evidence preserves the original and never acquires recovery powers."""

import ctypes
import hashlib
import json
import os
import socket
import sqlite3
from contextlib import closing

import pytest
import test_stream_control as fixtures
from test_stream_control import begin

from trading import read_control_evidence as shared
from trading import stream_control_evidence as evidence
from trading.stream_control import StreamControl

setup = fixtures.setup


@pytest.fixture(autouse=True)
def no_native_or_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("native credential or network boundary attempted")

    monkeypatch.setattr(ctypes, "WinDLL", forbidden, raising=False)
    monkeypatch.setattr(socket, "socket", forbidden)


def update(control, *, body=None, digest=None):
    with closing(sqlite3.connect(control.path)) as conn, conn:
        original = conn.execute("SELECT body,digest FROM control").fetchone()
        conn.execute(
            "UPDATE control SET body=?,digest=?",
            (original[0] if body is None else body, original[1] if digest is None else digest),
        )


def test_missing_owner_preserves_running_uncertainty_and_all_bound_files(setup, monkeypatch):
    _, journal, book, control = setup
    with control.ownership():
        expected = begin(control, journal)
    control.lock_path.unlink()
    files = {path: path.read_bytes() for path in (control.path, journal.path, book.path)}
    original = sqlite3.connect
    connections = []

    def connect(path, *args, **kwargs):
        connections.append(str(path))
        assert all(str(source) not in str(path) for source in files)
        assert "mode=ro&immutable=1" in str(path)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(evidence.sqlite3, "connect", connect)
    result = evidence.capture(control.path.parent, "synthetic")
    payload = result["payload"]
    state = payload["control_unverified"]["state_unverified"]
    assert connections and all(path.read_bytes() == before for path, before in files.items())
    assert state["phase"] == "RUNNING" and state["cleanup_unknown"]
    assert state["owner"] == expected["owner"] and state["revision"] == expected["revision"]
    assert (
        state["series"] == expected["series"]
        and state["cash_instance"] == expected["cash_instance"]
    )
    assert payload["owner_sample"]["status"] == "missing"
    assert not payload["owner_sample"]["identity_matches_control"]
    assert all(
        payload[key] is False
        for key in (
            "bound_stores_opened",
            "owner_absence_established",
            "recovery_authorized",
            "cross_store_atomic_snapshot",
            "source_opened_with_sqlite",
            "history_complete",
            "network_used",
        )
    )
    assert payload["source"]["sha256"] == hashlib.sha256(files[control.path]).hexdigest()
    assert result["sha256"] == hashlib.sha256(shared.canonical(payload).encode()).hexdigest()
    assert sorted(path.name for path in control.path.parent.iterdir()) == ["stream-control.sqlite"]


def test_capture_never_probes_os_ownership_even_while_a_live_owner_holds_it(setup, monkeypatch):
    _, journal, _, control = setup
    with control.ownership():
        begin(control, journal)

        def forbidden(*args, **kwargs):
            pytest.fail("OS ownership acquired by evidence capture")

        monkeypatch.setattr(StreamControl, "ownership", forbidden)
        if os.name == "nt":
            # Windows denies even reading the byte locked by the existing owner.
            with pytest.raises(evidence.EvidenceError, match="stream_evidence_capture_failed"):
                evidence.capture(control.path.parent, "synthetic")
        else:
            result = evidence.capture(control.path.parent, "synthetic")["payload"]
            assert result["owner_sample"]["identity_matches_control"]
            assert result["owner_sample"]["contents_match_instance"]
            assert not result["owner_absence_established"]


@pytest.mark.parametrize("case", ["replacement", "wrong_contents", "unsupported"])
def test_owner_mismatch_is_sampled_without_repair_or_binding_adoption(setup, case):
    *_, control = setup
    before = control.path.read_bytes()
    if case == "replacement":
        body = control.lock_path.read_bytes()
        control.lock_path.rename(control.lock_path.with_suffix(".original"))
        control.lock_path.write_bytes(body)
    elif case == "wrong_contents":
        control.lock_path.write_bytes(b"x" * 32)
    else:
        control.lock_path.write_bytes(b"unsupported")
    owner_before = control.lock_path.read_bytes()
    owner = evidence.capture(control.path.parent, "synthetic")["payload"]["owner_sample"]
    assert control.path.read_bytes() == before and control.lock_path.read_bytes() == owner_before
    if case == "replacement":
        assert not owner["identity_matches_control"] and owner["contents_match_instance"]
    elif case == "wrong_contents":
        assert owner["identity_matches_control"] and not owner["contents_match_instance"]
    else:
        assert owner["status"] == "unsupported_file" and not owner["identity_matches_control"]
    assert not owner["owner_absence_established"]


@pytest.mark.parametrize("sidecar", ["-journal", "-wal", "-shm"])
def test_sidecars_are_preserved_and_never_opened_by_sqlite(setup, sidecar, monkeypatch):
    *_, control = setup
    path = control.path.with_name(control.path.name + sidecar)
    path.write_bytes(b"preserve")
    before = control.path.read_bytes()

    def forbidden(*args, **kwargs):
        pytest.fail("SQLite opened with unresolved sidecars")

    monkeypatch.setattr(evidence.sqlite3, "connect", forbidden)
    with pytest.raises(evidence.EvidenceError, match="stream_evidence_source_sidecars_present"):
        evidence.capture(control.path.parent, "synthetic")
    assert control.path.read_bytes() == before and path.read_bytes() == b"preserve"


def test_wal_header_and_oversize_source_refuse_without_changes(setup, monkeypatch):
    *_, control = setup
    with closing(sqlite3.connect(control.path)) as conn:
        assert conn.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
    before = control.path.read_bytes()
    with pytest.raises(evidence.EvidenceError, match="stream_evidence_rollback_database_required"):
        evidence.capture(control.path.parent, "synthetic")
    monkeypatch.setattr(shared, "MAX_DB_BYTES", len(before) - 1)
    with pytest.raises(evidence.EvidenceError, match="stream_evidence_source_size_invalid"):
        evidence.capture(control.path.parent, "synthetic")
    assert control.path.read_bytes() == before


def test_concurrent_source_change_prevents_returning_stale_evidence(setup, monkeypatch):
    *_, control = setup
    original = evidence._owner

    def mutate(*args):
        with closing(sqlite3.connect(control.path)) as conn, conn:
            conn.execute("UPDATE control SET digest=?", ("b" * 64,))
        return original(*args)

    monkeypatch.setattr(evidence, "_owner", mutate)
    with pytest.raises(evidence.EvidenceError, match="stream_evidence_source_changed"):
        evidence.capture(control.path.parent, "synthetic")


def test_bound_stores_can_be_absent_without_any_attempt_to_recreate_them(setup):
    _, journal, book, control = setup
    journal.path.unlink()
    book.path.unlink()
    result = evidence.capture(control.path.parent, "synthetic")["payload"]
    assert not result["bound_stores_opened"]
    assert not journal.path.exists() and not book.path.exists()
    assert result["control_unverified"]["state_unverified"]["journal_path"] == str(journal.path)


@pytest.mark.parametrize("digest", [None, "invalid-sensitive-data", "b" * 64])
def test_malformed_or_mismatched_digest_never_reports_body_match(setup, digest):
    *_, control = setup
    with closing(sqlite3.connect(control.path)) as conn, conn:
        if digest is None:
            conn.execute("ALTER TABLE control RENAME TO original")
            conn.execute("CREATE TABLE control(id INTEGER,body TEXT,digest TEXT)")
            conn.execute("INSERT INTO control SELECT id,body,NULL FROM original")
        else:
            conn.execute("UPDATE control SET digest=?", (digest,))
    before = control.path.read_bytes()
    result = evidence.capture(control.path.parent, "synthetic")
    row = result["payload"]["control_unverified"]
    assert not row["digest_matches_body"] and row["body_matches_canonical_state"]
    assert row["digest"] == (digest if digest == "b" * 64 else None)
    assert "invalid-sensitive-data" not in json.dumps(result)
    assert control.path.read_bytes() == before


def test_noncanonical_body_is_kept_only_as_hash_and_explicitly_flagged(setup):
    *_, control = setup
    with closing(sqlite3.connect(control.path)) as conn:
        state = json.loads(conn.execute("SELECT body FROM control").fetchone()[0])
    body = json.dumps(state, indent=2)
    update(control, body=body, digest=hashlib.sha256(body.encode()).hexdigest())
    row = evidence.capture(control.path.parent, "synthetic")["payload"]["control_unverified"]
    assert row["digest_matches_body"] and not row["body_matches_canonical_state"]
    assert "body" not in row and row["body_sha256"] == hashlib.sha256(body.encode()).hexdigest()


@pytest.mark.parametrize("case", ["null", "invalid_json", "extra", "bad_phase", "too_large"])
def test_bad_control_body_is_rejected_without_leaking_or_mutating_it(setup, case):
    *_, control = setup
    with closing(sqlite3.connect(control.path)) as conn, conn:
        state = json.loads(conn.execute("SELECT body FROM control").fetchone()[0])
        if case == "null":
            conn.execute("ALTER TABLE control RENAME TO original")
            conn.execute("CREATE TABLE control(id INTEGER,body TEXT,digest TEXT)")
            conn.execute("INSERT INTO control VALUES(1,NULL,NULL)")
        else:
            if case == "extra":
                state["secret"] = "private-value"
            elif case == "bad_phase":
                state["phase"] = "private-value"
            body = (
                "private-value"
                if case == "invalid_json"
                else "x" * (shared.MAX_FIELD_BYTES + 1)
                if case == "too_large"
                else json.dumps(state)
            )
            conn.execute("UPDATE control SET body=?", (body,))
    before = control.path.read_bytes()
    reason = "field_too_large" if case == "too_large" else "control_shape_invalid"
    with pytest.raises(evidence.EvidenceError, match=reason) as refused:
        evidence.capture(control.path.parent, "synthetic")
    assert "private-value" not in str(refused.value) and control.path.read_bytes() == before


@pytest.mark.parametrize("case", ["missing_control", "missing_audit", "extra_control", "bad_audit"])
def test_schema_and_audit_shape_errors_fail_closed(setup, case):
    *_, control = setup
    with closing(sqlite3.connect(control.path)) as conn, conn:
        if case == "missing_control":
            conn.execute("DROP TABLE control")
        elif case == "missing_audit":
            conn.execute("DROP TABLE transitions")
        elif case == "extra_control":
            conn.execute("ALTER TABLE control RENAME TO original")
            conn.execute("CREATE TABLE control(id INTEGER,body TEXT,digest TEXT)")
            conn.execute("INSERT INTO control SELECT * FROM original")
            conn.execute("INSERT INTO control SELECT 2,body,digest FROM original")
        else:
            conn.execute("UPDATE transitions SET kind='private-value'")
    before = control.path.read_bytes()
    with pytest.raises(evidence.EvidenceError):
        evidence.capture(control.path.parent, "synthetic")
    assert control.path.read_bytes() == before


def test_audit_tail_is_bounded_and_does_not_claim_full_history(setup):
    *_, control = setup
    with closing(sqlite3.connect(control.path)) as conn, conn:
        digest = conn.execute("SELECT digest FROM control").fetchone()[0]
        conn.executemany(
            "INSERT INTO transitions VALUES(?,?,'STOPPED',1,?)",
            [(i, i, digest) for i in range(2, 35)],
        )
    payload = evidence.capture(control.path.parent, "synthetic")["payload"]
    assert [row["id"] for row in payload["audit_tail_unverified"]] == list(range(34, 14, -1))
    assert not payload["history_complete"]


def test_wrong_scope_and_missing_directory_do_not_create_stores(setup, tmp_path):
    *_, control = setup
    with pytest.raises(evidence.EvidenceError, match="scope_mismatch"):
        evidence.capture(control.path.parent, "other")
    absent = tmp_path / "absent"
    with pytest.raises(evidence.EvidenceError, match="capture_failed"):
        evidence.capture(absent, "synthetic")
    assert not absent.exists()


def test_cli_protects_source_and_existing_output_and_uses_fixed_error_codes(
    setup, tmp_path, capsys
):
    *_, control = setup
    before = control.path.read_bytes()
    args = ["--directory", str(control.path.parent), "--scope", "synthetic"]
    with pytest.raises(SystemExit) as refused:
        evidence.main([*args, "--output", str(control.lock_path)])
    assert refused.value.code == 2
    assert "stream_evidence_output_outside_source_required" in capsys.readouterr().err
    output = tmp_path / "evidence.json"
    evidence.main([*args, "--output", str(output)])
    saved = output.read_bytes()
    with pytest.raises(SystemExit) as refused:
        evidence.main([*args, "--output", str(output)])
    assert refused.value.code == 2 and output.read_bytes() == saved
    assert "stream_evidence_output_failed" in capsys.readouterr().err
    assert control.path.read_bytes() == before
    evidence.main(args)
    payload = json.loads(capsys.readouterr().out)["payload"]
    assert payload["format"] == "stream-control-evidence-v1"


def test_stream_artifacts_use_the_shared_complete_file_publication(
    setup, tmp_path, monkeypatch, capsys
):
    *_, control = setup
    args = ["--directory", str(control.path.parent), "--scope", "synthetic"]
    output = tmp_path / "evidence" / "stream.json"
    output.parent.mkdir()

    def failed(source, target):
        raise OSError("synthetic link failure")

    monkeypatch.setattr(shared.os, "link", failed)
    with pytest.raises(SystemExit) as refused:
        evidence.main([*args, "--output", str(output)])
    assert refused.value.code == 2
    assert "stream_evidence_output_failed" in capsys.readouterr().err
    assert list(output.parent.iterdir()) == []
