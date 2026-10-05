"""Stopped legacy GET upgrades use temporary stores, real local files and no network."""

import json
import socket
import sqlite3
import subprocess

import pytest
from test_read_orphan import Clock

from trading.paper_runner import python_process_args
from trading.post_control import PersistentPostLimiter
from trading.private_read import PrivateReadError
from trading.read_control import RECOVERY_CHECKS, PersistentReadLimiter
from trading.read_owner_upgrade import COMPLETION_CHECKS, UPGRADE_CHECKS, ReadOwnerUpgrade


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("real network attempted"))


def legacy(tmp_path, *, version=2, reason="operator_stop", bound=False):
    clock = Clock()
    original = PersistentReadLimiter.create(tmp_path / "reads", "synthetic", **clock.args())
    if bound:
        original.bind_stream("a" * 32)
        PersistentPostLimiter.create(
            tmp_path / "posts",
            original,
            wall_ns=lambda: clock.wall,
            monotonic=lambda: clock.mono / 1e9,
            sleep=clock.sleep,
        )
    original.stop(reason)
    # Build an old-format fixture. Production code never deletes/recreates an owner file.
    with sqlite3.connect(original.path) as conn:
        conn.execute("DROP TABLE owner_file")
        conn.execute("UPDATE control SET version=?", (version,))
    (original.path.parent / "read-owner.lock").unlink()
    return clock, ReadOwnerUpgrade(original.path.parent, "synthetic", **clock.args())


def approve(upgrade, plan, checks=UPGRADE_CHECKS):
    return upgrade.approve(plan["proposal"], plan["revision"], confirmations=checks)


def reopen(upgrade, clock):
    return PersistentReadLimiter(upgrade.reads.path.parent, "synthetic", **clock.args())


def rows(upgrade):
    with sqlite3.connect(upgrade.reads.path) as conn:
        return conn.execute("SELECT kind,token FROM events ORDER BY id").fetchall()


@pytest.mark.parametrize("version", [1, 2])
@pytest.mark.parametrize("reason", ["operator_stop", "client_stop", "clock_invalid", "interrupted"])
def test_upgrade_preserves_identity_and_stop_and_requires_separate_get_recovery(
    tmp_path, version, reason
):
    clock, upgrade = legacy(tmp_path, version=version, reason=reason)
    reads = upgrade.reads
    before = reads.status()
    plan = upgrade.prepare()
    assert not upgrade.owner_path.exists() and not plan["upgraded"]
    result = approve(upgrade, plan)
    assert result["upgraded"] and result["stopped"] and not result["claim_resolved"]
    after = reads.status()
    assert after["version"] == 3 and after["orphan_resolution_supported"]
    assert not after["owner_upgrade_incomplete"] and after["reopen_required"]
    for field in ("scope", "instance_id", "reason", "stopped", "in_flight"):
        assert after[field] == before[field]
    assert upgrade.owner_path.read_text() == before["instance_id"]
    with pytest.raises(PrivateReadError), reads.slot():
        pytest.fail("old GET handle was reused")
    fresh = reopen(upgrade, clock)
    with pytest.raises(PrivateReadError, match="reads_stopped"), fresh.slot():
        pytest.fail("upgrade resumed GET")
    recovery = fresh.prepare_recovery()
    fresh.approve_recovery(
        recovery["proposal"], recovery["revision"], confirmations=RECOVERY_CHECKS
    )
    fresh = reopen(upgrade, clock)
    peer = reopen(upgrade, clock)
    with fresh.slot():
        with pytest.raises(PrivateReadError), peer._owner_lock(required=True):
            pytest.fail("new owner lease was not held")
    assert rows(upgrade)[-1][0] == "COMPLETED"


def test_upgrade_preserves_post_and_stream_bindings_without_supplying_missing_history(tmp_path):
    clock, upgrade = legacy(tmp_path, bound=True)
    with sqlite3.connect(upgrade.reads.path) as conn:
        conn.execute("DELETE FROM events WHERE kind='STREAM_BOUND'")
    with upgrade.reads._transaction() as conn:
        before = upgrade._source(conn)
    post_bytes = (tmp_path / "posts" / "post-control.sqlite").read_bytes()
    approve(upgrade, upgrade.prepare())
    fresh = reopen(upgrade, clock)
    assert fresh.post_binding() == before["post"]
    with pytest.raises(PrivateReadError, match="history_confirmation_required"):
        fresh.stream_binding()
    assert (tmp_path / "posts" / "post-control.sqlite").read_bytes() == post_bytes
    assert not any(kind == "STREAM_BOUND" for kind, _ in rows(upgrade))


@pytest.mark.parametrize(
    "change", ["running", "claim", "history", "mismatch", "owner_file", "owner_table"]
)
def test_unsupported_legacy_states_refuse_without_preparation_or_files(tmp_path, change):
    _, upgrade = legacy(tmp_path)
    with sqlite3.connect(upgrade.reads.path) as conn:
        if change == "running":
            conn.execute("UPDATE control SET stopped=0,reason=NULL")
        elif change in {"claim", "history"}:
            conn.execute(
                "INSERT INTO events(wall_ns,kind,token) VALUES(0,'CLAIMED',?)", ("b" * 32,)
            )
            if change == "claim":
                conn.execute("UPDATE control SET in_flight=?", ("b" * 32,))
        elif change == "mismatch":
            conn.execute("UPDATE control SET reason='claim_mismatch'")
        elif change == "owner_table":
            conn.execute("CREATE TABLE owner_file(id INTEGER,device TEXT,inode TEXT)")
    if change == "owner_file":
        upgrade.owner_path.write_bytes(b"unrecognized file")
    before = upgrade.reads.path.read_bytes()
    file_before = upgrade.owner_path.read_bytes() if upgrade.owner_path.exists() else None
    with pytest.raises(PrivateReadError):
        upgrade.prepare()
    assert upgrade.reads.path.read_bytes() == before
    assert (upgrade.owner_path.read_bytes() if upgrade.owner_path.exists() else None) == file_before


@pytest.mark.parametrize("change", ["stop", "second_proposal", "version", "binding", "owner_file"])
def test_changes_after_preparation_require_new_review_without_starting_upgrade(tmp_path, change):
    _, upgrade = legacy(tmp_path)
    plan = upgrade.prepare()
    if change == "stop":
        upgrade.reads.stop("operator_stop")
    elif change == "second_proposal":
        upgrade.prepare()
    elif change == "owner_file":
        upgrade.owner_path.write_bytes(b"foreign")
    else:
        with sqlite3.connect(upgrade.reads.path) as conn:
            if change == "version":
                conn.execute("UPDATE control SET version=1")
            else:
                conn.execute("CREATE TABLE stream_binding(id INTEGER,supervisor_id TEXT)")
                conn.execute("INSERT INTO stream_binding VALUES(1,?)", ("c" * 32,))
    before = upgrade.reads.path.read_bytes()
    with pytest.raises(PrivateReadError):
        approve(upgrade, plan)
    assert upgrade.reads.path.read_bytes() == before
    assert not any(kind == "OWNER_UPGRADE_STARTED" for kind, _ in rows(upgrade))


@pytest.mark.parametrize("change", ["missing_checks", "extra_checks", "expiry", "backward_clock"])
def test_confirmations_and_fresh_clock_are_required_and_failed_proposals_cannot_be_replayed(
    tmp_path, change
):
    clock, upgrade = legacy(tmp_path)
    plan = upgrade.prepare()
    checks = UPGRADE_CHECKS
    if change == "missing_checks":
        checks = checks - {"old-clients-closed"}
    elif change == "extra_checks":
        checks = checks | {"resume-reads"}
    elif change == "expiry":
        clock.sleep(300)
    else:
        clock.wall -= 1
    with pytest.raises(PrivateReadError):
        approve(upgrade, plan, checks)
    assert not upgrade.owner_path.exists()
    assert not any(kind == "OWNER_UPGRADE_STARTED" for kind, _ in rows(upgrade))
    if change in {"expiry", "backward_clock"}:
        clock.wall += 1 if change == "backward_clock" else 0
        with pytest.raises(PrivateReadError, match="proposal_changed"):
            approve(upgrade, plan)


def test_failure_after_upgrade_intent_blocks_recovery_and_never_adopts_the_partial_file(
    tmp_path, monkeypatch
):
    clock, upgrade = legacy(tmp_path)
    plan = upgrade.prepare()
    original = upgrade._install

    def changed(*args):
        upgrade.reads.stop("operator_stop")
        return original(*args)

    monkeypatch.setattr(upgrade, "_install", changed)
    with pytest.raises(PrivateReadError, match="checkpoint_changed"):
        approve(upgrade, plan)
    fresh = reopen(upgrade, clock)
    assert fresh.status()["owner_upgrade_incomplete"] and fresh.status()["stopped"]
    assert fresh.status()["version"] == 2 and upgrade.owner_path.exists()
    with pytest.raises(PrivateReadError, match="upgrade_incomplete"):
        fresh.prepare_recovery()
    with pytest.raises(PrivateReadError, match="upgrade_incomplete"), fresh.slot():
        pytest.fail("partial upgrade enabled GET")
    fresh.stop("operator_stop")
    with pytest.raises(PrivateReadError, match="upgrade_incomplete"):
        ReadOwnerUpgrade(upgrade.reads.path.parent, "synthetic", **clock.args()).prepare()


def test_final_storage_failure_preserves_intent_and_original_stop(tmp_path):
    clock, upgrade = legacy(tmp_path)
    plan = upgrade.prepare()
    with sqlite3.connect(upgrade.reads.path) as conn:
        conn.executescript("""
            CREATE TRIGGER reject_upgrade BEFORE INSERT ON events
            WHEN NEW.kind='OWNER_UPGRADED'
            BEGIN SELECT RAISE(ABORT, 'synthetic write failure'); END;
        """)
    with pytest.raises(PrivateReadError, match="storage_failed"):
        approve(upgrade, plan)
    fresh = reopen(upgrade, clock)
    assert fresh.status()["version"] == 2 and fresh.status()["owner_upgrade_incomplete"]
    assert rows(upgrade)[-1][0] == "OWNER_UPGRADE_STARTED"
    with pytest.raises(PrivateReadError, match="upgrade_incomplete"):
        fresh.prepare_recovery()


@pytest.mark.parametrize("phase", ["intent", "file", "commit"])
def test_actual_process_exit_across_filesystem_and_database_boundary_stays_stopped(tmp_path, phase):
    clock, upgrade = legacy(tmp_path)
    plan = upgrade.prepare()
    code = f"""
import os, sys
from contextlib import contextmanager
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'tests'))
from test_read_orphan import Clock
from trading.read_owner_upgrade import ReadOwnerUpgrade, UPGRADE_CHECKS
clock = Clock()
clock.wall, clock.mono = {clock.wall}, {clock.mono}
upgrade = ReadOwnerUpgrade(Path({str(upgrade.reads.path.parent)!r}), 'synthetic', **clock.args())
original_install = upgrade._install
def install(*args):
    if {phase!r} == 'intent': os._exit(21)
    return original_install(*args)
upgrade._install = install
original_sync = os.fsync
def fsync(descriptor):
    original_sync(descriptor)
    if {phase!r} == 'file': os._exit(22)
os.fsync = fsync
original_transaction = upgrade.reads._transaction
@contextmanager
def transaction():
    with original_transaction() as conn:
        yield conn
        complete = conn.execute("SELECT COUNT(*) FROM events WHERE kind='OWNER_UPGRADED'")
        complete = complete.fetchone()[0]
    if complete and {phase!r} == 'commit': os._exit(23)
upgrade.reads._transaction = transaction
upgrade.approve({plan["proposal"]!r}, {plan["revision"]!r}, confirmations=UPGRADE_CHECKS)
os._exit(99)
"""
    process = subprocess.run(python_process_args(code), capture_output=True, text=True, timeout=30)
    assert process.returncode == {"intent": 21, "file": 22, "commit": 23}[phase], process.stderr
    fresh = reopen(upgrade, clock)
    status = fresh.status()
    assert status["stopped"] and status["blocked"] and not status["in_flight"]
    if phase == "commit":
        assert status["version"] == 3 and not status["owner_upgrade_incomplete"]
    else:
        assert status["version"] == 2 and status["owner_upgrade_incomplete"]
        with pytest.raises(PrivateReadError, match="upgrade_incomplete"):
            fresh.prepare_recovery()
    assert upgrade.owner_path.exists() == (phase != "intent")
    pending = ReadOwnerUpgrade(upgrade.reads.path.parent, "synthetic", **clock.args())
    if phase == "intent":
        digest = pending.context()["intent_sha256"]
        clock.sleep(3600)
        result = pending.complete(digest, confirmations=COMPLETION_CHECKS)
        assert result["completed_recorded_upgrade"] and result["stopped"]
        assert reopen(upgrade, clock).status()["version"] == 3
    elif phase == "file":
        with pytest.raises(PrivateReadError, match="unrecorded_owner_artifact"):
            pending.complete(pending.context()["intent_sha256"], confirmations=COMPLETION_CHECKS)


@pytest.mark.parametrize(
    "damage", ["plan", "receipt", "deleted_plan", "duplicate_receipt", "oversized", "blob"]
)
def test_upgrade_audit_damage_is_refused_after_completion(tmp_path, damage):
    clock, upgrade = legacy(tmp_path)
    approve(upgrade, upgrade.prepare())
    with sqlite3.connect(upgrade.reads.path) as conn:
        if damage == "plan":
            conn.execute("UPDATE read_owner_upgrade SET body='{}'")
        elif damage == "oversized":
            conn.execute("UPDATE read_owner_upgrade SET body=?", ("界" * 30_000,))
        elif damage == "blob":
            conn.execute("UPDATE read_owner_upgrade SET body=CAST(body AS BLOB)")
        elif damage == "deleted_plan":
            conn.execute("DROP TABLE read_owner_upgrade")
        elif damage == "receipt":
            conn.execute("DELETE FROM events WHERE kind='OWNER_UPGRADED'")
        else:
            conn.execute(
                "INSERT INTO events(wall_ns,kind,token) "
                "SELECT wall_ns,kind,token FROM events WHERE kind='OWNER_UPGRADED'"
            )
    with pytest.raises(PrivateReadError, match="upgrade_integrity_failed"):
        reopen(upgrade, clock).status()


def test_oversized_plan_is_rejected_before_fetching_body(tmp_path):
    _, upgrade = legacy(tmp_path)
    approve(upgrade, upgrade.prepare())
    with upgrade.reads._transaction() as conn:
        conn.execute("UPDATE read_owner_upgrade SET body=?", ("界" * 30_000,))
        statements = []
        conn.set_trace_callback(statements.append)
        with pytest.raises(PrivateReadError, match="upgrade_integrity_failed"):
            upgrade.reads._owner_upgrade_pending(conn)
        assert not any("SELECT body,digest" in statement for statement in statements)


def test_oversized_proposal_does_not_create_a_pending_upgrade(tmp_path, monkeypatch):
    _, upgrade = legacy(tmp_path)
    original = upgrade._source

    def oversized(conn):
        return {**original(conn), "unexpected_binding": "x" * 64_000}

    monkeypatch.setattr(upgrade, "_source", oversized)
    before = upgrade.reads.path.read_bytes()
    with pytest.raises(PrivateReadError, match="payload_too_large"):
        upgrade.prepare()
    assert upgrade.reads.path.read_bytes() == before
    assert not upgrade.owner_path.exists()
    assert not upgrade.reads.status()["owner_upgrade_incomplete"]


def test_cli_requires_interactive_explicit_approval_and_only_changes_local_stopped_control(
    tmp_path, monkeypatch, capsys
):
    from trading import read_owner_upgrade

    _, upgrade = legacy(tmp_path)
    monkeypatch.setattr(read_owner_upgrade, "ReadOwnerUpgrade", lambda *a: upgrade)
    args = ["--directory", str(upgrade.reads.path.parent), "--scope", "synthetic"]
    read_owner_upgrade.main(["prepare", *args])
    plan = json.loads(capsys.readouterr().out)
    flags = ["--confirm-" + item for item in sorted(UPGRADE_CHECKS)]
    approved = [
        "approve",
        *args,
        "--proposal",
        plan["proposal"],
        "--revision",
        plan["revision"],
        *flags,
    ]
    monkeypatch.setattr(read_owner_upgrade.sys.stdin, "isatty", lambda: False)
    with pytest.raises(SystemExit):
        read_owner_upgrade.main(approved)
    assert "interactive_read_owner_upgrade_required" in capsys.readouterr().err
    monkeypatch.setattr(read_owner_upgrade.sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _: "UPGRADE IDLE GET OWNER")
    read_owner_upgrade.main(approved)
    result = json.loads(capsys.readouterr().out)
    assert result["upgraded"] and result["stopped"] and not result["claim_resolved"]


@pytest.mark.parametrize("change", ["digest", "checks", "new_stop"])
def test_saved_upgrade_completion_requires_the_exact_unchanged_decision(
    tmp_path, monkeypatch, change
):
    clock, upgrade = legacy(tmp_path)
    plan = upgrade.prepare()

    def interrupted(*args):
        raise RuntimeError("before filesystem mutation")

    monkeypatch.setattr(upgrade, "_install", interrupted)
    with pytest.raises(RuntimeError):
        approve(upgrade, plan)
    pending = ReadOwnerUpgrade(upgrade.reads.path.parent, "synthetic", **clock.args())
    digest = pending.context()["intent_sha256"]
    checks = COMPLETION_CHECKS
    if change == "digest":
        digest = "f" * 64
    elif change == "checks":
        checks = UPGRADE_CHECKS
    else:
        pending.reads.stop("operator_stop")
    before = pending.reads.path.read_bytes()
    with pytest.raises(PrivateReadError):
        pending.complete(digest, confirmations=checks)
    assert pending.reads.path.read_bytes() == before and not pending.owner_path.exists()


def test_cli_completes_an_intent_without_creating_a_new_approval(tmp_path, monkeypatch, capsys):
    from trading import read_owner_upgrade

    clock, upgrade = legacy(tmp_path)
    plan = upgrade.prepare()

    def interrupted(*args):
        raise RuntimeError("before file creation")

    monkeypatch.setattr(upgrade, "_install", interrupted)
    with pytest.raises(RuntimeError):
        approve(upgrade, plan)
    pending = ReadOwnerUpgrade(upgrade.reads.path.parent, "synthetic", **clock.args())
    monkeypatch.setattr(read_owner_upgrade, "ReadOwnerUpgrade", lambda *args: pending)
    monkeypatch.setattr(read_owner_upgrade.sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _: "FINISH RECORDED GET UPGRADE")
    args = ["--directory", str(pending.reads.path.parent), "--scope", "synthetic"]
    read_owner_upgrade.main(["status", *args])
    context = json.loads(capsys.readouterr().out)
    assert context["owner_upgrade_incomplete"] and not context["owner_artifact_present"]
    clock.sleep(3600)
    read_owner_upgrade.main(
        [
            "complete",
            *args,
            "--intent-sha256",
            context["intent_sha256"],
            *["--confirm-" + item for item in sorted(COMPLETION_CHECKS)],
        ]
    )
    result = json.loads(capsys.readouterr().out)
    assert result["completed_recorded_upgrade"] and result["stopped"]
