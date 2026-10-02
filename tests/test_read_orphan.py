"""OS-lock tests use temporary local files and subprocesses, never broker HTTP."""

import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import pytest
from pydantic import SecretStr

from trading.broker_contracts import RequestPlan
from trading.private_read import PrivateReadClient, PrivateReadError
from trading.read_control import ORPHAN_CHECKS, RECOVERY_CHECKS, PersistentReadLimiter, main


class Clock:
    def __init__(self):
        self.wall = 1_790_726_400_000_000_000
        self.mono = 0

    def sleep(self, seconds):
        elapsed = round(seconds * 1_000_000_000)
        self.wall += elapsed
        self.mono += elapsed

    def args(self):
        return dict(wall_ns=lambda: self.wall, monotonic_ns=lambda: self.mono, sleep=self.sleep)


@pytest.fixture
def setup(tmp_path, monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("network attempted"))
    clock = Clock()
    control = PersistentReadLimiter.create(tmp_path / "control", "synthetic", **clock.args())
    # Model a durable claim left behind when an owner descriptor is closed by
    # process death. Separate real-process tests below validate the OS behavior.
    with control._owner_lock():
        control._claim()
    control.stop("operator_stop")
    return clock, control


def approve(control, plan, confirmations=ORPHAN_CHECKS):
    return control.approve_orphan_resolution(
        plan["proposal"], plan["revision"], confirmations=confirmations
    )


def test_resolve_preserves_stop_identity_version_and_claim_audit(setup):
    _, control = setup
    before = control.status()
    plan = control.prepare_orphan_resolution()
    assert control.status()["in_flight"]
    assert approve(control, plan) == {
        "resolved": True,
        "stopped": True,
        "live_orders_enabled": False,
    }
    after = control.status()
    assert after["blocked"] and after["stopped"] and not after["in_flight"]
    for field in ("instance_id", "scope", "version", "reason", "generation"):
        assert after[field] == before[field]
    with pytest.raises(PrivateReadError, match="reads_stopped"), control.slot():
        pytest.fail("resolution resumed GET")
    with pytest.raises(PrivateReadError, match="not_resolvable"):
        approve(control, plan)
    with sqlite3.connect(control.path) as conn:
        events = conn.execute("SELECT kind, token FROM events ORDER BY id").fetchall()
    assert events[-2] == ("ORPHAN_CHECKS_CONFIRMED", plan["proposal"])
    assert events[-1] == ("ORPHAN_RESOLVED", plan["claim"])
    assert ("CLAIMED", plan["claim"]) in events


@pytest.mark.parametrize("missing", sorted(ORPHAN_CHECKS))
def test_missing_confirmation_keeps_claim(setup, missing):
    _, control = setup
    plan = control.prepare_orphan_resolution()
    with pytest.raises(PrivateReadError, match="confirmation_required"):
        approve(control, plan, ORPHAN_CHECKS - {missing})
    assert control.status()["in_flight"]


@pytest.mark.parametrize("change", ["stop", "proposal", "state", "token", "mismatch"])
def test_state_changes_invalidate_proposal(setup, change):
    _, control = setup
    plan = control.prepare_orphan_resolution()
    if change == "stop":
        control.stop("operator_stop")
    elif change == "proposal":
        control.prepare_orphan_resolution()
    elif change == "mismatch":
        control.stop("claim_mismatch")
        control.stop("operator_stop")
    else:
        with sqlite3.connect(control.path) as conn:
            conn.execute(
                "UPDATE control SET last_wall_ns=last_wall_ns+1"
                if change == "state"
                else "UPDATE control SET in_flight='ffffffffffffffffffffffffffffffff'"
            )
    with pytest.raises(PrivateReadError):
        approve(control, plan)
    assert control.status()["in_flight"] and control.status()["stopped"]


@pytest.mark.parametrize("seconds", [300, 301, 10000])
def test_expiry_is_not_owner_death_evidence(setup, seconds):
    clock, control = setup
    plan = control.prepare_orphan_resolution()
    clock.sleep(seconds)
    with pytest.raises(PrivateReadError, match="expired"):
        approve(control, plan)
    assert control.status()["in_flight"]
    with pytest.raises(PrivateReadError, match="proposal_changed"):
        approve(control, plan)


def test_valid_just_before_expiry(setup):
    clock, control = setup
    plan = control.prepare_orphan_resolution()
    clock.sleep(299.999)
    assert approve(control, plan)["resolved"]


@pytest.mark.parametrize("phase", ["prepare", "approve"])
def test_clock_reversal_never_clears_claim(setup, phase):
    clock, control = setup
    plan = control.prepare_orphan_resolution()
    clock.wall -= 1
    with pytest.raises(PrivateReadError, match="clock_invalid"):
        if phase == "prepare":
            control.prepare_orphan_resolution()
        else:
            approve(control, plan)
    assert control.status()["in_flight"] and control.status()["stopped"]


@pytest.mark.parametrize("version", [1, 2])
def test_legacy_claim_cannot_use_owner_proof(setup, version):
    clock, control = setup
    with sqlite3.connect(control.path) as conn:
        conn.execute("UPDATE control SET version=?", (version,))
        conn.execute("DROP TABLE owner_file")
    (control.path.parent / "read-owner.lock").unlink()
    legacy = PersistentReadLimiter(control.path.parent, "synthetic", **clock.args())
    assert not legacy.status()["orphan_resolution_supported"]
    with pytest.raises(PrivateReadError, match="legacy_claim_not_resolvable"):
        legacy.prepare_orphan_resolution()
    with pytest.raises(PrivateReadError, match="legacy_claim_not_resolvable"):
        legacy.approve_orphan_resolution("a" * 32, "b" * 64, confirmations=ORPHAN_CHECKS)
    assert legacy.status()["in_flight"]


@pytest.mark.parametrize("version", [1, 2])
def test_legacy_idle_recovery_and_slot_still_work(tmp_path, version):
    clock = Clock()
    control = PersistentReadLimiter.create(tmp_path / "control", "synthetic", **clock.args())
    with sqlite3.connect(control.path) as conn:
        conn.execute("UPDATE control SET version=?", (version,))
        conn.execute("DROP TABLE owner_file")
    (control.path.parent / "read-owner.lock").unlink()
    legacy = PersistentReadLimiter(control.path.parent, "synthetic", **clock.args())
    legacy.stop()
    plan = legacy.prepare_recovery()
    legacy.approve_recovery(plan["proposal"], plan["revision"], confirmations=RECOVERY_CHECKS)
    fresh = PersistentReadLimiter(control.path.parent, "synthetic", **clock.args())
    assert fresh.status()["version"] == 2
    with fresh.slot():
        pass


@pytest.mark.parametrize("change", ["missing", "replacement", "contents", "metadata", "downgrade"])
def test_owner_changes_deny_resolution_and_never_recreate(setup, change):
    clock, control = setup
    plan = control.prepare_orphan_resolution()
    owner = control.path.parent / "read-owner.lock"
    if change in {"missing", "replacement"}:
        owner.rename(owner.with_suffix(".saved"))
        if change == "replacement":
            owner.write_bytes(plan["instance_id"].encode())
    elif change == "contents":
        owner.write_bytes(b"f" * 32)
    else:
        with sqlite3.connect(control.path) as conn:
            conn.execute(
                "UPDATE owner_file SET inode='invalid'"
                if change == "metadata"
                else "UPDATE control SET version=2"
            )
    with pytest.raises(PrivateReadError):
        approve(control, plan)
    if change != "downgrade":
        with pytest.raises(PrivateReadError):
            fresh = PersistentReadLimiter(control.path.parent, "synthetic", **clock.args())
            fresh.prepare_orphan_resolution()
    if change == "missing":
        assert not owner.exists()
    with sqlite3.connect(control.path) as conn:
        assert conn.execute("SELECT in_flight IS NOT NULL, stopped FROM control").fetchone() == (
            1,
            1,
        )


def test_active_os_owner_blocks_both_stages_even_in_same_process(setup):
    clock, control = setup
    peer = PersistentReadLimiter(control.path.parent, "synthetic", **clock.args())
    plan = control.prepare_orphan_resolution()
    with peer._owner_lock():
        with pytest.raises(PrivateReadError, match="claim_unresolved"):
            control.prepare_orphan_resolution()
        with pytest.raises(PrivateReadError, match="claim_unresolved"):
            approve(control, plan)
    assert approve(control, plan)["resolved"]


@pytest.mark.parametrize("change", ["not_stopped", "idle", "missing_audit"])
def test_resolution_requires_stopped_audited_claim(setup, change):
    _, control = setup
    with sqlite3.connect(control.path) as conn:
        conn.execute(
            {
                "not_stopped": "UPDATE control SET stopped=0,reason=NULL",
                "idle": "UPDATE control SET in_flight=NULL",
                "missing_audit": "DELETE FROM events WHERE kind='CLAIMED'",
            }[change]
        )
    with pytest.raises(PrivateReadError, match="not_resolvable"):
        control.prepare_orphan_resolution()


def test_other_database_proposal_is_rejected(setup, tmp_path):
    clock, control = setup
    peer = PersistentReadLimiter.create(tmp_path / "other", "synthetic", **clock.args())
    with peer._owner_lock():
        peer._claim()
    peer.stop("operator_stop")
    peer.prepare_orphan_resolution()
    with pytest.raises(PrivateReadError, match="proposal_changed"):
        approve(peer, control.prepare_orphan_resolution())
    assert peer.status()["in_flight"]


@pytest.mark.parametrize("kind", ["COMPLETED", "FAILED", "ORPHAN_RESOLVED"])
def test_claim_with_terminal_audit_is_inconsistent(setup, kind):
    _, control = setup
    with sqlite3.connect(control.path) as conn:
        conn.execute(
            "INSERT INTO events(wall_ns,kind,token) SELECT last_wall_ns,?,in_flight FROM control",
            (kind,),
        )
    with pytest.raises(PrivateReadError, match="not_resolvable"):
        control.prepare_orphan_resolution()
    assert control.status()["in_flight"]


def test_resolution_storage_failure_rolls_back_and_releases_owner(setup):
    clock, control = setup
    plan = control.prepare_orphan_resolution()
    with sqlite3.connect(control.path) as conn:
        conn.executescript("""
            CREATE TRIGGER reject_resolution BEFORE INSERT ON events
            WHEN NEW.kind='ORPHAN_RESOLVED'
            BEGIN SELECT RAISE(ABORT, 'synthetic disk failure'); END;
        """)
    with pytest.raises(PrivateReadError, match="storage_failed"):
        approve(control, plan)
    with sqlite3.connect(control.path) as conn:
        assert conn.execute("SELECT in_flight IS NOT NULL, stopped FROM control").fetchone() == (
            1,
            1,
        )
        assert conn.execute("SELECT kind FROM events ORDER BY id DESC LIMIT 1").fetchone() == (
            "ORPHAN_PROPOSED",
        )
        conn.execute("DROP TRIGGER reject_resolution")
    fresh = PersistentReadLimiter(control.path.parent, "synthetic", **clock.args())
    assert approve(fresh, plan)["resolved"]


def test_resolution_race_has_one_winner(setup):
    clock, control = setup
    peers = [
        PersistentReadLimiter(control.path.parent, "synthetic", **clock.args()) for _ in range(2)
    ]
    plan = control.prepare_orphan_resolution()

    def attempt(peer):
        try:
            return approve(peer, plan)["resolved"]
        except PrivateReadError:
            return False

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(attempt, peers)) == [False, True]
    assert control.status()["stopped"]


def test_owner_is_held_before_claim_through_wait_and_finish(tmp_path):
    clock = Clock()
    control = PersistentReadLimiter.create(tmp_path / "control", "synthetic", **clock.args())
    peer = PersistentReadLimiter(control.path.parent, "synthetic", **clock.args())
    claim, finish = control._claim, control._finish
    phases = []

    def check(phase):
        phases.append(phase)
        with pytest.raises(PrivateReadError, match="claim_unresolved"), peer._owner_lock():
            pytest.fail("owner lock released early")

    def claimed():
        check("before-claim")
        return claim()

    def wait(seconds):
        check("wait")
        clock.sleep(seconds)

    def finished(token, outcome):
        finish(token, outcome)
        check("after-finish")

    control._claim, control._wait, control._finish = claimed, wait, finished
    with control.slot():
        check("request")
    assert phases == ["before-claim", "wait", "request", "after-finish"]
    with peer._owner_lock():
        pass


@pytest.mark.parametrize("unlock_fails", [False, True])
@pytest.mark.parametrize("body_fails", [False, True])
def test_windows_owner_lock_unlocks_first_byte_before_close(
    tmp_path, monkeypatch, body_fails, unlock_fails
):
    control = PersistentReadLimiter.create(tmp_path / "control", "synthetic")
    calls = []

    def locking(descriptor, mode, size):
        calls.append((mode, os.lseek(descriptor, 0, os.SEEK_CUR), size))
        if unlock_fails and mode == windows.LK_UNLCK:
            raise OSError("unlock failed")

    # A stand-in for msvcrt, so the Windows branch also runs on POSIX CI.
    windows = SimpleNamespace(LK_NBLCK=2, LK_UNLCK=0, locking=locking)
    monkeypatch.setitem(sys.modules, "msvcrt", windows)
    monkeypatch.setattr("trading.read_control.os.name", "nt")
    if body_fails:
        with pytest.raises(RuntimeError, match="protected"), control._owner_lock():
            raise RuntimeError("protected operation failed")
    else:
        with control._owner_lock():
            pass
    # Lock and unlock the same first byte, although read() moved the position.
    assert calls == [(windows.LK_NBLCK, 0, 1), (windows.LK_UNLCK, 0, 1)]


@pytest.mark.parametrize("death", ["exit", "terminate"])
def test_real_process_death_requires_explicit_resolution(tmp_path, death):
    control = PersistentReadLimiter.create(tmp_path / "control", "synthetic")
    marker = tmp_path / "ready"
    script = """
import os, sys, time
from pathlib import Path
from trading.read_control import PersistentReadLimiter
c = PersistentReadLimiter(Path(sys.argv[1]), 'synthetic')
with c.slot():
    Path(sys.argv[2]).write_text('ready')
    if sys.argv[3] == 'exit':
        os._exit(23)
    time.sleep(15)
"""
    child = subprocess.Popen(
        [sys.executable, "-c", script, str(control.path.parent), str(marker), death],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 5
        while not marker.exists() and child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert marker.exists()
        control.stop("operator_stop")
        if death == "terminate":
            with pytest.raises(PrivateReadError, match="claim_unresolved"):
                control.prepare_orphan_resolution()
            child.terminate()
        child.communicate(timeout=5)
        if death == "exit":
            assert child.returncode == 23
        assert control.status()["blocked"] and control.status()["in_flight"]
        assert approve(control, control.prepare_orphan_resolution())["resolved"]
        assert control.status()["blocked"] and control.status()["stopped"]
    finally:
        if child.poll() is None:
            child.terminate()
            child.communicate(timeout=5)


def test_approval_reacquires_lock_against_other_process(setup, tmp_path):
    _, control = setup
    plan = control.prepare_orphan_resolution()
    marker = tmp_path / "owner-ready"
    script = """
import sys, time
from pathlib import Path
from trading.read_control import PersistentReadLimiter
c = PersistentReadLimiter(Path(sys.argv[1]), 'synthetic')
with c._owner_lock():
    Path(sys.argv[2]).write_text('ready')
    time.sleep(15)
"""
    child = subprocess.Popen(
        [sys.executable, "-c", script, str(control.path.parent), str(marker)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 5
        while not marker.exists() and child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert marker.exists()
        with pytest.raises(PrivateReadError, match="claim_unresolved"):
            approve(control, plan)
        assert control.status()["in_flight"]
        child.terminate()
        child.communicate(timeout=5)
        assert approve(control, plan)["resolved"]
    finally:
        if child.poll() is None:
            child.terminate()
            child.communicate(timeout=5)


def test_resolution_then_recovery_fences_old_mock_http_client(setup):
    clock, control = setup
    calls = []

    def client(limiter):
        def handler(request):
            calls.append(request)
            return httpx.Response(200, json={"status": 0, "data": {}})

        return PrivateReadClient(
            SecretStr("dummy-key"),
            SecretStr("dummy-secret"),
            limiter=limiter,
            transport=httpx.MockTransport(handler),
            clock=lambda: datetime.fromtimestamp(clock.wall / 1e9, UTC),
            monotonic=lambda: clock.mono / 1e9,
        )

    request = RequestPlan("GET", "/v1/account/assets")
    with client(control) as old:
        approve(control, control.prepare_orphan_resolution())
        with pytest.raises(PrivateReadError, match="reads_stopped"):
            old.get(request)
        recovery = control.prepare_recovery()
        control.approve_recovery(
            recovery["proposal"], recovery["revision"], confirmations=RECOVERY_CHECKS
        )
        with pytest.raises(PrivateReadError, match="reopen_required"):
            old.get(request)
        fresh = PersistentReadLimiter(control.path.parent, "synthetic", **clock.args())
        with client(fresh) as new:
            new.get(request)
    assert len(calls) == 1
    assert fresh.status()["version"] == 3


@pytest.mark.parametrize("mode", ["noninteractive", "missing", "wrong_text", "approve"])
def test_cli_confirmation_and_no_automatic_recovery(setup, monkeypatch, capsys, mode):
    _, control = setup
    args = ["--directory", str(control.path.parent), "--scope", "synthetic"]
    main(["prepare-orphan", *args])
    plan = json.loads(capsys.readouterr().out)
    args += ["--proposal", plan["proposal"], "--revision", plan["revision"]]
    checks = ORPHAN_CHECKS - {"get-only"} if mode == "missing" else ORPHAN_CHECKS
    args += ["--confirm-" + check for check in checks]
    monkeypatch.setattr(sys.stdin, "isatty", lambda: mode != "noninteractive")
    monkeypatch.setattr(
        "builtins.input", lambda _: "no" if mode == "wrong_text" else "CLEAR GET CLAIM"
    )
    if mode == "approve":
        main(["approve-orphan", *args])
        assert json.loads(capsys.readouterr().out)["resolved"]
        assert not control.status()["in_flight"]
    else:
        with pytest.raises(SystemExit) as caught:
            main(["approve-orphan", *args])
        assert caught.value.code == 2
        assert control.status()["in_flight"]
    assert control.status()["stopped"]
