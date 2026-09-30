import json
import socket
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

import httpx
import pytest
from pydantic import SecretStr

from trading.broker_contracts import RequestPlan
from trading.credential_store import CredentialError, CredentialVault
from trading.private_read import PrivateReadClient, PrivateReadError
from trading.read_control import RECOVERY_CHECKS, PersistentReadLimiter, main


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
    return clock, control


def reopen(control, clock):
    return PersistentReadLimiter(control.path.parent, "synthetic", **clock.args())


def approve(control, plan, checks=RECOVERY_CHECKS):
    return control.approve_recovery(plan["proposal"], plan["revision"], confirmations=checks)


@pytest.mark.parametrize("reason", ["client_stop", "operator_stop", "clock_invalid", "interrupted"])
def test_recoverable_stops_preserve_identity_and_fence_old_objects(setup, reason):
    clock, control = setup
    other = reopen(control, clock)
    identity = control.status()["instance_id"]
    control.stop(reason)
    plan = control.prepare_recovery()
    assert control.status()["stopped"]
    assert plan["reason"] == reason
    assert approve(control, plan) == {
        "recovered": True,
        "reopen_required": True,
        "live_orders_enabled": False,
    }
    for stale in (control, other):
        assert stale.status()["reopen_required"]
        assert stale.status()["blocked"] and not stale.status()["stopped"]
        with pytest.raises(PrivateReadError, match="reopen_required"), stale.slot():
            pytest.fail("old object reused")
    fresh = reopen(control, clock)
    assert fresh.status()["instance_id"] == identity
    assert not fresh.status()["blocked"]
    with fresh.slot():
        pass
    with sqlite3.connect(control.path) as conn:
        assert conn.execute("SELECT version FROM control").fetchone()[0] == 3
        log = conn.execute("SELECT kind FROM events ORDER BY id").fetchall()
    assert ("RECOVERY_CHECKS_CONFIRMED",) in log
    assert ("RECOVERY_APPROVED",) in log


@pytest.mark.parametrize("missing", sorted(RECOVERY_CHECKS))
def test_all_operator_confirmations_are_mandatory(setup, missing):
    _, control = setup
    control.stop()
    plan = control.prepare_recovery()
    with pytest.raises(PrivateReadError, match="confirmation_required"):
        approve(control, plan, RECOVERY_CHECKS - {missing})
    assert control.status()["stopped"]


@pytest.mark.parametrize("seconds", [300, 301, 10000])
def test_expired_proposal_cannot_be_approved(setup, seconds):
    clock, control = setup
    control.stop()
    plan = control.prepare_recovery()
    clock.sleep(seconds)
    with pytest.raises(PrivateReadError, match="expired"):
        approve(control, plan)
    assert control.status()["stopped"]
    with pytest.raises(PrivateReadError, match="proposal_changed"):
        approve(control, plan)


def test_valid_until_just_before_expiry(setup):
    clock, control = setup
    control.stop()
    plan = control.prepare_recovery()
    clock.sleep(299.999)
    assert approve(control, plan)["recovered"]


@pytest.mark.parametrize("change", ["same_stop", "other_stop", "new_proposal", "state"])
def test_state_or_stop_changes_invalidate_pending_proposal(setup, change):
    _, control = setup
    control.stop()
    plan = control.prepare_recovery()
    if change == "same_stop":
        control.stop()
    elif change == "other_stop":
        control.stop("operator_stop")
    elif change == "new_proposal":
        control.prepare_recovery()
    else:
        with sqlite3.connect(control.path) as conn:
            conn.execute("UPDATE control SET last_wall_ns=last_wall_ns+1")
    with pytest.raises(PrivateReadError, match="proposal_changed"):
        approve(control, plan)
    assert control.status()["stopped"]


def test_clock_cannot_be_reset_to_enable_recovery(setup):
    clock, control = setup
    control.stop()
    plan = control.prepare_recovery()
    clock.wall -= 1
    with pytest.raises(PrivateReadError, match="clock_invalid"):
        approve(control, plan)
    assert control.status()["stopped"]
    with pytest.raises(PrivateReadError, match="clock_invalid"):
        control.prepare_recovery()


def test_active_crashed_or_mismatched_claims_are_not_recoverable(setup):
    _, control = setup
    with pytest.raises(PrivateReadError, match="not_recoverable"):
        control.prepare_recovery()  # Not stopped.
    token = control._claim()  # Durable abandoned claim, as after a process crash.
    control.stop()
    with pytest.raises(PrivateReadError, match="not_recoverable"):
        control.prepare_recovery()
    assert control.status()["in_flight"]
    control._finish(token, "FAILED")
    control.stop("claim_mismatch")
    control.stop("operator_stop")  # Cannot weaken the severe stop reason.
    with pytest.raises(PrivateReadError, match="not_recoverable"):
        control.prepare_recovery()


def test_single_use_and_scope_bound_proposal(setup, tmp_path):
    clock, control = setup
    control.stop()
    plan = control.prepare_recovery()
    other = PersistentReadLimiter.create(tmp_path / "other", "synthetic", **clock.args())
    other.stop()
    other.prepare_recovery()
    with pytest.raises(PrivateReadError, match="proposal_changed"):
        approve(other, plan)
    approve(control, plan)
    current = reopen(control, clock)
    current.stop()
    with pytest.raises(PrivateReadError, match="proposal_changed"):
        approve(current, plan)


def test_concurrent_approvals_have_one_winner(setup):
    clock, control = setup
    control.stop()
    plan = control.prepare_recovery()
    a, b = reopen(control, clock), reopen(control, clock)

    def attempt(candidate):
        try:
            return approve(candidate, plan)["recovered"]
        except PrivateReadError:
            return False

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, (a, b)))
    assert sorted(results) == [False, True]
    with sqlite3.connect(control.path) as conn:
        assert (
            conn.execute("SELECT COUNT(*) FROM events WHERE kind='RECOVERY_APPROVED'").fetchone()[0]
            == 1
        )


def test_old_http_client_cannot_send_after_recovery(setup):
    clock, control = setup
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(401 if len(calls) == 1 else 200, json={"status": 0, "data": []})

    def client(limiter):
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
        with pytest.raises(PrivateReadError):
            old.get(request)
        plan = control.prepare_recovery()
        approve(control, plan)
        with pytest.raises(PrivateReadError, match="reopen_required"):
            old.get(request)
        with client(reopen(control, clock)) as fresh:
            assert fresh.get(request)["status"] == 0
    assert len(calls) == 2


def test_cli_approval_requires_interactive_checks_and_exact_phrase(setup, monkeypatch, capsys):
    _, control = setup
    control.stop()
    args = ["--directory", str(control.path.parent), "--scope", "synthetic"]
    main(["prepare-recovery", *args])
    plan = json.loads(capsys.readouterr().out)
    flags = ["--confirm-" + check for check in sorted(RECOVERY_CHECKS)]
    approval = [
        "approve-recovery",
        *args,
        "--proposal",
        plan["proposal"],
        "--revision",
        plan["revision"],
    ]
    monkeypatch.setattr(sys.stdin, "isatty", lambda: False)
    with pytest.raises(SystemExit):
        main([*approval, *flags])
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    with pytest.raises(SystemExit):
        main(approval)
    monkeypatch.setattr("builtins.input", lambda prompt: "no")
    with pytest.raises(SystemExit):
        main([*approval, *flags])
    assert control.status()["stopped"]
    monkeypatch.setattr("builtins.input", lambda prompt: "RESUME READS")
    main([*approval, *flags])
    assert json.loads(capsys.readouterr().out)["reopen_required"]
    assert control.status()["reopen_required"]


def test_credential_binding_survives_but_stale_control_is_rejected(setup):
    clock, control = setup

    class MemoryBackend:
        def __init__(self):
            self.records = {}

        def write_new(self, reference, blob):
            self.records[reference] = blob

        def read(self, reference):
            return self.records.get(reference)

    vault = CredentialVault(MemoryBackend())
    reference = vault.save(
        control, SecretStr("dummy-key"), SecretStr("dummy-secret"), read_only_confirmed=True
    )
    control.stop()
    approve(control, control.prepare_recovery())
    with pytest.raises(CredentialError, match="control_blocked"):
        vault.load(control, reference)
    credentials = vault.load(reopen(control, clock), reference)
    assert credentials.api_key.get_secret_value() == "dummy-key"


def test_preexisting_other_process_is_fenced_after_recovery(tmp_path):
    control = PersistentReadLimiter.create(tmp_path / "control", "synthetic")
    ready, release = tmp_path / "ready", tmp_path / "release"
    script = """
import sys, time
from pathlib import Path
from trading.read_control import PersistentReadLimiter
from trading.private_read import PrivateReadError
c = PersistentReadLimiter(Path(sys.argv[1]), 'synthetic')
Path(sys.argv[2]).write_text('ready')
deadline = time.monotonic() + 10
while not Path(sys.argv[3]).exists():
    if time.monotonic() > deadline:
        raise SystemExit(3)
    time.sleep(0.01)
try:
    with c.slot():
        raise SystemExit(4)
except PrivateReadError as e:
    print(str(e))
"""
    child = subprocess.Popen(
        [sys.executable, "-c", script, str(control.path.parent), str(ready), str(release)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 5
        while not ready.exists() and child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert ready.exists()
        control.stop()
        approve(control, control.prepare_recovery())
        release.write_text("release")
        output, error = child.communicate(timeout=5)
        assert child.returncode == 0, error.decode()
        assert output.decode().strip() == "read_control_reopen_required"
    finally:
        if child.poll() is None:
            child.terminate()
            child.communicate(timeout=5)
