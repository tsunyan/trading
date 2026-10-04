"""Readiness report over every local send gate, with temporary stores only."""

import ctypes
import json
import socket

import pytest
from test_live_operations import release
from test_live_operations import setup as operations_setup
from test_live_operations import unbound as operations_unbound

from trading import live_doctor
from trading.live_doctor import diagnose


@pytest.fixture(autouse=True)
def no_native_or_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("real credentials/network attempted")

    monkeypatch.setattr(ctypes, "WinDLL", forbidden, raising=False)
    monkeypatch.setattr(socket, "socket", forbidden)


@pytest.fixture
def setup(tmp_path):
    yield from operations_setup.__wrapped__(operations_unbound.__wrapped__(tmp_path))


def report(setup):
    return diagnose(setup[1][3], setup[0][0].wall)


def test_healthy_enabled_journal_with_a_prepared_order_is_send_ready(setup):
    live = setup[1]
    before = live[3].snapshot(), live[2].snapshot()
    result = report(setup)
    assert result["send_ready"], result["gates"]
    assert result["orders"] == [{"client_id": "Buy001", "state": "PREPARED"}]
    assert result["approval_seconds_left"] > 0 and not result["entry_halted"]
    assert (live[3].snapshot(), live[2].snapshot()) == before
    assert setup[0][3].reads == []


def test_each_blocking_gate_is_reported_together(setup):
    values, live = setup[0], setup[1]
    release(setup)
    live[2].stop("operator_stop")
    result = report(setup)
    gates = result["gates"]
    assert not result["send_ready"]
    assert gates["post_control"]["reason"] == "post_stopped:operator_stop"
    assert gates["sync_and_watchdog"]["reason"] == "live_sync_owner_missing"
    assert gates["approval"]["ok"] and gates["account_proof"]["ok"]
    values[0].advance(3600)
    late = report(setup)["gates"]
    assert late["approval"]["reason"] == "approval_expired"
    assert late["account_proof"]["reason"] == "account_proof_stale"


def test_halt_and_read_stop_are_named(setup):
    live = setup[1]
    live[3].halt()
    live[1].stop("operator_stop")
    gates = report(setup)["gates"]
    assert gates["approval"]["reason"] in {"phase_stopped", "journal_halted"}
    assert gates["read_control"]["reason"] == "read_control_blocked"


def test_unregistered_or_disabled_journal_explains_itself(tmp_path):
    values, live, _ = operations_unbound.__wrapped__(tmp_path)
    result = diagnose(live[3], values[0].wall)
    assert result["gates"]["approval"]["reason"] == "phase_disabled"
    assert result["gates"]["sync_and_watchdog"]["reason"] == "operations_not_registered"
    assert result["gates"]["account_proof"]["reason"] == "account_proof_missing"
    assert result["approval_seconds_left"] is None


def test_cli_exit_code_follows_readiness(setup, capsys):
    live = setup[1]
    code = live_doctor.main(
        [
            "--directory",
            str(live[3].path.parent),
            "--read-control-directory",
            str(live[1].path.parent),
            "--scope",
            "synthetic",
        ]
    )
    printed = json.loads(capsys.readouterr().out)
    # The CLI uses the real clock, far from the fixture clock: not ready, exit code 1.
    assert code == 1 and printed["send_ready"] is False and printed["network_used"] is False
