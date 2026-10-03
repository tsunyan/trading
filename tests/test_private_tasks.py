"""Bound watchdog task plans and actual action/PowerShell round trips, without registration."""

import ctypes
import json
import os
import socket
import subprocess
import sys
from pathlib import Path

import pytest
from test_private_sync import make_setup

from trading import private_tasks
from trading.paper_runner import python_process_args
from trading.private_operations import OperationsError, PrivateOperations
from trading.private_tasks import task_plan


@pytest.fixture(autouse=True)
def no_credentials_or_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("real credentials/network attempted")

    monkeypatch.setattr(ctypes, "WinDLL", forbidden, raising=False)
    monkeypatch.setattr(socket, "socket", forbidden)


@pytest.fixture
def workspace(tmp_path):
    root = tmp_path / "同期 with spaces"
    root.mkdir()
    result = make_setup(root)[-1]
    PrivateOperations.create(result.directory)
    return result


def test_plan_uses_actual_interpreter_and_one_local_monitor_task_without_mutation(workspace):
    paths = [
        workspace.directory / "sync-plan.json",
        workspace.control.path,
        workspace.journal.path,
        workspace.book.path,
        workspace.directory / "private-operations/operations.sqlite",
    ]
    before = [p.read_bytes() for p in paths]
    plan = task_plan(workspace.directory)
    assert plan["control_instance"] == workspace.control.snapshot()["instance"]
    assert plan["interval_seconds"] == 60 and len(plan["tasks"]) == 1
    spec = plan["tasks"][0]
    interpreter = Path(sys._base_executable)
    windowless = interpreter.with_name(f"pythonw{interpreter.suffix}")
    if os.name == "nt" and windowless.is_file():
        interpreter = windowless
    assert spec["executable"] == str(interpreter)
    assert str(workspace.directory) in spec["arguments"]
    assert "trading.private_operations" in spec["arguments"] and "watchdog" in spec["arguments"]
    assert "trading.private_sync" not in spec["arguments"]
    assert "credential_reference" not in json.dumps(plan)
    assert spec["name"].endswith("-Watchdog") and spec["execution_limit_seconds"] == 90
    assert not plan["complete"] and not plan["live_enabled"]
    assert [p.read_bytes() for p in paths] == before


@pytest.mark.parametrize("interval", [True, 0, 29, 121])
def test_invalid_or_too_slow_monitor_interval_is_refused(workspace, interval):
    with pytest.raises(OperationsError, match="interval_invalid"):
        task_plan(workspace.directory, interval_seconds=interval)


def test_missing_outbox_or_runtime_is_not_recreated_for_task_planning(workspace):
    monitor = PrivateOperations(workspace.directory)
    monitor.path.unlink()
    with pytest.raises(ValueError):
        task_plan(workspace.directory)
    assert not monitor.path.exists()


def test_actual_watchdog_action_round_trips_utf8_space_path_and_checks_local_health(workspace):
    spec = task_plan(workspace.directory)["tasks"][0]
    command = python_process_args(
        "from trading.private_operations import main; raise SystemExit(main(sys.argv[1:]))",
        "watchdog",
        "--directory",
        workspace.directory,
    )
    assert subprocess.list2cmdline(command[1:]) == spec["arguments"]
    process = subprocess.run(command, capture_output=True, text=True, encoding="utf-8", timeout=15)
    assert process.returncode == 0, process.stderr
    result = json.loads(process.stdout)
    assert result["ok"] and result["conditions"] == [] and result["notifications"]["submitted"] == 0
    assert PrivateOperations(workspace.directory).status()["last_check_at"] is not None
    assert workspace.control.snapshot()["phase"] == "READY"


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell boundary")
def test_actual_install_script_plan_only_round_trip_preserves_runtime(workspace):
    state = workspace.control.snapshot()
    powershell = Path(os.environ["SystemRoot"]) / "System32/WindowsPowerShell/v1.0/powershell.exe"
    script = Path(__file__).resolve().parents[1] / "scripts/install-private-watchdog.ps1"
    process = subprocess.run(
        [
            str(powershell),
            "-NoProfile",
            "-NonInteractive",
            "-WindowStyle",
            "Hidden",
            "-ExecutionPolicy",
            "Bypass",  # This test process only; user/machine policy stays unchanged.
            "-File",
            str(script),
            "-Directory",
            str(workspace.directory),
            "-PlanOnly",
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=20,
    )
    assert process.returncode == 0, process.stderr
    result = json.loads(process.stdout)
    assert result["control_instance"] == state["instance"] and len(result["tasks"]) == 1
    assert result["tasks"][0]["execution_limit_seconds"] == 90
    assert workspace.control.snapshot() == state
    assert PrivateOperations(workspace.directory).status()["last_check_at"] is None


def test_cli_emits_valid_plan_and_fixed_failure_codes(workspace, capsys):
    assert private_tasks.main(["plan", "--directory", str(workspace.directory)]) == 0
    assert json.loads(capsys.readouterr().out)["ok"]
    assert (
        private_tasks.main(
            ["plan", "--directory", str(workspace.directory), "--interval-seconds", "1"]
        )
        == 1
    )
    assert json.loads(capsys.readouterr().out) == {
        "ok": False,
        "reason": "private_task_plan_failed",
    }
