"""Hourly read-only live cycle task plans and notices, without registering any task."""

import ctypes
import json
import os
import socket
import subprocess
from pathlib import Path

import pytest
from test_live_flow import running as flow_running

from trading import live_cycle, live_tasks
from trading.live_cycle import CYCLE_CONFIRMATIONS
from trading.live_tasks import LiveTaskError, task_plan


@pytest.fixture(autouse=True)
def no_native_or_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("real credentials/network attempted")

    monkeypatch.setattr(ctypes, "WinDLL", forbidden, raising=False)
    monkeypatch.setattr(socket, "socket", forbidden)


@pytest.fixture
def running(tmp_path):
    yield from flow_running.__wrapped__(tmp_path)


def inputs(running, tmp_path, **changes):
    values, live, _ = running
    config = tmp_path / "fx live.toml"
    config.write_text('market = "fx"\nsymbol = "USD_JPY"\nbar_seconds = 3600\n')
    return {
        "directory": live[3].path.parent,
        "read_control_directory": live[1].path.parent,
        "scope": "synthetic",
        "credential_reference": values[5].plan.credential_reference,
        "config": config,
        "units": 1000,
        "max_slippage": "0.02",
        "quote_output": tmp_path / "quote.json",
        "result_output": tmp_path / "cycle.json",
        "confirmations": CYCLE_CONFIRMATIONS,
        **changes,
    }


def test_plan_runs_the_cycle_hourly_without_prepare_and_changes_nothing(running, tmp_path):
    live = running[1]
    before = live[3].snapshot()
    plan = task_plan(**inputs(running, tmp_path, valuation_tolerance="0.05"))
    spec = plan["tasks"][0]
    assert plan["start_minute"] == 1 and plan["interval_seconds"] == 3600
    assert not plan["prepares_orders"] and not plan["sends_orders"]
    assert spec["name"] == f"TradingLab-Live-{before['live_control']['instance'][:12]}-Cycle"
    arguments = spec["arguments"]
    assert "trading.live_cycle" in arguments and "--notify" in arguments
    assert "--prepare" not in arguments and "--flatten" not in arguments
    assert "--valuation-tolerance 0.05" in arguments
    assert all(f"--confirm {item}" in arguments for item in CYCLE_CONFIRMATIONS)
    assert '"' in arguments  # The config path with a space stays one argument.
    assert spec["execution_limit_seconds"] == 300
    assert live[3].snapshot() == before and running[0][3].reads == []


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"confirmations": set()}, "cycle_confirmations_required"),
        ({"credential_reference": "../x"}, "invalid_credential_reference"),
        ({"units": 0}, "invalid_units"),
        ({"max_slippage": "inf"}, "invalid_decimal_option"),
    ],
)
def test_invalid_plans_are_refused(running, tmp_path, change, reason):
    with pytest.raises(LiveTaskError, match=reason):
        task_plan(**inputs(running, tmp_path, **change))


def test_unregistered_journal_cannot_be_scheduled(tmp_path, capsys):
    from test_live_operations import unbound

    _, live, _ = unbound.__wrapped__(tmp_path)
    config = tmp_path / "fx.toml"
    config.write_text('market = "fx"\nsymbol = "USD_JPY"\nbar_seconds = 3600\n')
    code = live_tasks.main(
        [
            "plan",
            "--directory",
            str(live[3].path.parent),
            "--read-control-directory",
            str(live[1].path.parent),
            "--scope",
            "synthetic",
            "--credential-reference",
            "a" * 32,
            "--config",
            str(config),
            "--units",
            "1000",
            "--max-slippage",
            "0.02",
            "--quote-output",
            str(tmp_path / "q.json"),
            "--result-output",
            str(tmp_path / "r.json"),
            *[item for c in sorted(CYCLE_CONFIRMATIONS) for item in ("--confirm", c)],
        ]
    )
    assert code == 1
    assert json.loads(capsys.readouterr().out) == {"ok": False, "reason": "live_task_plan_failed"}


def cycle_args(tmp_path):
    return [
        "--config",
        str(tmp_path / "fx.toml"),
        "--directory",
        str(tmp_path / "live"),
        "--read-control-directory",
        str(tmp_path / "reads"),
        "--scope",
        "synthetic",
        "--credential-reference",
        "a" * 32,
        "--units",
        "1000",
        "--max-slippage",
        "0.02",
        "--quote-output",
        str(tmp_path / "quote.json"),
        "--result-output",
        str(tmp_path / "cycle.json"),
        "--notify",
    ]


class FakeCycle:
    outcome = None

    def __init__(self, *args, **kwargs):
        pass

    def run(self, *args, **kwargs):
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


@pytest.mark.parametrize("action", ["open", "close", "hold"])
def test_cycle_notifies_only_for_proposals_and_writes_the_result(
    tmp_path, monkeypatch, capsys, action
):
    (tmp_path / "fx.toml").write_text('market = "fx"\nsymbol = "USD_JPY"\nbar_seconds = 3600\n')
    intent = None if action == "hold" else {"client_id": "S2026100510OB"}
    FakeCycle.outcome = {"decision": {"action": action, "intent": intent}, "orders_sent": False}
    monkeypatch.setattr(live_cycle, "LiveCycle", FakeCycle)
    sent = []
    live_cycle.main(cycle_args(tmp_path), send=lambda alert, source: sent.append(alert))
    saved = json.loads((tmp_path / "cycle.json").read_text(encoding="utf-8"))
    assert saved["ok"] and saved["decision"]["action"] == action
    if action == "hold":
        assert sent == [] and "notified" not in saved
    else:
        assert sent == [{"kind": "live_cycle_proposal", "id": "S2026100510OB"}]
        assert saved["notified"] is True
    assert json.loads(capsys.readouterr().out)["ok"]


def test_cycle_failure_notifies_writes_and_exits_even_if_the_toast_fails(tmp_path, monkeypatch):
    from trading.live_account import LiveAccountError

    (tmp_path / "fx.toml").write_text('market = "fx"\nsymbol = "USD_JPY"\nbar_seconds = 3600\n')
    FakeCycle.outcome = LiveAccountError("valuation_time_mismatch")
    monkeypatch.setattr(live_cycle, "LiveCycle", FakeCycle)

    def broken(alert, source):
        raise OSError("toast unavailable")

    with pytest.raises(SystemExit) as raised:
        live_cycle.main(cycle_args(tmp_path), send=broken)
    assert raised.value.code == 2
    saved = json.loads((tmp_path / "cycle.json").read_text(encoding="utf-8"))
    assert saved == {
        "ok": False,
        "reason": "valuation_time_mismatch",
        "orders_sent": False,
        "finished_at": saved["finished_at"],
        "notified": False,
    }


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell boundary")
def test_install_script_plan_only_round_trip(running, tmp_path):
    values = inputs(running, tmp_path)
    powershell = Path(os.environ["SystemRoot"]) / "System32/WindowsPowerShell/v1.0/powershell.exe"
    script = Path(__file__).resolve().parents[1] / "scripts/install-live-cycle.ps1"
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
            str(values["directory"]),
            "-ReadControlDirectory",
            str(values["read_control_directory"]),
            "-Scope",
            "synthetic",
            "-CredentialReference",
            values["credential_reference"],
            "-Config",
            str(values["config"]),
            "-Units",
            "1000",
            "-MaxSlippage",
            "0.02",
            "-QuoteOutput",
            str(values["quote_output"]),
            "-ResultOutput",
            str(values["result_output"]),
            "-Confirm",
            ",".join(sorted(CYCLE_CONFIRMATIONS)),
            "-PlanOnly",
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=60,
    )
    assert process.returncode == 0, process.stderr
    plan = json.loads(process.stdout)
    assert plan["ok"] and not plan["sends_orders"]
    assert "--prepare" not in plan["tasks"][0]["arguments"]
