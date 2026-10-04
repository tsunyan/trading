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
    journal = None
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
            "-MaxSlippage",
            "0.02",
            "-QuoteOutput",
            str(values["quote_output"]),
            "-ResultOutput",
            str(values["result_output"]),
            "-Confirm",
            ",".join(sorted(CYCLE_CONFIRMATIONS)),
            "-HistoryOutput",
            str(tmp_path / "cycles.jsonl"),
            "-DashboardOutput",
            str(tmp_path / "live.html"),
            "-Units",
            "auto",
            "-DoctorIntervalSeconds",
            "900",
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
    arguments = plan["tasks"][0]["arguments"]
    assert "--prepare" not in arguments and "--units auto" in arguments
    assert "--history-output" in arguments and "--dashboard-output" in arguments
    assert plan["tasks"][1]["name"].endswith("-Doctor")


class ExpiringCycle(FakeCycle):
    expires = None

    def __init__(self, *args, **kwargs):
        expires = self.expires
        self.journal = type(
            "J",
            (),
            {
                "snapshot": staticmethod(
                    lambda: {
                        "live_control": {
                            "approval": None
                            if expires is None
                            else {"expires_at": expires.isoformat()}
                        }
                    }
                )
            },
        )()


@pytest.mark.parametrize("hours", [2, 30])
def test_approval_expiry_is_noticed_once_per_approval(tmp_path, monkeypatch, capsys, hours):
    from datetime import UTC, datetime, timedelta

    (tmp_path / "fx.toml").write_text('market = "fx"\nsymbol = "USD_JPY"\nbar_seconds = 3600\n')
    ExpiringCycle.outcome = {"decision": {"action": "hold", "intent": None}, "orders_sent": False}
    ExpiringCycle.expires = datetime.now(UTC) + timedelta(hours=hours)
    monkeypatch.setattr(live_cycle, "LiveCycle", ExpiringCycle)
    sent = []
    for _ in range(3):
        live_cycle.main(cycle_args(tmp_path), send=lambda alert, source: sent.append(alert))
    capsys.readouterr()
    saved = json.loads((tmp_path / "cycle.json").read_text(encoding="utf-8"))
    assert saved["approval_expires_at"] == ExpiringCycle.expires.isoformat()
    if hours == 2:
        assert [a["kind"] for a in sent] == ["live_cycle_approval_expiring"]
        assert sent[0]["id"] in {"1h left", "2h left"}
        assert saved["approval_notice_for"] == ExpiringCycle.expires.isoformat()
    else:
        assert sent == [] and "approval_notice_for" not in saved
    # A renewed approval (new expiry) is noticed again when it nears its own end.
    ExpiringCycle.expires += timedelta(minutes=1)
    live_cycle.main(cycle_args(tmp_path), send=lambda alert, source: sent.append(alert))
    assert len(sent) == (2 if hours == 2 else 0)


def test_persisting_failure_is_noticed_once_per_reason(tmp_path, monkeypatch):
    from trading.live_account import LiveAccountError

    (tmp_path / "fx.toml").write_text('market = "fx"\nsymbol = "USD_JPY"\nbar_seconds = 3600\n')
    monkeypatch.setattr(live_cycle, "LiveCycle", FakeCycle)
    sent = []
    for reason in ["valuation_time_mismatch"] * 3 + ["read_control_blocked"]:
        FakeCycle.outcome = LiveAccountError(reason)
        with pytest.raises(SystemExit):
            live_cycle.main(cycle_args(tmp_path), send=lambda alert, source: sent.append(alert))
    assert [a["id"] for a in sent] == ["valuation_time_mismatch", "read_control_blocked"]


def test_failed_toast_is_retried_on_the_next_failure(tmp_path, monkeypatch):
    from trading.live_account import LiveAccountError

    (tmp_path / "fx.toml").write_text('market = "fx"\nsymbol = "USD_JPY"\nbar_seconds = 3600\n')
    monkeypatch.setattr(live_cycle, "LiveCycle", FakeCycle)
    FakeCycle.outcome = LiveAccountError("valuation_time_mismatch")
    attempts = []

    def flaky(alert, source):
        attempts.append(alert)
        if len(attempts) == 1:
            raise OSError("toast unavailable")

    for _ in range(3):
        with pytest.raises(SystemExit):
            live_cycle.main(cycle_args(tmp_path), send=flaky)
    assert len(attempts) == 2  # The failed first notice is retried once, then remembered.


def test_unsent_prepared_order_is_noticed_once_per_order(tmp_path, monkeypatch):
    (tmp_path / "fx.toml").write_text('market = "fx"\nsymbol = "USD_JPY"\nbar_seconds = 3600\n')
    monkeypatch.setattr(live_cycle, "LiveCycle", FakeCycle)
    sent = []
    for waiting in (["S2026100510OB"], ["S2026100510OB"], [], ["S2026100511CS"]):
        FakeCycle.outcome = {
            "decision": {"action": "hold", "reason": "unsettled_local_order", "intent": None},
            "waiting_prepared": waiting,
            "orders_sent": False,
        }
        live_cycle.main(cycle_args(tmp_path), send=lambda alert, source: sent.append(alert))
    assert [(a["kind"], a["id"]) for a in sent] == [
        ("live_cycle_prepared_waiting", "S2026100510OB"),
        ("live_cycle_prepared_waiting", "S2026100511CS"),
    ]


def test_settled_orders_are_noticed_with_their_final_state(tmp_path, monkeypatch):
    (tmp_path / "fx.toml").write_text('market = "fx"\nsymbol = "USD_JPY"\nbar_seconds = 3600\n')
    monkeypatch.setattr(live_cycle, "LiveCycle", FakeCycle)
    FakeCycle.outcome = {
        "decision": {"action": "hold", "intent": None},
        "reconciled_orders": [
            {"client_id": "S2026100510OB", "state": "FILLED"},
            {"client_id": "L001", "state": "WORKING"},
        ],
        "orders_sent": False,
    }
    sent = []
    live_cycle.main(cycle_args(tmp_path), send=lambda alert, source: sent.append(alert))
    assert sent == [{"kind": "live_cycle_order_settled", "id": "S2026100510OB FILLED"}]


def test_plan_with_candidate_requires_live_promotion(running, tmp_path):
    from test_live_cycle import live_ledger

    from trading.promotion import PromotionError

    values = inputs(running, tmp_path)
    config_text = 'market = "fx"\nsymbol = "USD_JPY"\nbar_seconds = 3600\nfast = 2\nslow = 4\n'
    values["config"].write_text(config_text)
    paper = live_ledger(tmp_path / "paper", stage="paper")
    with pytest.raises(PromotionError, match="strategy_not_promoted_for_live"):
        task_plan(**values, ledger=paper, hypothesis="H001")
    live = live_ledger(tmp_path / "live", stage="live")
    plan = task_plan(**values, ledger=live, hypothesis="H001")
    assert "--hypothesis H001" in plan["tasks"][0]["arguments"]
    with pytest.raises(LiveTaskError, match="ledger_and_hypothesis_required_together"):
        task_plan(**values, ledger=live)


def test_history_output_appends_one_line_per_run(tmp_path, monkeypatch, capsys):
    from trading.live_account import LiveAccountError

    (tmp_path / "fx.toml").write_text('market = "fx"\nsymbol = "USD_JPY"\nbar_seconds = 3600\n')
    monkeypatch.setattr(live_cycle, "LiveCycle", FakeCycle)
    args = [*cycle_args(tmp_path), "--history-output", str(tmp_path / "cycles.jsonl")]
    FakeCycle.outcome = {"decision": {"action": "hold", "intent": None}, "orders_sent": False}
    live_cycle.main(args, send=lambda alert, source: None)
    FakeCycle.outcome = LiveAccountError("valuation_time_mismatch")
    with pytest.raises(SystemExit):
        live_cycle.main(args, send=lambda alert, source: None)
    capsys.readouterr()
    lines = (tmp_path / "cycles.jsonl").read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["ok"] for line in lines] == [True, False]
    assert json.loads(lines[1])["reason"] == "valuation_time_mismatch"


def test_plan_passes_the_history_output(running, tmp_path):
    plan = task_plan(**inputs(running, tmp_path), history_output=tmp_path / "cycles.jsonl")
    assert "--history-output" in plan["tasks"][0]["arguments"]
    with pytest.raises(LiveTaskError, match="output_directory_required"):
        task_plan(**inputs(running, tmp_path), history_output=tmp_path / "missing" / "x.jsonl")


def test_prepared_cycle_prints_a_submit_command_for_review(tmp_path, monkeypatch, capsys):
    (tmp_path / "fx.toml").write_text('market = "fx"\nsymbol = "USD_JPY"\nbar_seconds = 3600\n')
    monkeypatch.setattr(live_cycle, "LiveCycle", FakeCycle)
    FakeCycle.outcome = {
        "decision": {"action": "open", "intent": {"client_id": "S2026100510OB"}},
        "prepared": True,
        "client_id": "S2026100510OB",
        "checkpoint_sha256": "c" * 64,
        "orders_sent": False,
    }
    live_cycle.main(cycle_args(tmp_path), send=lambda alert, source: None)
    command = json.loads(capsys.readouterr().out)["submit_command"]
    assert "trading.order_runtime submit" in command and "c" * 64 in command
    assert "--client-id S2026100510OB" in command and "<order_reference>" in command
    assert "dispatch.jsonl" in command and "--order-permission-confirmed" in command


def test_plan_adds_an_optional_doctor_task_with_its_own_interval(running, tmp_path):
    plan = task_plan(**inputs(running, tmp_path), doctor_interval_seconds=900)
    cycle, doctor = plan["tasks"]
    assert doctor["name"].endswith("-Doctor") and doctor["interval_seconds"] == 900
    assert "trading.live_doctor" in doctor["arguments"]
    assert "--cycle-result" in doctor["arguments"] and "--notify-state" in doctor["arguments"]
    assert "interval_seconds" not in cycle
    for invalid in (True, 299, 3601):
        with pytest.raises(LiveTaskError, match="invalid_doctor_interval"):
            task_plan(**inputs(running, tmp_path), doctor_interval_seconds=invalid)
