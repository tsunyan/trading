import json
import sqlite3
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta

import pytest

from trading import paper_runner
from trading.observer import initialize, observe
from trading.paper_runner import PaperRunner, RunnerPolicy, read_alerts, status
from trading.windows_notify import deliver_alerts
from trading.windows_tasks import task_plan


class Clock:
    def __init__(self):
        self.now = datetime(2026, 9, 29, 12, tzinfo=UTC)
        self.elapsed = 0.0
        self.sleeps = []

    def tick(self, seconds):
        self.now += timedelta(seconds=seconds)
        self.elapsed += seconds

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.tick(seconds)

    def args(self):
        return {"clock": lambda: self.now, "monotonic": lambda: self.elapsed, "sleep": self.sleep}


@pytest.fixture
def account(tmp_path, cfg):
    clock = Clock()
    directory = tmp_path / "paper"
    initialize(directory, cfg, clock.now)
    return directory, clock


def runner(account, results, policy=None):
    directory, clock = account
    calls = []

    def worker(path, timeout):
        assert path == directory.resolve()
        calls.append(timeout)
        return results.pop(0)

    return PaperRunner(directory, policy, worker=worker, **clock.args()), calls


def rows(directory, query):
    with sqlite3.connect(directory / "operations.sqlite") as conn:
        return conn.execute(query).fetchall()


@pytest.mark.parametrize(
    "patch",
    [
        {"max_attempts": 0},
        {"max_attempts": 11},
        {"max_attempts": True},
        {"cycle_timeout_seconds": 301},
        {"attempt_timeout_seconds": 241},
        {"max_backoff_seconds": 1},
        {"stale_after_seconds": 300},
        {"extra": 1},
    ],
)
def test_invalid_policy_is_rejected(patch):
    with pytest.raises(ValueError):
        RunnerPolicy(**patch)


def test_bounded_backoff_preserves_each_attempt(account):
    job, calls = runner(
        account,
        [
            {"status": "error", "error_type": "ReadTimeout"},
            {"status": "busy"},
            {"status": "ok", "action": "same_signal"},
        ],
    )
    result = job.step()
    assert result["status"] == "ok"
    assert calls == [180, 180, 180]
    assert account[1].sleeps == [5, 10]
    assert len(rows(account[0], "SELECT * FROM attempts")) == 3
    assert status(account[0], now=account[1].now)["last_attempts"] == 3
    assert read_alerts(account[0]) == []


def test_deadline_bounds_worker_timeout_and_prevents_extra_attempt(account):
    directory, clock = account
    calls = []

    def worker(path, timeout):
        calls.append(timeout)
        clock.tick(timeout)
        return {"status": "error", "error_type": "WorkerTimeout", "outcome_unknown": True}

    policy = RunnerPolicy(attempt_timeout_seconds=170, cycle_timeout_seconds=180)
    job = PaperRunner(directory, policy, worker=worker, **clock.args())
    result = job.step()
    assert calls == [170, 5]
    assert result["duration_seconds"] == 180
    assert result["observation"]["outcome_unknown"] is True


def test_safety_error_is_not_retried(account):
    job, calls = runner(
        account,
        [{"status": "error", "error_type": "ValueError", "error": "observation spec/code changed"}],
    )
    assert job.step()["attempts"] == 1
    assert len(calls) == 1
    assert account[1].sleeps == []


def test_one_alert_per_failure_episode_and_recovery(account):
    error = {"status": "error", "error_type": "ValueError"}
    job, _ = runner(account, [error] * 4 + [{"status": "market_closed"}] + [error] * 3)
    for _ in range(4):
        job.step()
    assert [a["kind"] for a in read_alerts(account[0])] == ["consecutive_failures"]
    job.step()
    assert status(account[0], now=account[1].now)["conditions"] == []
    for _ in range(3):
        job.step()
    assert [a["kind"] for a in read_alerts(account[0])] == [
        "consecutive_failures",
        "recovered",
        "consecutive_failures",
    ]


def test_risk_halt_raises_once_without_stopping_liquidation_observations(account):
    halt = {"status": "ok", "event": {"state": {"halted": True}}}
    job, calls = runner(account, [halt, halt])
    job.step()
    job.step()
    assert len(calls) == 2
    assert [a["kind"] for a in read_alerts(account[0])] == ["risk_halted"]


def test_watchdog_detects_stopped_job_and_future_clock(account):
    job, _ = runner(account, [{"status": "ok"}, {"status": "market_closed"}])
    job.step()
    account[1].tick(901)
    assert job.check()["status"] == "stale"
    job.check()
    assert len(read_alerts(account[0])) == 1
    job.step()
    assert [a["kind"] for a in read_alerts(account[0])] == ["stale", "recovered"]
    assert status(account[0], now=account[1].now - timedelta(seconds=1))["stale"] is True


def test_readonly_status_does_not_initialize_operations(account):
    assert status(account[0], now=account[1].now)["status"] == "not_started"
    assert read_alerts(account[0]) == []
    assert not (account[0] / "operations.sqlite").exists()


def test_live_owner_refuses_second_runner_and_does_not_recover_its_cycle(account):
    directory, clock = account
    second = PaperRunner(
        directory, worker=lambda *_: pytest.fail("second must not trade"), **clock.args()
    )

    def worker(*_):
        assert second.step()["status"] == "busy"
        assert second.check()["status"] == "busy"
        assert status(directory, now=clock.now)["status"] == "running"
        assert read_alerts(directory) == []
        return {"status": "ok"}

    assert PaperRunner(directory, worker=worker, **clock.args()).step()["status"] == "ok"
    assert len(rows(directory, "SELECT * FROM cycles")) == 1


def test_process_termination_preserves_unknown_cycle_and_releases_lock(account):
    directory, clock = account
    code = (
        "import os,sys; from trading.paper_runner import PaperRunner; "
        "PaperRunner(sys.argv[1], worker=lambda *_: os._exit(88)).step()"
    )
    process = subprocess.run(paper_runner.python_process_args(code, directory), timeout=15)
    assert process.returncode == 88
    assert rows(directory, "SELECT status FROM cycles") == [("running",)]
    job, _ = runner(account, [{"status": "ok"}, {"status": "ok"}])
    job.step()
    job.step()
    assert rows(directory, "SELECT status FROM cycles ORDER BY id") == [
        ("interrupted",),
        ("ok",),
        ("ok",),
    ]
    alerts = read_alerts(directory)
    assert len(alerts) == 1
    assert alerts[0]["kind"] == "interrupted"
    assert alerts[0]["detail"]["outcome_unknown"] is True


def test_policy_or_observer_change_cannot_reuse_operations(account):
    job, _ = runner(account, [{"status": "ok"}])
    job.step()
    other = PaperRunner(account[0], RunnerPolicy(max_attempts=2), **account[1].args())
    with pytest.raises(ValueError, match="policy changed"):
        other.step()
    manifest_path = account[0] / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["observer_id"] = "another account"
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="another observer"):
        PaperRunner(account[0]).step()
    with pytest.raises(ValueError, match="identity mismatch"):
        status(account[0])


def test_timeout_subprocess_is_reported_as_unknown(monkeypatch, tmp_path):
    def timeout(*args, **kwargs):
        assert kwargs["timeout"] == 2
        raise subprocess.TimeoutExpired(args[0], 2)

    monkeypatch.setattr(subprocess, "run", timeout)
    assert paper_runner.run_observation(tmp_path, 2) == {
        "status": "error",
        "error_type": "WorkerTimeout",
        "outcome_unknown": True,
    }


@pytest.mark.parametrize(
    "output,exit_code",
    [
        ("not JSON", 0),
        (json.dumps({"status": "ok"}), 1),
        (json.dumps({"status": "unknown"}), 0),
        ("[]", 0),
        ("x" * 256001, 0),
    ],
    ids=["invalid_json", "exit_disagrees", "unexpected_status", "list", "oversized"],
)
def test_worker_output_cannot_silently_report_success(monkeypatch, tmp_path, output, exit_code):
    monkeypatch.setattr(
        subprocess, "run", lambda *_, **kw: subprocess.CompletedProcess([], exit_code, output)
    )
    assert paper_runner.run_observation(tmp_path, 1)["error_type"] == "InvalidWorkerOutput"


def test_toast_failure_is_durable_retried_and_does_not_acknowledge(account):
    directory, clock = account
    job, _ = runner(account, [{"status": "error"}], RunnerPolicy(failure_alert_threshold=1))
    job.step()
    calls = []

    def send(alert, observer_id):
        calls.append((alert["id"], observer_id))
        if len(calls) == 1:
            raise OSError("notifications disabled")

    assert deliver_alerts(directory, send=send, clock=lambda: clock.now)["failed"] == 1
    assert deliver_alerts(directory, send=send, clock=lambda: clock.now)["submitted"] == 0
    clock.tick(300)
    assert deliver_alerts(directory, send=send, clock=lambda: clock.now)["submitted"] == 1
    assert deliver_alerts(directory, send=send, clock=lambda: clock.now)["submitted"] == 0
    assert calls[0] == calls[1]
    assert len(read_alerts(directory)) == 1
    assert rows(directory, "SELECT attempts,error_type FROM deliveries") == [(2, None)]


def test_explicit_acknowledgement_does_not_clear_active_fault(account):
    job, _ = runner(account, [{"status": "error"}], RunnerPolicy(failure_alert_threshold=1))
    job.step()
    assert (
        paper_runner.main(
            [
                "ack",
                "--directory",
                str(account[0]),
                "--alert-id",
                "1",
                "--policy",
                str(write_policy(account[0], job.policy)),
            ]
        )
        == 0
    )
    assert read_alerts(account[0]) == []
    assert status(account[0], now=account[1].now)["conditions"] == ["consecutive_failures"]


def write_policy(directory, policy):
    path = directory / "policy.json"
    path.write_text(policy.model_dump_json())
    return path


def test_timeout_after_paper_commit_retry_does_not_duplicate_fill(tmp_path, bars, cfg):
    from test_observer import FakeAPI, FakeCalendar

    clock = Clock()
    clock.now = (bars.timestamp.iloc[2] + timedelta(hours=1, seconds=5)).to_pydatetime()
    directory = tmp_path / "paper"
    initialize(directory, cfg, clock.now)
    api, calendar = FakeAPI(bars, clock.now), FakeCalendar()
    calls = []

    def worker(*_):
        result = observe(directory, api, calendar, lambda: clock.now)
        calls.append(result)
        if len(calls) == 1:
            assert result["action"] == "buy"
            return {"status": "error", "error_type": "WorkerTimeout", "outcome_unknown": True}
        return result

    job = PaperRunner(directory, worker=worker, **clock.args())
    assert job.step()["status"] == "ok"
    assert calls[-1]["action"] == "duplicate_quote"
    with sqlite3.connect(directory / "paper.sqlite") as conn:
        events = [json.loads(row[0]) for row in conn.execute("SELECT payload_json FROM events")]
    assert sum(bool(event["filled_units"]) for event in events) == 1
    assert [a["kind"] for a in read_alerts(directory)] == ["paper_fill"]


def test_existing_fill_is_not_notified_again_when_runner_is_adopted(tmp_path, bars, cfg):
    from test_observer import FakeAPI, FakeCalendar

    now = (bars.timestamp.iloc[2] + timedelta(hours=1, seconds=5)).to_pydatetime()
    directory = tmp_path / "paper"
    initialize(directory, cfg, now)
    assert observe(directory, FakeAPI(bars, now), FakeCalendar(), lambda: now)["action"] == "buy"
    job = PaperRunner(directory, worker=lambda *_: {"status": "market_closed"})
    job.step()
    assert read_alerts(directory) == []


def test_task_plan_uses_actual_interpreter_and_preserves_fixed_account(account):
    directory, _ = account
    before = (directory / "manifest.json").read_bytes()
    plan = task_plan(directory)
    assert plan["interval_seconds"] == 300
    assert len(plan["tasks"]) == 2
    assert {t["executable"] for t in plan["tasks"]} == {sys._base_executable}
    assert all(str(directory) in t["arguments"] for t in plan["tasks"])
    assert plan["tasks"][0]["execution_limit_seconds"] > RunnerPolicy().cycle_timeout_seconds
    assert (directory / "manifest.json").read_bytes() == before
    assert not (directory / "operations.sqlite").exists()


def test_scheduled_action_round_trips_spaces_and_utf8_path(tmp_path, cfg):
    directory = tmp_path / "模擬口座 with spaces"
    initialize(directory, cfg)
    plan = task_plan(directory)
    # Watchdog can be run without network or a registered task. Avoid a desktop
    # notification in this CLI smoke by allowing a recent existing successful cycle.
    PaperRunner(directory, worker=lambda *_: {"status": "market_closed"}).step()
    argv = paper_runner.python_process_args(
        "from trading.windows_tasks import main; raise SystemExit(main(sys.argv[1:]))",
        "watchdog",
        "--directory",
        directory,
    )
    result = subprocess.run(argv, capture_output=True, text=True, encoding="utf-8", timeout=15)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["health"]["status"] == "market_closed"
    assert plan["tasks"][1]["name"].endswith("Watchdog")


def test_actual_worker_command_matches_frozen_runtime(account):
    from trading.provenance import runtime_versions

    result = subprocess.run(
        paper_runner.python_process_args(
            "from trading.provenance import runtime_versions; print(json.dumps(runtime_versions()))"
        ),
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == runtime_versions()


def test_real_timeout_releases_the_actual_worker_lock(account, tmp_path):
    directory, _ = account
    ready = tmp_path / "timeout-ready"
    code = (
        "import time; from pathlib import Path; from trading.paper_runner import _process_lock; "
        "ctx=_process_lock(Path(sys.argv[1])); owned=ctx.__enter__(); "
        "Path(sys.argv[2]).write_text(str(owned)); time.sleep(30)"
    )
    with pytest.raises(subprocess.TimeoutExpired):
        subprocess.run(
            paper_runner.python_process_args(code, directory / "operations.lock", ready), timeout=2
        )
    assert ready.read_text() == "True"
    job, _ = runner(account, [{"status": "market_closed"}])
    assert job.step()["status"] == "market_closed"


def test_subprocess_lock_released_after_kill(account, tmp_path):
    directory, _ = account
    ready = tmp_path / "ready"
    code = (
        "import sys,time; from pathlib import Path; "
        "from trading.paper_runner import _process_lock; "
        "ctx=_process_lock(Path(sys.argv[1])); owned=ctx.__enter__(); "
        "Path(sys.argv[2]).write_text(str(owned)); time.sleep(30)"
    )
    child = subprocess.Popen(
        paper_runner.python_process_args(code, directory / "operations.lock", ready)
    )
    try:
        deadline = time.monotonic() + 10
        while not ready.exists() and child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert ready.read_text() == "True"
        job, calls = runner(account, [{"status": "ok"}])
        assert job.step()["status"] == "busy"
        assert calls == []
        assert not (directory / "operations.sqlite").exists()
        child.kill()
        child.wait(timeout=10)
        assert job.step()["status"] == "ok"
    finally:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=10)
