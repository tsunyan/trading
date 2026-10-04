"""No key access, persistent condition episodes, independent crash detection and toast retry."""

import ctypes
import json
import socket
import sqlite3
import subprocess
import time
from contextlib import closing
from datetime import timedelta
from xml.etree.ElementTree import fromstring

import pytest
from test_private_sync import make_setup

from trading import private_operations, windows_notify
from trading.paper_runner import _process_lock, python_process_args
from trading.private_operations import OperationsError, PrivateOperations
from trading.stream_control import StreamControl


@pytest.fixture(autouse=True)
def no_credentials_or_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("real credentials/network attempted")

    monkeypatch.setattr(ctypes, "WinDLL", forbidden, raising=False)
    monkeypatch.setattr(socket, "socket", forbidden)


@pytest.fixture
def setup(tmp_path):
    clock, _, reads, backend, _, workspace = make_setup(tmp_path)
    monitor = PrivateOperations.create(
        workspace.directory, clock=lambda: clock.wall, monotonic=lambda: clock.mono
    )
    return clock, reads, backend, workspace, monitor


def begin(workspace):
    return workspace.control.begin(
        workspace.journal,
        expected_revision=workspace.control.snapshot()["revision"],
        expected_head=workspace.journal.head(),
    )


def stop(workspace):
    with workspace.control.ownership():
        state = begin(workspace)
        workspace.control.finish(state["owner"], workspace.journal, reason="stream_failed")


def alert_kinds(monitor):
    return [a["kind"] for a in monitor.alerts()]


def test_create_reopen_checks_are_local_and_do_not_change_frozen_stores(setup):
    _, _, backend, workspace, monitor = setup
    paths = [
        workspace.directory / "sync-plan.json",
        workspace.control.path,
        workspace.journal.path,
        workspace.book.path,
        workspace.catalog.path,
        workspace.plan.read_control_directory / "read-control.sqlite",
    ]
    before = [p.read_bytes() for p in paths]
    assert monitor.check()["conditions"] == []
    assert monitor.status()["complete"] is False and monitor.status()["live_enabled"] is False
    peer = PrivateOperations(workspace.directory)
    assert peer.status()["monitor_instance"] == monitor.status()["monitor_instance"]
    assert [p.read_bytes() for p in paths] == before and backend.reads == []
    with pytest.raises(FileExistsError):
        PrivateOperations.create(workspace.directory)


def test_stopped_condition_is_deduplicated_and_acknowledgement_never_clears_stop(setup):
    clock, _, backend, workspace, monitor = setup
    stop(workspace)
    state = workspace.control.snapshot()
    assert monitor.check()["conditions"] == ["private_sync_stopped"]
    for _ in range(3):
        clock.advance(60)
        assert monitor.check()["alert_count"] == 1
    monitor.acknowledge(1)
    assert monitor.alerts() == [] and monitor.check()["conditions"] == ["private_sync_stopped"]
    assert workspace.control.snapshot() == state and backend.reads == []
    with pytest.raises(OperationsError, match="not_pending"):
        monitor.acknowledge(1)


def test_owner_absence_comes_from_the_os_lock_not_the_lock_file(setup):
    _, _, _, workspace, monitor = setup
    with workspace.control.ownership():
        begin(workspace)
        assert monitor.check()["conditions"] == []
    assert workspace.control.lock_path.exists()
    assert monitor.check()["conditions"] == ["private_sync_owner_missing"]
    peer = PrivateOperations(workspace.directory, clock=monitor.clock)
    assert peer.check()["alert_count"] == 1


def test_stale_successes_are_detected_despite_retries_and_clear_only_on_success(setup):
    clock, _, _, workspace, monitor = setup
    with workspace.control.ownership():
        state = begin(workspace)
        # Observe progress independently of the runtime's durable timestamp clock.
        monitor.check()
        clock.advance(119)
        workspace.control.update(state["owner"], retry=True)
        assert monitor.check()["conditions"] == []
        clock.advance(1)
        assert monitor.check()["conditions"] == ["private_sync_stale"]
        workspace.control.update(state["owner"], retry=True)
        assert monitor.check()["alert_count"] == 1
        workspace.control.update(state["owner"], success=True)
        assert monitor.check()["conditions"] == []
    assert alert_kinds(monitor) == ["private_sync_stale", "private_condition_cleared"]
    assert monitor.alerts()[0]["resolved_at"] is not None


def test_new_generation_gets_a_startup_grace_and_ready_does_not_imply_stale(setup):
    clock, _, _, workspace, monitor = setup
    with workspace.control.ownership():
        state = begin(workspace)
        monitor.check()
        clock.advance(120)
        assert monitor.check()["conditions"] == ["private_sync_stale"]
        workspace.control.finish(state["owner"], workspace.journal)
    assert monitor.check()["conditions"] == []
    clock.advance(1000)
    assert monitor.check()["conditions"] == []
    with workspace.control.ownership():
        begin(workspace)
        assert monitor.check()["conditions"] == []


def test_in_flight_get_and_brief_unacknowledged_frame_are_not_normal_runtime_failures(setup):
    clock, reads, _, workspace, monitor = setup
    with workspace.control.ownership():
        begin(workspace)
        session = workspace.journal.start_session(
            expected_head=workspace.journal.head(), at=clock.wall, monotonic_ns=0
        )
        record = workspace.journal.record(session, "HEARTBEAT", at=clock.wall, monotonic_ns=0)
        with reads.slot():
            assert monitor.check()["conditions"] == []
        workspace.journal.acknowledge(session, record)
    assert set(monitor.check()["conditions"]) == {
        "private_sync_owner_missing",
        "private_journal_unresolved",
    }


def test_read_stop_is_reported_separately_without_loading_credentials(setup):
    _, reads, backend, workspace, monitor = setup
    reads.stop()
    before = workspace.control.snapshot()
    assert monitor.check()["conditions"] == ["private_reads_blocked"]
    assert workspace.control.snapshot() == before and backend.reads == []


def test_missing_runtime_keeps_previous_conditions_and_never_recreates_data(setup):
    _, _, _, workspace, monitor = setup
    stop(workspace)
    monitor.check()
    saved = (workspace.directory / "sync-plan.json").read_bytes()
    (workspace.directory / "sync-plan.json").unlink()
    assert set(monitor.check()["conditions"]) == {
        "private_sync_stopped",
        "private_sync_unavailable",
    }
    assert not (workspace.directory / "sync-plan.json").exists()
    (workspace.directory / "sync-plan.json").write_bytes(saved)
    assert monitor.check()["conditions"] == ["private_sync_stopped"]
    assert alert_kinds(monitor)[-1] == "private_condition_cleared"


def test_failed_delivery_is_durable_retried_and_preserves_the_same_toast_identity(setup):
    clock, _, _, workspace, monitor = setup
    stop(workspace)
    monitor.check()
    calls = []

    def send(alert, identity):
        calls.append((alert, identity))
        if len(calls) == 1:
            raise OSError("synthetic-secret-that-must-not-be-saved")

    assert monitor.notify(send=send)["failed"] == 1
    assert monitor.alerts()[0]["error"] == "notification_failed"
    assert monitor.notify(send=send)["submitted"] == 0
    clock.advance(300)
    assert monitor.notify(send=send)["submitted"] == 1
    assert monitor.notify(send=send)["submitted"] == 0
    assert calls[0] == calls[1] and calls[0][1] == workspace.control.snapshot()["instance"]
    assert monitor.alerts()[0]["acknowledged_at"] is None
    assert monitor.alerts()[0]["attempts"] == 2
    assert b"synthetic-secret" not in monitor.path.read_bytes()


def test_resolved_and_acknowledged_alerts_are_not_submitted_later(setup):
    _, _, _, workspace, monitor = setup
    stop(workspace)
    monitor.check()
    state = workspace.control.snapshot()
    workspace.recover(
        expected_plan_sha256=workspace.plan_sha256,
        expected_revision=state["revision"],
        expected_head=workspace.journal.head(),
        expected_reason=state["reason"],
    )
    monitor.check()
    calls = []
    monitor.notify(send=lambda alert, _: calls.append(alert))
    assert [a["kind"] for a in calls] == ["private_condition_cleared"]
    monitor.test_notification()
    monitor.acknowledge(3)
    assert monitor.notify(send=lambda *a: pytest.fail("acknowledged alert sent"))["submitted"] == 0


def test_interrupt_after_windows_submission_retries_the_same_id_after_restart(setup):
    clock, _, _, workspace, monitor = setup
    monitor.test_notification()
    calls = []

    def interrupt(alert, identity):
        calls.append((alert, identity))
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        monitor.notify(send=interrupt)
    peer = PrivateOperations(workspace.directory, clock=monitor.clock)
    assert peer.alerts()[0]["attempts"] == 1 and peer.alerts()[0]["submitted_at"] is None
    clock.advance(300)
    assert peer.notify(send=lambda a, i: calls.append((a, i)))["submitted"] == 1
    assert calls[0] == calls[1]


@pytest.mark.parametrize("changed", ["body", "digest", "count", "condition"])
def test_corrupted_outbox_refuses_without_touching_runtime(setup, changed):
    _, _, _, workspace, monitor = setup
    monitor.test_notification()
    before = workspace.control.snapshot()
    with closing(sqlite3.connect(monitor.path)) as conn:
        if changed == "body":
            conn.execute("UPDATE alerts SET body='{}'")
        elif changed == "digest":
            conn.execute("UPDATE alerts SET digest='bad'")
        elif changed == "count":
            conn.execute("DELETE FROM alerts")
        else:
            conn.execute("INSERT INTO conditions VALUES('untrusted')")
        conn.commit()
    with pytest.raises(OperationsError, match="integrity_failed"):
        monitor.check()
    assert workspace.control.snapshot() == before


@pytest.mark.parametrize("target", ["database", "lock"])
def test_missing_outbox_files_are_not_recreated(setup, target):
    _, _, _, _, monitor = setup
    path = monitor.path if target == "database" else monitor.lock_path
    path.unlink()
    with pytest.raises(OperationsError):
        monitor.check()
    assert not path.exists()


def test_backward_clock_refuses_without_clearing_conditions_or_delivery(setup):
    clock, _, _, workspace, monitor = setup
    stop(workspace)
    monitor.check()
    clock.advance(10)
    monitor.test_notification()
    clock.wall -= timedelta(seconds=1)
    before = monitor.path.read_bytes()
    with pytest.raises(OperationsError, match="clock_invalid"):
        monitor.acknowledge(1)
    assert monitor.path.read_bytes() == before


def test_outbox_is_bounded_and_live_notification_owner_excludes_mutation(setup, monkeypatch):
    _, _, _, _, monitor = setup
    with _process_lock(monitor.lock_path):
        with pytest.raises(OperationsError, match="busy"):
            monitor.test_notification()
    monkeypatch.setattr(private_operations, "MAX_ALERTS", 1)
    monitor.test_notification()
    with pytest.raises(OperationsError, match="capacity"):
        monitor.test_notification()
    assert monitor.status()["alert_count"] == 1


def test_native_toast_uses_private_fixed_labels_without_exposing_detail(setup, monkeypatch):
    _, _, _, _, monitor = setup
    monitor.test_notification()
    content, tag = windows_notify.toast_payload(
        {"id": 1, "kind": "private_sync_stopped", "detail": "secret"}, "a" * 32
    )
    text = " ".join(t.text for t in fromstring(content).iter("text"))
    assert "実口座同期が停止" in text and "private_operations" in text
    assert "secret" not in text and tag.startswith("p-")


def test_actual_process_death_is_detected_after_os_ownership_is_released(setup, tmp_path):
    _, _, _, workspace, monitor = setup
    ready = tmp_path / "ready"
    # Save RUNNING, then leave the real OS owner live until the test kills it.
    code = (
        "import time; from pathlib import Path; "
        "from trading.private_sync import PrivateSyncWorkspace; "
        "w=PrivateSyncWorkspace(sys.argv[1]); lease=w.control.ownership(); lease.__enter__(); "
        "w.control.begin(w.journal,expected_revision=0,expected_head=w.journal.head()); "
        "Path(sys.argv[2]).write_text('ready'); time.sleep(30)"
    )
    child = subprocess.Popen(python_process_args(code, workspace.directory, ready))
    try:
        deadline = time.monotonic() + 10
        while not ready.exists() and child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert ready.read_text() == "ready"
        assert monitor.check()["conditions"] == []
        child.kill()
        child.wait(timeout=10)
        assert monitor.check()["conditions"] == ["private_sync_owner_missing"]
        assert StreamControl(workspace.control.path.parent).snapshot()["phase"] == "RUNNING"
    finally:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=10)


def test_cli_is_local_and_does_not_echo_untrusted_arguments(setup, capsys):
    _, _, backend, workspace, monitor = setup
    assert private_operations.main(["status", "--directory", str(workspace.directory)]) == 0
    assert (
        json.loads(capsys.readouterr().out)["monitor_instance"]
        == monitor.status()["monitor_instance"]
    )
    with pytest.raises(SystemExit):
        private_operations.main(
            ["status", "--directory", str(workspace.directory), "--secret=do-not-print"]
        )
    output = capsys.readouterr()
    assert "do-not-print" not in output.err and backend.reads == []
