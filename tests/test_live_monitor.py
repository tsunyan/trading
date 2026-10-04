"""Independent local watchdog stops with original bindings and real OS ownership."""

import base64
import ctypes
import json
import os
import socket
import sqlite3
import subprocess
import time
from xml.etree.ElementTree import fromstring

import pytest
from test_account_guard import account, fill, policy, quote
from test_live_order_catalog import accepted
from test_live_order_catalog import setup as catalog_setup
from test_private_operations import begin, stop
from test_private_order import client, ready
from test_private_sync import make_setup

from trading import private_operations, windows_notify
from trading.account_guard import Position
from trading.execution_lab import fixture_evidence
from trading.live_journal import LiveOrderError, LiveOrderJournal
from trading.live_monitor_target import LiveMonitorBinding, LiveMonitorTarget
from trading.paper_runner import python_process_args
from trading.post_control import PersistentPostLimiter
from trading.private_operations import OperationsError, PrivateOperations, _body, _hash, _json
from trading.private_sync import PrivateSyncWorkspace
from trading.stream_control import StreamControlError


@pytest.fixture(autouse=True)
def no_native_or_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("real credentials/network attempted")

    monkeypatch.setattr(ctypes, "WinDLL", forbidden, raising=False)
    monkeypatch.setattr(socket, "socket", forbidden)


@pytest.fixture
def setup(tmp_path):
    values, live = catalog_setup.__wrapped__(tmp_path)
    clock, _, _, backend, _, workspace = values
    monitor = PrivateOperations.create(
        workspace.directory, clock=lambda: clock.wall, monotonic=lambda: clock.mono
    )
    return values, live, monitor, backend


def watchdog(monitor):
    return monitor.watchdog(send=lambda *a: None)


def monitor_events(journal):
    with sqlite3.connect(journal.path) as conn:
        return [
            json.loads(row[0])
            for row in conn.execute(
                "SELECT payload_json FROM events WHERE kind='LIVE_MONITOR_STOPPED'"
            )
        ]


def test_diagnostics_are_local_metadata_only_and_do_not_change_bound_live_stores(setup):
    values, live, monitor, backend = setup
    ready(live)
    workspace = values[5]
    paths = [live[3].path, live[2].path, live[1].path, workspace.control.path]
    before = [p.read_bytes() for p in paths]
    assert monitor.check()["conditions"] == []
    view = workspace.status()["live"]
    assert view["status"]["approval_valid"]
    assert not view["status"]["complete"] and not view["status"]["live_enabled"]
    assert set(view["status"]) == {
        "instance",
        "revision",
        "phase",
        "halted",
        "entry_halted",
        "approval_valid",
        "complete",
        "live_enabled",
    }
    assert monitor.status()["live_binding"] == view["binding"]
    assert [p.read_bytes() for p in paths] == before and backend.reads == []


def test_healthy_ready_and_running_watchdogs_do_not_stop(setup):
    values, live, monitor, _ = setup
    ready(live)
    before = live[3].snapshot()
    assert watchdog(monitor)["live_protection"]["status"] == "unnecessary"
    with values[5].control.ownership():
        begin(values[5])
        assert watchdog(monitor)["conditions"] == []
    assert live[3].snapshot() == before and monitor_events(live[3]) == []


def test_check_only_reports_failure_but_watchdog_stops_once_and_preserves_history(setup):
    values, live, monitor, _ = setup
    accepted((values, live))
    journal, posts = live[3], live[2]
    before, post_before = journal.snapshot(), posts.snapshot()
    stop(values[5])
    assert monitor.check()["conditions"] == ["private_sync_stopped"]
    assert journal.snapshot() == before
    first = watchdog(monitor)
    assert first["live_protection"] == {"status": "stopped", "changed": True}
    after = journal.snapshot()
    assert after["halted"] and after["live_control"]["phase"] == "STOPPED"
    for key in ("orders", "account_guard", "fills"):
        if key in before:
            assert after[key] == before[key]
    assert after["live_control"]["approval"] == before["live_control"]["approval"]
    assert posts.snapshot() == post_before
    assert monitor_events(journal) == [
        {"monitor_instance": first["monitor_instance"], "reasons": ["private_sync_stopped"]}
    ]
    assert watchdog(monitor)["live_protection"]["status"] == "already_stopped"
    assert journal.snapshot() == after and len(monitor_events(journal)) == 1
    for alert in monitor.alerts():
        monitor.acknowledge(alert["id"])
    assert journal.snapshot() == after


@pytest.mark.parametrize("cause", ["reads", "posts", "journal"])
def test_read_post_and_unresolved_receive_failures_stop_the_original_live_journal(setup, cause):
    values, live, monitor, _ = setup
    ready(live)
    if cause == "reads":
        live[1].stop()
        reason = "private_reads_blocked"
    elif cause == "posts":
        live[2].stop()
        reason = "private_posts_stopped"
    else:
        session = values[5].journal.start_session(
            expected_head=values[5].journal.head(), at=values[0].wall, monotonic_ns=0
        )
        values[5].journal.record(session, "HEARTBEAT", at=values[0].wall, monotonic_ns=0)
        reason = "private_journal_unresolved"
    post_before = live[2].snapshot()
    result = watchdog(monitor)
    assert result["live_protection"]["status"] == "stopped"
    assert reason in result["conditions"]
    assert reason in monitor_events(live[3])[0]["reasons"]
    assert live[2].snapshot() == post_before


def test_stale_running_owner_gets_grace_but_retries_do_not_prevent_stop(setup):
    values, live, monitor, _ = setup
    ready(live)
    with values[5].control.ownership():
        state = begin(values[5])
        assert watchdog(monitor)["conditions"] == []
        values[0].advance(119)
        values[5].control.update(state["owner"], retry=True)
        assert watchdog(monitor)["conditions"] == []
        values[0].advance(1)
        result = watchdog(monitor)
    assert result["live_protection"]["status"] == "stopped"
    assert monitor_events(live[3])[0]["reasons"] == ["private_sync_stale"]


def test_success_between_sampling_and_stop_defers_the_stop(setup, monkeypatch):
    values, live, monitor, _ = setup
    ready(live)
    workspace = values[5]
    original = monitor._observe
    with workspace.control.ownership():
        begin(workspace)
        monitor.check()
        values[0].advance(120)
        calls = []

        def observe(state):
            view = original(state)
            calls.append(view)
            if len(calls) == 1:
                workspace.control.update(workspace.control.snapshot()["owner"], success=True)
            return view

        monkeypatch.setattr(monitor, "_observe", observe)
        result = watchdog(monitor)
        assert result["live_protection"]["status"] == "checkpoint_changed"
        assert watchdog(monitor)["conditions"] == []
    assert not live[3].snapshot()["halted"] and monitor_events(live[3]) == []


@pytest.mark.parametrize("damage", ["manifest", "control", "catalog", "journal"])
def test_cached_target_can_stop_when_original_sync_workspace_is_unavailable(setup, damage):
    values, live, monitor, _ = setup
    ready(live)
    workspace = values[5]
    target = {
        "manifest": workspace.directory / "sync-plan.json",
        "control": workspace.control.path,
        "catalog": workspace.catalog.path,
        "journal": workspace.journal.path,
    }[damage]
    target.unlink()
    result = watchdog(monitor)
    assert result["live_protection"]["status"] == "stopped"
    assert {"private_sync_unavailable", "private_live_unavailable", "private_live_stopped"} <= set(
        result["conditions"]
    )
    assert not target.exists()
    assert live[3].snapshot()["halted"]


def test_unavailable_view_that_recovers_before_stop_is_rechecked(setup, monkeypatch):
    values, live, monitor, _ = setup
    ready(live)
    original = monitor._observe
    calls = []

    def observe(state):
        calls.append(1)
        return (None, None) if len(calls) == 1 else original(state)

    monkeypatch.setattr(monitor, "_observe", observe)
    result = watchdog(monitor)
    assert result["live_protection"]["status"] == "checkpoint_changed"
    assert not live[3].snapshot()["halted"]
    assert watchdog(monitor)["conditions"] == []


@pytest.mark.parametrize("store", ["reads", "posts", "live"])
def test_damaged_bound_store_reports_stop_failure_without_recreating_it(setup, store):
    values, live, monitor, _ = setup
    ready(live)
    paths = {"reads": live[1].path, "posts": live[2].path, "live": live[3].path}
    paths[store].unlink()
    before = {name: path.read_bytes() for name, path in paths.items() if path.exists()}
    result = watchdog(monitor)
    assert result["live_protection"]["status"] == "failed"
    assert "private_live_stop_failed" in result["conditions"]
    assert not paths[store].exists()
    assert {name: paths[name].read_bytes() for name in before} == before


@pytest.mark.parametrize("change", ["binding_missing", "binding_changed"])
def test_watchdog_refuses_to_follow_a_different_live_target(setup, monkeypatch, change):
    values, live, monitor, _ = setup
    ready(live)
    original = PrivateSyncWorkspace.status

    def status(workspace):
        view = original(workspace)
        if change == "binding_missing":
            view["live"] = None
        else:
            view["live"]["binding"]["live_instance"] = "f" * 32
        return view

    monkeypatch.setattr(PrivateSyncWorkspace, "status", status)
    binding = monitor.status()["live_binding"]
    result = watchdog(monitor)
    assert result["live_protection"]["status"] == "stopped"
    assert monitor.status()["live_binding"] == binding
    assert live[3].snapshot()["halted"]


@pytest.mark.parametrize("reason", ["expired", "code_changed", "entry_halted"])
def test_approval_and_loss_alerts_preserve_general_live_permission_and_loss_stop(
    setup, monkeypatch, reason
):
    values, live, monitor, _ = setup
    if reason == "entry_halted":
        opened = accepted((values, live))
        journal, clock = live[3], live[0]
        journal.reconcile(
            fixture_evidence(
                opened, 101, 201, "EXECUTED", [fill(timestamp=clock.now.isoformat())], clock.now
            )
        )
        clock.advance(1)
        journal.update_account(
            account(
                clock.now,
                balance="999997",
                equity="969987",
                required_margin="6000.4",
                available_margin="963986.6",
                positions=(
                    Position(position_id=401, side="BUY", units=1000, average_price="150.01"),
                ),
            ),
            quote(clock.now, bid="120", ask="120.01"),
            now=clock.now,
        )
        expected = "private_live_entry_halted"
    else:
        ready(live)
        if reason == "expired":
            values[0].advance(3600)
        else:
            monkeypatch.setattr(LiveOrderJournal, "_current_implementation", lambda self: "f" * 64)
        expected = "private_live_approval_invalid"
    before, posts = live[3].snapshot(), live[2].snapshot()
    result = watchdog(monitor)
    assert result["conditions"] == [expected]
    assert result["live_protection"]["status"] == "unnecessary"
    assert live[3].snapshot() == before and live[2].snapshot() == posts


@pytest.mark.parametrize("kind", ["sync", "post"])
def test_actual_worker_death_stops_live_and_preserves_claim(setup, tmp_path, kind):
    values, live, monitor, _ = setup
    ready(live)
    marker = tmp_path / "owner-ready"
    if kind == "sync":
        code = (
            "import time; from pathlib import Path; "
            "from trading.private_sync import PrivateSyncWorkspace; "
            "w=PrivateSyncWorkspace(sys.argv[1]); lease=w.control.ownership(); lease.__enter__(); "
            "w.control.begin(w.journal,expected_revision=0,expected_head=w.journal.head()); "
            "Path(sys.argv[2]).write_text('ready'); time.sleep(30)"
        )
        args = (values[5].directory, marker)
        reason = "private_sync_owner_missing"
    else:
        code = (
            "import time; from pathlib import Path; "
            "from trading.read_control import PersistentReadLimiter; "
            "from trading.post_control import PersistentPostLimiter; "
            "r=PersistentReadLimiter(sys.argv[1],'synthetic'); "
            "p=PersistentPostLimiter(sys.argv[2],r); "
            "lease=p.operation('order',request_sha256='a'*64); lease.__enter__(); "
            "Path(sys.argv[3]).write_text('ready'); time.sleep(30)"
        )
        args = (live[1].path.parent, live[2].path.parent, marker)
        reason = "private_posts_owner_missing"
    child = subprocess.Popen(python_process_args(code, *args))
    try:
        deadline = time.monotonic() + 10
        while not marker.exists() and child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert marker.read_text() == "ready"
        assert watchdog(monitor)["conditions"] == []
        child.kill()
        child.wait(timeout=10)
        post_before = live[2].snapshot()
        orders = live[3].snapshot()["orders"]
        result = watchdog(monitor)
        assert result["live_protection"]["status"] == "stopped"
        assert reason in result["conditions"]
        assert live[3].snapshot()["halted"] and live[3].snapshot()["orders"] == orders
        assert live[2].snapshot() == post_before
        assert (post_before["claim"] is not None) == (kind == "post")
        assert values[5].status()["control"]["phase"] == ("RUNNING" if kind == "sync" else "READY")
    finally:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=10)


@pytest.mark.parametrize("owner", ["sync", "post"])
def test_owner_reacquisition_at_final_fence_defers_stop(setup, monkeypatch, owner):
    values, live, monitor, _ = setup
    ready(live)
    workspace = values[5]
    reason = "private_sync_owner_missing" if owner == "sync" else "private_posts_owner_missing"
    if owner == "sync":
        with workspace.control.ownership():
            begin(workspace)
    else:
        # Only the final fence is injected; report a stable orphan view separately.
        pass
    view = workspace.status()
    if owner == "post":
        view["posts"]["phase"] = "IN_FLIGHT"
        view["post_owner_present"] = False
    monkeypatch.setattr(monitor, "_observe", lambda state: (view, False))
    original = LiveMonitorTarget.__init__
    leases = []

    def init(target, *args, **kwargs):
        original(target, *args, **kwargs)
        if owner == "sync":
            lease = workspace.control.ownership()
        else:
            lease = live[2]._ownership()
        lease.__enter__()
        leases.append(lease)

    monkeypatch.setattr(LiveMonitorTarget, "__init__", init)
    try:
        result = watchdog(monitor)
        assert reason in result["conditions"]
        assert result["live_protection"]["status"] == "checkpoint_changed"
        assert not live[3].snapshot()["halted"]
    finally:
        for lease in leases:
            lease.__exit__(None, None, None)


def test_stop_commits_even_when_notification_capacity_is_exhausted(setup, monkeypatch):
    values, live, monitor, _ = setup
    ready(live)
    monitor.test_notification()
    monkeypatch.setattr(private_operations, "MAX_ALERTS", 1)
    stop(values[5])
    result = watchdog(monitor)
    assert result["check_failed"]
    assert live[3].snapshot()["halted"] and len(monitor_events(live[3])) == 1
    # Delivery can drain the old outbox; no claim or stop is released.
    assert result["notifications"]["submitted"] == 1
    monkeypatch.setattr(private_operations, "MAX_ALERTS", 10000)
    assert watchdog(monitor)["live_protection"]["status"] == "already_stopped"
    assert len(monitor_events(live[3])) == 1


def test_binding_added_after_monitor_creation_is_saved_before_alert_capacity_failure(
    tmp_path, monkeypatch
):
    clock, _, reads, _, _, workspace = make_setup(tmp_path)
    monitor = PrivateOperations.create(
        workspace.directory, clock=lambda: clock.wall, monotonic=lambda: clock.mono
    )
    monitor.test_notification()
    _, live = catalog_setup.__wrapped__(tmp_path / "other")
    posts = PersistentPostLimiter.create(
        tmp_path / "posts", reads, wall_ns=lambda: int(clock.wall.timestamp() * 1e9)
    )
    journal = LiveOrderJournal.create(
        tmp_path / "live",
        posts,
        live[3].limits,
        policy(),
        clock=lambda: clock.wall,
    )
    stop(workspace)
    monkeypatch.setattr(private_operations, "MAX_ALERTS", 1)
    with pytest.raises(OperationsError, match="operations_alert_capacity"):
        monitor.check()
    reopened = PrivateOperations(
        workspace.directory, clock=monitor.clock, monotonic=monitor.monotonic
    )
    assert (
        reopened.status()["live_binding"]["live_instance"]
        == journal.monitoring_status()["instance"]
    )
    assert not journal.snapshot()["halted"]


def test_interrupt_after_live_stop_before_alert_commit_recovers_idempotently(setup, monkeypatch):
    values, live, monitor, _ = setup
    ready(live)
    stop(values[5])
    original = monitor._alert

    def interrupt(*args, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(monitor, "_alert", interrupt)
    with pytest.raises(KeyboardInterrupt):
        watchdog(monitor)
    assert monitor.status()["alert_count"] == 0
    assert live[3].snapshot()["halted"]
    monkeypatch.setattr(monitor, "_alert", original)
    result = watchdog(monitor)
    assert result["live_protection"]["status"] == "already_stopped"
    assert len(monitor_events(live[3])) == 1


def test_real_monitor_exit_after_live_stop_retains_stop_and_releases_os_ownership(setup):
    values, live, monitor, _ = setup
    ready(live)
    stop(values[5])
    code = """
import os
from datetime import datetime
from trading.private_operations import PrivateOperations
m = PrivateOperations(sys.argv[1], clock=lambda: datetime.fromisoformat(sys.argv[2]))
m._alert = lambda *args, **kwargs: os._exit(23)
m.check(protect_live=True)
"""
    result = subprocess.run(
        python_process_args(code, values[5].directory, values[0].wall.isoformat()), timeout=20
    )
    assert result.returncode == 23
    assert monitor.status()["alert_count"] == 0
    assert live[3].snapshot()["halted"] and len(monitor_events(live[3])) == 1
    assert watchdog(monitor)["live_protection"]["status"] == "already_stopped"
    assert len(monitor_events(live[3])) == 1


def test_monitor_stop_storage_failure_is_reported_and_retry_preserves_history(setup):
    values, live, monitor, _ = setup
    ready(live)
    stop(values[5])
    with sqlite3.connect(live[3].path) as conn:
        conn.execute(
            "CREATE TRIGGER fail_stop BEFORE UPDATE ON live_control "
            "BEGIN SELECT RAISE(ABORT,'synthetic'); END"
        )
    before = live[3].snapshot()
    result = watchdog(monitor)
    assert result["live_protection"]["status"] == "failed"
    assert "private_live_stop_failed" in result["conditions"]
    assert live[3].snapshot() == before and monitor_events(live[3]) == []
    with sqlite3.connect(live[3].path) as conn:
        conn.execute("DROP TRIGGER fail_stop")
    result = watchdog(monitor)
    assert result["live_protection"]["status"] == "stopped"
    assert "private_live_stop_failed" not in result["conditions"]
    assert live[3].snapshot()["orders"] == before["orders"]


def test_existing_order_client_refuses_dispatch_after_watchdog_stop(setup):
    values, live, monitor, _ = setup
    order = ready(live)
    calls = []
    with client(live, lambda request: calls.append(request)) as sender:
        stop(values[5])
        assert watchdog(monitor)["live_protection"]["status"] == "stopped"
        with pytest.raises(ValueError):
            sender.submit(order.client_id, quote=quote(live[0].now))
    assert calls == [] and live[3].snapshot()["halted"]


def test_long_binding_record_is_readable_and_size_limit_is_checked_before_writes(setup):
    _, _, monitor, _ = setup
    with monitor._store(write=True) as conn:
        state = monitor._verify(conn)[0]
        data = state.live_binding.model_dump()
        # Canonical absolute paths without creating or opening any storage.
        long_path = str((monitor.directory / ("abc/" * 300) / "target").resolve())
        for key in ("read_directory", "post_directory", "live_directory"):
            data[key] = long_path
        updated = state.model_copy(update={"live_binding": LiveMonitorBinding(**data)})
        assert len(_body(updated).encode()) > 4096
        monitor._write(conn, updated)
        conn.commit()
    assert monitor.status()["live_binding"] == data
    before = monitor.path.read_bytes()
    with monitor._store(write=True) as conn:
        data = updated.live_binding.model_dump()
        unicode_path = str((monitor.directory / ("漢字/" * 580) / "target").resolve())
        for key in ("read_directory", "post_directory", "live_directory"):
            data[key] = unicode_path
        oversized = updated.model_copy(update={"live_binding": LiveMonitorBinding(**data)})
        with pytest.raises(OperationsError, match="operations_monitor_capacity"):
            monitor._write(conn, oversized)
    assert monitor.path.read_bytes() == before


@pytest.mark.parametrize("reasons", [[], "private_sync_stopped", {"secret-message"}, {123}, [[]]])
def test_journal_monitor_stop_rejects_arbitrary_reasons_without_mutation(setup, reasons):
    _, live, monitor, _ = setup
    before = live[3].path.read_bytes()
    with pytest.raises(LiveOrderError, match="invalid_live_monitor_stop"):
        live[3].halt_for_monitor(monitor.status()["monitor_instance"], reasons)
    assert live[3].path.read_bytes() == before


def test_monitor_verifies_every_condition_and_original_sync_instance(setup):
    _, _, monitor, _ = setup
    with sqlite3.connect(monitor.path) as conn:
        conn.executemany(
            "INSERT INTO conditions VALUES(?)",
            [(kind,) for kind in sorted(private_operations.CONDITIONS)],
        )
        conn.commit()
    assert set(monitor.status()["conditions"]) == private_operations.CONDITIONS
    with sqlite3.connect(monitor.path) as conn:
        state = json.loads(conn.execute("SELECT body FROM monitor").fetchone()[0])
        state["live_binding"]["sync_instance"] = "f" * 32
        body = _json(state)
        conn.execute("UPDATE monitor SET body=?,digest=?", (body, _hash(body)))
        conn.commit()
    with pytest.raises(OperationsError, match="integrity_failed"):
        monitor.status()


@pytest.mark.parametrize(
    "kind",
    sorted(
        private_operations.CONDITIONS
        - {
            "private_sync_stopped",
            "private_sync_owner_missing",
            "private_sync_stale",
            "private_reads_blocked",
            "private_cash_halted",
            "private_journal_unresolved",
            "private_sync_unavailable",
        }
    ),
)
def test_windows_toast_uses_fixed_live_labels_and_excludes_payload(kind):
    alert = {"id": 7, "kind": kind, "detail": "secret-key-and-balance"}
    content, tag = windows_notify.toast_payload(alert, "a" * 32)
    text = " ".join(t.text for t in fromstring(content).iter("text"))
    assert windows_notify.LABELS[kind] in text and "private_operations" in text
    assert "secret-key-and-balance" not in text
    assert tag == "p-aaaaaa-7"


@pytest.mark.skipif(os.name != "nt", reason="Windows notification boundary")
def test_windows_toast_submission_passes_the_payload_to_hidden_powershell(monkeypatch):
    captured = []

    def submit(argv, **kwargs):
        captured.append(kwargs["env"])
        assert "-WindowStyle" in argv and "Hidden" in argv
        return subprocess.CompletedProcess(argv, 0, stdout=b"submitted")

    monkeypatch.setattr(windows_notify.subprocess, "run", submit)
    alert = {"id": 7, "kind": "private_live_stopped", "detail": "secret"}
    windows_notify.send_toast(alert, "a" * 32)
    content, tag = windows_notify.toast_payload(alert, "a" * 32)
    assert base64.b64decode(captured[0]["TRADINGLAB_TOAST_XML"]) == content
    assert captured[0]["TRADINGLAB_TOAST_TAG"] == tag


def test_windows_toast_submission_refuses_other_platforms(monkeypatch):
    monkeypatch.setattr(windows_notify.os, "name", "posix")
    with pytest.raises(OSError, match="require Windows"):
        windows_notify.send_toast({"id": 1, "kind": "private_live_stopped"}, "a" * 32)


def test_sync_free_owner_validation_stays_inside_held_os_lease(setup, monkeypatch):
    values, _, monitor, _ = setup
    original = PrivateSyncWorkspace.status
    sampled = []

    def status(workspace):
        result = original(workspace)
        with pytest.raises(StreamControlError, match="stream_owner_busy"):
            with values[5].control.ownership():
                pytest.fail("free owner escaped the sampling guard")
        sampled.append(1)
        return result

    monkeypatch.setattr(PrivateSyncWorkspace, "status", status)
    assert monitor.check()["conditions"] == []
    assert sampled == [1]


def test_busy_sync_owner_disappearing_during_sample_is_reprobed(setup, monkeypatch):
    values, _, monitor, _ = setup
    workspace = values[5]
    lease = workspace.control.ownership()
    lease.__enter__()
    begin(workspace)
    original = PrivateSyncWorkspace.status
    sampled = []

    def status(workspace):
        result = original(workspace)
        sampled.append(1)
        if len(sampled) == 1:
            lease.__exit__(None, None, None)
        return result

    monkeypatch.setattr(PrivateSyncWorkspace, "status", status)
    assert monitor.check()["conditions"] == ["private_sync_owner_missing"]
    assert sampled == [1, 1]
