"""Registered dispatch prerequisites with original stores and synthetic broker boundaries."""

import ctypes
import json
import socket
import sqlite3
import subprocess
import time
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from test_account_guard import account, fill, intent, quote
from test_cancel_authorization import permit
from test_live_order_catalog import setup as catalog_setup
from test_private_cancel import envelope
from test_private_operations import begin
from test_private_order import client, ready, response

from trading import private_operations, private_order_operations
from trading.account_guard import Position
from trading.broker_contracts import Settlement
from trading.execution_lab import fixture_evidence
from trading.live_journal import LiveOrderError, LiveOrderJournal
from trading.paper_runner import python_process_args
from trading.private_operations import OperationsError, PrivateOperations, _hash, _json


@pytest.fixture(autouse=True)
def no_native_or_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("real credentials/network attempted")

    monkeypatch.setattr(ctypes, "WinDLL", forbidden, raising=False)
    monkeypatch.setattr(socket, "socket", forbidden)


@pytest.fixture
def unbound(tmp_path):
    values, live = catalog_setup.__wrapped__(tmp_path)
    clock, _, _, _, _, workspace = values
    clock.wall = datetime.fromtimestamp(
        workspace.control.snapshot()["wall_ns"] / 1e9, UTC
    ) + timedelta(seconds=1)
    workspace.control._wall = lambda: int(clock.wall.timestamp() * 1e9)
    monitor = PrivateOperations.create(
        workspace.directory, clock=lambda: clock.wall, monotonic=lambda: clock.mono
    )
    return values, live, monitor


def bind(unbound, **updates):
    values, live, monitor = unbound
    return live[3].bind_operations(
        values[5].directory,
        **{
            "expected_revision": live[3].snapshot()["live_control"]["revision"],
            "expected_plan_sha256": values[5].plan_sha256,
            "expected_monitor_instance": monitor.status()["monitor_instance"],
            "max_sync_age_seconds": 30,
            "max_watchdog_age_seconds": 40,
            "operations_confirmed": True,
            **updates,
        },
    )


def watchdog(monitor):
    return monitor.watchdog(send=lambda *a: None)


@pytest.fixture
def setup(unbound):
    values, live, monitor = unbound
    bind(unbound)
    order = ready(live)
    lease = values[5].control.ownership()
    lease.__enter__()
    leases = [lease]
    begin(values[5])
    watchdog(monitor)
    values[0].advance(1)
    values[5].control.update(values[5].control.snapshot()["owner"], success=True)
    watchdog(monitor)
    try:
        yield values, live, monitor, order, leases
    finally:
        for lease in leases:
            lease.__exit__(None, None, None)


def release(setup):
    for lease in setup[4]:
        lease.__exit__(None, None, None)
    setup[4].clear()


def test_registration_changes_acceptance_fingerprint_and_is_permanent_and_local(unbound):
    values, live, monitor = unbound
    journal = live[3]
    before, posts = journal.snapshot(), live[2].snapshot()
    frozen = [
        (values[5].directory / "sync-plan.json").read_bytes(),
        live[1].path.read_bytes(),
        monitor.path.read_bytes(),
    ]
    context = bind(unbound)
    after = journal.snapshot()["live_control"]
    assert after["phase"] == "DISABLED" and after["approval"] is None
    assert context["configuration_sha256"] != before["live_control"]["configuration_sha256"]
    assert context["revision"] == before["live_control"]["revision"] + 1
    assert after["operations"]["sync_instance"] == values[5].control.snapshot()["instance"]
    assert after["operations"]["monitor_instance"] == monitor.status()["monitor_instance"]
    assert live[2].snapshot() == posts and values[3].reads == []
    assert frozen == [
        (values[5].directory / "sync-plan.json").read_bytes(),
        live[1].path.read_bytes(),
        monitor.path.read_bytes(),
    ]
    reopened = LiveOrderJournal(journal.path.parent, live[2], clock=lambda: values[0].wall)
    assert reopened.snapshot()["live_control"] == after
    with pytest.raises(LiveOrderError, match="registration_refused"):
        bind(unbound)
    assert reopened.snapshot()["live_control"] == after


@pytest.mark.parametrize(
    "update",
    [
        {"operations_confirmed": False},
        {"expected_revision": 1},
        {"expected_plan_sha256": "f" * 64},
        {"expected_monitor_instance": "f" * 32},
        {"max_sync_age_seconds": 121},
        {"max_watchdog_age_seconds": 121},
        {"max_sync_age_seconds": 0},
        {"max_watchdog_age_seconds": True},
    ],
)
def test_invalid_registration_does_not_mutate_any_store(unbound, update):
    values, live, monitor = unbound
    paths = (live[3].path, live[2].path, live[1].path, monitor.path, values[5].control.path)
    before = [p.read_bytes() for p in paths]
    with pytest.raises(ValueError):
        bind(unbound, **update)
    assert [p.read_bytes() for p in paths] == before


def test_already_enabled_journal_cannot_add_or_change_dispatch_prerequisites(unbound):
    _, live, _ = unbound
    ready(live)
    before = live[3].snapshot()
    with pytest.raises(LiveOrderError, match="registration_refused"):
        bind(unbound)
    assert live[3].snapshot() == before


def test_healthy_original_sync_and_watchdog_allow_one_signed_post(setup):
    values, live, monitor, order, _ = setup
    checkpoint = monitor.status()["watchdog_checkpoint"]
    assert checkpoint["sync_successes"] > checkpoint["generation_start_successes"]
    calls = []

    def handler(request):
        calls.append(request)
        return response(live[0], request)

    with client(live, handler) as sender:
        sender.submit(order.client_id, quote=quote(live[0].now))
    assert len(calls) == 1 and calls[0].url.path == "/private/v1/order"
    assert live[3].snapshot()["orders"][0]["state"] == "RECONCILING"
    assert values[3].reads == []
    assert monitor.status()["watchdog_checkpoint"] == checkpoint


@pytest.mark.parametrize("source", ["check_only", "watchdog_before_success", "check_after_success"])
def test_diagnostic_check_and_pre_success_watchdog_cannot_grant_dispatch(unbound, source):
    values, live, monitor = unbound
    bind(unbound)
    order = ready(live)
    with values[5].control.ownership():
        begin(values[5])
        if source == "check_only":
            monitor.check()
        else:
            watchdog(monitor)
        values[0].advance(1)
        values[5].control.update(values[5].control.snapshot()["owner"], success=True)
        if source == "check_after_success":
            monitor.check()
        before = live[3].snapshot()
        with pytest.raises(LiveOrderError, match="live_(watchdog|sync)_unhealthy"):
            live[3].request(order.client_id)
        assert live[3].snapshot() == before


@pytest.mark.parametrize(
    "cause",
    [
        "sync_stale",
        "watchdog_stale",
        "watchdog_future",
        "ready",
        "stopped",
        "owner_missing",
        "generation_changed",
    ],
)
def test_unhealthy_sync_or_watchdog_refuses_preflight_without_consuming_claim(setup, cause):
    values, live, monitor, order, _ = setup
    workspace = values[5]
    if cause == "sync_stale":
        values[0].advance(31)
    elif cause == "watchdog_stale":
        values[0].advance(41)
        workspace.control.update(workspace.control.snapshot()["owner"], success=True)
    elif cause == "watchdog_future":
        values[0].wall -= timedelta(seconds=1)
    elif cause in {"ready", "stopped", "generation_changed"}:
        current = workspace.control.snapshot()
        workspace.control.finish(
            current["owner"],
            workspace.journal,
            reason="closed" if cause != "stopped" else "stream_failed",
        )
        if cause == "generation_changed":
            begin(workspace)
            values[0].advance(1)
            workspace.control.update(workspace.control.snapshot()["owner"], success=True)
    else:
        release(setup)
    before, post_before = live[3].snapshot(), live[2].snapshot()
    with client(live, lambda request: pytest.fail("unhealthy preflight sent HTTP")) as sender:
        with pytest.raises(ValueError, match="order_preflight_refused"):
            sender.submit(order.client_id, quote=quote(live[0].now))
    assert live[3].snapshot() == before and live[2].snapshot() == post_before
    assert not before["halted"]


@pytest.mark.parametrize(
    "damage", ["manifest", "control", "catalog", "journal", "cash", "monitor", "monitor_lock"]
)
def test_missing_original_prerequisite_store_is_refused_and_never_recreated(setup, damage):
    values, live, monitor, order, _ = setup
    workspace = values[5]
    path = {
        "manifest": workspace.directory / "sync-plan.json",
        "control": workspace.control.path,
        "catalog": workspace.catalog.path,
        "journal": workspace.journal.path,
        "cash": workspace.book.path,
        "monitor": monitor.path,
        "monitor_lock": monitor.lock_path,
    }[damage]
    before = live[3].path.read_bytes()
    path.unlink()
    with pytest.raises(LiveOrderError, match="live_operations_unavailable"):
        live[3].request(order.client_id)
    assert not path.exists() and live[3].path.read_bytes() == before


def test_sync_dying_during_health_sample_is_refused_by_second_owner_probe(setup, monkeypatch):
    values, live, _, order, _ = setup
    from trading.execution_cash_book import ExecutionCashBook

    original = ExecutionCashBook.snapshot

    def snapshot(book):
        result = original(book)
        release(setup)
        return result

    monkeypatch.setattr(ExecutionCashBook, "snapshot", snapshot)
    with pytest.raises(LiveOrderError, match="live_sync_owner_missing"):
        live[3].request(order.client_id)


@pytest.mark.parametrize("boundary", ["post_wait", "final_dispatch"])
def test_health_is_rechecked_after_wait_and_after_submission_claim(setup, monkeypatch, boundary):
    _, live, monitor, order, _ = setup
    calls = []
    if boundary == "post_wait":
        sleep = live[2]._sleep

        def wait(seconds):
            sleep(seconds)
            release(setup)

        monkeypatch.setattr(live[2], "_sleep", wait)
    else:
        original = live[3].begin_submission

        def begin(*args, **kwargs):
            plan = original(*args, **kwargs)
            release(setup)
            return plan

        monkeypatch.setattr(live[3], "begin_submission", begin)
    with client(live, lambda request: calls.append(request)) as sender:
        with pytest.raises(ValueError) as raised:
            sender.submit(order.client_id, quote=quote(live[0].now))
    assert calls == []
    post = live[2].snapshot()
    journal = live[3].snapshot()
    assert post["phase"] == "READY" and post["claim"] is None
    assert not journal["halted"]
    if boundary == "post_wait":
        assert journal["orders"][0]["state"] == "PREPARED"
    else:
        # Claimed, then refused before the HTTP send: provably not sent, so the claim is
        # closed instead of becoming an unresolvable unknown outcome.
        assert str(raised.value) == "order_not_sent:live_sync_owner_missing"
        assert journal["orders"][0]["state"] == "ABANDONED"
        refused = [e for e in journal["events"] if e["kind"] == "SUBMISSION_NOT_SENT"]
        assert [e["payload"] for e in refused] == [{"reason": "live_sync_owner_missing"}]
        assert live[3].catalog_orders()  # The catalog accepts the recorded refusal.
        # The client ID is never prepared or sent again.
        assert live[3].prepare(order) == "ABANDONED"
        assert live[3].snapshot()["orders"][0]["state"] == "ABANDONED"
    assert monitor.status()["conditions"] == []


def test_watchdog_refresh_is_required_after_new_sync_generation(setup):
    values, live, monitor, order, _ = setup
    workspace = values[5]
    workspace.control.finish(
        workspace.control.snapshot()["owner"], workspace.journal, reason="closed"
    )
    begin(workspace)
    watchdog(monitor)
    values[0].advance(1)
    workspace.control.update(workspace.control.snapshot()["owner"], success=True)
    with pytest.raises(LiveOrderError, match="live_sync_unhealthy"):
        live[3].request(order.client_id)
    watchdog(monitor)
    assert live[3].request(order.client_id).path == "/v1/order"


@pytest.mark.parametrize("healthy", [True, False])
def test_loss_halted_closing_order_uses_the_same_mandatory_operations_gate(setup, healthy):
    _, live, monitor, opened, _ = setup
    clock, _, _, journal = live
    with client(live, lambda request: response(clock, request)) as sender:
        sender.submit(opened.client_id, quote=quote(clock.now))
    journal.reconcile(
        fixture_evidence(
            opened, 101, 201, "EXECUTED", [fill(timestamp=clock.now.isoformat())], clock.now
        )
    )
    clock.advance(1)
    marked = quote(clock.now, bid="120", ask="120.01")
    journal.update_account(
        account(
            clock.now,
            balance="999997",
            equity="969987",
            required_margin="6000.4",
            available_margin="963986.6",
            positions=(Position(position_id=401, side="BUY", units=1000, average_price="150.01"),),
        ),
        marked,
        now=clock.now,
    )
    closed = intent(
        client_id="Close001",
        side="SELL",
        effect="CLOSE",
        kind="MARKET",
        price=None,
        bound="119.99",
        positions=(Settlement(position_id=401, units=1000),),
    )
    journal.prepare(closed)
    watchdog(monitor)
    assert monitor.status()["conditions"] == ["private_live_entry_halted"]
    if not healthy:
        release(setup)
    calls = []

    def handler(request):
        calls.append(request)
        return response(clock, request)

    with client(live, handler) as sender:
        if healthy:
            sender.submit(closed.client_id, quote=marked)
        else:
            with pytest.raises(ValueError, match="order_preflight_refused"):
                sender.submit(closed.client_id, quote=marked)
    assert len(calls) == int(healthy)
    if healthy:
        assert calls[0].url.path == "/private/v1/closeOrder"
    assert journal.snapshot()["account_guard"]["entry_halted"]


@pytest.mark.parametrize("restricted", [True, False])
def test_regular_cancel_checks_health_but_explicit_target_only_cancel_remains_available(
    setup, restricted
):
    _, live, _, order, _ = setup
    clock, _, _, journal = live
    with client(live, lambda request: response(clock, request)) as sender:
        sender.submit(order.client_id, quote=quote(clock.now))
    evidence = fixture_evidence(order, 101, 201, "ORDERED", [], clock.now)
    journal.reconcile(evidence)
    release(setup)
    token = permit(live, order) if restricted else None
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json=envelope(clock))

    with client(live, handler) as sender:
        if restricted:
            assert sender.cancel(order.client_id, authorization_sha256=token).accepted
        else:
            with pytest.raises(ValueError, match="cancel_preflight_refused"):
                sender.cancel(order.client_id)
    assert len(calls) == int(restricted)
    assert journal.snapshot()["live_control"]["operations"] is not None


def test_registration_commit_failure_rolls_back_binding_fingerprint_and_event(unbound):
    _, live, _ = unbound
    journal = live[3]
    with sqlite3.connect(journal.path) as conn:
        conn.execute(
            "CREATE TRIGGER fail_binding BEFORE INSERT ON events "
            "WHEN NEW.kind='LIVE_OPERATIONS_BOUND' BEGIN SELECT RAISE(ABORT,'synthetic'); END"
        )
    before = journal.snapshot()
    with pytest.raises(sqlite3.Error):
        bind(unbound)
    assert journal.snapshot() == before


def test_full_outbox_does_not_publish_a_new_protective_checkpoint(setup, monkeypatch):
    values, live, monitor, order, _ = setup
    checkpoint = monitor.status()["watchdog_checkpoint"]
    monitor.test_notification()
    values[0].advance(41)
    values[5].control.update(values[5].control.snapshot()["owner"], success=True)
    with sqlite3.connect(live[3].path) as conn:
        conn.execute("UPDATE account_gate SET entry_halted=1")
    monkeypatch.setattr(private_operations, "MAX_ALERTS", 1)
    assert watchdog(monitor)["check_failed"]
    assert monitor.status()["watchdog_checkpoint"] == checkpoint
    with pytest.raises(LiveOrderError, match="live_watchdog_unhealthy"):
        live[3].request(order.client_id)


def test_diagnostic_checks_do_not_refresh_a_previous_watchdog_checkpoint(setup):
    values, live, monitor, order, _ = setup
    checkpoint = monitor.status()["watchdog_checkpoint"]
    values[0].advance(41)
    values[5].control.update(values[5].control.snapshot()["owner"], success=True)
    monitor.check()
    assert monitor.status()["watchdog_checkpoint"] == checkpoint
    with pytest.raises(LiveOrderError, match="live_watchdog_unhealthy"):
        live[3].request(order.client_id)
    watchdog(monitor)
    assert live[3].request(order.client_id).path == "/v1/order"


@pytest.mark.parametrize(
    "damage", ["remove_binding", "remove_event", "change_event", "duplicate_event"]
)
def test_missing_changed_or_removed_permanent_registration_refuses_reopen(setup, damage):
    _, live, _, _, _ = setup
    journal = live[3]
    with sqlite3.connect(journal.path) as conn:
        if damage == "remove_binding":
            state = json.loads(conn.execute("SELECT body FROM live_control").fetchone()[0])
            state.pop("operations")
            body = _json(state)
            conn.execute("UPDATE live_control SET body=?,digest=?", (body, _hash(body)))
        elif damage == "remove_event":
            conn.execute("DELETE FROM events WHERE kind='LIVE_OPERATIONS_BOUND'")
        elif damage == "change_event":
            conn.execute("UPDATE events SET payload_json='{}' WHERE kind='LIVE_OPERATIONS_BOUND'")
        else:
            conn.execute(
                "INSERT INTO events(client_id,kind,payload_json,recorded_at) "
                "SELECT client_id,kind,payload_json,recorded_at FROM events "
                "WHERE kind='LIVE_OPERATIONS_BOUND'"
            )
    with pytest.raises(LiveOrderError, match="integrity_or_binding_failed"):
        LiveOrderJournal(journal.path.parent, live[2], clock=lambda: live[0].now)


@pytest.mark.parametrize("field", ["checked_at", "generation_start_successes"])
def test_invalid_protective_checkpoint_is_rejected_even_with_a_new_hash(setup, field):
    _, live, monitor, order, _ = setup
    with sqlite3.connect(monitor.path) as conn:
        state = json.loads(conn.execute("SELECT body FROM monitor").fetchone()[0])
        checkpoint = state["watchdog_checkpoint"]
        if field == "checked_at":
            checkpoint[field] = (live[0].now + timedelta(seconds=1)).isoformat()
        else:
            checkpoint[field] = checkpoint["sync_successes"] + 1
        body = _json(state)
        conn.execute("UPDATE monitor SET body=?,digest=?", (body, _hash(body)))
    with pytest.raises(OperationsError, match="integrity_failed"):
        monitor.status()
    with pytest.raises(LiveOrderError, match="live_operations_unavailable"):
        live[3].request(order.client_id)


def test_actual_sync_worker_exit_is_refused_before_next_watchdog(setup, tmp_path):
    values, live, monitor, order, _ = setup
    release(setup)
    marker = tmp_path / "owner-ready"
    code = (
        "import time; from pathlib import Path; from trading.stream_control import StreamControl; "
        "c=StreamControl(sys.argv[1]); lease=c.ownership(); lease.__enter__(); "
        "Path(sys.argv[2]).write_text('ready'); time.sleep(30)"
    )
    child = subprocess.Popen(python_process_args(code, values[5].control.path.parent, marker))
    try:
        deadline = time.monotonic() + 10
        while not marker.exists() and child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert marker.read_text() == "ready"
        assert live[3].request(order.client_id).path == "/v1/order"
        checkpoint = monitor.status()["watchdog_checkpoint"]
        child.kill()
        child.wait(timeout=10)
        with pytest.raises(LiveOrderError, match="live_sync_owner_missing"):
            live[3].request(order.client_id)
        assert monitor.status()["watchdog_checkpoint"] == checkpoint
        assert monitor.status()["conditions"] == []
    finally:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=10)


def test_binding_cli_context_and_registration_are_local_and_hide_invalid_arguments(unbound, capsys):
    values, live, monitor = unbound
    common = [
        "--directory",
        str(live[3].path.parent),
        "--read-control-directory",
        str(live[1].path.parent),
        "--scope",
        "synthetic",
    ]
    private_order_operations.main(["context", *common])
    assert json.loads(capsys.readouterr().out)["operations"] is None
    private_order_operations.main(
        [
            "bind",
            *common,
            "--sync-directory",
            str(values[5].directory),
            "--expected-revision",
            "0",
            "--expected-plan-sha256",
            values[5].plan_sha256,
            "--expected-monitor-instance",
            monitor.status()["monitor_instance"],
            "--confirm-operations",
        ]
    )
    assert json.loads(capsys.readouterr().out)["revision"] == 1
    assert live[3].snapshot()["live_control"]["operations"] is not None
    assert values[3].reads == []
    with pytest.raises(SystemExit):
        private_order_operations.main(["context", *common, "--key=secret-do-not-echo"])
    assert "secret-do-not-echo" not in capsys.readouterr().err
