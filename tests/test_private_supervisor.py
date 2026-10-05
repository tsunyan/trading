"""Scheduled baselines and planned rollover without discarding locally received fills."""

import socket
import threading
from datetime import timedelta
from decimal import Decimal

import httpx
import pytest
from test_account_events import execution, raw
from test_account_sync import report
from test_execution_reconciliation import read_order
from test_private_stream import NOW, Clock, FakeSocket, client, frame, response

from trading.account_sync import SyncError
from trading.event_capture import JournaledEventCapture
from trading.execution_cash_book import ExecutionCashBook, OpeningCash
from trading.execution_positions import OpeningPosition, PositionBasis
from trading.private_stream import PrivateStreamReceiver
from trading.private_supervisor import PrivateStreamSupervisor, SupervisorError, SupervisorPolicy
from trading.segmented_journal import SegmentedEventJournal
from trading.stream_control import StreamControl, StreamControlError


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("real network attempted"))


class Socket(FakeSocket):
    def recv(self, *, timeout, decode):
        if self.messages:
            return super().recv(timeout=timeout, decode=decode)
        raise TimeoutError  # Only the coordinator test advances the virtual clock.

    def ping(self, **kwargs):
        pong = super().ping(**kwargs)
        pong.set()
        return pong


def setup(tmp_path, *, max_records=8, collect=None, handler=None, policy=None, position_basis=None):
    clock = Clock()
    journal = SegmentedEventJournal.create(
        tmp_path / "journal", "synthetic", max_records=max_records
    )
    book = ExecutionCashBook.create(
        tmp_path / "cash",
        "synthetic",
        OpeningCash(
            balance="1000000", cutoff=NOW - timedelta(seconds=1), position_basis=position_basis
        ),
    )
    control = StreamControl.create(
        tmp_path / "control", journal, book, wall_ns=lambda: int(clock.wall.timestamp() * 1e9)
    )
    limiter = clock.limiter()
    sockets, receivers, calls, rows, requests = [], [], [], [], []

    def token_handler(request):
        calls.append((request.method, clock.mono))
        return handler(request, clock) if handler else response(clock, request.method)

    def factory(journal, shared):
        assert shared is limiter
        capture = JournaledEventCapture(
            journal, clock=lambda: clock.wall, monotonic_ns=lambda: int(clock.mono * 1e9)
        )
        tokens = client(clock, token_handler, limiter=shared)
        sock = Socket(clock)
        stream = PrivateStreamReceiver(
            tokens, capture, connector=lambda _: sock, monotonic=lambda: clock.mono
        )
        sockets.append(sock)
        receivers.append(stream)
        return stream

    def collect_orders(ids):
        requests.append(ids)
        return tuple(
            read_order(
                clock,
                [r for r in rows if r["orderId"] == identity],
                order_changes={"status": "EXECUTED"},
            )
            for identity in ids
        )

    def default_collect():
        return report(clock, units=None, orders=False, balance=str(1000000 - 2 * len(rows)))

    runner = PrivateStreamSupervisor(
        control,
        journal,
        book,
        limiter,
        factory,
        collect or default_collect,
        collect_orders=collect_orders,
        policy=policy or SupervisorPolicy(join_timeout_seconds=0.05),
        monotonic=lambda: clock.mono,
    )
    return clock, journal, book, control, runner, sockets, receivers, calls, rows, requests


def start(runner):
    runner.start(
        expected_revision=runner.control.snapshot()["revision"], expected_head=runner.journal.head()
    )


def settle(runner):
    if runner._worker is not None:
        runner._worker.join(timeout=3)
        assert not runner._worker.is_alive(), "REST test worker did not finish"
    runner.step()


@pytest.mark.parametrize("kind", ["units", "missing", "flat"])
def test_equal_cash_with_position_difference_stops_without_reconnect(tmp_path, kind):
    from test_position_reservations import account

    basis = PositionBasis(
        positions=()
        if kind == "flat"
        else (OpeningPosition(position_id=401, side="BUY", units=400, average_price="150"),)
    )
    clock, _, _, control, runner, sockets, _, calls, _, _ = setup(
        tmp_path,
        max_records=64,
        position_basis=basis,
        policy=SupervisorPolicy(max_sync_retries=0),
    )
    runner._collect = lambda: account(now=clock.wall, units=None if kind == "missing" else 300)
    start(runner)
    with pytest.raises(SupervisorError, match="sync_failed"):
        settle(runner)
    assert control.snapshot()["phase"] == "STOPPED"
    assert control.snapshot()["sync_successes"] == 0 and sockets[0].closed
    assert [method for method, _ in calls].count("POST") == 1


@pytest.mark.parametrize("kind", ["reservation", "valuation"])
@pytest.mark.parametrize("matches", [True, False])
def test_supplied_diagnostic_mismatch_is_enforced_even_when_inventory_matches(
    tmp_path, kind, matches
):
    from test_account_valuation_sync import quote
    from test_position_reservations import account, order

    from trading.account_valuation_lab import synthetic_account, synthetic_policy

    basis = PositionBasis(
        positions=(OpeningPosition(position_id=401, side="BUY", units=400, average_price="150"),)
    )
    clock, _, book, control, runner, _, _, _, _, _ = setup(
        tmp_path,
        max_records=64,
        position_basis=basis,
        policy=SupervisorPolicy(max_sync_retries=0),
    )
    if kind == "reservation":
        runner._collect = lambda: account(
            order(now=clock.wall), now=clock.wall, ordered=300 if matches else 100
        )
        runner._options.update(
            collect_reservations=lambda: (order(now=clock.wall),), reservation_book=book
        )
    else:
        runner._collect = lambda: synthetic_account(
            clock.wall, equity="999960" if matches else "999950"
        )
        runner._options.update(
            collect_quote=lambda: quote(clock.wall),
            valuation_policy=synthetic_policy(),
            valuation_book=book,
        )
    start(runner)
    if matches:
        settle(runner)
        assert control.snapshot()["sync_successes"] == 1
        assert runner._last_result.account_inventory["positions"]["position_match"]
        runner.close()
    else:
        with pytest.raises(SupervisorError, match="sync_failed"):
            settle(runner)
        assert control.snapshot()["phase"] == "STOPPED"
        assert control.snapshot()["sync_successes"] == 0


@pytest.mark.parametrize("kind", ["notification", "expiry"])
def test_result_invalidated_after_worker_finishes_is_retried_before_success(tmp_path, kind):
    clock, _, _, control, runner, _, receivers, _, _, _ = setup(tmp_path, max_records=64)
    start(runner)
    runner._worker.join(timeout=3)
    assert not runner._worker.is_alive()
    if kind == "notification":
        receivers[0]._capture.heartbeat()
    else:
        clock.advance(31)
    # A heartbeat retains observations; use a position notification to invalidate.
    if kind == "notification":
        from test_account_sync import position

        receivers[0]._capture.ingest(1, position(timestamp=clock.wall))
    runner.step()
    assert control.snapshot()["sync_successes"] == 0
    assert control.snapshot()["sync_retries"] == 1
    assert runner._last_result is None and not receivers[0].status()["stream_closed"]
    clock.advance(1)
    runner._collect = lambda: (
        report(clock, orders=False, balance="1000000")
        if kind == "notification"
        else report(clock, units=None, orders=False, balance="1000000")
    )
    runner.step()
    settle(runner)
    assert control.snapshot()["sync_successes"] == 1
    runner.close()


def test_position_failure_after_cash_commit_preserves_receipt(tmp_path):
    clock, _, book, control, runner, sockets, _, _, rows, _ = setup(
        tmp_path,
        max_records=64,
        position_basis=PositionBasis(positions=()),
        policy=SupervisorPolicy(max_sync_retries=0),
    )
    start(runner)
    settle(runner)
    rows.append(fill(clock, 0))
    sockets[0].messages.append(raw(rows[0]))
    runner.step()
    clock.advance(1)
    runner._next_sync = clock.mono
    runner.step()
    with pytest.raises(SupervisorError, match="sync_failed"):
        settle(runner)
    assert control.snapshot()["phase"] == "STOPPED"
    assert book.snapshot()["execution_ids"] == (501,)
    assert Decimal(book.snapshot()["balance"]) == 999998


def test_sync_failure_before_worker_returns_preserves_reason_and_owner(tmp_path, monkeypatch):
    shutdown, release = threading.Event(), threading.Event()
    original = PrivateStreamReceiver.resync

    def paused_resync(stream, *args, **kwargs):
        try:
            return original(stream, *args, **kwargs)
        finally:
            shutdown.set()
            assert release.wait(timeout=10), "test worker was not released"

    def failed_collection():
        raise ValueError("callback secret must not escape")

    monkeypatch.setattr(PrivateStreamReceiver, "resync", paused_resync)
    _, _, _, control, runner, sockets, receivers, _, _, _ = setup(
        tmp_path, max_records=64, collect=failed_collection
    )
    try:
        start(runner)
        assert shutdown.wait(timeout=3)
        assert receivers[0].status()["stream_reason"] == "private_stream_cash_sync_failed"
        assert runner._results.empty() and runner._worker.is_alive()
        with pytest.raises(SupervisorError, match="^sync_failed$"):
            runner.step()
        state = runner.status()
        assert state["reason"] == state["control"]["reason"] == "sync_failed"
        assert state["control"]["phase"] == "STOPPED" and sockets[0].closed
        assert state["rest_worker_alive"] and state["owner_retained"]
        with pytest.raises(StreamControlError, match="owner_busy"), control.ownership():
            pytest.fail("worker ownership was released early")
    finally:
        release.set()
        if runner._worker is not None:
            runner._worker.join(timeout=3)
        runner.close()
    assert not runner.status()["owner_retained"]
    assert control.snapshot()["reason"] == "sync_failed"
    with control.ownership():
        pass


def fill(clock, index):
    return execution(
        orderId=201 + index,
        rootOrderId=201 + index,
        clientOrderId=f"Fill{index}",
        executionId=501 + index,
        positionId=401 + index,
        executionSize="1000",
        orderExecutedSize="1000",
        orderTimestamp=clock.wall.isoformat(),
        executionTimestamp=clock.wall.isoformat(),
    )


def test_construction_and_status_never_start_network_or_workers(tmp_path):
    _, _, _, control, runner, sockets, _, calls, _, _ = setup(tmp_path)
    assert sockets == calls == []
    assert control.snapshot()["phase"] == "READY"
    assert runner.status()["stream"] is None and not runner.status()["rest_worker_alive"]
    runner.close()
    assert control.snapshot()["phase"] == "READY" and not runner.status()["owner_retained"]


def test_worker_start_failure_stops_closes_transport_and_releases_ownership(tmp_path, monkeypatch):
    _, _, _, control, runner, sockets, _, calls, _, _ = setup(tmp_path)

    def unavailable(_):
        raise RuntimeError("sensitive callback text must not escape")

    monkeypatch.setattr(threading.Thread, "start", unavailable)
    with pytest.raises(SupervisorError, match="^startup_failed$"):
        start(runner)
    assert control.snapshot()["phase"] == "STOPPED"
    assert sockets[-1].closed and calls[-1][0] == "DELETE"
    assert not runner.status()["owner_retained"] and runner._worker is None
    runner.close()
    with control.ownership():
        pass


def test_regular_rest_and_clean_restart_require_new_baselines_and_fence_old_owner(tmp_path):
    clock, _, book, control, runner, sockets, receivers, calls, _, _ = setup(
        tmp_path, max_records=64
    )
    start(runner)
    settle(runner)
    assert control.snapshot()["sync_successes"] == 1
    clock.advance(16)
    runner.step()
    settle(runner)
    assert control.snapshot()["sync_successes"] == 2
    runner.close()
    assert control.snapshot()["phase"] == "READY" and sockets[-1].closed
    assert receivers[-1].status()["account"]["phase"] == "DISCONNECTED"
    new = PrivateStreamSupervisor(
        control,
        runner.journal,
        book,
        runner.limiter,
        runner._factory,
        runner._collect,
        collect_orders=runner._options["collect_orders"],
        monotonic=lambda: clock.mono,
    )
    start(new)
    settle(new)
    assert new.journal.audit_history()["archived_segments"] == 1
    assert control.snapshot()["sync_successes"] == 3
    assert not new.status()["complete"] and not new.status()["live_enabled"]
    assert "journal_rollover_gap_not_repaired" in new.status()["stream"]["account"]["blockers"]
    new.close()
    assert [method for method, _ in calls] == ["POST", "DELETE", "POST", "DELETE"]


@pytest.mark.parametrize("elapsed", [0, 24])
def test_normal_close_waits_only_the_original_collection_deadline(tmp_path, elapsed, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    state, calls = {}, []

    def collect():
        calls.append("collect")
        entered.set()
        assert release.wait(10)
        return report(state["clock"], units=None, orders=False, balance="1000000")

    clock, _, _, control, runner, sockets, _, _, _, _ = setup(
        tmp_path,
        max_records=64,
        collect=collect,
        policy=SupervisorPolicy(join_timeout_seconds=0.01),
    )
    state["clock"] = clock
    start(runner)
    assert entered.wait(3)
    clock.advance(elapsed)
    original = runner._worker.join
    waits = []

    def join(timeout=None):
        waits.append(timeout)
        # The join budget is asserted below (35s, not the old two-second budget), so the
        # collection only needs to still be running when close() starts waiting.
        release.set()
        return original(timeout=timeout)

    monkeypatch.setattr(runner._worker, "join", join)
    try:
        runner.close()
    finally:
        release.set()
    assert waits[0] == pytest.approx(35 - elapsed)
    assert calls == ["collect"]  # Shutdown must not launch another collection.
    assert control.snapshot()["phase"] == "READY"
    assert control.snapshot()["sync_successes"] == 1
    assert sockets[0].closed and not runner.status()["owner_retained"]


def test_normal_close_deadline_does_not_release_a_hung_worker_owner(tmp_path, short_owner_wait):
    entered, release = threading.Event(), threading.Event()
    state = {}

    def collect():
        entered.set()
        assert release.wait(10)
        return report(state["clock"], units=None, orders=False, balance="1000000")

    clock, journal, book, control, runner, _, _, _, _, _ = setup(
        tmp_path,
        max_records=64,
        collect=collect,
        policy=SupervisorPolicy(sync_timeout_seconds=1, join_timeout_seconds=0),
    )
    state["clock"] = clock
    start(runner)
    assert entered.wait(3)
    try:
        with pytest.raises(SupervisorError, match="worker_not_joined"):
            runner.close()
        assert runner.status()["owner_retained"]
        saved = control.snapshot()
        assert saved["phase"] == "STOPPED"
        peer = StreamControl(control.path.parent, wall_ns=control._wall)
        with pytest.raises(StreamControlError, match="owner_busy"):
            peer.recover(
                journal,
                book,
                expected_revision=saved["revision"],
                expected_reason=saved["reason"],
                expected_head=journal.head(),
                acknowledge_token_uncertainty=True,
                at=clock.wall,
            )
    finally:
        release.set()
        runner._worker.join(timeout=3)
        runner.close()
    assert not runner.status()["owner_retained"]
    assert control.snapshot()["phase"] == "STOPPED"


def test_many_fills_cross_capacity_in_multiple_connections_without_double_booking(tmp_path):
    clock, _, book, control, runner, sockets, _, _, rows, requests = setup(tmp_path, max_records=8)
    start(runner)
    settle(runner)
    for index in range(20):
        row = fill(clock, index)
        rows.append(row)
        sockets[-1].messages.append(raw(row))
        runner.step()
        # Trigger another bounded collection before the next local fill.
        clock.advance(1)
        runner._next_sync = clock.mono
        runner.step()
        settle(runner)
        if runner._worker is not None:
            settle(runner)
    assert book.snapshot()["executions"] == 20
    assert Decimal(book.snapshot()["balance"]) == 999960
    assert runner.status()["connection"] >= 5
    assert runner.journal.audit_history()["archived_segments"] >= 4
    assert sum(len(ids) for ids in requests) == 20
    assert all(not r["unacknowledged_records"] for r in [runner.journal.inspect()])
    runner.close()
    assert control.snapshot()["phase"] == "READY"


def test_time_rollover_clears_old_observation_and_launches_a_fresh_collection(tmp_path):
    clock, _, _, control, runner, _, receivers, _, _, _ = setup(
        tmp_path, max_records=64, policy=SupervisorPolicy(max_connection_seconds=60)
    )
    start(runner)
    settle(runner)
    for _ in range(2):
        clock.advance(30)
        runner.step()  # ping
        settle(runner)  # received pong; liveness advances on persisted receipt only
    if runner.status()["connection"] == 1:
        settle(runner)
    assert runner.status()["connection"] == 2
    assert receivers[0].status()["stream_closed"]
    settle(runner)
    assert control.snapshot()["rotations"] == 1
    runner.close()


def test_notification_during_cash_resync_invalidates_attempt_but_keeps_transport(tmp_path):
    entered, release = threading.Event(), threading.Event()
    state = {}

    def collect():
        entered.set()
        assert release.wait(3)
        clock = state["clock"]
        return report(clock, units=None, orders=False, balance="1000000")

    clock, _, _, control, runner, sockets, receivers, _, _, _ = setup(
        tmp_path, max_records=64, collect=collect
    )
    state["clock"] = clock
    start(runner)
    assert entered.wait(3)
    sockets[-1].messages.append(frame(timestamp=clock.wall.isoformat()))
    assert runner.step()
    release.set()
    settle(runner)
    assert control.snapshot()["sync_retries"] == 1
    assert receivers[0].status()["stream_running"] and not sockets[0].closed
    clock.advance(1)
    # Use a matching current position snapshot for the retry.
    runner._collect = lambda: report(clock, orders=False, balance="1000000")
    runner.step()
    settle(runner)
    runner.close()


def test_hung_rest_keeps_os_owner_until_worker_exit_and_persists_stop(tmp_path, short_owner_wait):
    entered, release = threading.Event(), threading.Event()
    state = {}

    def collect():
        entered.set()
        release.wait(5)
        return report(state["clock"], units=None, orders=False, balance="1000000")

    clock, journal, book, control, runner, sockets, _, _, _, _ = setup(
        tmp_path,
        max_records=64,
        collect=collect,
        policy=SupervisorPolicy(sync_timeout_seconds=2, join_timeout_seconds=0.0),
    )
    state["clock"] = clock
    start(runner)
    assert entered.wait(3)
    clock.advance(2)
    try:
        with pytest.raises(SupervisorError, match="sync_deadline"):
            runner.step()
        assert sockets[0].closed and runner.status()["owner_retained"]
        assert control.snapshot()["phase"] == "STOPPED"
        peer = StreamControl(control.path.parent, wall_ns=control._wall)
        with pytest.raises(StreamControlError, match="owner_busy"):
            peer.recover(
                journal,
                book,
                expected_revision=peer.snapshot()["revision"],
                expected_reason="sync_deadline",
                expected_head=journal.head(),
                acknowledge_token_uncertainty=True,
                at=clock.wall,
            )
        with pytest.raises(SupervisorError, match="worker_not_joined"):
            runner.close()
    finally:
        release.set()
        runner._worker.join(timeout=3)
        runner.close()
    assert not runner.status()["owner_retained"]
    assert control.snapshot()["phase"] == "STOPPED" and control.snapshot()["sync_successes"] == 0


@pytest.mark.parametrize("kind", ["transport", "balance", "callback_secret", "cleanup"])
def test_material_failures_never_automatically_reconnect_and_do_not_expose_errors(tmp_path, kind):
    secret = "secret-error-do-not-store"

    def handler(request, clock):
        if kind == "cleanup" and request.method == "DELETE":
            return httpx.Response(503, content=secret)
        return response(clock, request.method)

    values = setup(
        tmp_path,
        max_records=64,
        handler=handler,
        policy=SupervisorPolicy(max_sync_retries=0),
    )
    clock, journal, book, control, runner, sockets, _, calls, _, _ = values
    if kind == "balance":
        runner._collect = lambda: report(clock, units=None, orders=False, balance="1000001")
    elif kind == "callback_secret":

        def failed():
            raise RuntimeError(secret)

        runner._collect = failed
    start(runner)
    if kind in {"balance", "callback_secret"}:
        with pytest.raises(SupervisorError, match="sync_failed"):
            settle(runner)
    else:
        settle(runner)
        if kind == "transport":
            sockets[0].messages.append(RuntimeError(secret))
            with pytest.raises(SupervisorError, match="stream_failed"):
                runner.step()
        else:
            with pytest.raises(SupervisorError):
                runner.close()
    assert control.snapshot()["phase"] == "STOPPED"
    assert [method for method, _ in calls].count("POST") == 1
    assert secret not in repr(runner.status()) and secret.encode() not in control.path.read_bytes()
    replacement = PrivateStreamSupervisor(
        control,
        journal,
        book,
        clock.limiter(),
        runner._factory,
        runner._collect,
        collect_orders=runner._options["collect_orders"],
        monotonic=lambda: clock.mono,
    )
    with pytest.raises(StreamControlError, match="recovery_required"):
        start(replacement)
    assert [method for method, _ in calls].count("POST") == 1


def test_backpressure_at_hard_reserve_finishes_receipts_before_rotation(tmp_path):
    entered, release = threading.Event(), threading.Event()
    clock, _, book, _, runner, sockets, _, _, rows, _ = setup(tmp_path)
    start(runner)
    settle(runner)
    for index in range(2):
        row = fill(clock, index)
        rows.append(row)
        sockets[0].messages.append(raw(row))
        runner.step()
    # Five records leave three slots, so a heartbeat + frame cannot both fit with END.
    row = fill(clock, 2)
    sockets[0].messages.append(raw(row))  # Not yet received; must remain in the library queue.
    original = runner._collect

    def collect():
        entered.set()
        assert release.wait(3)
        return original()

    runner._collect = collect
    runner._next_sync = clock.mono
    try:
        runner.step()
        assert entered.wait(3)
        assert runner.status()["receive_paused"] and len(sockets[0].messages) == 1
        assert runner.status()["connection"] == 1
    finally:
        release.set()
    settle(runner)
    assert book.snapshot()["executions"] == 2
    assert runner.status()["connection"] == 2
    settle(runner)
    assert "journal_rollover_gap_not_repaired" in runner.status()["stream"]["account"]["blockers"]
    runner.close()


def test_retry_exhaustion_is_persistent_and_does_not_restore_old_results(tmp_path):
    values = setup(tmp_path, max_records=64, policy=SupervisorPolicy(max_sync_retries=1))
    clock, _, _, control, runner, _, _, _, _, _ = values
    runner._collect = lambda: (_ for _ in ()).throw(SyncError("stream_changed_during_collection"))
    start(runner)
    settle(runner)
    assert control.snapshot()["sync_retries"] == 1
    clock.advance(1)
    runner.step()
    with pytest.raises(SupervisorError, match="sync_failed"):
        settle(runner)
    assert control.snapshot()["phase"] == "STOPPED"
    assert runner._last_result is None


def test_pong_and_frame_in_the_same_step_leave_room_for_a_clean_end(tmp_path):
    clock, _, book, control, runner, sockets, _, _, rows, _ = setup(tmp_path, max_records=6)
    start(runner)
    settle(runner)
    clock.advance(31)
    runner.step()  # Sends ping; the fake socket's corresponding pong is ready.
    if runner._worker is not None:
        runner._worker.join(timeout=3)  # Do not service the pong yet.
        assert not runner._worker.is_alive()
    assert runner.journal.inspect()["records"] == 1
    row = fill(clock, 0)
    rows.append(row)
    sockets[0].messages.append(raw(row))
    runner.step()
    assert runner.journal.inspect()["records"] == 5  # BEGIN, HEARTBEAT+ACK, EVENT+ACK.
    assert not runner.journal.inspect()["unacknowledged_records"]
    clock.advance(1)
    runner.step()  # Pause before any further receive; collect the final durable receipt.
    settle(runner)
    settle(runner)
    assert book.snapshot()["executions"] == 1
    assert runner.journal.audit_history()["archived_segments"] == 1
    runner.close()
    assert control.snapshot()["phase"] == "READY"


def test_late_completed_worker_is_rejected_instead_of_restoring_a_past_observation(tmp_path):
    values = setup(tmp_path, max_records=64, policy=SupervisorPolicy(sync_timeout_seconds=2))
    clock, _, _, control, runner, _, _, _, _, _ = values

    def late():
        clock.advance(3)
        return report(clock, units=None, orders=False, balance="1000000")

    runner._collect = late
    start(runner)
    with pytest.raises(SupervisorError, match="sync_deadline"):
        settle(runner)
    assert control.snapshot()["phase"] == "STOPPED" and runner._last_result is None


def test_event_count_rollover_precedes_monitor_capacity(tmp_path):
    values = setup(tmp_path, max_records=64, policy=SupervisorPolicy(rotation_margin_events=100))
    clock, _, _, control, runner, sockets, receivers, _, _, _ = values
    # Small monitor capacity is a trusted test seam; production defaults to 1,000.
    original = runner._factory

    def factory(journal, limiter):
        stream = original(journal, limiter)
        stream._capture._monitor._capacity = 3
        return stream

    runner._factory = factory
    runner._collect = lambda: report(clock, orders=False, balance="1000000")
    start(runner)
    settle(runner)
    for _ in range(2):
        sockets[-1].messages.append(frame(timestamp=clock.wall.isoformat()))
        runner.step()
    runner.step()
    settle(runner)
    assert runner.status()["connection"] == 2
    assert receivers[0].status()["receive_sequence"] == 2
    assert control.snapshot()["phase"] == "RUNNING"
    runner.close()


def test_byte_budget_rollover_uses_the_same_clean_boundary(tmp_path, monkeypatch):
    _, _, _, control, runner, _, _, _, _, _ = setup(tmp_path, max_records=64)
    start(runner)
    settle(runner)
    original = runner.journal.capacity
    monkeypatch.setattr(
        runner.journal, "capacity", lambda: {**original(), "bytes_remaining": 100_000}
    )
    runner.step()
    settle(runner)
    assert runner.status()["connection"] == 2 and control.snapshot()["rotations"] == 1
    runner.close()
