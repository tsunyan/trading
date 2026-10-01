import json
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest

from trading.account_read_lab import Transcript, demo_transcript, replay
from trading.account_sync import BLOCKERS, AccountSyncMonitor, SyncError

NOW = datetime(2026, 9, 30, tzinfo=UTC)


class Clock:
    def __init__(self):
        self.wall, self.mono = NOW, 0

    def advance(self, seconds):
        self.wall += timedelta(seconds=seconds)
        self.mono += seconds

    def args(self):
        return {"clock": lambda: self.wall, "monotonic": lambda: self.mono}


def report(clock, *, units=400, orders=True, balance="999998"):
    data = demo_transcript(clock.wall).model_dump(mode="json")
    # Preserve pagination exchanges when removing rows: AccountReader will then
    # request fewer pages, so remove now-unnecessary cursor follow-ups too.
    exchanges = []
    for exchange in data["exchanges"]:
        path = exchange["path"]
        if path == "/v1/account/assets":
            exchange["response"]["data"][0]["balance"] = balance
        if path in {"/v1/openPositions", "/v1/activeOrders"}:
            empty = units is None if path == "/v1/openPositions" else not orders
            if empty:
                if len(exchange["query"]) > 1:
                    continue
                exchange["response"]["data"]["list"] = []
            elif path == "/v1/openPositions":
                for row in exchange["response"]["data"]["list"]:
                    row["size"] = str(units)
        exchanges.append(exchange)
    data["exchanges"] = exchanges
    return replay(Transcript.model_validate(data))


def position(*, units=400, msg="UPR", timestamp=NOW, **changes):
    return json.dumps(
        {
            "channel": "positionEvents",
            "positionId": 401,
            "symbol": "USD_JPY",
            "side": "BUY",
            "size": str(units),
            "orderdSize": "0",
            "price": "150",
            "lossGain": "-40",
            "totalSwap": "0",
            "timestamp": timestamp.isoformat(),
            "msgType": msg,
            **changes,
        }
    ).encode()


def order(*, status="ORDERED"):
    row = {
        "channel": "orderEvents",
        "rootOrderId": 201,
        "orderId": 201,
        "clientOrderId": "DemoOpen",
        "symbol": "USD_JPY",
        "settleType": "OPEN",
        "orderType": "NORMAL",
        "executionType": "LIMIT",
        "side": "BUY",
        "orderStatus": status,
        "orderTimestamp": NOW.isoformat(),
        "orderPrice": "150",
        "orderSize": "1000",
        "expiry": "20261001",
        "msgType": "NOR",
    }
    if status == "CANCELED":
        row.update(msgType="COR", cancelType="USER")
    return json.dumps(row).encode()


def execution(**changes):
    row = json.loads(order())
    row.pop("orderStatus")
    row.pop("expiry")
    row.update(
        channel="executionEvents",
        msgType="ER",
        amount="-2",
        executionId=501,
        executionPrice="150",
        executionSize="400",
        positionId=401,
        lossGain="0",
        settledSwap="0",
        fee="-2",
        orderExecutedSize="400",
        executionTimestamp=NOW.isoformat(),
    )
    return json.dumps({**row, **changes}).encode()


@pytest.fixture
def setup(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("network attempted"))
    clock = Clock()
    monitor = AccountSyncMonitor(**clock.args())
    return clock, monitor, monitor.start_session()


def test_initial_repeated_rest_remains_unverified(setup):
    clock, monitor, session = setup
    assert monitor.status()["resync_required"]
    assessment = monitor.resync(session, lambda: report(clock))
    assert assessment.structural_match
    assert set(BLOCKERS) <= set(assessment.blockers)
    assert not assessment.complete and not assessment.live_enabled
    assert monitor.status()["phase"] == "OBSERVED_UNVERIFIED"
    assert not assessment.report.account_identity_verified


def test_mismatch_stays_pending_until_fresh_matching_rest(setup):
    clock, monitor, session = setup
    monitor.resync(session, lambda: report(clock))
    clock.advance(1)
    monitor.ingest(session, 1, position(units=600))
    assert monitor.status()["resync_required"]
    result = monitor.resync(session, lambda: report(clock))
    assert result.mismatches == ("position_event_mismatch:401",)
    assert monitor.status()["pending_positions"] == 1
    clock.advance(1)
    result = monitor.resync(session, lambda: report(clock, units=600))
    assert result.structural_match and not monitor.status()["resync_required"]
    assert monitor.status()["pending_positions"] == 0


@pytest.mark.parametrize(
    "change,reason",
    [
        ({"units": 500}, "position_change_without_event:401"),
        ({"units": None}, "position_change_without_event:401"),
        ({"orders": False}, "order_change_without_event:201"),
        ({"balance": "999900"}, "balance_change_unverified"),
    ],
)
def test_unannounced_changes_cannot_be_cleared_by_repeating_rest(setup, change, reason):
    clock, monitor, session = setup
    monitor.resync(session, lambda: report(clock))
    for _ in range(2):
        clock.advance(1)
        result = monitor.resync(session, lambda: report(clock, **change))
        assert reason in result.mismatches
        assert monitor.status()["resync_required"]


def test_removed_position_and_cancelled_order_must_be_absent(setup):
    clock, monitor, session = setup
    monitor.resync(session, lambda: report(clock))
    monitor.ingest(session, 1, position(msg="CPR"))
    monitor.ingest(session, 2, order(status="CANCELED"))
    assert len(monitor.resync(session, lambda: report(clock)).mismatches) == 2
    assert monitor.resync(session, lambda: report(clock, units=None, orders=False)).structural_match


def test_active_order_checks_total_size_and_identity(setup):
    clock, monitor, session = setup
    monitor.ingest(session, 1, order())
    assert monitor.resync(session, lambda: report(clock)).structural_match
    bad = json.loads(order())
    bad["orderSize"] = "600"
    monitor.ingest(session, 2, json.dumps(bad).encode())
    assert monitor.resync(session, lambda: report(clock)).mismatches == (
        "order_event_mismatch:201",
    )


def test_repeated_position_a_b_a_is_not_global_deduplicated(setup):
    clock, monitor, session = setup
    for sequence, units in enumerate((400, 600, 400), 1):
        monitor.ingest(session, sequence, position(units=units))
    assert monitor.resync(session, lambda: report(clock)).structural_match


@pytest.mark.parametrize("sequence", [0, 2, True, "1", -1])
def test_local_gap_or_reorder_latches_disconnect(setup, sequence):
    _, monitor, session = setup
    with pytest.raises(SyncError, match="gap_or_reorder"):
        monitor.ingest(session, sequence, position())
    assert monitor.status()["history_gap_observed"]
    with pytest.raises(SyncError, match="not_connected"):
        monitor.heartbeat(session)


def test_restart_and_old_session_fencing(setup):
    clock, monitor, session = setup
    monitor.resync(session, lambda: report(clock))
    monitor.disconnect(session)
    new = monitor.start_session()
    assert new != session and monitor.status()["resync_required"]
    before = monitor.status()
    with pytest.raises(SyncError, match="stale_stream_session"):
        monitor.ingest(session, 1, position())
    assert monitor.status() == before
    result = monitor.resync(new, lambda: report(clock, units=900))
    assert result.structural_match and "history_gap_not_repaired" in result.blockers
    fresh_process = AccountSyncMonitor(**clock.args())
    assert fresh_process.status()["phase"] == "DISCONNECTED"


@pytest.mark.parametrize("action", ["event", "disconnect", "reconnect", "duplicate"])
def test_changes_during_collection_invalidate_result(setup, action):
    clock, monitor, session = setup
    if action == "duplicate":
        monitor.ingest(session, 1, position())

    def collect():
        result = report(clock)
        if action in {"event", "duplicate"}:
            monitor.ingest(session, 2 if action == "duplicate" else 1, position())
        elif action == "disconnect":
            monitor.disconnect(session)
        else:
            monitor.start_session()
        return result

    with pytest.raises(SyncError):
        monitor.resync(session, collect)
    assert monitor.status()["resync_required"]


def test_threaded_event_does_not_wait_for_collection_lock(setup):
    clock, monitor, session = setup
    entered, release = threading.Event(), threading.Event()

    def collect():
        entered.set()
        assert release.wait(5)
        return report(clock)

    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(monitor.resync, session, collect)
        try:
            assert entered.wait(5)
            with pytest.raises(SyncError, match="already_running"):
                monitor.resync(session, lambda: report(clock))
            monitor.ingest(session, 1, position())
        finally:
            release.set()
        with pytest.raises(SyncError, match="changed_during_collection"):
            pending.result(timeout=5)


def test_old_collection_cannot_overwrite_new_session_result(setup):
    clock, monitor, old = setup

    def collect():
        new = monitor.start_session()
        monitor.resync(new, lambda: report(clock, units=700))
        return report(clock)

    with pytest.raises(SyncError, match="stale_stream_session"):
        monitor.resync(old, collect)
    assert monitor.status()["phase"] == "OBSERVED_UNVERIFIED"
    assert monitor.status()["epoch"] == 2


def test_heartbeat_is_liveness_not_snapshot_freshness(setup):
    clock, monitor, session = setup
    monitor.resync(session, lambda: report(clock))
    clock.advance(31)
    monitor.heartbeat(session)
    assert monitor.status()["reason"] == "rest_observation_expired"
    assert monitor.status()["resync_required"]
    clock.advance(76)
    assert monitor.status()["phase"] == "DISCONNECTED"
    with pytest.raises(SyncError, match="not_connected"):
        monitor.heartbeat(session)


def test_order_timestamp_cannot_be_used_for_delivery_staleness(setup):
    clock, monitor, session = setup
    monitor.ingest(session, 1, position(timestamp=NOW - timedelta(days=30)))
    assert monitor.resync(session, lambda: report(clock)).structural_match


@pytest.mark.parametrize("kind", ["wall", "mono", "nan", "callback"])
def test_clock_errors_fail_closed(setup, kind):
    clock, monitor, session = setup
    if kind == "wall":
        clock.wall -= timedelta(seconds=1)
    elif kind == "mono":
        clock.mono = -1
    elif kind == "nan":
        clock.mono = float("nan")
    else:
        monitor._clock = lambda: (_ for _ in ()).throw(RuntimeError("secret"))
    with pytest.raises(SyncError, match="clock_invalid"):
        monitor.ingest(session, 1, position())
    assert monitor.status()["phase"] == "DISCONNECTED"


@pytest.mark.parametrize("failure", [RuntimeError, KeyboardInterrupt])
def test_collection_failure_redacted_and_ticket_released(setup, failure):
    clock, monitor, session = setup

    def broken():
        raise failure("private details")

    with pytest.raises(SyncError if failure is RuntimeError else KeyboardInterrupt) as caught:
        monitor.resync(session, broken)
    if failure is RuntimeError:
        assert str(caught.value) == "sync_collection_failed"
        assert monitor.resync(session, lambda: report(clock)).structural_match
    else:
        assert monitor.status()["phase"] == "DISCONNECTED"


@pytest.mark.parametrize("case", ["old", "future", "empty", "wrong_type", "duration"])
def test_invalid_or_old_reports_rejected(setup, case):
    clock, monitor, session = setup
    result = report(clock)
    if case == "old":
        clock.advance(1)
    elif case == "future":
        other = Clock()
        other.advance(1)
        result = report(other)
    elif case == "empty":
        result = result.model_copy(update={"observations": ()})
    elif case == "wrong_type":
        result = {"complete": True}

    def collect():
        if case == "duration":
            clock.advance(31)
        return result

    with pytest.raises(SyncError):
        monitor.resync(session, collect)
    assert monitor.status()["resync_required"]


def test_execution_dedup_conflict_and_no_automatic_booking(setup):
    clock, monitor, session = setup
    monitor.ingest(session, 1, execution())
    monitor.ingest(session, 2, execution())
    result = monitor.resync(session, lambda: report(clock))
    assert result.structural_match and result.unverified_execution_ids == (501,)
    assert "execution_events_not_reconciled" in result.blockers
    assert monitor.status()["unverified_executions"] == 1
    with pytest.raises(SyncError, match="identity_conflict"):
        monitor.ingest(session, 3, execution(fee="-3"))
    assert monitor.status()["phase"] == "DISCONNECTED"


def test_capacity_does_not_drop_older_events_silently():
    clock = Clock()
    monitor = AccountSyncMonitor(**clock.args(), max_session_events=1)
    session = monitor.start_session()
    monitor.ingest(session, 1, position())
    with pytest.raises(SyncError, match="capacity"):
        monitor.ingest(session, 2, position())
    assert monitor.status()["phase"] == "DISCONNECTED"


def test_malformed_event_invalidates_existing_observation(setup):
    clock, monitor, session = setup
    monitor.resync(session, lambda: report(clock))
    with pytest.raises(SyncError, match="event_rejected"):
        monitor.ingest(session, 1, b'{"secret":"never echo"}')
    assert monitor.status()["phase"] == "DISCONNECTED"


@pytest.mark.parametrize("pending", [False, True])
def test_full_execution_explains_order_disappearance_in_same_epoch(setup, pending):
    clock, monitor, session = setup
    monitor.resync(session, lambda: report(clock))
    if pending:
        monitor.ingest(session, 1, order())
    monitor.ingest(session, 2 if pending else 1, execution(orderExecutedSize="1000"))
    for _ in range(3):
        result = monitor.resync(session, lambda: report(clock, orders=False))
        assert result.structural_match
        assert "execution_events_not_reconciled" in result.blockers
        assert monitor.status()["phase"] == "OBSERVED_UNVERIFIED"
        assert monitor.status()["epoch"] == 1


def test_partial_execution_does_not_explain_disappearance(setup):
    clock, monitor, session = setup
    monitor.resync(session, lambda: report(clock))
    monitor.ingest(session, 1, execution())
    result = monitor.resync(session, lambda: report(clock, orders=False))
    assert "order_change_without_event:201" in result.mismatches


def test_completed_order_still_active_and_balance_changes_remain_unverified(setup):
    clock, monitor, session = setup
    monitor.resync(session, lambda: report(clock))
    monitor.ingest(session, 1, execution(orderExecutedSize="1000"))
    result = monitor.resync(session, lambda: report(clock))
    assert "executed_order_still_active:201" in result.mismatches
    result = monitor.resync(session, lambda: report(clock, orders=False, balance="999997"))
    assert result.mismatches == ("balance_change_unverified",)


@pytest.mark.parametrize("offset", [-101, -100, 50, 100, 101])
def test_sync_response_skew_boundaries(offset):
    clock = Clock()
    monitor = AccountSyncMonitor(**clock.args(), clock_skew_ms=100)
    session = monitor.start_session()
    source = report(clock)
    adjusted = source.model_copy(
        update={
            "observations": tuple(
                o.model_copy(update={"response_at": o.response_at + timedelta(milliseconds=offset)})
                for o in source.observations
            )
        }
    )
    if abs(offset) <= 100:
        assert monitor.resync(session, lambda: adjusted).structural_match
    else:
        with pytest.raises(SyncError, match="stale_or_invalid"):
            monitor.resync(session, lambda: adjusted)


def test_tolerance_does_not_allow_old_local_receipt():
    clock = Clock()
    monitor = AccountSyncMonitor(**clock.args(), clock_skew_ms=100)
    session = monitor.start_session()
    old = report(clock)
    clock.advance(0.05)
    with pytest.raises(SyncError, match="stale_or_invalid"):
        monitor.resync(session, lambda: old)
