"""Synthetic WS/REST discrepancies, race fences, and repeat-safe cash diagnostics."""

import copy
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from decimal import Decimal

import pytest
from test_account_events import NOW, execution, raw
from test_account_sync import Clock, report

from trading.account_events import parse_event
from trading.account_reader import AccountReader
from trading.account_sync import AccountSyncMonitor, SyncError
from trading.broker_contracts import OrderIntent
from trading.event_capture import JournaledEventCapture
from trading.event_journal import EventJournal
from trading.execution_reconciliation import reconcile_executions


def read_order(clock, rows=None, *, order_changes=None, fill_changes=None, empty=False):
    rows = rows or [execution()]
    first = rows[0]
    order = {
        "rootOrderId": first["rootOrderId"],
        "orderId": first["orderId"],
        "clientOrderId": first["clientOrderId"],
        "symbol": first["symbol"],
        "side": first["side"],
        "settleType": first["settleType"],
        "orderType": "NORMAL",
        "executionType": "LIMIT",
        "size": first["orderSize"],
        "price": first["orderPrice"],
        "status": "ORDERED",
        "timestamp": first["orderTimestamp"],
        **(order_changes or {}),
    }
    fills = []
    for row in rows:
        fill = {
            k: row[k]
            for k in (
                "executionId",
                "positionId",
                "orderId",
                "clientOrderId",
                "symbol",
                "side",
                "settleType",
                "amount",
                "fee",
                "lossGain",
                "settledSwap",
            )
        }
        fill.update(
            size=row["executionSize"],
            price=row["executionPrice"],
            timestamp=row["executionTimestamp"],
        )
        fill.update(fill_changes or {})
        fills.append(fill)

    class Transport:
        def get(self, request):
            assert request.method == "GET"
            assert request.query == (("orderId", str(order["orderId"])),)
            data = [order] if request.path == "/v1/orders" else ([] if empty else fills)
            return {
                "status": 0,
                "data": {"list": copy.deepcopy(data)},
                "responsetime": clock.wall.isoformat(),
            }

    positions = (
        ()
        if order["settleType"] == "OPEN"
        else ({"position_id": first["positionId"], "units": int(order["size"])},)
    )
    intent = OrderIntent(
        client_id=order["clientOrderId"],
        symbol=order["symbol"],
        side=order["side"],
        effect=order["settleType"],
        kind="LIMIT",
        price=order["price"],
        units=int(order["size"]),
        positions=positions,
    )
    return AccountReader(Transport(), clock=lambda: clock.wall).collect_order(
        intent, order["orderId"]
    )


@pytest.fixture
def setup(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("network attempted"))
    clock = Clock()
    monitor = AccountSyncMonitor(**clock.args())
    session = monitor.start_session()
    monitor.ingest(session, 1, raw(execution()))
    return clock, monitor, session


def collect(clock, monitor, session, callback=None):
    return monitor.resync(
        session,
        lambda: report(clock),
        collect_orders=callback or (lambda ids: (read_order(clock),)),
    )


def test_matching_partial_execution_is_repeat_safe_and_never_books(setup):
    clock, monitor, session = setup
    for sequence in (2, 3):
        monitor.ingest(session, sequence, raw(execution()))
        result = collect(clock, monitor, session)
        reconciliation = result.execution_reconciliation
        assert result.structural_match and not result.unverified_execution_ids
        assert reconciliation.matched_execution_ids == (501,)
        assert reconciliation.matched_cash_amount == Decimal("-2")
        assert reconciliation.matched_fee_debit == Decimal("2")
        assert not reconciliation.accounting_applied
        assert not result.complete and not result.live_enabled
        assert "execution_accounting_not_applied" in result.blockers
        assert "execution_history_not_proven" in result.blockers
        assert "execution_events_not_reconciled" not in result.blockers
        assert monitor.status()["reconciled_executions"] == 1
    # Revalidation is explicit; a later account-only resync cannot reuse old fills.
    assert monitor.resync(session, lambda: report(clock)).unverified_execution_ids == (501,)


@pytest.mark.parametrize(
    "changes",
    [
        {"positionId": 999},
        {"price": "149.9"},
        {"size": "300"},
        {"fee": "-3", "amount": "-3"},
        {"lossGain": "20", "amount": "18"},
        {"settledSwap": "1", "amount": "-1"},
        {"timestamp": (NOW - timedelta(seconds=1)).isoformat()},
    ],
)
def test_rest_fill_field_mismatches_require_resync(setup, changes):
    clock, monitor, session = setup
    result = collect(
        clock, monitor, session, lambda ids: (read_order(clock, fill_changes=changes),)
    )
    assert result.mismatches == ("execution_fields_mismatch:501",)
    assert result.unverified_execution_ids == (501,)
    assert result.execution_reconciliation.matched_cash_amount == 0
    assert monitor.status()["resync_required"]


@pytest.mark.parametrize(
    "changes",
    [
        {"rootOrderId": 202},
        {"price": "151"},
        {"size": "1200"},
    ],
)
def test_order_terms_must_match_notification(setup, changes):
    clock, monitor, session = setup
    result = collect(
        clock, monitor, session, lambda ids: (read_order(clock, order_changes=changes),)
    )
    assert result.mismatches == ("execution_order_mismatch:501",)


@pytest.mark.parametrize("missing_order", [False, True])
def test_missing_rest_execution_or_order_never_confirms_notification(setup, missing_order):
    clock, monitor, session = setup
    result = collect(
        clock,
        monitor,
        session,
        lambda ids: () if missing_order else (read_order(clock, empty=True),),
    )
    assert result.unverified_execution_ids == (501,)
    assert not result.structural_match


def test_cash_equation_and_cumulative_quantity_are_verified():
    clock = Clock()
    for changes, reason in (
        ({"amount": "999"}, "execution_amount_mismatch:501"),
        ({"orderExecutedSize": "600"}, "execution_cumulative_size_mismatch:501"),
    ):
        result = reconcile_executions(
            (parse_event(raw(execution(**changes)), NOW),), (read_order(clock),)
        )
        assert result.mismatches == (reason,)
        assert result.matched_cash_amount == 0


def test_closing_pnl_swap_and_fee_remain_separate():
    row = execution(
        settleType="CLOSE",
        side="SELL",
        lossGain="100.125",
        settledSwap="1.75",
        fee="-2.5",
        amount="99.375",
    )
    result = reconcile_executions((parse_event(raw(row), NOW),), (read_order(Clock(), [row]),))
    assert result.matched_loss_gain == Decimal("100.125")
    assert result.matched_settled_swap == Decimal("1.75")
    assert result.matched_fee_debit == Decimal("2.5")
    assert result.matched_cash_amount == Decimal("99.375")
    assert not result.accounting_applied


def test_multiple_fills_deduplicate_and_expose_rest_only_history():
    rows = [execution(), execution(executionId=502, executionSize="600", orderExecutedSize="1000")]
    events = tuple(parse_event(raw(r), NOW) for r in rows)
    rest = read_order(Clock(), rows, order_changes={"status": "EXECUTED"})
    result = reconcile_executions((*events, events[0]), (rest,))
    assert result.matched_execution_ids == (501, 502)
    assert result.matched_cash_amount == -4
    subset = reconcile_executions(events[:1], (rest,))
    assert subset.rest_only_execution_ids == (502,)
    assert subset.matched_cash_amount == -2
    assert not subset.complete


@pytest.mark.parametrize("kind", ["duplicate_order", "foreign_order", "conflicting_notice"])
def test_ambiguous_identity_is_rejected(kind):
    notice = parse_event(raw(execution()), NOW)
    rest = read_order(Clock())
    events, reports = (notice,), (rest, rest)
    if kind == "foreign_order":
        reports = (read_order(Clock(), [execution(orderId=202)]),)
    elif kind == "conflicting_notice":
        events = (notice, parse_event(raw(execution(amount="100")), NOW))
        reports = (rest,)
    with pytest.raises(ValueError):
        reconcile_executions(events, reports)


@pytest.mark.parametrize(
    "kind", ["old", "future", "empty", "query", "path", "complete", "observation"]
)
def test_invalid_order_read_evidence_is_rejected(setup, kind):
    clock, monitor, session = setup
    rest = read_order(clock)
    if kind == "old":
        clock.advance(1)
    elif kind == "future":
        future = Clock()
        future.advance(1)
        rest = read_order(future)
    elif kind == "empty":
        rest = rest.model_copy(update={"observations": ()})
    elif kind in {"query", "path"}:
        observations = list(rest.observations)
        changes = {"query": (("orderId", "202"),)} if kind == "query" else {"path": "/unknown"}
        observations[0] = observations[0].model_copy(update=changes)
        rest = rest.model_copy(update={"observations": tuple(observations)})
    else:
        changes = (
            {"executions_complete": True}
            if kind == "complete"
            else {"observed_at": NOW + timedelta(seconds=1)}
        )
        rest = rest.model_copy(update={"evidence": rest.evidence.model_copy(update=changes)})
    with pytest.raises(SyncError):
        collect(clock, monitor, session, lambda ids: (rest,))
    assert monitor.status()["resync_required"]
    assert monitor.status()["reconciled_executions"] == 0


@pytest.mark.parametrize("action", ["event", "disconnect", "reconnect", "expire", "exception"])
def test_order_collection_races_and_failure_cannot_publish_verified_state(setup, action):
    clock, monitor, session = setup

    def orders(ids):
        assert ids == (201,)
        if action == "event":
            monitor.ingest(session, 2, raw(execution()))
        elif action == "disconnect":
            monitor.disconnect(session)
        elif action == "reconnect":
            monitor.start_session()
        elif action == "expire":
            clock.advance(31)
        else:
            raise RuntimeError("secret")
        return (read_order(clock),)

    with pytest.raises(SyncError) as error:
        collect(clock, monitor, session, orders)
    assert "secret" not in str(error.value)
    assert monitor.status()["resync_required"]
    assert monitor.status()["reconciled_executions"] == 0


def test_event_delivery_does_not_wait_for_order_reads(setup):
    clock, monitor, session = setup
    entered, release = threading.Event(), threading.Event()

    def orders(ids):
        entered.set()
        assert release.wait(5)
        return (read_order(clock),)

    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(collect, clock, monitor, session, orders)
        try:
            assert entered.wait(5)
            monitor.ingest(session, 2, raw(execution()))
        finally:
            release.set()
        with pytest.raises(SyncError, match="changed_during_collection"):
            pending.result(timeout=5)


def test_balance_difference_is_not_cleared_by_matching_executions(setup):
    clock, monitor, session = setup
    collect(clock, monitor, session)
    result = monitor.resync(
        session,
        lambda: report(clock, balance="999996"),
        collect_orders=lambda ids: (read_order(clock),),
    )
    assert "balance_change_unverified" in result.mismatches
    assert not result.complete
    assert monitor.status()["reconciled_executions"] == 0


def test_account_collection_race_prevents_followup_order_reads(setup):
    clock, monitor, session = setup

    def account():
        monitor.ingest(session, 2, raw(execution()))
        return report(clock)

    with pytest.raises(SyncError):
        monitor.resync(session, account, collect_orders=lambda ids: pytest.fail("stale read"))


def test_reconciled_execution_status_expires_with_account_observation(setup):
    clock, monitor, session = setup
    collect(clock, monitor, session)
    clock.advance(31)
    status = monitor.status()
    assert status["reconciled_executions"] == 0
    assert status["unverified_executions"] == 1
    assert "execution_events_not_reconciled" in status["blockers"]


def test_journaled_capture_forwards_order_collector_but_never_restores_proof(tmp_path):
    clock = Clock()
    journal = EventJournal.create(tmp_path / "journal", "synthetic")
    capture = JournaledEventCapture(
        journal, clock=lambda: clock.wall, monotonic_ns=lambda: int(clock.mono * 1e9)
    )
    capture.start_session(expected_head=journal.inspect()["head"])
    capture.ingest(1, raw(execution()))
    result = capture.resync(lambda: report(clock), collect_orders=lambda ids: (read_order(clock),))
    assert result.execution_reconciliation.matched_execution_ids == (501,)
    assert not capture.status()["unverified_executions"]
    reopened = EventJournal(tmp_path / "journal", "synthetic").replay()
    assert reopened["resync_required"]
    assert not reopened["live_enabled"]
