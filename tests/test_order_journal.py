import json
import socket
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from trading.broker_contracts import BrokerResponseError, OrderIntent, OrderLimits
from trading.execution_lab import demo, fixture_evidence
from trading.order_journal import OrderBlocked, OrderJournal

NOW = datetime(2026, 9, 29, tzinfo=UTC)


@pytest.fixture
def journal(tmp_path):
    return OrderJournal.create(
        tmp_path / "lab",
        OrderLimits(
            min_units=100,
            max_units=1000,
            unit_step=100,
            price_tick="0.001",
            max_reference_notional="200000",
        ),
    )


@pytest.fixture
def intent():
    return OrderIntent(
        client_id="Test001", side="BUY", effect="OPEN", units=1000, kind="LIMIT", price="150"
    )


def fill(identifier=301, units="500"):
    return {
        "executionId": identifier,
        "positionId": 401,
        "size": units,
        "price": "150",
        "fee": "1.5",
        "lossGain": "0",
        "settledSwap": "0",
        "timestamp": NOW.isoformat(),
    }


def evidence(intent, status="ORDERED", fills=(), seconds=0):
    return fixture_evidence(intent, 101, 201, status, fills, NOW + timedelta(seconds=seconds))


def submitted(journal, intent):
    journal.prepare(intent)
    journal.begin_submission(intent.client_id)


def test_demo_offline_no_socket_and_restart(tmp_path, monkeypatch):
    def no_network(*args, **kwargs):
        raise AssertionError("network is forbidden")

    monkeypatch.setattr(socket, "socket", no_network)
    report = demo(tmp_path / "demo")
    assert report["live_enabled"] is False and report["synthetic_only"] is True
    assert report["resend_blocked_after_restart"]
    assert [o["state"] for o in report["orders"]] == ["FILLED", "FILLED"]
    assert json.loads((tmp_path / "demo" / "report.json").read_text())["synthetic_only"]
    with pytest.raises(FileExistsError):
        demo(tmp_path / "demo")


def test_intent_idempotency_and_conflicting_reuse(journal, intent):
    assert journal.prepare(intent) == journal.prepare(intent) == "PREPARED"
    assert len(journal.snapshot()["events"]) == 1
    with pytest.raises(OrderBlocked, match="reused"):
        journal.prepare(intent.model_copy(update={"units": 500}))


def test_restart_after_claim_blocks_resend_and_new_order(journal, intent):
    submitted(journal, intent)
    restarted = OrderJournal(journal.path.parent)
    with pytest.raises(OrderBlocked, match="resend"):
        restarted.begin_submission(intent.client_id)
    with pytest.raises(OrderBlocked, match="unresolved"):
        restarted.prepare(intent.model_copy(update={"client_id": "Other"}))
    restarted.unknown(intent.client_id)
    with pytest.raises(OrderBlocked, match="resend"):
        restarted.begin_submission(intent.client_id)
    with pytest.raises(OrderBlocked, match="abandon"):
        restarted.abandon(intent.client_id)


def test_concurrent_claim_exactly_one_local_winner(journal, intent):
    journal.prepare(intent)

    def claim(_):
        try:
            OrderJournal(journal.path.parent).begin_submission(intent.client_id)
            return True
        except OrderBlocked:
            return False

    with ThreadPoolExecutor(max_workers=4) as pool:
        assert sum(pool.map(claim, range(4))) == 1


def test_partial_and_duplicate_fill_reconciliation(journal, intent):
    submitted(journal, intent)
    first = evidence(intent, fills=[fill(), fill()])
    assert journal.reconcile(first) == "PARTIAL"
    events = len(journal.snapshot()["events"])
    assert journal.reconcile(first) == "PARTIAL"
    assert len(journal.snapshot()["events"]) == events
    assert (
        journal.reconcile(
            evidence(
                intent,
                "EXECUTED",
                [fill(), fill(302)],
                seconds=1,
            )
        )
        == "FILLED"
    )
    assert len(journal.snapshot()["orders"][0]["evidence"]["executions"]) == 2


def test_executed_without_all_fills_and_unproven_completeness_block(journal, intent):
    submitted(journal, intent)
    assert journal.reconcile(evidence(intent, "EXECUTED", [fill()])) == "RECONCILING"
    incomplete = evidence(intent, "EXECUTED", [fill(), fill(302)], 1).model_copy(
        update={"executions_complete": False},
    )
    assert journal.reconcile(incomplete) == "RECONCILING"
    with pytest.raises(OrderBlocked):
        journal.prepare(intent.model_copy(update={"client_id": "Other"}))


@pytest.mark.parametrize("terminal", ["CANCELED", "EXPIRED"])
def test_partial_then_terminal_preserves_fills(journal, intent, terminal):
    submitted(journal, intent)
    journal.reconcile(evidence(intent, fills=[fill()]))
    assert journal.reconcile(evidence(intent, terminal, [fill()], 1)) == terminal
    assert journal.snapshot()["orders"][0]["evidence"]["executions"][0]["units"] == 500


def test_cancel_ack_not_completion_and_fill_can_win_race(journal, intent):
    submitted(journal, intent)
    journal.reconcile(evidence(intent))
    journal.begin_cancel(intent.client_id)
    assert journal.cancellation_response(
        intent.client_id,
        {
            "status": 0,
            "data": {"success": [{"rootOrderId": 101, "clientOrderId": intent.client_id}]},
        },
    )
    assert journal.snapshot()["orders"][0]["state"] == "CANCEL_PENDING"
    assert journal.reconcile(evidence(intent, seconds=1)) == "CANCEL_PENDING"
    with pytest.raises(OrderBlocked):
        journal.begin_cancel(intent.client_id)
    assert journal.reconcile(evidence(intent, "EXECUTED", [fill(units="1000")], 2)) == "FILLED"


@pytest.mark.parametrize("mutation", ["stale", "changed_fill", "missing_fill", "other_id"])
def test_mismatches_persist_halt_and_preserve_last_good_state(journal, intent, mutation):
    submitted(journal, intent)
    first = evidence(intent, fills=[fill()], seconds=1)
    journal.reconcile(first)
    later = evidence(intent, fills=[fill()], seconds=2)
    updates = {
        "stale": {"observed_at": NOW},
        "changed_fill": {
            "executions": (later.executions[0].model_copy(update={"fee": Decimal(2)}),)
        },
        "missing_fill": {"executions": ()},
        "other_id": {"order_id": 999},
    }
    with pytest.raises(OrderBlocked):
        journal.reconcile(later.model_copy(update=updates[mutation]))
    state = OrderJournal(journal.path.parent).snapshot()
    assert state["halted"] and state["orders"][0]["state"] == "PARTIAL"
    assert state["orders"][0]["evidence"] == first.model_dump(mode="json")


def test_missing_raw_order_halts_not_rejected(journal, intent):
    submitted(journal, intent)
    with pytest.raises(ValueError):
        journal.reconcile_responses(
            intent.client_id, {"status": 0, "data": []}, {"status": 0, "data": []}, NOW
        )
    assert journal.snapshot()["halted"]
    assert journal.snapshot()["orders"][0]["state"] == "SUBMITTING"


def test_halt_blocks_submissions_but_allows_cancel_and_reconcile(journal, intent):
    submitted(journal, intent)
    journal.reconcile(evidence(intent))
    journal.halt()
    journal.begin_cancel(intent.client_id)
    assert journal.reconcile(evidence(intent, "CANCELED", seconds=1)) == "CANCELED"
    with pytest.raises(OrderBlocked, match="halted"):
        journal.prepare(intent.model_copy(update={"client_id": "Other"}))


def test_unsubmitted_abandon_and_never_reuse(journal, intent):
    journal.prepare(intent)
    journal.abandon(intent.client_id)
    assert journal.prepare(intent) == "ABANDONED"
    with pytest.raises(OrderBlocked):
        journal.begin_submission(intent.client_id)


def test_late_fill_after_final_cancel_halts(journal, intent):
    submitted(journal, intent)
    journal.reconcile(evidence(intent, "CANCELED"))
    with pytest.raises(OrderBlocked, match="final evidence"):
        journal.reconcile(evidence(intent, "CANCELED", [fill()], 1))
    assert journal.snapshot()["halted"]


def test_existing_nonlab_directory_untouched(tmp_path):
    with pytest.raises(sqlite3.OperationalError):
        OrderJournal(tmp_path)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("partial", [False, True])
def test_cancel_rejection_restores_confirmed_state_for_retry(journal, intent, partial):
    submitted(journal, intent)
    executions = [fill()] if partial else []
    journal.reconcile(evidence(intent, fills=executions))
    journal.begin_cancel(intent.client_id)
    assert not journal.cancellation_response(
        intent.client_id,
        {"status": 0, "data": {"success": [], "failed": [{"rootOrderId": 101}]}},
    )
    journal = OrderJournal(journal.path.parent)
    assert journal.snapshot()["orders"][0]["state"] == ("PARTIAL" if partial else "WORKING")
    journal.begin_cancel(intent.client_id)
    assert journal.reconcile(evidence(intent, "CANCELED", executions, 1)) == "CANCELED"
    journal.prepare(intent.model_copy(update={"client_id": "Next001"}))


@pytest.mark.parametrize("status", ["EXECUTED", "CANCELED", "EXPIRED"])
def test_terminal_reobservation_does_not_mutate_journal(journal, intent, status):
    submitted(journal, intent)
    original = evidence(intent, status, [fill(units="1000")] if status == "EXECUTED" else [])
    journal.reconcile(original)
    before = journal.snapshot()
    journal.reconcile(original.model_copy(update={"observed_at": NOW + timedelta(seconds=1)}))
    assert journal.snapshot() == before


def test_malformed_cancel_response_keeps_pending_state(journal, intent):
    submitted(journal, intent)
    journal.reconcile(evidence(intent))
    journal.begin_cancel(intent.client_id)
    with pytest.raises(ValueError):
        journal.cancellation_response(intent.client_id, {})
    assert journal.snapshot()["orders"][0]["state"] == "CANCEL_PENDING"


@pytest.mark.parametrize("partial", [False, True])
def test_cancel_api_error_recovers_by_reconciliation_after_restart(journal, intent, partial):
    submitted(journal, intent)
    executions = [fill()] if partial else []
    journal.reconcile(evidence(intent, fills=executions))
    journal.begin_cancel(intent.client_id)
    with pytest.raises(BrokerResponseError):
        journal.cancellation_response(intent.client_id, {"status": 1, "messages": []})
    journal = OrderJournal(journal.path.parent)
    assert journal.snapshot()["orders"][0]["state"] == "CANCEL_PENDING"
    with pytest.raises(OrderBlocked, match="unresolved"):
        journal.prepare(intent.model_copy(update={"client_id": "Other"}))
    journal.unknown(intent.client_id)
    assert journal.reconcile(evidence(intent, fills=executions, seconds=1)) == (
        "PARTIAL" if partial else "WORKING"
    )
    journal.begin_cancel(intent.client_id)
    assert journal.reconcile(evidence(intent, "CANCELED", executions, 2)) == "CANCELED"
    assert not journal.snapshot()["halted"]


def test_different_client_ids_cannot_bypass_unresolved_order_concurrently(journal, intent):
    def prepare(i):
        try:
            journal.prepare(intent.model_copy(update={"client_id": f"Concurrent{i}"}))
            return True
        except OrderBlocked:
            return False

    with ThreadPoolExecutor(max_workers=4) as pool:
        assert sum(pool.map(prepare, range(4))) == 1


def test_broker_id_collision_halts(journal, intent):
    submitted(journal, intent)
    journal.reconcile(evidence(intent, "EXECUTED", [fill(units="1000")]))
    other = intent.model_copy(update={"client_id": "Other"})
    submitted(journal, other)
    with pytest.raises(OrderBlocked, match="already bound"):
        journal.reconcile(evidence(other))
    assert journal.snapshot()["halted"]


def test_closing_wrong_position_rejected_before_state_update(journal):
    from trading.broker_contracts import Settlement

    close = OrderIntent(
        client_id="Close",
        side="SELL",
        effect="CLOSE",
        units=1000,
        kind="LIMIT",
        price="150",
        positions=(Settlement(position_id=999, units=1000),),
    )
    submitted(journal, close)
    with pytest.raises(ValueError, match="closing position"):
        evidence(close, "EXECUTED", [fill(units="1000")])


def test_terminal_cannot_regress_or_silently_add_execution(journal, intent):
    submitted(journal, intent)
    journal.reconcile(evidence(intent, "EXECUTED", [fill(units="1000")]))
    with pytest.raises(OrderBlocked, match="terminal broker status"):
        journal.reconcile(evidence(intent, "ORDERED", [fill(units="1000")], 1))
    assert journal.snapshot()["halted"]
