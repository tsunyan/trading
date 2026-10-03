"""Bounded lag tolerance while preserving fatal execution and storage contradictions."""

import socket
from decimal import Decimal

import pytest
from test_account_events import order, raw
from test_account_sync import position, report
from test_execution_reconciliation import read_order
from test_private_supervisor import fill, settle, setup, start

from trading.private_supervisor import SupervisorError, SupervisorPolicy


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("real network attempted"))


def collect_again(clock, runner):
    clock.advance(1)
    runner.step()
    settle(runner)


def test_rest_cancel_before_notification_retries_then_accepts_without_reconnect(tmp_path):
    clock, _, book, control, runner, sockets, _, calls, _, _ = setup(tmp_path, max_records=64)
    runner._collect = lambda: report(clock, units=None, orders=True, balance="1000000")
    start(runner)
    settle(runner)
    prior = runner._last_result
    assert control.snapshot()["sync_successes"] == 1
    runner._collect = lambda: report(clock, units=None, orders=False, balance="1000000")
    clock.advance(15)
    runner.step()
    settle(runner)
    assert runner._last_result is None and prior is not None
    assert runner.status()["pending_sync_reasons"] == ("order_change_without_event:201",)
    assert control.snapshot()["phase"] == "RUNNING"
    assert control.snapshot()["sync_successes"] == 1
    sockets[0].messages.append(
        raw(
            order(
                orderStatus="CANCELED",
                msgType="COR",
                cancelType="USER",
                orderTimestamp=clock.wall.isoformat(),
            )
        )
    )
    assert runner.step()
    collect_again(clock, runner)
    assert control.snapshot()["sync_successes"] == 2
    assert runner.status()["consecutive_sync_retries"] == 0
    assert runner.status()["pending_sync_reasons"] == ()
    assert runner.status()["connection"] == 1
    assert book.snapshot()["executions"] == 0
    runner.close()
    assert [method for method, _ in calls].count("POST") == 1


def test_rest_position_before_notification_retries_until_matching_notice(tmp_path):
    clock, _, _, control, runner, sockets, _, _, _, _ = setup(tmp_path, max_records=64)
    runner._collect = lambda: report(clock, units=400, orders=False, balance="1000000")
    start(runner)
    settle(runner)
    runner._collect = lambda: report(clock, units=300, orders=False, balance="1000000")
    clock.advance(15)
    runner.step()
    settle(runner)
    assert "position_change_without_event:401" in runner.status()["pending_sync_reasons"]
    assert control.snapshot()["sync_successes"] == 1
    sockets[0].messages.append(position(units=300, timestamp=clock.wall))
    runner.step()
    collect_again(clock, runner)
    assert control.snapshot()["sync_successes"] == 2
    assert not runner._last_result.complete and not runner._last_result.live_enabled
    runner.close()


def test_notification_before_rest_fill_does_not_post_until_evidence_arrives(tmp_path):
    clock, _, book, control, runner, sockets, _, _, rows, _ = setup(tmp_path, max_records=64)
    start(runner)
    settle(runner)
    row = fill(clock, 0)
    rows.append(row)
    sockets[0].messages.append(raw(row))
    runner.step()
    original = runner._options["collect_orders"]
    runner._options["collect_orders"] = lambda ids: (read_order(clock, [row], empty=True),)
    collect_again(clock, runner)
    assert "execution_missing_from_rest:501" in runner.status()["pending_sync_reasons"]
    assert control.snapshot()["sync_successes"] == 1
    assert book.snapshot()["executions"] == 0
    runner._options["collect_orders"] = original
    collect_again(clock, runner)
    assert control.snapshot()["sync_successes"] == 2
    assert book.snapshot()["executions"] == 1
    assert Decimal(book.snapshot()["balance"]) == 999998
    collect_again(clock, runner)
    assert book.snapshot()["executions"] == 1
    runner.close()


def test_completed_order_still_in_active_rest_is_retried_before_cash_posting(tmp_path):
    clock, _, book, control, runner, sockets, _, _, rows, _ = setup(tmp_path, max_records=64)
    start(runner)
    settle(runner)
    row = fill(clock, 0)
    rows.append(row)
    sockets[0].messages.append(raw(row))
    runner.step()
    runner._collect = lambda: report(clock, units=None, orders=True, balance="999998")
    collect_again(clock, runner)
    assert "executed_order_still_active:201" in runner.status()["pending_sync_reasons"]
    assert book.snapshot()["executions"] == 0
    runner._collect = lambda: report(clock, units=None, orders=False, balance="999998")
    collect_again(clock, runner)
    assert control.snapshot()["sync_successes"] == 2
    assert book.snapshot()["executions"] == 1
    runner.close()


@pytest.mark.parametrize("kind", ["balance", "position"])
def test_transient_account_inventory_difference_does_not_record_success(tmp_path, kind):
    from trading.execution_positions import PositionBasis

    clock, _, book, control, runner, _, _, _, _, _ = setup(
        tmp_path,
        max_records=64,
        position_basis=PositionBasis(positions=()),
    )
    start(runner)
    settle(runner)
    baseline_head = book.snapshot()["head"]
    runner._collect = lambda: report(
        clock,
        units=400 if kind == "position" else None,
        orders=False,
        balance="1000001" if kind == "balance" else "1000000",
    )
    clock.advance(15)
    runner.step()
    settle(runner)
    assert control.snapshot()["sync_successes"] == 1
    assert control.snapshot()["phase"] == "RUNNING"
    assert book.snapshot()["head"] == baseline_head
    runner._collect = lambda: report(clock, units=None, orders=False, balance="1000000")
    # A delayed position removal is also required to explain the REST structure.
    if kind == "position":
        runner._receiver._socket.messages.append(
            position(units=400, timestamp=clock.wall, msg="CPR")
        )
        runner.step()
    collect_again(clock, runner)
    assert control.snapshot()["sync_successes"] == 2
    assert book.snapshot()["head"] == baseline_head
    runner.close()


@pytest.mark.parametrize("kind", ["structure", "balance", "position"])
def test_persistent_difference_exhausts_budget_and_keeps_last_result_unusable(tmp_path, kind):
    from trading.execution_positions import PositionBasis

    clock, _, _, control, runner, _, _, calls, _, _ = setup(
        tmp_path,
        max_records=64,
        policy=SupervisorPolicy(max_sync_retries=1),
        position_basis=PositionBasis(positions=()),
    )
    runner._collect = lambda: report(clock, units=None, orders=True, balance="1000000")
    start(runner)
    settle(runner)
    runner._collect = lambda: report(
        clock,
        units=400 if kind == "position" else None,
        orders=kind != "structure",
        balance="1000001" if kind == "balance" else "1000000",
    )
    clock.advance(15)
    runner.step()
    settle(runner)
    assert runner._last_result is None
    assert control.snapshot()["sync_retries"] == 1
    with pytest.raises(SupervisorError, match="sync_failed"):
        collect_again(clock, runner)
    assert control.snapshot()["phase"] == "STOPPED"
    assert control.snapshot()["sync_successes"] == 1
    assert runner._last_result is None
    assert [method for method, _ in calls].count("POST") == 1


@pytest.mark.parametrize("kind", ["fill_fields", "order_identity"])
def test_execution_contradiction_stops_immediately_without_using_lag_budget(tmp_path, kind):
    clock, _, book, control, runner, sockets, _, _, rows, _ = setup(tmp_path, max_records=64)
    start(runner)
    settle(runner)
    row = fill(clock, 0)
    rows.append(row)
    sockets[0].messages.append(raw(row))
    runner.step()
    runner._options["collect_orders"] = lambda ids: (
        read_order(
            clock,
            [row],
            fill_changes={"price": "150.001"} if kind == "fill_fields" else None,
            order_changes={"rootOrderId": 999}
            if kind == "order_identity"
            else {"status": "EXECUTED"},
        ),
    )
    with pytest.raises(SupervisorError, match="sync_failed"):
        collect_again(clock, runner)
    assert control.snapshot()["phase"] == "STOPPED"
    assert control.snapshot()["sync_retries"] == 0
    assert book.snapshot()["executions"] == 0


def test_cash_head_change_is_fatal_even_when_structure_is_waiting_for_notification(tmp_path):
    clock, _, _, control, runner, _, _, _, _, _ = setup(tmp_path, max_records=64)
    runner._collect = lambda: report(clock, units=None, orders=True, balance="1000000")
    start(runner)
    settle(runner)
    runner._collect = lambda: report(clock, units=None, orders=False, balance="1000000")
    clock.advance(15)
    runner.step()
    runner._worker.join(timeout=3)
    stream, connection, finished, (kind, result) = runner._results.get_nowait()
    runner._results.put_nowait(
        (
            stream,
            connection,
            finished,
            (
                kind,
                result.model_copy(
                    update={"booked_cash_head": "unexpected-head"},
                ),
            ),
        )
    )
    with pytest.raises(SupervisorError, match="sync_failed"):
        runner.step()
    assert control.snapshot()["phase"] == "STOPPED"
    assert control.snapshot()["sync_retries"] == 0
