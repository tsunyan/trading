"""Submission claims whose order is accepted and still active at the broker."""

import socket

import httpx
import pytest
from test_account_guard import account, fill, quote
from test_order_resolution import acceptance, reopen
from test_private_cancel import working
from test_private_order import client, ready
from test_private_order import setup as live_setup

from trading.account_guard import Position, WorkingOrder
from trading.execution_lab import fixture_evidence
from trading.live_journal import (
    ACTIVE_RESOLUTION_CONFIRMATIONS,
    RESOLUTION_CONFIRMATIONS,
    LiveOrderError,
)
from trading.private_order import OrderTransportError


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("real network attempted"))


@pytest.fixture
def setup(tmp_path):
    return live_setup.__wrapped__(tmp_path)


def unknown_submission(setup):
    clock, _, _, journal = setup
    order = ready(setup)
    with client(setup, lambda _: httpx.Response(500)) as sender:
        with pytest.raises(OrderTransportError):
            sender.submit(order.client_id, quote=quote(clock.now))
    clock.advance(1)
    return order


def active(setup, order, *, partial=False, status="ORDERED", complete=True):
    clock, _, _, journal = setup
    fills = [fill(size="400", fee="-2", timestamp=clock.now.isoformat())] if partial else []
    evidence = fixture_evidence(order, 101, 201, status, fills, clock.now)
    if not complete:
        evidence = evidence.model_copy(update={"executions_complete": False})
    journal.reconcile(evidence)
    values = {
        "working_orders": (
            WorkingOrder(
                client_id=order.client_id, order_id=201, remaining_units=600 if partial else 1000
            ),
        )
    }
    if partial:
        values.update(
            balance="999998",
            equity="999994",
            required_margin="2400.16",
            available_margin="997593.84",
            positions=(Position(position_id=401, side="BUY", units=400, average_price="150.01"),),
        )
    if complete:
        journal.update_account(account(clock.now, **values), quote(clock.now), now=clock.now)
    else:
        # Incomplete history cannot reconcile the account; the old proof stays behind it.
        with pytest.raises(ValueError, match="unresolved_order|incomplete_executions"):
            journal.update_account(account(clock.now, **values), quote(clock.now), now=clock.now)
    return evidence


@pytest.mark.parametrize("partial", [False, True])
def test_active_submission_claim_resolves_and_keeps_the_working_order(setup, partial):
    clock, _, posts, journal = setup
    order = unknown_submission(setup)
    evidence = active(setup, order, partial=partial)
    before = journal.snapshot()
    assert before["orders"][0]["state"] == ("PARTIAL" if partial else "WORKING")
    context = journal.order_resolution_context(order.client_id)
    assert context["active_state"] == before["orders"][0]["state"]
    assert context["terminal_state"] is None
    approved = acceptance(setup, order)
    # The terminal confirmation set never resolves a still-active order.
    with pytest.raises(LiveOrderError, match="confirmations_required"):
        journal.resolve_order_claim(
            order.client_id, approved, confirmations=RESOLUTION_CONFIRMATIONS
        )
    assert posts.snapshot()["claim"] is not None
    result = journal.resolve_order_claim(
        order.client_id, approved, confirmations=ACTIVE_RESOLUTION_CONFIRMATIONS
    )
    assert result["post_claim_resolved"] and result["restart_required"]
    _, _, fresh, journal = reopen(setup)
    post = fresh.snapshot()
    assert post["phase"] == "STOPPED" and post["claim"] is None
    after = journal.snapshot()
    assert after["halted"] and after["orders"] == before["orders"]
    assert after["orders"][0]["evidence"] == evidence.model_dump(mode="json")
    # The resolved order can be found again by a restart context, never resent.
    assert journal.restart_context()["checkpoint_sha256"]


@pytest.mark.parametrize("change", ["incomplete", "stale_account", "waiting_executed"])
def test_active_resolution_requires_complete_history_and_matching_account(setup, change):
    clock, _, posts, journal = setup
    order = unknown_submission(setup)
    if change == "incomplete":
        active(setup, order, complete=False)
    elif change == "stale_account":
        active(setup, order)
        clock.advance(61)
    else:
        active(setup, order, status="WAITING")
        import sqlite3

        from trading.broker_contracts import OrderEvidence

        row = journal.snapshot()["orders"][0]
        damaged = OrderEvidence.model_validate(row["evidence"]).model_copy(
            update={"status": "EXECUTED"}
        )
        with sqlite3.connect(journal.path) as conn:
            conn.execute("UPDATE orders SET evidence_json=?", (damaged.model_dump_json(),))
    before = posts.snapshot()
    with pytest.raises(ValueError):
        journal.order_resolution_context(order.client_id)
    assert posts.snapshot() == before


def test_terminal_resolution_refuses_the_active_confirmation_set(setup):
    from test_order_resolution import terminal

    _, _, posts, journal = setup
    order, _ = terminal(setup)
    with pytest.raises(LiveOrderError, match="confirmations_required"):
        journal.resolve_order_claim(
            order.client_id,
            acceptance(setup, order),
            confirmations=ACTIVE_RESOLUTION_CONFIRMATIONS,
        )
    assert posts.snapshot()["claim"] is not None


def test_unknown_cancel_on_a_still_active_order_is_not_resolved(setup):
    clock, _, posts, journal = setup
    order, _ = working(setup)
    with client(setup, lambda _: httpx.Response(500)) as sender:
        with pytest.raises(OrderTransportError):
            sender.cancel(order.client_id)
    clock.advance(1)
    active(setup, order)
    before = posts.snapshot()
    with pytest.raises(LiveOrderError, match="order_resolution_terminal_evidence_required"):
        journal.order_resolution_context(order.client_id)
    assert posts.snapshot() == before and before["claim"] is not None
