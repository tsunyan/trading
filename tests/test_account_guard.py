import socket
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from trading.account_guard import (
    AccountPolicy,
    AccountQuote,
    AccountSnapshot,
    Position,
    WorkingOrder,
    evaluate_risk,
    expected_account,
)
from trading.broker_contracts import OrderIntent, OrderLimits, Settlement
from trading.execution_lab import fixture_evidence
from trading.order_journal import OrderBlocked, OrderJournal

NOW = datetime(2026, 9, 29, 10, tzinfo=UTC)


def policy(**changes):
    values = dict(
        account_id="fixture-account",
        bootstrap_at=NOW - timedelta(hours=1),
        starting_balance="1000000",
        max_order_notional="500000",
        max_gross_notional="1000000",
        max_leverage="1",
        max_loss_jpy="20000",
        max_drawdown="0.05",
        margin_rate="0.04",
        min_margin_ratio="2",
        min_available_margin="100000",
        fee_buffer_rate="0.00002",
        max_spread="0.05",
    )
    return AccountPolicy(**{**values, **changes})


def quote(time=NOW, **changes):
    return AccountQuote(
        **{
            "bid": "150",
            "ask": "150.01",
            "observed_at": time,
            "market_open": True,
            **changes,
        }
    )


def account(time=NOW, **changes):
    return AccountSnapshot(
        **{
            "account_id": "fixture-account",
            "observed_at": time,
            "complete": True,
            "balance": "1000000",
            "equity": "1000000",
            "required_margin": "0",
            "available_margin": "1000000",
            **changes,
        }
    )


def intent(**changes):
    return OrderIntent(
        **{
            "client_id": "Buy001",
            "side": "BUY",
            "effect": "OPEN",
            "units": 1000,
            "kind": "LIMIT",
            "price": "150.01",
            **changes,
        }
    )


def make_journal(tmp_path, **changes):
    return OrderJournal.create(
        tmp_path / "guarded",
        OrderLimits(
            min_units=100,
            max_units=5000,
            unit_step=100,
            price_tick="0.001",
            max_reference_notional="1000000",
        ),
        account_policy=policy(**changes),
    )


def fill(execution_id=301, **changes):
    return {
        "executionId": execution_id,
        "positionId": 401,
        "size": "1000",
        "price": "150.01",
        "fee": "3",
        "lossGain": "0",
        "settledSwap": "0",
        "timestamp": (NOW + timedelta(seconds=1)).isoformat(),
        **changes,
    }


def open_position(journal):
    order = intent()
    journal.update_account(account(), quote(), now=NOW)
    journal.prepare(order)
    journal.begin_submission(order.client_id, quote=quote(), now=NOW)
    journal.reconcile(
        fixture_evidence(
            order,
            101,
            201,
            "EXECUTED",
            [fill()],
            NOW + timedelta(seconds=1),
        )
    )


def positioned_account(time=NOW + timedelta(seconds=2), **changes):
    return account(
        time,
        **{
            "balance": "999997",
            "equity": "999987",
            "required_margin": "6000.4",
            "available_margin": "993986.6",
            "positions": (
                Position(position_id=401, side="BUY", units=1000, average_price="150.01"),
            ),
            **changes,
        },
    )


def test_guard_cannot_be_omitted_or_removed_after_restart(tmp_path):
    journal = make_journal(tmp_path)
    journal.prepare(intent())
    with pytest.raises(OrderBlocked, match="proof"):
        journal.begin_submission("Buy001")
    journal.update_account(account(), quote(), now=NOW)
    with pytest.raises(OrderBlocked, match="quote"):
        OrderJournal(journal.path.parent).begin_submission("Buy001", now=NOW)
    with sqlite3.connect(journal.path) as conn:
        conn.execute("DELETE FROM account_gate")
    with pytest.raises(OrderBlocked, match="missing"):
        OrderJournal(journal.path.parent)


@pytest.mark.parametrize("target", ["quote", "account"])
@pytest.mark.parametrize("seconds", [-61, 1])
def test_account_clock_rejection_can_recover(tmp_path, target, seconds):
    journal = make_journal(tmp_path)
    bad_time = NOW + timedelta(seconds=seconds)
    with pytest.raises(OrderBlocked, match=f"stale_or_future_{target}"):
        journal.update_account(
            account(bad_time if target == "account" else NOW),
            quote(bad_time if target == "quote" else NOW),
            now=NOW,
        )
    journal = OrderJournal(journal.path.parent)
    assert not journal.snapshot()["halted"]
    journal.update_account(account(), quote(), now=NOW)
    journal.prepare(intent())
    journal.begin_submission("Buy001", quote=quote(), now=NOW)


@pytest.mark.parametrize("state", ["SUBMITTING", "UNKNOWN", "CANCEL_PENDING", "RECONCILING"])
def test_account_update_during_order_processing_can_recover(tmp_path, state):
    journal = make_journal(tmp_path)
    order = intent()
    journal.update_account(account(), quote(), now=NOW)
    journal.prepare(order)
    journal.begin_submission(order.client_id, quote=quote(), now=NOW)
    if state == "UNKNOWN":
        journal.unknown(order.client_id)
    elif state == "CANCEL_PENDING":
        journal.reconcile(fixture_evidence(order, 101, 201, "ORDERED", [], NOW))
        journal.begin_cancel(order.client_id)
    elif state == "RECONCILING":
        journal.reconcile(
            fixture_evidence(order, 101, 201, "ORDERED", [], NOW).model_copy(
                update={"executions_complete": False}
            )
        )
    with pytest.raises(OrderBlocked, match="unresolved_order"):
        journal.update_account(account(), quote(), now=NOW)
    journal = OrderJournal(journal.path.parent)
    assert not journal.snapshot()["halted"]
    with pytest.raises(OrderBlocked, match="unresolved"):
        journal.prepare(intent(client_id="Next001"))
    journal.reconcile(fixture_evidence(order, 101, 201, "CANCELED", [], NOW + timedelta(seconds=1)))
    later = NOW + timedelta(seconds=2)
    journal.update_account(account(later), quote(later), now=later)
    journal.prepare(intent(client_id="Next001"))
    journal.begin_submission("Next001", quote=quote(later), now=later)


def test_account_before_fill_evidence_retries_without_false_balance_halt(tmp_path):
    journal = make_journal(tmp_path)
    open_position(journal)
    with pytest.raises(OrderBlocked, match="account_precedes_order_evidence"):
        journal.update_account(account(), quote(), now=NOW + timedelta(seconds=2))
    assert not journal.snapshot()["halted"]
    later = NOW + timedelta(seconds=2)
    journal.update_account(positioned_account(), quote(later), now=later)


def test_terminal_reobservation_keeps_account_proof_and_allows_next_claim(tmp_path):
    journal = make_journal(tmp_path)
    open_position(journal)
    time = NOW + timedelta(seconds=2)
    journal.update_account(positioned_account(), quote(time), now=time)
    before = journal.snapshot()
    journal.reconcile(
        fixture_evidence(intent(), 101, 201, "EXECUTED", [fill()], time + timedelta(seconds=1))
    )
    assert journal.snapshot() == before
    journal.update_account(positioned_account(), quote(time), now=time)
    journal.prepare(intent(client_id="Next001"))
    journal.begin_submission("Next001", quote=quote(time), now=time)


def test_older_account_is_rejected_without_losing_latest_proof(tmp_path):
    journal = make_journal(tmp_path)
    open_position(journal)
    time = NOW + timedelta(seconds=2)
    journal.update_account(positioned_account(), quote(time), now=time)
    proof = journal.snapshot()["account_guard"]["last_proof"]
    with pytest.raises(OrderBlocked, match="account_time_moved_backwards"):
        journal.update_account(account(), quote(), now=time)
    assert not journal.snapshot()["halted"]
    assert journal.snapshot()["account_guard"]["last_proof"] == proof


def test_same_account_snapshot_can_use_a_refreshed_quote(tmp_path):
    journal = make_journal(tmp_path)
    journal.update_account(account(), quote(), now=NOW)
    time = NOW + timedelta(seconds=1)
    journal.update_account(account(), quote(time), now=time)
    assert not journal.snapshot()["halted"]


def test_valid_fresh_guarded_submission_without_network(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("network forbidden")

    monkeypatch.setattr(socket, "socket", forbidden)
    journal = make_journal(tmp_path)
    journal.update_account(account(), quote(), now=NOW)
    journal.prepare(intent())
    request = journal.begin_submission("Buy001", quote=quote(), now=NOW)
    assert request.path == "/v1/order"
    state = journal.snapshot()
    assert not state["live_enabled"] and state["account_guard"] is not None
    assert state["orders"][0]["state"] == "SUBMITTING"
    risk = [e["payload"] for e in state["events"] if e["kind"] == "RISK_CHECK"][0]
    assert Decimal(risk["projected_gross"]) == Decimal("150010")
    assert Decimal(risk["available_after_reserve"]) < Decimal("994000")


@pytest.mark.parametrize(
    "updates,reason",
    [
        ({"account_id": "other"}, "account_mismatch"),
        ({"complete": False}, "incomplete_account"),
        (
            {"balance": "999999", "equity": "999999", "available_margin": "999999"},
            "balance_mismatch",
        ),
        ({"equity": "999999", "available_margin": "999999"}, "equity_mismatch"),
        ({"available_margin": "1000001"}, "inconsistent_available"),
        (
            {
                "working_orders": (
                    WorkingOrder(client_id="External", order_id=999, remaining_units=100),
                )
            },
            "working_orders_mismatch",
        ),
        (
            {"positions": (Position(position_id=999, side="BUY", units=100, average_price="150"),)},
            "positions_mismatch",
        ),
    ],
)
def test_account_discrepancies_halt_persistently(tmp_path, updates, reason):
    journal = make_journal(tmp_path)
    journal.update_account(account(), quote(), now=NOW)
    with pytest.raises(OrderBlocked, match=reason):
        journal.update_account(account(**updates), quote(), now=NOW)
    state = OrderJournal(journal.path.parent).snapshot()
    assert state["halted"]
    assert state["events"][-1]["kind"] == "ACCOUNT_REJECTED"
    assert state["account_guard"]["last_proof"]["snapshot"]["balance"] == "1000000"
    with pytest.raises(OrderBlocked):
        journal.prepare(intent())


@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"max_order_notional": "100000"}, "order_notional_limit"),
        ({"max_gross_notional": "100000"}, "gross_notional_limit"),
        ({"max_leverage": "0.1"}, "leverage_limit"),
        ({"min_available_margin": "999000"}, "available_margin_limit"),
        ({"min_margin_ratio": "1000"}, "margin_ratio_limit"),
    ],
)
def test_risk_limits_deny_without_consuming_claim(tmp_path, changes, reason):
    journal = make_journal(tmp_path, **changes)
    journal.update_account(account(), quote(), now=NOW)
    journal.prepare(intent())
    with pytest.raises(OrderBlocked, match=reason):
        journal.begin_submission("Buy001", quote=quote(), now=NOW)
    state = journal.snapshot()
    assert state["orders"][0]["state"] == "PREPARED"
    assert not state["halted"]
    assert reason in state["events"][-1]["payload"]["reasons"]


@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"market_open": False}, "market_closed"),
        ({"ask": "150.06"}, "spread_exceeds_limit"),
        ({"observed_at": NOW - timedelta(seconds=61)}, "stale_or_future_quote"),
        ({"observed_at": NOW + timedelta(seconds=1)}, "stale_or_future_quote"),
    ],
)
def test_quote_checks_at_claim_not_just_reconciliation(tmp_path, changes, reason):
    journal = make_journal(tmp_path)
    journal.update_account(account(), quote(), now=NOW)
    journal.prepare(intent())
    with pytest.raises(OrderBlocked, match=reason):
        journal.begin_submission("Buy001", quote=quote(**changes), now=NOW)


def test_expired_account_and_proof_invalidated_by_fill(tmp_path):
    journal = make_journal(tmp_path)
    open_position(journal)
    other = intent(client_id="Buy002")
    journal.prepare(other)
    later = NOW + timedelta(seconds=2)
    with pytest.raises(OrderBlocked, match="invalidated"):
        journal.begin_submission(other.client_id, quote=quote(later), now=later)
    journal.update_account(positioned_account(), quote(later), now=later)
    with pytest.raises(OrderBlocked, match="stale_or_future_account"):
        journal.begin_submission(
            other.client_id,
            quote=quote(later + timedelta(seconds=61)),
            now=later + timedelta(seconds=61),
        )


def test_open_close_reconciles_balance_and_position_from_fills(tmp_path):
    journal = make_journal(tmp_path)
    open_position(journal)
    later = NOW + timedelta(seconds=2)
    journal.update_account(positioned_account(), quote(later), now=later)
    close = intent(
        client_id="Close001",
        side="SELL",
        effect="CLOSE",
        price="149.99",
        positions=(Settlement(position_id=401, units=1000),),
    )
    journal.prepare(close)
    journal.begin_submission(close.client_id, quote=quote(later), now=later)
    closed_at = later + timedelta(seconds=1)
    journal.reconcile(
        fixture_evidence(
            close,
            102,
            202,
            "EXECUTED",
            [
                fill(
                    302,
                    price="150",
                    lossGain="-10",
                    timestamp=closed_at.isoformat(),
                )
            ],
            closed_at,
        )
    )
    after = later + timedelta(seconds=2)
    journal.update_account(
        account(after, balance="999984", equity="999984", available_margin="999984"),
        quote(after),
        now=after,
    )
    assert not journal.snapshot()["halted"]


@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"side": "BUY"}, "invalid_closing_position"),
        ({"positions": (Settlement(position_id=999, units=1000),)}, "invalid_closing_position"),
        (
            {"units": 1100, "positions": (Settlement(position_id=401, units=1100),)},
            "closing_quantity",
        ),
    ],
)
def test_close_identity_direction_and_remaining_size(tmp_path, changes, reason):
    journal = make_journal(tmp_path)
    open_position(journal)
    later = NOW + timedelta(seconds=2)
    journal.update_account(positioned_account(), quote(later), now=later)
    close = intent(
        **{
            "client_id": "Close001",
            "side": "SELL",
            "effect": "CLOSE",
            "price": "149.99",
            "positions": (Settlement(position_id=401, units=1000),),
            **changes,
        }
    )
    journal.prepare(close)
    with pytest.raises(OrderBlocked, match=reason):
        journal.begin_submission(close.client_id, quote=quote(later), now=later)


def test_loss_stop_persists_but_valid_close_is_allowed(tmp_path):
    journal = make_journal(tmp_path, max_loss_jpy="100")
    open_position(journal)
    later = NOW + timedelta(seconds=2)
    falling = quote(later, bid="149.8", ask="149.81")
    snapshot = positioned_account(equity="999787", available_margin="993786.6")
    assert journal.update_account(snapshot, falling, now=later)["entry_halted"]
    journal = OrderJournal(journal.path.parent)
    journal.prepare(intent(client_id="Buy002"))
    with pytest.raises(OrderBlocked, match="entry_loss_halt"):
        journal.begin_submission("Buy002", quote=falling, now=later)
    journal.abandon("Buy002")
    close = intent(
        client_id="Close",
        side="SELL",
        effect="CLOSE",
        price="149.8",
        positions=(Settlement(position_id=401, units=1000),),
    )
    journal.prepare(close)
    assert journal.begin_submission("Close", quote=falling, now=later).path == "/v1/closeOrder"


def test_peak_drawdown_not_reset_by_restart_or_recovery(tmp_path):
    journal = make_journal(tmp_path, max_drawdown="0.0001", max_loss_jpy="10000")
    open_position(journal)
    later = NOW + timedelta(seconds=2)
    up = quote(later, bid="151", ask="151.01")
    journal.update_account(
        positioned_account(equity="1000987", available_margin="994986.6"), up, now=later
    )
    later += timedelta(seconds=1)
    down = quote(later, bid="150.8", ask="150.81")
    journal = OrderJournal(journal.path.parent)
    journal.update_account(
        positioned_account(later, equity="1000787", available_margin="994786.6"), down, now=later
    )
    assert journal.snapshot()["account_guard"]["entry_halted"]


def test_pending_open_reserved_and_closes_never_net_exposure():
    pending = intent(client_id="Pending", side="SELL", price="150")
    rows = [{"client_id": pending.client_id, "intent_json": pending.model_dump_json()}]
    snapshot = account(
        working_orders=(WorkingOrder(client_id="Pending", order_id=1, remaining_units=1000),)
    )
    result = evaluate_risk(
        policy(max_pending_orders=2, max_gross_notional="250000"),
        snapshot,
        quote(),
        rows,
        intent(),
        NOW,
        Decimal(1000000),
        False,
    )
    assert Decimal(result["projected_gross"]) == Decimal("300020")
    assert "gross_notional_limit" in result["reasons"]


def test_concurrent_risk_claims_still_have_single_winner(tmp_path):
    journal = make_journal(tmp_path)
    journal.update_account(account(), quote(), now=NOW)
    journal.prepare(intent())

    def claim(_):
        try:
            OrderJournal(journal.path.parent).begin_submission("Buy001", quote=quote(), now=NOW)
            return True
        except OrderBlocked:
            return False

    with ThreadPoolExecutor(max_workers=4) as pool:
        assert sum(pool.map(claim, range(4))) == 1


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
def test_nonfinite_normalized_numbers_rejected(value):
    with pytest.raises(ValidationError):
        account(balance=value)
    with pytest.raises(ValidationError):
        policy(max_leverage=value)


def test_no_unseen_positions_or_unnormalized_negative_fee(tmp_path):
    journal = make_journal(tmp_path)
    open_position(journal)
    with journal._transaction() as conn:
        rows = [dict(row) for row in conn.execute("SELECT * FROM orders")]
    import json

    evidence = json.loads(rows[0]["evidence_json"])
    evidence["executions"][0]["fee"] = "-3"
    rows[0]["evidence_json"] = json.dumps(evidence)
    with pytest.raises(ValueError, match="normalized"):
        expected_account(policy(), rows)


def test_guard_demo_no_network_and_reports_synthetic(tmp_path, monkeypatch):
    from trading.execution_lab import guard_demo

    def forbidden(*args, **kwargs):
        raise AssertionError("network forbidden")

    monkeypatch.setattr(socket, "socket", forbidden)
    result = guard_demo(tmp_path / "demo")
    assert result["synthetic_only"] and not result["live_enabled"]
    assert result["new_entry_blocked"] and result["reducing_close_allowed"]
    assert result["account_guard"]["entry_halted"]
    assert [o["state"] for o in result["orders"]] == ["FILLED", "ABANDONED", "FILLED"]
    with pytest.raises(FileExistsError):
        guard_demo(tmp_path / "demo")


def test_projected_fee_crossing_loss_limit_denies_without_latching(tmp_path):
    journal = make_journal(tmp_path, max_loss_jpy="1")
    journal.update_account(account(), quote(), now=NOW)
    journal.prepare(intent())
    with pytest.raises(OrderBlocked, match="projected_loss_limit"):
        journal.begin_submission("Buy001", quote=quote(), now=NOW)
    assert not journal.snapshot()["account_guard"]["entry_halted"]


def test_unreconciled_order_blocks_account_update(tmp_path):
    journal = make_journal(tmp_path)
    journal.update_account(account(), quote(), now=NOW)
    journal.prepare(intent())
    journal.begin_submission("Buy001", quote=quote(), now=NOW)
    with pytest.raises(OrderBlocked, match="unresolved_order"):
        journal.update_account(
            account(NOW + timedelta(seconds=1)), quote(), now=NOW + timedelta(seconds=1)
        )
    assert not journal.snapshot()["halted"]


def test_missing_working_order_and_wrong_remaining_quantity_fail(tmp_path):
    journal = make_journal(tmp_path)
    journal.update_account(account(), quote(), now=NOW)
    order = intent()
    journal.prepare(order)
    journal.begin_submission(order.client_id, quote=quote(), now=NOW)
    later = NOW + timedelta(seconds=1)
    journal.reconcile(fixture_evidence(order, 101, 201, "ORDERED", [fill(size="500")], later))
    snapshot = positioned_account(
        NOW + timedelta(seconds=2),
        equity="999992",
        available_margin="993991.6",
        positions=(Position(position_id=401, side="BUY", units=500, average_price="150.01"),),
    )
    with pytest.raises(OrderBlocked, match="working_orders_mismatch"):
        journal.update_account(snapshot, quote(snapshot.observed_at), now=snapshot.observed_at)


def test_closing_quantity_already_reserved_blocks():
    pending = intent(
        client_id="PendingClose",
        side="SELL",
        effect="CLOSE",
        price="149.9",
        positions=(Settlement(position_id=401, units=1000),),
    )
    rows = [{"client_id": pending.client_id, "intent_json": pending.model_dump_json()}]
    snapshot = positioned_account(
        working_orders=(WorkingOrder(client_id=pending.client_id, order_id=1, remaining_units=500),)
    )
    close = pending.model_copy(update={"client_id": "AnotherClose"})
    result = evaluate_risk(
        policy(max_pending_orders=2),
        snapshot,
        quote(snapshot.observed_at),
        rows,
        close,
        snapshot.observed_at,
        Decimal(1000000),
        False,
    )
    assert "closing_quantity_reserved_or_exceeded" in result["reasons"]


def test_stale_quote_cannot_move_peak_or_latch_loss():
    old = quote(NOW - timedelta(seconds=61), bid="1", ask="1.01")
    result = evaluate_risk(
        policy(), positioned_account(NOW), old, [], intent(), NOW, Decimal(1000000), False
    )
    assert not result["allowed"] and not result["entry_halted"]
    assert result["peak"] == "1000000"


def test_fresh_claim_quote_can_trigger_persistent_loss_stop(tmp_path):
    journal = make_journal(tmp_path, max_loss_jpy="100")
    open_position(journal)
    later = NOW + timedelta(seconds=2)
    journal.update_account(positioned_account(), quote(later), now=later)
    journal.prepare(intent(client_id="Buy002"))
    falling = quote(later + timedelta(seconds=1), bid="149.8", ask="149.81")
    with pytest.raises(OrderBlocked, match="entry_loss_halt"):
        journal.begin_submission("Buy002", quote=falling, now=falling.observed_at)
    assert OrderJournal(journal.path.parent).snapshot()["account_guard"]["entry_halted"]


def test_partial_fill_and_working_remainder_reconcile_together(tmp_path):
    journal = make_journal(tmp_path)
    journal.update_account(account(), quote(), now=NOW)
    order = intent()
    journal.prepare(order)
    journal.begin_submission(order.client_id, quote=quote(), now=NOW)
    journal.reconcile(
        fixture_evidence(order, 101, 201, "ORDERED", [fill(size="500")], NOW + timedelta(seconds=1))
    )
    later = NOW + timedelta(seconds=2)
    snapshot = positioned_account(
        later,
        equity="999992",
        available_margin="993991.6",
        positions=(Position(position_id=401, side="BUY", units=500, average_price="150.01"),),
        working_orders=(
            WorkingOrder(client_id=order.client_id, order_id=201, remaining_units=500),
        ),
    )
    assert journal.update_account(snapshot, quote(later), now=later)["reconciled"]
    with pytest.raises(OrderBlocked, match="unresolved"):
        journal.prepare(intent(client_id="Another"))


def test_unexplained_swap_without_position_cannot_inflate_equity(tmp_path):
    journal = make_journal(tmp_path)
    with pytest.raises(OrderBlocked, match="swap_without_positions"):
        journal.update_account(
            account(unrealized_swap="1000000", equity="2000000"), quote(), now=NOW
        )


def test_order_notional_boundary_is_inclusive(tmp_path):
    journal = make_journal(tmp_path, max_order_notional="150010")
    journal.update_account(account(), quote(), now=NOW)
    journal.prepare(intent())
    journal.begin_submission("Buy001", quote=quote(), now=NOW)
    assert journal.snapshot()["orders"][0]["state"] == "SUBMITTING"


def test_projected_spread_loss_is_reserved_before_new_order(tmp_path):
    journal = make_journal(tmp_path, max_loss_jpy="5", fee_buffer_rate="0")
    journal.update_account(account(), quote(), now=NOW)
    journal.prepare(intent())
    with pytest.raises(OrderBlocked, match="projected_loss_limit"):
        journal.begin_submission("Buy001", quote=quote(), now=NOW)
    result = journal.snapshot()["events"][-1]["payload"]
    assert Decimal(result["opening_mark_buffer"]) == Decimal("10")
    assert not result["entry_halted"]


def test_pending_fees_reserved_as_well_as_new_order():
    pending = intent(client_id="Pending")
    rows = [{"client_id": pending.client_id, "intent_json": pending.model_dump_json()}]
    snapshot = account(
        working_orders=(WorkingOrder(client_id="Pending", order_id=1, remaining_units=1000),)
    )
    result = evaluate_risk(
        policy(max_pending_orders=2),
        snapshot,
        quote(),
        rows,
        intent(),
        NOW,
        Decimal(1000000),
        False,
    )
    assert Decimal(result["fee_buffer"]) == Decimal("6.0004")
    assert Decimal(result["opening_mark_buffer"]) == Decimal("20")
