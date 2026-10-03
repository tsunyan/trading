"""Reservations use declared allocation minus booked fills, never active total size."""

import socket
from datetime import timedelta
from decimal import Decimal, localcontext

import pytest
from test_account_events import NOW
from test_account_sync import Clock, report
from test_execution_positions import batch, close

from trading.account_reader import ActiveOrder, Observation, OrderReadReport
from trading.broker_contracts import OrderEvidence, OrderIntent, Settlement
from trading.cash_transfer_lab import synthetic_match
from trading.cash_transfers import CashTransferPolicy
from trading.execution_cash_book import CashBookError, ExecutionCashBook, OpeningCash
from trading.execution_positions import OpeningPosition, PositionBasis
from trading.position_reservation_lab import demo


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("network attempted"))


def create(tmp_path, *, basis=True, transfers=False, positions=None):
    return ExecutionCashBook.create(
        tmp_path / "book",
        "synthetic",
        OpeningCash(
            balance="1000000",
            cutoff=NOW - timedelta(seconds=1),
            position_basis=PositionBasis(
                positions=positions
                or (OpeningPosition(position_id=401, side="BUY", units=400, average_price="150"),)
            )
            if basis
            else None,
            transfer_policy=CashTransferPolicy(
                primary_source="synthetic-broker", confirmation_source="synthetic-bank"
            )
            if transfers
            else None,
        ),
    )


def order(
    *,
    identity=201,
    units=300,
    allocations=None,
    effect="CLOSE",
    side="SELL",
    status="ORDERED",
    fills=(),
    now=NOW,
):
    intent = OrderIntent(
        client_id=f"Known{identity}",
        side=side,
        effect=effect,
        units=units,
        kind="LIMIT",
        price="150.1",
        positions=tuple(
            Settlement(position_id=pid, units=q)
            for pid, q in (
                allocations
                if allocations is not None
                else ((401, units),)
                if effect == "CLOSE"
                else ()
            )
        ),
    )
    return OrderReadReport(
        evidence=OrderEvidence(
            intent=intent,
            root_order_id=identity,
            order_id=identity,
            status=status,
            observed_at=now,
            executions=fills,
            executions_complete=False,
        ),
        observations=tuple(
            Observation(
                path="/v1/orders" if i % 2 == 0 else "/v1/executions",
                query=(("orderId", str(identity)),),
                response_at=now,
                received_at=now,
                sha256=str(i + 1) * 64,
            )
            for i in range(4)
        ),
    )


def account(*orders, ordered=300, units=400, now=NOW):
    clock = Clock()
    clock.wall = now
    source = report(clock, units=units, orders=False, balance="1000000")
    return source.model_copy(
        update={
            "positions": tuple(
                p.model_copy(update={"ordered_units": ordered}) for p in source.positions
            ),
            "active_orders": tuple(
                ActiveOrder(
                    root_order_id=r.evidence.root_order_id,
                    order_id=r.evidence.order_id,
                    client_id=r.evidence.intent.client_id,
                    symbol="USD_JPY",
                    side=r.evidence.intent.side,
                    effect=r.evidence.intent.effect,
                    kind="LIMIT",
                    units=r.evidence.intent.units,
                    price=r.evidence.intent.price,
                    status=r.evidence.status,
                    timestamp=now,
                )
                for r in orders
            ),
        }
    )


def test_matching_close_allocation_is_read_only_and_survives_reopen(tmp_path):
    book = create(tmp_path)
    evidence = order()
    before = book.snapshot()
    result = book.compare_reservations(account(evidence), (evidence,))
    assert result["reservation_match"] and not result["mismatches"]
    assert result["reservations"] == (
        {
            "position_id": 401,
            "book_units": 400,
            "expected_ordered_units": 300,
            "observed_ordered_units": 300,
            "unreserved_units": 100,
        },
    )
    assert result["head"] == before["head"]
    assert not result["complete"] and not result["live_enabled"]
    assert "reservation_execution_history_not_proven" in result["blockers"]
    assert book.snapshot() == before
    assert (
        ExecutionCashBook(book.path.parent, "synthetic").compare_reservations(
            account(evidence), (evidence,)
        )
        == result
    )


def partial(book):
    source = batch(partial_row())
    book.apply(source)
    return source.reports[0]


def partial_row():
    return {**close(units=100, pnl="10", seconds=0), "orderSize": "300", "orderExecutedSize": "100"}


def test_partial_close_subtracts_booked_fill_per_position(tmp_path):
    book = create(tmp_path)
    evidence = partial(book)
    with localcontext() as context:
        context.prec = 3
        result = book.compare_reservations(account(evidence, units=300, ordered=200), (evidence,))
    assert result["reservation_match"]
    assert result["orders"][0]["remaining_units"] == 200
    assert result["reservations"][0]["unreserved_units"] == 100


@pytest.mark.parametrize("status", ["CANCELED", "EXPIRED"])
def test_terminal_evidence_releases_remaining_allocation_without_reverting_fill(tmp_path, status):
    book = create(tmp_path)
    evidence = partial(book)
    terminal = evidence.model_copy(
        update={"evidence": evidence.evidence.model_copy(update={"status": status})}
    )
    before = book.snapshot()
    result = book.compare_reservations(account(units=300, ordered=0), (terminal,))
    assert result["reservation_match"] and not result["orders"]
    assert result["reservations"][0]["expected_ordered_units"] == 0
    assert book.snapshot() == before


def test_multiple_close_orders_and_positions_sum_allocations(tmp_path):
    book = create(
        tmp_path,
        positions=(
            OpeningPosition(position_id=401, side="BUY", units=400, average_price="150"),
            OpeningPosition(position_id=402, side="BUY", units=500, average_price="150"),
        ),
    )
    first = order(units=400, allocations=((401, 200), (402, 200)))
    second = order(identity=202, units=100)
    source = account(first, second)
    source = source.model_copy(
        update={
            "positions": (
                source.positions[0],
                source.positions[0].model_copy(
                    update={"position_id": 402, "units": 500, "ordered_units": 200}
                ),
            )
        }
    )
    result = book.compare_reservations(source, (first, second))
    assert result["reservation_match"]
    assert [r["expected_ordered_units"] for r in result["reservations"]] == [300, 200]


def test_open_order_does_not_reserve_held_position(tmp_path):
    book = create(tmp_path)
    evidence = order(effect="OPEN", side="BUY", units=1000)
    result = book.compare_reservations(account(evidence, ordered=0), (evidence,))
    assert result["reservation_match"] and result["orders"][0]["remaining_units"] == 1000
    assert result["orders"][0]["positions"] == ()


@pytest.mark.parametrize("kind", ["unknown", "wrong_side", "overflow", "quantity", "inventory"])
def test_unknown_and_excessive_reservations_remain_differences(tmp_path, kind):
    book = create(tmp_path)
    evidence = (
        order(allocations=((999, 300),))
        if kind == "unknown"
        else order(side="BUY" if kind == "wrong_side" else "SELL")
    )
    orders = (evidence, order(identity=202, units=200)) if kind == "overflow" else (evidence,)
    source = account(
        *orders, ordered=0 if kind == "quantity" else 300, units=399 if kind == "inventory" else 400
    )
    before = book.snapshot()
    result = book.compare_reservations(source, orders)
    assert not result["reservation_match"] and result["mismatches"]
    assert "position_reservation_difference_unexplained" in result["blockers"]
    assert book.snapshot() == before


@pytest.mark.parametrize("status", ["WAITING", "MODIFYING"])
def test_uncertain_order_states_do_not_produce_expected_reservations(tmp_path, status):
    book = create(tmp_path)
    evidence = order(status=status)
    result = book.compare_reservations(account(evidence), (evidence,))
    assert result["unverified_order_ids"] == (201,)
    assert result["reservations"][0]["expected_ordered_units"] is None


def test_missing_order_report_never_assumes_zero_reservation(tmp_path):
    book = create(tmp_path)
    result = book.compare_reservations(account(order()), ())
    assert not result["reservation_match"] and result["unverified_order_ids"] == (201,)
    assert result["reservations"][0]["unreserved_units"] is None


def test_unbooked_close_execution_is_not_used_or_automatically_applied(tmp_path):
    book = create(tmp_path)
    evidence = batch(partial_row()).reports[0]
    before = book.snapshot()
    result = book.compare_reservations(account(evidence, ordered=200), (evidence,))
    assert result["mismatches"] == ("reservation_execution_not_booked:601",)
    assert result["reservations"][0]["expected_ordered_units"] is None
    assert book.snapshot() == before


@pytest.mark.parametrize(
    "kind", ["missing", "changed", "intent", "active_missing", "active_fields"]
)
def test_history_and_identity_differences_do_not_change_or_halt_book(tmp_path, kind):
    book = create(tmp_path)
    evidence = partial(book)
    source = account(evidence, units=300, ordered=200)
    if kind == "missing":
        evidence = evidence.model_copy(
            update={"evidence": evidence.evidence.model_copy(update={"executions": ()})}
        )
    elif kind == "changed":
        changed = evidence.evidence.executions[0].model_copy(update={"fee": Decimal("3")})
        evidence = evidence.model_copy(
            update={"evidence": evidence.evidence.model_copy(update={"executions": (changed,)})}
        )
    elif kind == "intent":
        evidence = evidence.model_copy(
            update={
                "evidence": evidence.evidence.model_copy(
                    update={
                        "intent": evidence.evidence.intent.model_copy(
                            update={"price": Decimal("149.9")}
                        )
                    }
                )
            }
        )
    elif kind == "active_missing":
        source = account(units=300, ordered=0)
    else:
        source = source.model_copy(
            update={
                "active_orders": (
                    source.active_orders[0].model_copy(update={"root_order_id": 999}),
                )
            }
        )
    before = book.snapshot()
    result = book.compare_reservations(source, (evidence,))
    assert not result["reservation_match"] and result["unverified_order_ids"] == (301,)
    assert book.snapshot() == before


@pytest.mark.parametrize(
    "kind", ["short", "query", "hash", "path", "after_account", "late_receipt", "complete"]
)
def test_invalid_order_observations_fail_with_fixed_code(tmp_path, kind):
    book = create(tmp_path)
    evidence = order()
    if kind == "complete":
        evidence = evidence.model_copy(
            update={"evidence": evidence.evidence.model_copy(update={"executions_complete": True})}
        )
    else:
        observations = evidence.observations
        if kind == "short":
            observations = observations[:3]
        else:
            changes = {
                "query": {"query": (("orderId", "999"),)},
                "hash": {"sha256": "private input"},
                "path": {"path": "/v1/activeOrders"},
                "after_account": {
                    "response_at": NOW + timedelta(seconds=1),
                    "received_at": NOW + timedelta(seconds=1),
                },
                "late_receipt": {"received_at": NOW + timedelta(seconds=1)},
            }[kind]
            observations = (observations[0].model_copy(update=changes), *observations[1:])
        evidence = evidence.model_copy(update={"observations": observations})
    before = book.snapshot()
    with pytest.raises(CashBookError, match="^cash_book_reservation_report_invalid$"):
        book.compare_reservations(account(order()), (evidence,))
    assert book.snapshot() == before


def test_v1_requires_position_basis_and_v3_uses_combined_head_and_boundary(tmp_path):
    book = create(tmp_path, basis=False)
    with pytest.raises(CashBookError, match="cash_book_position_basis_required"):
        book.compare_reservations(account(ordered=0), ())
    other = create(tmp_path / "other", transfers=True)
    other.apply_transfers((synthetic_match(NOW + timedelta(seconds=1)),))
    with pytest.raises(CashBookError, match="cash_book_balance_report_before_postings"):
        other.compare_reservations(account(ordered=0), ())
    result = other.compare_reservations(account(ordered=0, now=NOW + timedelta(seconds=2)), ())
    assert result["reservation_match"] and result["head"] == other.snapshot()["head"]


def test_order_evidence_before_latest_booking_is_rejected(tmp_path):
    book = create(tmp_path)
    partial(book)
    evidence = order(now=NOW - timedelta(microseconds=1))
    with pytest.raises(CashBookError, match="cash_book_reservation_report_before_postings"):
        book.compare_reservations(account(evidence, units=300), (evidence,))


@pytest.mark.parametrize(
    "kind",
    [
        "duplicate_order",
        "duplicate_position",
        "bad_units",
        "huge_money",
        "list",
        "capacity",
        "naive",
    ],
)
def test_unvalidated_models_and_input_bounds_are_checked(tmp_path, kind):
    book = create(tmp_path)
    evidence, source = order(), account(order())
    orders = (evidence,)
    if kind == "duplicate_order":
        orders = (evidence, evidence)
    elif kind == "duplicate_position":
        source = source.model_copy(update={"positions": source.positions * 2})
    elif kind == "bad_units":
        source = source.model_copy(
            update={"positions": (source.positions[0].model_copy(update={"ordered_units": 401}),)}
        )
    elif kind == "huge_money":
        source = source.model_copy(
            update={
                "active_orders": (
                    source.active_orders[0].model_copy(update={"price": Decimal("1e30")}),
                )
            }
        )
    elif kind == "list":
        orders = [evidence]
    elif kind == "capacity":
        orders = (evidence,) * 1001
    else:
        evidence = evidence.model_copy(
            update={
                "evidence": evidence.evidence.model_copy(
                    update={"observed_at": NOW.replace(tzinfo=None)}
                )
            }
        )
        orders = (evidence,)
    with pytest.raises(CashBookError, match="^cash_book_reservation_report_invalid$"):
        book.compare_reservations(source, orders)


def test_executed_terminal_order_and_empty_inventory_have_zero_reservation(tmp_path):
    book = create(tmp_path)
    source = batch(close(seconds=0))
    book.apply(source)
    terminal = source.reports[0].model_copy(
        update={"evidence": source.reports[0].evidence.model_copy(update={"status": "EXECUTED"})}
    )
    result = book.compare_reservations(account(units=None, ordered=0), (terminal,))
    assert result["reservation_match"] and result["reservations"] == ()


def test_fully_filled_order_still_active_is_a_difference(tmp_path):
    book = create(tmp_path)
    source = batch(close(seconds=0))
    book.apply(source)
    result = book.compare_reservations(account(source.reports[0], units=None), source.reports)
    assert not result["reservation_match"]
    assert "reservation_active_order_fully_filled:301" in result["mismatches"]


def test_short_position_is_reserved_by_buy_close(tmp_path):
    book = create(
        tmp_path,
        positions=(OpeningPosition(position_id=401, side="SELL", units=400, average_price="150"),),
    )
    evidence = order(side="BUY")
    source = account(evidence)
    source = source.model_copy(
        update={"positions": (source.positions[0].model_copy(update={"side": "SELL"}),)}
    )
    assert book.compare_reservations(source, (evidence,))["reservation_match"]


def test_no_orders_and_unexplained_reservation_do_not_cancel_each_other(tmp_path):
    book = create(tmp_path)
    assert book.compare_reservations(account(ordered=0), ())["reservation_match"]
    result = book.compare_reservations(account(ordered=1), ())
    assert result["mismatches"] == ("reservation_units_mismatch:401",)


def test_client_identity_cannot_be_reused_for_another_order(tmp_path):
    book = create(tmp_path)
    booked = partial(book)
    evidence = order(identity=999, units=100)
    evidence = evidence.model_copy(
        update={
            "evidence": evidence.evidence.model_copy(
                update={
                    "intent": evidence.evidence.intent.model_copy(
                        update={"client_id": booked.evidence.intent.client_id}
                    )
                }
            )
        }
    )
    result = book.compare_reservations(account(evidence, units=300, ordered=100), (evidence,))
    assert "reservation_client_identity_conflict:999" in result["mismatches"]


def test_clock_skew_is_explicit_and_limited(tmp_path):
    book = create(tmp_path)
    evidence = order()
    evidence = evidence.model_copy(
        update={
            "observations": tuple(
                o.model_copy(update={"received_at": NOW - timedelta(milliseconds=10)})
                for o in evidence.observations
            )
        }
    )
    with pytest.raises(CashBookError, match="cash_book_reservation_report_invalid"):
        book.compare_reservations(account(order()), (evidence,))
    assert book.compare_reservations(account(order()), (evidence,), clock_skew_ms=10)[
        "reservation_match"
    ]


def test_bound_evidence_bytes_and_corruption_fail_closed(tmp_path, monkeypatch):
    import sqlite3

    import trading.execution_cash_book as module

    book = create(tmp_path)
    evidence = order()
    before = book.snapshot()
    with monkeypatch.context() as context:
        context.setattr(module, "MAX_PROOF", 100)
        with pytest.raises(CashBookError, match="cash_book_reservation_report_invalid"):
            book.compare_reservations(account(evidence), (evidence,))
    assert book.snapshot() == before
    with sqlite3.connect(book.path) as conn:
        conn.execute("UPDATE postings SET amount='1' WHERE account='cash'")
    with pytest.raises(CashBookError, match="cash_book_integrity_failed"):
        book.compare_reservations(account(evidence), (evidence,))


def test_offline_demo_matches_partial_restart_and_cancel_without_overwrite(tmp_path):
    result = demo(tmp_path / "demo")
    for name in ("initial", "partial", "restart", "cancelled"):
        assert result[name]["reservation_match"]
    assert result["initial"]["reservations"][0]["expected_ordered_units"] == 300
    assert result["partial"]["reservations"][0]["expected_ordered_units"] == 200
    assert result["cancelled"]["reservations"][0]["expected_ordered_units"] == 0
    assert not result["unexplained_reservation"]["reservation_match"]
    assert result["snapshot"]["balance"] == "1000008.00000000"
    with pytest.raises(FileExistsError):
        demo(tmp_path / "demo")
