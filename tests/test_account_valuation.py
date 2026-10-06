"""Declared local model, exact economics, freshness, bounds and read-only behavior."""

import socket
import sqlite3
from datetime import timedelta
from decimal import Decimal, localcontext
from fractions import Fraction

import pytest
from test_account_events import NOW, execution
from test_account_sync import Clock, report
from test_execution_positions import batch, close
from test_position_reservations import create

from trading.account_valuation import ValuationQuote
from trading.account_valuation_lab import demo, synthetic_account, synthetic_policy
from trading.cash_transfer_lab import synthetic_match
from trading.execution_cash_book import CashBookError, ExecutionCashBook, OpeningCash
from trading.execution_positions import OpeningPosition


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("network attempted"))


def quote(now=NOW, **changes):
    return ValuationQuote(bid="149.9", ask="150.1", observed_at=now).model_copy(update=changes)


def compare(book, source=None, *, policy=None, price=None, now=NOW):
    return book.compare_valuation(
        source or synthetic_account(now),
        price or quote(now),
        policy or synthetic_policy(),
        evaluated_at=now,
    )


def exact(result, field, kind="modeled"):
    ratio = result["comparisons"][field][kind]["exact"]
    return Fraction(int(ratio["numerator"]), int(ratio["denominator"]))


def test_matched_read_only_restart_and_context_independence(tmp_path):
    book = create(tmp_path)
    before = book.snapshot()
    bytes_before = book.path.read_bytes()
    normal = compare(book)
    with localcontext() as context:
        context.prec = 2
        low_precision = compare(book)
    assert normal == low_precision
    assert normal["diagnostics_match"]
    assert exact(normal, "position_loss_gain") == -40
    assert exact(normal, "margin") == 2402
    assert exact(normal, "equity") == 999960
    assert exact(normal, "available_amount") == 997558
    assert not normal["complete"] and not normal["live_enabled"]
    assert "local_valuation_model_not_broker_verified" in normal["blockers"]
    assert normal["head"] == before["head"]
    assert book.snapshot() == before and book.path.read_bytes() == bytes_before
    assert compare(ExecutionCashBook(book.path.parent, "synthetic")) == normal


def test_both_sides_mark_at_exit_quote_and_margin_gross_without_netting(tmp_path):
    book = create(
        tmp_path,
        positions=(
            OpeningPosition(position_id=401, side="BUY", units=400, average_price="150"),
            OpeningPosition(position_id=402, side="SELL", units=400, average_price="150"),
        ),
    )
    result = compare(book)
    assert exact(result, "position_loss_gain") == -80
    assert exact(result, "margin") == 4804
    assert "position_missing:402" in result["mismatches"]
    assert [r["loss_gain"]["exact"]["numerator"] for r in result["positions"]] == ["-40", "-40"]


@pytest.mark.parametrize(
    "rounding,scope,expected",
    [
        ("CEILING", "ACCOUNT", 13),
        ("CEILING", "POSITION", 14),
        ("FLOOR", "ACCOUNT", 13),
        ("FLOOR", "POSITION", 12),
        ("HALF_EVEN", "ACCOUNT", 13),
        ("HALF_EVEN", "POSITION", 12),
    ],
)
def test_explicit_margin_rounding_scope_and_half_even_ties(tmp_path, rounding, scope, expected):
    book = create(
        tmp_path,
        positions=tuple(
            OpeningPosition(position_id=pid, side="BUY", units=1, average_price="150")
            for pid in (401, 402)
        ),
    )
    policy = synthetic_policy().model_copy(
        update={
            "margin_rounding": rounding,
            "margin_rounding_scope": scope,
        }
    )
    result = compare(book, policy=policy, price=quote(bid=Decimal("162.5"), ask=Decimal("162.5")))
    assert exact(result, "margin") == expected


def test_swap_and_signed_fee_are_explicit_inputs_not_replayed(tmp_path):
    book = create(tmp_path)
    source = synthetic_account(NOW)
    source = source.model_copy(
        update={
            "assets": source.assets.model_copy(
                update={
                    "total_swap": Decimal("-15"),
                    "estimated_trade_fee": Decimal("-3"),
                }
            ),
            "positions": (source.positions[0].model_copy(update={"total_swap": Decimal("-15")}),),
        }
    )
    disabled = compare(book, source)
    enabled = compare(
        book,
        source,
        policy=synthetic_policy().model_copy(
            update={
                "include_reported_swap": True,
                "subtract_reported_estimated_fee": True,
            }
        ),
    )
    assert exact(disabled, "equity") == 999960
    assert exact(enabled, "equity") == 999948
    assert "reported_unsettled_swap_not_rebuilt" in enabled["blockers"]
    assert "reported_fee_estimate_not_verified" in enabled["blockers"]
    broken = source.model_copy(update={"positions": ()})
    assert (
        "reported_swap_aggregate_difference"
        in compare(
            book,
            broken,
            policy=synthetic_policy().model_copy(update={"include_reported_swap": True}),
        )["mismatches"]
    )


def test_negative_equity_and_available_are_not_clamped(tmp_path):
    book = create(
        tmp_path,
        positions=(OpeningPosition(position_id=401, side="BUY", units=400, average_price="3000"),),
    )
    result = compare(book, price=quote(bid=Decimal("0.01"), ask=Decimal("999999")))
    assert exact(result, "margin") == 15999984
    assert exact(result, "equity") == -199996
    assert exact(result, "available_amount") == -16199980


@pytest.mark.parametrize(
    "rounding,expected",
    [
        ("CEILING", Fraction(9607, 4)),
        ("FLOOR", Fraction(4803, 2)),
        ("HALF_EVEN", Fraction(4803, 2)),
    ],
)
def test_fractional_margin_quantum_is_exact(tmp_path, rounding, expected):
    policy = synthetic_policy().model_copy(
        update={
            "margin_quantum_jpy": Decimal("0.25"),
            "margin_rounding": rounding,
        }
    )
    assert exact(compare(create(tmp_path), policy=policy), "margin") == expected


def test_tolerance_uses_exact_differences_not_display_rounding(tmp_path):
    book = create(tmp_path)
    source = synthetic_account(NOW, equity="999960.01")
    assert not compare(book, source)["diagnostics_match"]
    assert compare(
        book,
        source,
        policy=synthetic_policy().model_copy(
            update={
                "tolerance_jpy": Decimal("0.01"),
            }
        ),
    )["diagnostics_match"]


def test_active_total_order_size_never_becomes_remaining_margin(tmp_path):
    book = create(tmp_path)
    source = report(Clock(), balance="1000000")
    result = compare(book, source)
    assert result["active_order_ids_not_modeled"] == (201,)
    assert "active_order_margin_not_modeled" in result["mismatches"]
    assert not result["diagnostics_match"]
    assert exact(result, "margin") == 2402


@pytest.mark.parametrize(
    "change",
    [
        {"margin_rate": Decimal("0")},
        {"margin_rate": Decimal("1.01")},
        {"margin_quantum_jpy": Decimal("0")},
        {"margin_quantum_jpy": Decimal("0.000000001")},
        {"margin_rate": Decimal("NaN")},
        {"margin_rounding": "OTHER"},
        {"include_reported_swap": 1},
        {"tolerance_jpy": Decimal("1.01")},
        {"max_report_age_seconds": True},
        {"price_tolerance_jpy": Decimal("0.02")},
    ],
)
def test_policy_revalidation_is_sanitized_and_read_only(tmp_path, change):
    book = create(tmp_path)
    before = book.snapshot()
    with pytest.raises(CashBookError, match="^cash_book_valuation_input_invalid$"):
        compare(book, policy=synthetic_policy().model_copy(update=change))
    assert book.snapshot() == before


def test_quote_revalidation_and_freshness(tmp_path, subtests):
    book = create(tmp_path)
    before = book.snapshot()
    for change in [
        {"bid": Decimal("151")},
        {"ask": Decimal("Infinity")},
        {"bid": Decimal("0")},
        {"ask": Decimal("1e19")},
        {"observed_at": NOW + timedelta(microseconds=1)},
        {"observed_at": NOW - timedelta(seconds=61)},
    ]:
        with (
            subtests.test(change=change),
            pytest.raises(CashBookError, match="^cash_book_valuation_input_invalid$"),
        ):
            compare(book, price=quote(**change))
    assert book.snapshot() == before


@pytest.mark.parametrize(
    "change",
    [
        {"positions": lambda r: r.positions * 2},
        {"positions": lambda r: (r.positions[0].model_copy(update={"units": 10**12 + 1}),)},
        {"observations": lambda r: r.observations[:-1]},
        {"assets": lambda r: r.assets.model_copy(update={"margin": Decimal("1e19")})},
        {"assets": lambda r: r.assets.model_copy(update={"equity": Decimal("NaN")})},
    ],
)
def test_report_shape_caps_and_money_validation(tmp_path, change):
    source = synthetic_account(NOW)
    source = source.model_copy(update={k: f(source) for k, f in change.items()})
    with pytest.raises(CashBookError, match="^cash_book_valuation_input_invalid$"):
        compare(create(tmp_path), source)


def test_report_time_current_and_naive_evaluation_rejected(tmp_path):
    book = create(tmp_path)
    for now in (NOW - timedelta(seconds=1), NOW + timedelta(seconds=61), NOW.replace(tzinfo=None)):
        with pytest.raises(CashBookError, match="^cash_book_valuation_input_invalid$"):
            book.compare_valuation(
                synthetic_account(NOW), quote(), synthetic_policy(), evaluated_at=now
            )


def test_exact_arithmetic_capacity_failure_does_not_mutate_book(tmp_path, monkeypatch):
    book = create(tmp_path)
    before = book.snapshot()
    monkeypatch.setattr("trading.account_valuation.MAX_RATIO_BITS", 4)
    with pytest.raises(CashBookError, match="^cash_book_valuation_precision_capacity$"):
        compare(book)
    assert book.snapshot() == before


def test_cash_only_v1_refuses_implicit_flat_position_basis(tmp_path):
    with pytest.raises(CashBookError, match="cash_book_position_basis_required"):
        compare(create(tmp_path, basis=False))


def test_declared_flat_and_fractional_execution_cost_are_supported(tmp_path):
    flat = ExecutionCashBook.create(
        tmp_path / "flat",
        "synthetic",
        OpeningCash(
            balance="1000000",
            cutoff=NOW - timedelta(seconds=1),
            position_basis={"positions": (), "realized_pnl_tolerance": "0.00000001"},
        ),
    )
    assert exact(compare(flat), "margin") == 0
    first = execution(
        executionSize="1", orderExecutedSize="1", executionPrice="150", orderPrice="151"
    )
    flat.apply(batch(first))
    flat.apply(
        batch(
            first,
            execution(
                executionId=502,
                executionSize="2",
                orderExecutedSize="3",
                executionPrice="150.01",
                orderPrice="151",
            ),
        )
    )
    result = compare(flat)
    assert exact(result, "position_loss_gain") == Fraction(-8, 25)
    flat.apply(batch(close(units=1, price="150", pnl="-0.00666667")))
    later = compare(flat, now=NOW + timedelta(seconds=1))
    assert exact(later, "position_loss_gain") == Fraction(-16, 75)


def test_posted_close_changes_cash_and_held_margin(tmp_path):
    book = create(tmp_path)
    book.apply(batch(close(units=100, pnl="10", seconds=0)))
    result = compare(book)
    assert exact(result, "balance") == 1000008
    assert exact(result, "position_loss_gain") == -30
    assert exact(result, "margin") == 1802


def test_transfer_head_cash_and_both_observation_boundaries(tmp_path):
    book = create(tmp_path, transfers=True)
    before = compare(book)
    book.apply_transfers((synthetic_match(NOW, amount="100", fee="3"),))
    later = compare(book)
    assert later["head"] != before["head"]
    assert exact(later, "balance") == 1000097
    with pytest.raises(CashBookError, match="cash_book_valuation_quote_before_postings"):
        compare(book, price=quote(NOW - timedelta(microseconds=1)))
    with pytest.raises(CashBookError, match="cash_book_balance_report_before_postings"):
        compare(book, synthetic_account(NOW - timedelta(microseconds=1)))


def test_halted_and_corrupt_books_do_not_gain_permission(tmp_path):
    book = create(tmp_path)
    with sqlite3.connect(book.path) as conn:
        conn.execute("UPDATE book SET halted=1, reason='cash_book_identity_conflict'")
    result = compare(book)
    assert result["halted"] and "cash_book_halted" in result["blockers"]
    assert not result["live_enabled"]
    with sqlite3.connect(book.path) as conn:
        conn.execute("UPDATE book SET head='broken'")
    with pytest.raises(CashBookError):
        compare(book)


def test_offline_demo_and_no_overwrite(tmp_path):
    result = demo(tmp_path / "demo")
    assert result["matched"]["diagnostics_match"]
    assert result["restart"] == result["matched"]
    assert result["equity_difference"]["mismatches"] == ("local_valuation_difference:equity",)
    with pytest.raises(FileExistsError):
        demo(tmp_path / "demo")
