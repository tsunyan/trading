"""Position replay, exact cost, atomic rejection and compatibility with v1."""

import json
import socket
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from decimal import Decimal, localcontext

import pytest
from test_account_events import NOW, execution, raw
from test_account_sync import Clock, report
from test_execution_reconciliation import read_order

from trading.account_events import parse_event
from trading.execution_cash_book import (
    CashBookError,
    ExecutionCashBatch,
    ExecutionCashBook,
    OpeningCash,
)
from trading.execution_positions import OpeningPosition, PositionBasis


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("network attempted"))


def create(tmp_path, *, positions=(), tolerance="0"):
    return ExecutionCashBook.create(
        tmp_path / "book",
        "synthetic",
        OpeningCash(
            balance="1000000",
            cutoff=NOW - timedelta(seconds=1),
            position_basis=PositionBasis(positions=positions, realized_pnl_tolerance=tolerance),
        ),
    )


def batch(*rows):
    clock = Clock()
    clock.wall = max(parse_event(raw(row), NOW + timedelta(days=1)).occurred_at for row in rows)
    grouped = {}
    for row in rows:
        grouped.setdefault(row["orderId"], []).append(row)
    return ExecutionCashBatch(
        events=tuple(parse_event(raw(row), clock.wall) for row in rows),
        reports=tuple(read_order(clock, group) for group in grouped.values()),
    )


def close(
    *,
    units=400,
    price="150.1",
    pnl="40",
    side="SELL",
    identity=601,
    order_id=301,
    position_id=401,
    seconds=1,
    swap="0",
    **changes,
):
    with localcontext() as context:
        context.prec = 80
        amount = str(Decimal(pnl) - 2 + Decimal(swap))
    return execution(
        executionId=identity,
        orderId=order_id,
        rootOrderId=order_id,
        clientOrderId=f"Close{order_id}",
        positionId=position_id,
        settleType="CLOSE",
        side=side,
        orderSize=str(units),
        orderExecutedSize=str(units),
        executionSize=str(units),
        orderPrice=price,
        executionPrice=price,
        lossGain=pnl,
        settledSwap=swap,
        amount=amount,
        executionTimestamp=(NOW + timedelta(seconds=seconds)).isoformat(),
        **changes,
    )


def test_long_partial_and_full_close_rebuild_positions_and_cash_on_reopen(tmp_path):
    book = create(tmp_path)
    result = book.apply(batch(execution()))
    assert result["position_accounting_applied"]
    assert result["positions"][0]["units"] == 400
    book.apply(batch(close(units=100, price="150.2", pnl="20")))
    state = ExecutionCashBook(book.path.parent, "synthetic").snapshot()
    assert state["positions"][0]["units"] == 300
    assert state["positions"][0]["average_price_exact"] == {"numerator": "150", "denominator": "1"}
    book.apply(
        batch(close(units=300, price="149.9", pnl="-30", identity=602, order_id=302, seconds=2))
    )
    state = ExecutionCashBook(book.path.parent, "synthetic").snapshot()
    assert not state["positions"]
    assert state["position_realized_pnl_exact"] == {"numerator": "-10", "denominator": "1"}
    assert Decimal(state["balance"]) == 999_984
    assert "position_accounting_not_applied" not in state["blockers"]
    assert "opening_position_boundary_not_verified" in state["blockers"]
    assert not state["complete"] and not state["live_enabled"]


def test_short_profit_and_negative_settled_swap_have_correct_sign(tmp_path):
    book = create(tmp_path)
    book.apply(batch(execution(side="SELL")))
    book.apply(batch(close(side="BUY", price="149.5", pnl="200", swap="-3")))
    state = book.snapshot()
    assert not state["positions"]
    assert Decimal(state["balance"]) == 1_000_193
    assert state["position_realized_pnl_exact"]["numerator"] == "200"


def test_declared_starting_position_can_close_without_reinventing_its_opening_fill(tmp_path):
    seed = OpeningPosition(position_id=401, side="BUY", units=400, average_price="150")
    book = create(tmp_path, positions=(seed,))
    book.apply(batch(close()))
    assert not book.snapshot()["positions"]
    assert Decimal(book.snapshot()["balance"]) == 1_000_038


def test_exact_weighted_cost_survives_partial_closes_and_low_decimal_context(tmp_path):
    book = create(tmp_path, tolerance="0.00000001")
    first = execution(executionSize="2", orderSize="3", orderExecutedSize="2", orderPrice="151")
    second = execution(
        executionId=502,
        executionSize="1",
        executionPrice="151",
        orderSize="3",
        orderExecutedSize="3",
        orderPrice="151",
    )
    opening_batch = batch(first, second)
    partial_batch = batch(close(units=1, price="152", pnl="1.66666667"))
    final_batch = batch(
        close(units=2, price="152", pnl="3.33333333", identity=602, order_id=302, seconds=2)
    )
    with localcontext() as context:
        context.prec = 4
        book.apply(opening_batch)
        before = book.snapshot()["positions"][0]
        assert before["average_price_exact"] == {"numerator": "451", "denominator": "3"}
        book.apply(partial_batch)
        remaining = book.snapshot()["positions"][0]
        assert remaining["average_price_exact"] == before["average_price_exact"]
        assert book.snapshot()["position_pnl_rounding_difference_exact"] == {
            "numerator": "1",
            "denominator": "300000000",
        }
        book.apply(final_batch)
        state = book.snapshot()
        assert state["position_realized_pnl_exact"] == {"numerator": "5", "denominator": "1"}
        assert state["position_pnl_rounding_difference_exact"] == {
            "numerator": "0",
            "denominator": "1",
        }
        assert Decimal(state["balance"]) == 999_997


@pytest.mark.parametrize("kind", ["missing", "side", "overclose", "pnl", "open_pnl", "open_swap"])
def test_invalid_position_accounting_rolls_back_cash_evidence_and_inventory(tmp_path, kind):
    book = create(tmp_path)
    if kind != "missing":
        book.apply(batch(execution()))
    before = book.snapshot()
    if kind == "missing":
        row = close()
    elif kind == "side":
        row = close(side="BUY")
    elif kind == "overclose":
        row = close(units=500, pnl="50")
    elif kind == "pnl":
        row = close(pnl="41")
    else:
        extra = (
            {"lossGain": "1", "amount": "-1"}
            if kind == "open_pnl"
            else {"settledSwap": "1", "amount": "-1"}
        )
        row = execution(
            executionId=503,
            orderId=203,
            rootOrderId=203,
            clientOrderId="AnotherOpen",
            executionTimestamp=(NOW + timedelta(seconds=1)).isoformat(),
            **extra,
        )
    with pytest.raises(CashBookError, match="position_"):
        book.apply(batch(row))
    assert book.snapshot() == before
    assert ExecutionCashBook(book.path.parent, "synthetic").snapshot() == before


def test_missing_open_can_be_supplied_with_close_as_one_chronologically_replayed_batch(tmp_path):
    book = create(tmp_path)
    closing = close(identity=100)  # Storage ID order is intentionally opposite to economic time.
    result = book.apply(batch(closing, execution()))
    assert result["applied_execution_ids"] == (100, 501)
    assert not result["positions"]
    assert Decimal(result["balance"]) == 1_000_036
    assert ExecutionCashBook(book.path.parent, "synthetic").snapshot()["positions"] == ()


def test_late_old_open_cannot_rewrite_a_previously_valid_close_pnl(tmp_path):
    book = create(tmp_path)
    book.apply(batch(execution(executionTimestamp=(NOW + timedelta(seconds=1)).isoformat())))
    book.apply(batch(close(seconds=2)))
    before = book.snapshot()
    old = execution(
        executionId=502,
        orderId=202,
        rootOrderId=202,
        clientOrderId="LateOpen",
        executionPrice="149",
        orderPrice="150",
    )
    with pytest.raises(CashBookError, match="position_realized_pnl_mismatch"):
        book.apply(batch(old))
    assert book.snapshot() == before


def test_same_time_open_close_is_rejected_but_same_time_opens_commute(tmp_path):
    book = create(tmp_path)
    with pytest.raises(CashBookError, match="position_event_order_ambiguous"):
        book.apply(batch(execution(), close(seconds=0)))
    assert book.snapshot()["executions"] == 0
    second = execution(executionId=502, orderExecutedSize="800")
    book.apply(batch(second, execution()))
    assert book.snapshot()["positions"][0]["units"] == 800


def test_fully_closed_position_id_cannot_be_reused(tmp_path):
    book = create(tmp_path)
    book.apply(batch(execution(), close()))
    before = book.snapshot()
    reused = execution(
        executionId=502,
        orderId=202,
        rootOrderId=202,
        clientOrderId="ReusedOpen",
        executionTimestamp=(NOW + timedelta(seconds=2)).isoformat(),
    )
    with pytest.raises(CashBookError, match="position_open_identity_conflict"):
        book.apply(batch(reused))
    assert book.snapshot() == before


@pytest.mark.parametrize("version", [1, 2])
def test_seed_version_is_bound_and_v1_canonical_body_has_no_implicit_flat_positions(
    tmp_path, version
):
    if version == 1:
        book = ExecutionCashBook.create(
            tmp_path / "book",
            "synthetic",
            OpeningCash(balance="1000000", cutoff=NOW - timedelta(seconds=1)),
        )
        with pytest.raises(CashBookError, match="cash_book_position_basis_required"):
            book.compare_positions(report(Clock()))
    else:
        book = create(tmp_path)
    with sqlite3.connect(book.path) as conn:
        actual_version, body = conn.execute("SELECT version,opening FROM book").fetchone()
        assert actual_version == version
        assert ("position_basis" in json.loads(body)) == (version == 2)
        conn.execute("UPDATE book SET version=?", (3 - version,))
    with pytest.raises(CashBookError, match="cash_book_integrity_failed"):
        ExecutionCashBook(book.path.parent, "synthetic")


def test_duplicate_opening_positions_are_rejected_before_storage_creation(tmp_path):
    seed = OpeningPosition(position_id=401, side="BUY", units=400, average_price="150")
    with pytest.raises(CashBookError, match="invalid_cash_book_opening"):
        create(tmp_path, positions=(seed, seed))
    assert not (tmp_path / "book").exists()


@pytest.mark.parametrize("kind", ["match", "missing", "units", "price", "side", "unexpected"])
def test_rest_inventory_is_compared_without_importing_or_repairing_it(tmp_path, kind):
    book = create(tmp_path)
    book.apply(batch(execution()))
    before = book.snapshot()
    source = report(Clock(), units=None if kind == "missing" else 500 if kind == "units" else 400)
    if kind in {"price", "side", "unexpected"}:
        changes = (
            {"price": Decimal("151")}
            if kind == "price"
            else {"side": "SELL"}
            if kind == "side"
            else {"position_id": 402}
        )
        source = source.model_copy(
            update={"positions": (source.positions[0].model_copy(update=changes),)}
        )
    result = book.compare_positions(source)
    assert result["position_match"] == (kind == "match")
    assert not result["complete"] and not result["live_enabled"]
    assert "position_reservations_not_reconciled" in result["blockers"]
    assert book.snapshot() == before


def test_rest_rounded_price_needs_explicit_tolerance_and_duplicate_or_stale_reports_fail(tmp_path):
    book = create(tmp_path)
    first = execution(executionSize="2", orderSize="3", orderExecutedSize="2", orderPrice="151")
    second = execution(
        executionId=502,
        executionSize="1",
        executionPrice="151",
        orderPrice="151",
        orderSize="3",
        orderExecutedSize="3",
    )
    book.apply(batch(first, second))
    source = report(Clock(), units=3)
    source = source.model_copy(
        update={
            "positions": (
                source.positions[0].model_copy(update={"price": Decimal("150.33333333")}),
            )
        }
    )
    assert not book.compare_positions(source)["position_match"]
    assert book.compare_positions(source, price_tolerance_jpy=Decimal("0.00000001"))[
        "position_match"
    ]
    duplicated = source.model_copy(update={"positions": source.positions * 2})
    with pytest.raises(CashBookError, match="cash_book_position_report_invalid"):
        book.compare_positions(duplicated)
    book.apply(batch(close(units=3, price="152", pnl="5")))
    with pytest.raises(CashBookError, match="cash_book_balance_report_before_postings"):
        book.compare_positions(source)


def test_concurrent_duplicate_postings_keep_one_inventory_and_proof(tmp_path):
    book = create(tmp_path)
    source = batch(execution())

    def apply(_):
        return ExecutionCashBook(book.path.parent, "synthetic").apply(source)

    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(apply, range(3)))
    assert sum(bool(r["applied_execution_ids"]) for r in results) == 1
    state = book.snapshot()
    assert state["executions"] == state["proofs"] == 1
    assert state["positions"][0]["units"] == 400


def test_multiple_starting_positions_close_independently(tmp_path):
    seeds = (
        OpeningPosition(position_id=401, side="BUY", units=400, average_price="150"),
        OpeningPosition(position_id=402, side="SELL", units=200, average_price="150"),
    )
    book = create(tmp_path, positions=seeds)
    book.apply(
        batch(
            close(),
            close(
                units=200,
                position_id=402,
                side="BUY",
                price="149.8",
                pnl="40",
                identity=602,
                order_id=302,
            ),
        )
    )
    state = book.snapshot()
    assert not state["positions"]
    assert state["position_realized_pnl_exact"] == {"numerator": "80", "denominator": "1"}
    assert Decimal(state["balance"]) == 1_000_076


def test_position_precision_capacity_rejects_without_truncating_or_posting(tmp_path, monkeypatch):
    import trading.execution_positions as positions

    monkeypatch.setattr(positions, "MAX_RATIO_BITS", 8)
    book = create(tmp_path)
    first = execution(executionSize="2", orderSize="3", orderExecutedSize="2", orderPrice="151")
    book.apply(batch(first))
    before = book.snapshot()
    second = execution(
        executionId=502,
        executionSize="1",
        executionPrice="151",
        orderPrice="151",
        orderSize="3",
        orderExecutedSize="3",
    )
    with pytest.raises(CashBookError, match="position_precision_capacity"):
        book.apply(batch(first, second))
    assert book.snapshot() == before


def test_position_basis_changes_and_oversized_opening_are_rejected_on_reopen(tmp_path):
    book = create(tmp_path)
    with sqlite3.connect(book.path) as conn:
        body = json.loads(conn.execute("SELECT opening FROM book").fetchone()[0])
        body["position_basis"]["realized_pnl_tolerance"] = "1.00000000"
        conn.execute(
            "UPDATE book SET opening=?", (json.dumps(body, sort_keys=True, separators=(",", ":")),)
        )
    with pytest.raises(CashBookError, match="cash_book_integrity_failed"):
        ExecutionCashBook(book.path.parent, "synthetic")
    with sqlite3.connect(book.path) as conn:
        conn.execute("UPDATE book SET opening=?", ("x" * 1_000_001,))
    with pytest.raises(CashBookError, match="cash_book_integrity_failed"):
        ExecutionCashBook(book.path.parent, "synthetic")


def test_existing_sync_path_uses_declared_position_basis(tmp_path):
    from test_execution_cash_sync import capture_for, resync

    from trading.event_journal import EventJournal

    book = create(tmp_path)
    clock = Clock()
    journal = EventJournal.create(tmp_path / "journal", "synthetic")
    capture = capture_for(journal, clock)
    capture.ingest(1, raw(execution()))
    accepted = resync(clock, capture, book)
    assert accepted.execution_cash["position_accounting_applied"]
    assert accepted.execution_cash["positions"][0]["units"] == 400
    assert book.compare_positions(accepted.report)["position_match"]
    assert not accepted.complete and not accepted.live_enabled


def test_position_demo_shows_matching_open_and_unexplained_stale_rest_inventory(tmp_path):
    from trading.execution_cash_lab import demo

    result = demo(tmp_path / "demo", position_accounting=True)
    assert result["position_before_close"]["position_match"]
    assert not result["position_after_close_with_unchanged_rest"]["position_match"]
    assert result["position_after_close_with_unchanged_rest"]["mismatches"] == (
        "position_unexpected:401",
    )
    assert not result["snapshot"]["positions"]
    assert result["snapshot"]["position_realized_pnl_exact"] == {
        "numerator": "40",
        "denominator": "1",
    }


def test_cash_and_position_evidence_roll_back_together_on_sql_failure(tmp_path):
    book = create(tmp_path)
    before = book.snapshot()
    with sqlite3.connect(book.path) as conn:
        conn.execute(
            "CREATE TRIGGER reject_post BEFORE INSERT ON postings WHEN new.sequence>0 "
            "BEGIN SELECT RAISE(ABORT,'simulated failure'); END"
        )
    with pytest.raises(CashBookError, match="cash_book_storage_failed"):
        book.apply(batch(execution()))
    assert ExecutionCashBook(book.path.parent, "synthetic").snapshot() == before


def test_process_exit_after_v2_commit_reopens_exact_inventory_and_duplicate_is_noop(tmp_path):
    script = """
import os, sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from trading.execution_cash_book import ExecutionCashBook, OpeningCash
from trading.execution_cash_lab import synthetic_batch
from trading.execution_positions import PositionBasis
now = datetime(2026, 10, 2, tzinfo=UTC)
book = ExecutionCashBook.create(Path(sys.argv[1]), 'synthetic',
    OpeningCash(balance='1000000', cutoff=now-timedelta(seconds=1),
                position_basis=PositionBasis(positions=())))
book.apply(synthetic_batch(now))
os._exit(17)
"""
    path = tmp_path / "crash"
    process = subprocess.run(
        [sys.executable, "-c", script, str(path)], capture_output=True, timeout=15
    )
    assert process.returncode == 17, process.stderr.decode()
    from trading.execution_cash_lab import synthetic_batch

    book = ExecutionCashBook(path, "synthetic")
    before = book.snapshot()
    assert before["positions"][0]["units"] == 400
    assert before["position_realized_pnl_exact"] == {"numerator": "0", "denominator": "1"}
    result = book.apply(synthetic_batch(NOW.replace(month=10, day=2)))
    assert result["already_applied_execution_ids"] == (501,)
    assert not result["applied_execution_ids"]
    assert book.snapshot() == before
