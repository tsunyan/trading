"""Confirmed external flows: accounting identity, stops, bounds and restart."""

import json
import socket
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from decimal import Decimal, localcontext

import pytest
from test_account_events import NOW
from test_account_sync import Clock, report
from test_execution_positions import batch, close

from trading import execution_cash_book as cash_module
from trading.cash_transfer_lab import demo, synthetic_match
from trading.cash_transfers import CashTransferPolicy
from trading.execution_cash_book import CashBookError, ExecutionCashBook, OpeningCash
from trading.execution_cash_lab import synthetic_batch
from trading.execution_positions import OpeningPosition, PositionBasis


def create(tmp_path, *, position_basis=None, balance="1000000", max_entries=5000):
    return ExecutionCashBook.create(
        tmp_path / "book",
        "synthetic",
        OpeningCash(
            balance=balance,
            cutoff=NOW - timedelta(seconds=1),
            position_basis=position_basis,
            transfer_policy=CashTransferPolicy(
                primary_source="synthetic-broker", confirmation_source="synthetic-bank"
            ),
        ),
        max_entries=max_entries,
    )


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("network attempted"))


def test_deposit_withdrawal_fees_and_three_balanced_legs_survive_restart(tmp_path):
    book = create(tmp_path)
    before = book.snapshot()
    posted = book.apply_transfers((synthetic_match(NOW),))
    assert posted["applied_transfer_ids"] == ("Deposit1",)
    assert posted["head"] != before["head"]
    withdrawn = book.apply_transfers(
        (synthetic_match(NOW, identity="W1", kind="WITHDRAWAL", amount="2500", fee="3"),)
    )
    state = ExecutionCashBook(book.path.parent, "synthetic").snapshot()
    assert withdrawn["head"] == state["head"]
    assert Decimal(state["balance"]) == 1_007_497
    assert Decimal(state["external_capital_amount"]) == 7_500
    assert Decimal(state["external_cash_amount"]) == 7_497
    assert Decimal(state["transfer_fee_debit"]) == 3
    assert state["external_cash_accounting_applied"] and not state["complete"]
    assert "external_cash_history_not_proven" in state["blockers"]
    assert "position_accounting_not_applied" in state["blockers"]
    with sqlite3.connect(book.path) as conn:
        sums = {}
        for seq, _, amount in conn.execute("SELECT * FROM transfer_postings"):
            sums[seq] = sums.get(seq, 0) + int(amount)
    assert sums == {1: 0, 2: 0}


def test_repeat_in_batch_after_restart_or_new_document_is_noop(tmp_path):
    book = create(tmp_path)
    item = synthetic_match(NOW)
    book.apply_transfers((item, item))
    before = book.snapshot()
    fresh = item.model_copy(
        update={
            "primary": item.primary.model_copy(
                update={"document_sha256": "3" * 64, "observed_at": NOW + timedelta(days=1)}
            ),
        }
    )
    result = ExecutionCashBook(book.path.parent, "synthetic").apply_transfers((fresh,))
    assert not result["applied_transfer_ids"]
    assert result["already_applied_transfer_ids"] == ("Deposit1",)
    assert book.snapshot() == before


@pytest.mark.parametrize("field", ["amount", "fee_debit", "kind", "occurred_at", "reference"])
def test_changed_confirmed_transfer_identity_persists_stop_without_changing_cash(tmp_path, field):
    book = create(tmp_path)
    item = synthetic_match(NOW)
    book.apply_transfers((item,))
    before = book.snapshot()
    if field == "reference":
        changed = item.model_copy(
            update={"primary": item.primary.model_copy(update={"reference": "Other"})}
        )
    else:
        value = {
            "amount": Decimal("10001"),
            "fee_debit": Decimal("1"),
            "kind": "WITHDRAWAL",
            "occurred_at": NOW - timedelta(microseconds=1),
        }[field]
        record = item.primary.record.model_copy(update={field: value})
        changed = item.model_copy(
            update={
                "primary": item.primary.model_copy(update={"record": record}),
                "confirmation": item.confirmation.model_copy(update={"record": record}),
            }
        )
    with pytest.raises(CashBookError, match="cash_book_transfer_identity_conflict"):
        book.apply_transfers((changed,))
    state = ExecutionCashBook(book.path.parent, "synthetic").snapshot()
    assert state["halted"] and state["reason"] == "cash_book_transfer_identity_conflict"
    assert state["balance"] == before["balance"] and state["head"] == before["head"]
    with pytest.raises(CashBookError, match="cash_book_halted"):
        book.apply_transfers((item,))
    with pytest.raises(CashBookError, match="cash_book_halted"):
        book.apply(synthetic_batch(NOW))


@pytest.mark.parametrize("role", ["primary", "confirmation"])
def test_native_statement_reference_cannot_be_reused_under_another_id(tmp_path, role):
    book = create(tmp_path)
    first, second = synthetic_match(NOW), synthetic_match(NOW, identity="Other")
    second = second.model_copy(
        update={
            role: getattr(second, role).model_copy(
                update={"reference": getattr(first, role).reference}
            )
        }
    )
    with pytest.raises(CashBookError, match="cash_book_transfer_identity_conflict"):
        book.apply_transfers((first, second))
    assert book.snapshot()["halted"] and book.snapshot()["external_transfers"] == 0


@pytest.mark.parametrize("kind", ["amount", "fee", "source", "same_document", "old", "observation"])
def test_unmatched_batch_never_partially_books_and_never_turns_a_difference_into_a_deposit(
    tmp_path, kind
):
    book = create(tmp_path)
    first, second = synthetic_match(NOW), synthetic_match(NOW, identity="Second")
    if kind in {"amount", "fee"}:
        field = "amount" if kind == "amount" else "fee_debit"
        record = second.confirmation.record.model_copy(update={field: Decimal("1")})
        second = second.model_copy(
            update={"confirmation": second.confirmation.model_copy(update={"record": record})}
        )
    elif kind == "source":
        second = second.model_copy(
            update={"primary": second.primary.model_copy(update={"source": "wrong"})}
        )
    elif kind == "same_document":
        second = second.model_copy(
            update={
                "confirmation": second.confirmation.model_copy(update={"document_sha256": "1" * 64})
            }
        )
    elif kind == "old":
        second = synthetic_match(NOW - timedelta(seconds=1), identity="Second")
    else:
        second = second.model_copy(
            update={
                "confirmation": second.confirmation.model_copy(
                    update={"observed_at": NOW - timedelta(seconds=1)}
                )
            }
        )
    before = book.snapshot()
    with pytest.raises(CashBookError, match="cash_book_transfer_evidence_not_matched"):
        book.apply_transfers((first, second))
    assert book.snapshot() == before
    difference = book.compare_balance(report(Clock(), balance="1000100"))
    assert Decimal(difference["difference"]) == 100
    assert not difference["balance_match"]
    assert book.snapshot() == before


def test_v3_cash_positions_and_executions_share_balance_capacity_and_public_head(tmp_path):
    book = create(tmp_path, position_basis=PositionBasis(positions=()), max_entries=2)
    book.apply_transfers((synthetic_match(NOW, amount="100"),))
    execution = book.apply(synthetic_batch(NOW))
    state = book.snapshot()
    assert execution["head"] == state["head"] and state["position_accounting_applied"]
    assert Decimal(state["balance"]) == 1_000_098
    comparison = book.compare_balance(report(Clock(), balance="1000098"))
    assert comparison["balance_match"] and comparison["head"] == state["head"]
    assert book.compare_positions(report(Clock()))["head"] == state["head"]
    with pytest.raises(CashBookError, match="cash_book_capacity_reached"):
        book.apply_transfers((synthetic_match(NOW, identity="Another"),))
    assert not book.apply_transfers((synthetic_match(NOW, amount="100"),))["applied_transfer_ids"]
    assert book.snapshot() == state


def test_execution_capacity_is_also_shared_with_transfers(tmp_path):
    book = create(tmp_path, max_entries=1)
    book.apply_transfers((synthetic_match(NOW),))
    before = book.snapshot()
    with pytest.raises(CashBookError, match="cash_book_capacity_reached"):
        book.apply(synthetic_batch(NOW))
    assert book.snapshot() == before


def test_latest_transfer_time_fences_balance_and_position_comparison(tmp_path):
    book = create(tmp_path, position_basis=PositionBasis(positions=()))
    book.apply_transfers((synthetic_match(NOW + timedelta(seconds=1)),))
    for compare in (book.compare_balance, book.compare_positions):
        with pytest.raises(CashBookError, match="cash_book_balance_report_before_postings"):
            compare(report(Clock()))


def test_fixed_scale_external_cash_is_independent_of_decimal_context(tmp_path):
    book = create(tmp_path)
    item = synthetic_match(NOW, amount="12345.67890123", fee="0.00000001")
    with localcontext() as context:
        context.prec = 3
        result = book.apply_transfers((item,))
        assert result["balance"] == "1012345.67890122"
        assert book.snapshot()["transfer_fee_debit"] == "0.00000001"


@pytest.mark.parametrize("version", [1, 2])
def test_existing_versions_are_not_upgraded_or_assumed_to_include_transfer_evidence(
    tmp_path, version
):
    book = ExecutionCashBook.create(
        tmp_path / "book",
        "synthetic",
        OpeningCash(
            balance="1000000",
            cutoff=NOW - timedelta(seconds=1),
            position_basis=PositionBasis(positions=()) if version == 2 else None,
        ),
    )
    before = book.snapshot()
    with pytest.raises(CashBookError, match="cash_book_transfer_policy_required"):
        book.apply_transfers((synthetic_match(NOW),))
    assert book.snapshot() == before
    with sqlite3.connect(book.path) as conn:
        body = json.loads(conn.execute("SELECT opening FROM book").fetchone()[0])
        assert "transfer_policy" not in body
        assert conn.execute("SELECT version FROM book").fetchone()[0] == version


def test_concurrent_writers_keep_one_transfer_and_one_cash_change(tmp_path):
    book = create(tmp_path)
    item = synthetic_match(NOW)

    def apply(_):
        return ExecutionCashBook(book.path.parent, "synthetic").apply_transfers((item,))

    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(apply, range(3)))
    assert sum(bool(r["applied_transfer_ids"]) for r in results) == 1
    assert book.snapshot()["external_transfers"] == 1
    assert Decimal(book.snapshot()["balance"]) == 1_010_000


def test_offsetting_external_cash_keeps_a_valid_combined_balance_after_realized_pnl(tmp_path):
    basis = PositionBasis(
        positions=(OpeningPosition(position_id=401, side="BUY", units=400, average_price="150"),)
    )
    book = create(tmp_path, position_basis=basis, balance="1000000000000000000")
    book.apply_transfers((synthetic_match(NOW, kind="WITHDRAWAL", amount="100"),))
    book.apply(batch(close()))
    state = ExecutionCashBook(book.path.parent, "synthetic").snapshot()
    assert state["balance"] == "999999999999999938.00000000"
    assert not state["positions"]


def test_offline_demo_has_matching_cash_and_positions_and_preserves_an_unknown_difference(tmp_path):
    result = demo(tmp_path / "demo")
    assert result["balance_match"]["balance_match"] and result["position_match"]["position_match"]
    assert result["snapshot"]["balance"] == "1007495.00000000"
    assert Decimal(result["unexplained_cash"]["difference"]) == 100
    assert not result["complete"] and not result["live_enabled"]
    with pytest.raises(FileExistsError):
        demo(tmp_path / "demo")


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM cash_transfers",
        "UPDATE cash_transfers SET body='{}'",
        "UPDATE cash_transfers SET digest='bad'",
        "UPDATE cash_transfers SET transfer_id='Other'",
        "UPDATE cash_transfers SET sequence=2",
        "DELETE FROM transfer_postings WHERE account='cash'",
        "UPDATE transfer_postings SET amount='1' WHERE account='cash'",
        "UPDATE transfer_postings SET account='other' WHERE account='cash'",
        "DELETE FROM transfer_state",
        "UPDATE transfer_state SET count=2",
        "UPDATE transfer_state SET bytes=0",
        "UPDATE transfer_state SET head='bad'",
        "UPDATE book SET opening=replace(opening,'synthetic-bank','another-bank')",
        "DROP TABLE transfer_postings",
    ],
)
def test_corrupt_transfer_storage_fails_closed_without_repair(tmp_path, sql):
    book = create(tmp_path)
    book.apply_transfers((synthetic_match(NOW),))
    with sqlite3.connect(book.path) as conn:
        conn.execute(sql)
    damaged = book.path.read_bytes()
    with pytest.raises(CashBookError, match="cash_book_integrity_failed"):
        book.snapshot()
    with pytest.raises(CashBookError, match="cash_book_failed_closed"):
        book.apply_transfers((synthetic_match(NOW),))
    with pytest.raises(CashBookError, match="cash_book_integrity_failed"):
        ExecutionCashBook(book.path.parent, "synthetic")
    assert book.path.read_bytes() == damaged


@pytest.mark.parametrize(
    "field,value",
    [
        ("amount", Decimal("0")),
        ("amount", Decimal("-1")),
        ("amount", Decimal("NaN")),
        ("amount", Decimal("Infinity")),
        ("amount", Decimal("1000000000000000001")),
        ("amount", Decimal("0.000000001")),
        ("fee_debit", Decimal("-1")),
        ("currency", "USD"),
        ("status", "PENDING"),
        ("occurred_at", NOW.replace(tzinfo=None)),
        ("transfer_id", "x" * 129),
    ],
)
def test_unvalidated_caller_models_are_rechecked_before_any_write(tmp_path, field, value):
    book = create(tmp_path)
    item = synthetic_match(NOW)
    record = item.primary.record.model_copy(update={field: value})
    invalid = item.model_copy(
        update={
            "primary": item.primary.model_copy(update={"record": record}),
            "confirmation": item.confirmation.model_copy(update={"record": record}),
        }
    )
    before = book.snapshot()
    with pytest.raises(CashBookError, match="^cash_book_transfer_input_invalid$"):
        book.apply_transfers((item, invalid))
    assert book.snapshot() == before


def test_batch_shape_and_proof_size_are_bounded_before_writing(tmp_path, monkeypatch):
    book = create(tmp_path)
    item = synthetic_match(NOW)
    before = book.snapshot()
    for invalid in ((), [item], (item,) * 1001, (object(),)):
        with pytest.raises(CashBookError, match="cash_book_transfer_input_invalid"):
            book.apply_transfers(invalid)
    monkeypatch.setattr(cash_module, "MAX_PROOF", 100)
    with pytest.raises(CashBookError, match="cash_book_transfer_input_invalid"):
        book.apply_transfers((item,))
    assert book.snapshot() == before


def test_total_cash_overflow_is_rejected_without_partial_booking(tmp_path):
    book = create(tmp_path, balance="1000000000000000000")
    before = book.snapshot()
    with pytest.raises(CashBookError, match="cash_book_money_out_of_range"):
        book.apply_transfers((synthetic_match(NOW),))
    assert book.snapshot() == before


@pytest.mark.parametrize("transfers_first", [False, True])
def test_evidence_byte_limit_is_shared_in_both_directions(tmp_path, monkeypatch, transfers_first):
    book = create(tmp_path)
    if transfers_first:
        book.apply_transfers((synthetic_match(NOW),))
    else:
        book.apply(synthetic_batch(NOW))
    before = book.snapshot()
    with sqlite3.connect(book.path) as conn:
        used = sum(
            conn.execute(f"SELECT bytes FROM {table}").fetchone()[0]
            for table in ("book", "transfer_state")
        )
    monkeypatch.setattr(cash_module, "MAX_BYTES", used)
    with pytest.raises(CashBookError, match="cash_book_capacity_reached"):
        if transfers_first:
            book.apply(synthetic_batch(NOW))
        else:
            book.apply_transfers((synthetic_match(NOW),))
    assert book.snapshot() == before


def test_posting_write_failure_rolls_back_all_rows_and_state(tmp_path):
    book = create(tmp_path)
    before = book.snapshot()
    with sqlite3.connect(book.path) as conn:
        conn.execute(
            "CREATE TRIGGER fail_cash BEFORE INSERT ON transfer_postings "
            "WHEN NEW.sequence=2 BEGIN SELECT RAISE(ABORT,'private detail'); END"
        )
    items = (synthetic_match(NOW), synthetic_match(NOW, identity="Second"))
    with pytest.raises(CashBookError, match="^cash_book_storage_failed$"):
        book.apply_transfers(items)
    restarted = ExecutionCashBook(book.path.parent, "synthetic")
    assert restarted.snapshot() == before
    with sqlite3.connect(book.path) as conn:
        conn.execute("DROP TRIGGER fail_cash")
    assert restarted.apply_transfers(items)["applied_transfer_ids"] == ("Deposit1", "Second")


@pytest.mark.parametrize("committed", [False, True])
def test_process_exit_during_batch_or_after_commit_is_repeat_safe(tmp_path, committed):
    book = create(tmp_path)
    script = """
import os, sys
from datetime import datetime
from pathlib import Path
from trading.cash_transfer_lab import synthetic_match
from trading.execution_cash_book import ExecutionCashBook
book = ExecutionCashBook(Path(sys.argv[1]), 'synthetic')
now = datetime.fromisoformat(sys.argv[2])
items = (synthetic_match(now), synthetic_match(now, identity='Second'))
if sys.argv[3] == 'False':
    original = ExecutionCashBook._transfer_digest
    def crash(previous, sequence, identity, body):
        if sequence == 2:
            os._exit(17)
        return original(previous, sequence, identity, body)
    ExecutionCashBook._transfer_digest = staticmethod(crash)
book.apply_transfers(items)
os._exit(17)
"""
    done = subprocess.run(
        [sys.executable, "-c", script, str(book.path.parent), NOW.isoformat(), str(committed)],
        capture_output=True,
        timeout=15,
    )
    assert done.returncode == 17, done.stderr.decode()
    restarted = ExecutionCashBook(book.path.parent, "synthetic")
    items = (synthetic_match(NOW), synthetic_match(NOW, identity="Second"))
    result = restarted.apply_transfers(items)
    assert result["applied_transfer_ids"] == (() if committed else ("Deposit1", "Second"))
    assert restarted.snapshot()["balance"] == "1020000.00000000"
