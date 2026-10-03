import json
import socket
import sqlite3
import subprocess
import sys
import threading
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
from trading.execution_cash_lab import demo


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("network attempted"))


def batch(rows=None, *, notices=None, **read_kwargs):
    rows = rows or [execution()]
    return ExecutionCashBatch(
        events=tuple(parse_event(raw(r), NOW) for r in (notices if notices is not None else rows)),
        reports=(read_order(Clock(), rows, **read_kwargs),),
    )


def create(tmp_path, *, balance="1000000", cutoff=None, **kwargs):
    return ExecutionCashBook.create(
        tmp_path / "cash",
        "synthetic",
        OpeningCash(balance=balance, cutoff=cutoff or NOW - timedelta(seconds=1)),
        **kwargs,
    )


def test_initial_cash_and_balanced_open_close_postings_survive_restart(tmp_path):
    book = create(tmp_path)
    opening = book.snapshot()
    assert Decimal(opening["balance"]) == 1_000_000
    assert opening["executions"] == opening["proofs"] == 0
    posted = book.apply(batch())
    assert posted["applied_execution_ids"] == (501,)
    assert Decimal(posted["cash_delta"]) == -2
    assert posted["accounting_applied"] and not posted["complete"] and not posted["live_enabled"]
    close = execution(
        executionId=601,
        orderId=301,
        rootOrderId=301,
        clientOrderId="DemoClose",
        settleType="CLOSE",
        side="SELL",
        orderSize="400",
        orderExecutedSize="400",
        executionPrice="150.1",
        orderPrice="150.1",
        lossGain="40",
        settledSwap="3",
        amount="41",
    )
    book.apply(batch([close], order_changes={"status": "EXECUTED"}))
    restored = ExecutionCashBook(book.path.parent, "synthetic")
    snapshot = restored.snapshot()
    assert Decimal(snapshot["balance"]) == Decimal("1000039")
    assert Decimal(snapshot["loss_gain"]) == 40
    assert Decimal(snapshot["fee_debit"]) == 4
    assert Decimal(snapshot["settled_swap"]) == 3
    assert snapshot["execution_ids"] == (501, 601)
    assert not snapshot["halted"] and not snapshot["complete"]
    assert "position_accounting_not_applied" in snapshot["blockers"]
    with sqlite3.connect(book.path) as conn:
        groups = {}
        for sequence, _, amount in conn.execute("SELECT * FROM postings"):
            groups[sequence] = groups.get(sequence, 0) + int(amount)
    assert groups == {0: 0, 1: 0, 2: 0}


def test_repeat_after_restart_or_changed_wire_representation_never_adds_proof_or_cash(tmp_path):
    book = create(tmp_path)
    first = batch()
    book.apply(first)
    snapshot = book.snapshot()
    # Cumulative / wire formatting changes are not a new economic execution.
    row = execution(
        executionPrice="150.0000",
        fee="-2.00000000",
        amount="-2.0",
        orderTimestamp=(NOW - timedelta(seconds=10)).isoformat(),
    )
    duplicate = batch([row])
    assert first.events[0].payload_sha256 != duplicate.events[0].payload_sha256
    for source in (first, duplicate):
        reopened = ExecutionCashBook(book.path.parent, "synthetic")
        result = reopened.apply(source)
        assert result["applied_execution_ids"] == ()
        assert result["already_applied_execution_ids"] == (501,)
        assert Decimal(result["cash_delta"]) == 0
        assert reopened.snapshot() == snapshot


def test_only_selected_matched_notices_are_booked_then_remaining_fill_once(tmp_path):
    first = execution()
    second = execution(
        executionId=502, executionSize="600", orderExecutedSize="1000", fee="-3", amount="-3"
    )
    book = create(tmp_path)
    result = book.apply(batch([first, second], notices=[first]))
    assert result["applied_execution_ids"] == (501,)
    result = book.apply(batch([first, second], notices=[second]))
    assert result["applied_execution_ids"] == (502,)
    assert Decimal(book.snapshot()["balance"]) == 999_995
    result = book.apply(
        batch(
            [
                first,
                second,
            ],
            notices=[first, second, first],
        )
    )
    assert result["already_applied_execution_ids"] == (501, 502)
    assert book.snapshot()["proofs"] == 2


@pytest.mark.parametrize(
    "changes",
    [
        {"fee": "-3", "amount": "-3"},
        {"positionId": 402},
        {"executionPrice": "149"},
        {"executionSize": "300", "orderExecutedSize": "300"},
        {"orderId": 202, "rootOrderId": 202, "clientOrderId": "OtherOrder"},
        {"orderSize": "1200"},
        {"rootOrderId": 202},
    ],
)
def test_conflicting_execution_or_order_persists_stop_without_changing_cash(tmp_path, changes):
    book = create(tmp_path)
    book.apply(batch())
    previous = book.snapshot()
    with pytest.raises(CashBookError, match="cash_book_identity_conflict"):
        book.apply(batch([execution(**changes)]))
    reopened = ExecutionCashBook(book.path.parent, "synthetic")
    stopped = reopened.snapshot()
    assert stopped["halted"]
    assert stopped["head"] == previous["head"]
    assert stopped["balance"] == previous["balance"]
    assert stopped["proofs"] == previous["proofs"]
    with pytest.raises(CashBookError, match="cash_book_halted"):
        reopened.apply(batch())


def test_conflicting_rest_only_fill_stops_and_missing_known_fill_refuses_batch(tmp_path):
    book = create(tmp_path)
    book.apply(batch())
    second = execution(executionId=502, executionSize="600", orderExecutedSize="600")
    # An incomplete read cannot cause a second booking of unseen history.
    with pytest.raises(CashBookError, match="cash_book_known_history_missing"):
        book.apply(batch([second]))
    assert not book.snapshot()["halted"] and book.snapshot()["executions"] == 1
    changed_first = execution(fee="-3", amount="-3")
    with pytest.raises(CashBookError, match="cash_book_identity_conflict"):
        book.apply(batch([changed_first, second], notices=[second]))
    assert book.snapshot()["executions"] == 1


def test_reused_client_id_in_new_order_or_one_batch_is_rejected(tmp_path):
    book = create(tmp_path)
    first = batch()
    other = batch([execution(executionId=502, orderId=202, rootOrderId=202)])
    source = ExecutionCashBatch(
        events=(*first.events, *other.events), reports=(*first.reports, *other.reports)
    )
    with pytest.raises(CashBookError, match="cash_book_identity_conflict"):
        book.apply(source)
    assert book.snapshot()["executions"] == 0 and book.snapshot()["halted"]


@pytest.mark.parametrize("cutoff", [NOW, NOW + timedelta(seconds=1)])
def test_opening_boundary_prevents_including_preexisting_cash_effect_twice(tmp_path, cutoff):
    book = create(tmp_path, cutoff=cutoff)
    with pytest.raises(CashBookError, match="cash_book_before_opening_boundary"):
        book.apply(batch())
    assert book.snapshot()["executions"] == 0


def test_unmatched_or_missing_evidence_has_no_accounting_effect(tmp_path):
    book = create(tmp_path)
    source = batch(fill_changes={"price": "149"})
    with pytest.raises(CashBookError, match="cash_book_executions_not_matched"):
        book.apply(source)
    with pytest.raises(CashBookError, match="cash_book_batch_capacity"):
        book.apply(source.model_copy(update={"reports": ()}))
    with pytest.raises(CashBookError, match="cash_book_batch_required"):
        book.apply({"matched_cash_amount": "99999"})
    assert book.snapshot()["executions"] == 0


@pytest.mark.parametrize(
    "field,value",
    [
        ("path", "/v1/account/assets"),
        ("query", (("orderId", "202"),)),
        ("sha256", "invalid"),
        ("received_at", NOW - timedelta(seconds=1)),
    ],
)
def test_wrong_rest_observation_is_not_postable(tmp_path, field, value):
    book = create(tmp_path)
    source = batch()
    read = source.reports[0]
    bad = read.model_copy(
        update={
            "observations": (
                read.observations[0].model_copy(update={field: value}),
                *read.observations[1:],
            )
        }
    )
    with pytest.raises(CashBookError, match="cash_book_order_observations_invalid"):
        book.apply(source.model_copy(update={"reports": (bad,)}))
    assert book.snapshot()["executions"] == 0


def test_decimal_context_cannot_change_cash_or_duplicate_identity(tmp_path):
    book = create(tmp_path, balance="99999999999999999.12345678")
    source = batch([execution(fee="-2.12345678", amount="-2.12345678")])
    with localcontext() as context:
        context.prec = 6
        book.apply(source)
        book.apply(source)
        value = book.snapshot()
    assert value["balance"] == "99999999999999997.00000000"
    assert value["fee_debit"] == "2.12345678"


@pytest.mark.parametrize("balance", ["1.000000001", "1e19", "NaN", "Infinity"])
def test_invalid_opening_does_not_create_directory(tmp_path, balance):
    with pytest.raises((CashBookError, ValueError)):
        create(tmp_path, balance=balance)
    assert not (tmp_path / "cash").exists()


def test_precision_limit_and_capacity_leave_last_book_unchanged(tmp_path):
    book = create(tmp_path, max_entries=1)
    with pytest.raises(CashBookError, match="cash_book_money_precision"):
        book.apply(batch([execution(fee="-0.000000001", amount="-0.000000001")]))
    book.apply(batch())
    previous = book.snapshot()
    second = execution(executionId=502, executionSize="600", orderExecutedSize="1000")
    with pytest.raises(CashBookError, match="cash_book_capacity_reached"):
        book.apply(batch([execution(), second], notices=[second]))
    book.apply(batch())  # Idempotent repeats are permitted at capacity.
    assert book.snapshot() == previous


def test_balance_comparison_detects_unexplained_cash_without_mutating_book(tmp_path):
    book = create(tmp_path)
    book.apply(batch())
    previous = book.snapshot()
    account = report(Clock())
    assets = account.assets.model_copy(update={"balance": Decimal("999998")})
    matched = book.compare_balance(account.model_copy(update={"assets": assets}))
    assert matched["balance_match"] and matched["difference"] == "0.00000000"
    assert not matched["complete"] and not matched["live_enabled"]
    assert "external_cash_flows_not_reconciled" in matched["blockers"]
    result = book.compare_balance(
        account.model_copy(
            update={"assets": assets.model_copy(update={"balance": Decimal("1000098")})}
        )
    )
    assert not result["balance_match"] and result["difference"] == "100.00000000"
    assert "cash_balance_difference_unexplained" in result["blockers"]
    assert book.snapshot() == previous


def test_balance_observation_before_known_execution_or_without_observations_refused(tmp_path):
    book = create(tmp_path)
    book.apply(batch())
    account = report(Clock())
    stale = account.model_copy(
        update={
            "observations": tuple(
                o.model_copy(
                    update={
                        "response_at": NOW - timedelta(seconds=1),
                        "received_at": NOW - timedelta(seconds=1),
                    }
                )
                for o in account.observations
            )
        }
    )
    with pytest.raises(CashBookError, match="cash_book_balance_report_before_postings"):
        book.compare_balance(stale)
    with pytest.raises(CashBookError, match="cash_book_balance_report_invalid"):
        book.compare_balance(account.model_copy(update={"observations": ()}))


def test_two_concurrent_writers_commit_one_execution_and_one_proof(tmp_path):
    book = create(tmp_path)
    barrier = threading.Barrier(2)
    source = batch()

    def apply():
        writer = ExecutionCashBook(book.path.parent, "synthetic")
        barrier.wait(timeout=5)
        return writer.apply(source)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(apply) for _ in range(2)]
        results = [f.result(timeout=5) for f in futures]
    assert sorted(len(r["applied_execution_ids"]) for r in results) == [0, 1]
    assert book.snapshot()["executions"] == book.snapshot()["proofs"] == 1
    assert Decimal(book.snapshot()["balance"]) == 999998


def test_failure_between_execution_and_postings_rolls_back_everything(tmp_path):
    book = create(tmp_path)
    before = book.snapshot()
    with sqlite3.connect(book.path) as conn:
        conn.executescript("""
        CREATE TRIGGER abort_post BEFORE INSERT ON postings WHEN NEW.account='fee_expense'
        BEGIN SELECT RAISE(ABORT, 'fixture-secret'); END;
        """)
    with pytest.raises(CashBookError, match="cash_book_storage_failed") as caught:
        book.apply(batch())
    assert "fixture-secret" not in str(caught.value)
    assert ExecutionCashBook(book.path.parent, "synthetic").snapshot() == before
    with sqlite3.connect(book.path) as conn:
        conn.execute("DROP TRIGGER abort_post")
    restarted = ExecutionCashBook(book.path.parent, "synthetic")
    restarted.apply(batch())
    assert restarted.snapshot()["executions"] == 1


@pytest.mark.parametrize("committed", [False, True])
def test_process_exit_before_commit_or_after_commit_is_repeat_safe(tmp_path, committed):
    book = create(tmp_path)
    source = batch()
    input_path = tmp_path / "batch.json"
    input_path.write_text(source.model_dump_json(), encoding="utf-8")
    script = """
import os, sqlite3, sys
from pathlib import Path
from trading.execution_cash_book import ExecutionCashBook, ExecutionCashBatch
book = ExecutionCashBook(Path(sys.argv[1]), 'synthetic')
source = ExecutionCashBatch.model_validate_json(Path(sys.argv[2]).read_text())
if sys.argv[3] == 'True':
    book.apply(source)
else:
    conn = sqlite3.connect(book.path)
    conn.execute('BEGIN IMMEDIATE')
    conn.execute("UPDATE postings SET amount='123' WHERE sequence=0 AND account='cash'")
os._exit(17)
"""
    done = subprocess.run(
        [sys.executable, "-c", script, str(book.path.parent), str(input_path), str(committed)],
        capture_output=True,
        timeout=15,
    )
    assert done.returncode == 17, done.stderr.decode()
    restarted = ExecutionCashBook(book.path.parent, "synthetic")
    result = restarted.apply(source)
    assert result["applied_execution_ids"] == (() if committed else (501,))
    assert restarted.snapshot()["balance"] == "999998.00000000"


def test_hot_journal_from_crash_during_commit_is_rolled_back_on_reopen(tmp_path):
    book = create(tmp_path)
    book.apply(batch())
    before = book.snapshot()
    # A one-page cache spills uncommitted pages into the database file, so the
    # remaining rollback journal is hot: only a writable connection can undo it.
    script = """
import os, sqlite3, sys
conn = sqlite3.connect(sys.argv[1])
conn.execute('PRAGMA cache_size=1')
conn.execute('BEGIN IMMEDIATE')
conn.execute('UPDATE book SET bytes=bytes+1')
for i in range(3000):
    conn.execute('INSERT INTO proofs VALUES(?,?)', (100 + i, 'x' * 500))
os._exit(17)
"""
    done = subprocess.run(
        [sys.executable, "-c", script, str(book.path)], capture_output=True, timeout=15
    )
    assert done.returncode == 17, done.stderr.decode()
    assert book.path.with_name("execution-cash.sqlite-journal").exists()
    reopened = ExecutionCashBook(book.path.parent, "synthetic")
    assert reopened.snapshot() == before
    assert not book.path.with_name("execution-cash.sqlite-journal").exists()


def test_each_posting_stores_only_the_evidence_for_its_new_executions(tmp_path):
    rows = [
        execution(
            executionId=1000 + i, orderId=5000 + i, rootOrderId=5000 + i, clientOrderId=f"C{i}"
        )
        for i in range(5)
    ]
    events = tuple(parse_event(raw(r), NOW) for r in rows)
    reports = tuple(read_order(Clock(), [r]) for r in rows)
    cumulative = create(tmp_path / "cumulative")
    single = create(tmp_path / "single")
    for k in range(1, len(rows) + 1):
        # A monitor batch repeats every notice and order since connect.
        result = cumulative.apply(ExecutionCashBatch(events=events[:k], reports=reports[:k]))
        assert result["applied_execution_ids"] == (1000 + k - 1,)
        assert len(result["already_applied_execution_ids"]) == k - 1
        single.apply(ExecutionCashBatch(events=events[k - 1 : k], reports=reports[k - 1 : k]))

    def proofs(book):
        with sqlite3.connect(book.path) as conn:
            return conn.execute("SELECT id,body FROM proofs ORDER BY id").fetchall()

    assert proofs(cumulative) == proofs(single)
    reopened = ExecutionCashBook(cumulative.path.parent, "synthetic")
    repeated = reopened.apply(ExecutionCashBatch(events=events, reports=reports))
    assert repeated["applied_execution_ids"] == ()
    assert reopened.snapshot()["proofs"] == len(rows)


def test_busy_write_is_retried_once_without_duplicate(tmp_path, monkeypatch):
    monkeypatch.setattr("trading.execution_cash_book.BUSY_TIMEOUT_SECONDS", 0.05)
    book = create(tmp_path)
    reader = sqlite3.connect(book.path)
    waits = []

    def wait(seconds):
        waits.append(seconds)
        reader.rollback()

    book._wait = wait
    try:
        # A diagnostic reader's shared lock makes the writer's COMMIT busy.
        reader.execute("BEGIN")
        reader.execute("SELECT count(*) FROM executions").fetchone()
        assert book.apply(batch())["applied_execution_ids"] == (501,)
    finally:
        reader.close()
    assert waits == [0.05]
    assert book.snapshot()["executions"] == book.snapshot()["proofs"] == 1


def test_busy_exhaustion_is_not_treated_as_corruption(tmp_path, monkeypatch):
    monkeypatch.setattr("trading.execution_cash_book.BUSY_TIMEOUT_SECONDS", 0.05)
    book = create(tmp_path)
    before = book.snapshot()
    waits = []
    book._wait = waits.append
    with sqlite3.connect(book.path) as lock:
        lock.execute("BEGIN EXCLUSIVE")
        with pytest.raises(CashBookError, match="cash_book_busy"):
            book.apply(batch())
        with pytest.raises(CashBookError, match="cash_book_busy"):
            book.snapshot()
        lock.rollback()
    assert waits == [0.05, 0.05] * 2
    assert not book._failed
    assert book.snapshot() == before
    assert book.apply(batch())["applied_execution_ids"] == (501,)


@pytest.mark.parametrize(
    "mutation",
    [
        "DELETE FROM executions",
        "DELETE FROM proofs",
        "DELETE FROM postings WHERE sequence=1 AND account='cash'",
        "UPDATE postings SET amount='999' WHERE sequence=1 AND account='cash'",
        "UPDATE executions SET digest='broken'",
        "UPDATE proofs SET body='{}'",
        "UPDATE executions SET body='{}'",
        "UPDATE book SET count=0",
        "UPDATE book SET head='broken'",
        "UPDATE book SET opening='{}'",
        "UPDATE book SET bytes=0",
        "UPDATE book SET version=2",
    ],
)
def test_corruption_rejected_without_reinitialization(tmp_path, mutation):
    book = create(tmp_path)
    book.apply(batch())
    with sqlite3.connect(book.path) as conn:
        conn.execute(mutation)
    with pytest.raises(CashBookError, match="cash_book_integrity_failed"):
        book.snapshot()
    with pytest.raises(CashBookError, match="cash_book_failed_closed"):
        book.apply(batch())
    with pytest.raises(CashBookError, match="cash_book_integrity_failed"):
        ExecutionCashBook(book.path.parent, "synthetic")


def test_existing_directory_wrong_scope_and_missing_database_are_refused(tmp_path):
    book = create(tmp_path)
    with pytest.raises(FileExistsError):
        create(tmp_path)
    with pytest.raises(CashBookError, match="cash_book_integrity_failed"):
        ExecutionCashBook(book.path.parent, "other")
    book.path.unlink()
    with pytest.raises(CashBookError, match="cash_book_storage_failed"):
        book.apply(batch())
    assert not book.path.exists()


@pytest.mark.parametrize("offset", [50, 100, 101])
def test_explicit_response_clock_tolerance_is_saved_in_booking_proof(tmp_path, offset):
    book = create(tmp_path)
    source = batch()
    observed = NOW + timedelta(milliseconds=offset)
    read = source.reports[0]
    shifted = read.model_copy(
        update={
            "evidence": read.evidence.model_copy(update={"observed_at": observed}),
            "observations": tuple(
                o.model_copy(update={"response_at": observed}) for o in read.observations
            ),
        }
    )
    source = source.model_copy(update={"reports": (shifted,), "clock_skew_ms": 100})
    if offset <= 100:
        book.apply(source)
        assert ExecutionCashBook(book.path.parent, "synthetic").snapshot()["executions"] == 1
        with sqlite3.connect(book.path) as conn:
            assert (
                json.loads(conn.execute("SELECT body FROM proofs").fetchone()[0])["clock_skew_ms"]
                == 100
            )
    else:
        with pytest.raises(CashBookError, match="cash_book_order_observations_invalid"):
            book.apply(source)


@pytest.mark.parametrize("value", [-1, 1001, True, 0.1])
def test_invalid_clock_tolerance_cannot_be_smuggled_through_model_copy(tmp_path, value):
    book = create(tmp_path)
    with pytest.raises(CashBookError, match="cash_book_input_invalid"):
        book.apply(batch().model_copy(update={"clock_skew_ms": value}))
    assert book.snapshot()["executions"] == 0


def test_zero_and_negative_cash_effects_are_distinct_balanced_entries(tmp_path):
    book = create(tmp_path)
    zero = execution(fee="0", amount="0")
    book.apply(batch([zero]))
    close = execution(
        executionId=601,
        orderId=301,
        rootOrderId=301,
        clientOrderId="DemoClose",
        settleType="CLOSE",
        side="SELL",
        orderSize="400",
        orderExecutedSize="400",
        lossGain="-40",
        settledSwap="-3",
        amount="-45",
    )
    book.apply(batch([close]))
    result = book.snapshot()
    assert result["balance"] == "999955.00000000"
    assert result["executions"] == 2
    with sqlite3.connect(book.path) as conn:
        amounts = list(conn.execute("SELECT amount FROM postings WHERE sequence=1"))
    assert amounts == [("0",)] * 4


def test_late_out_of_order_execution_after_cutoff_is_accounted_once(tmp_path):
    book = create(tmp_path, cutoff=NOW - timedelta(seconds=5))
    ordered = (NOW - timedelta(seconds=2)).isoformat()
    first = execution(
        executionId=502, orderExecutedSize="1000", executionSize="600", orderTimestamp=ordered
    )
    earlier = execution(
        executionTimestamp=(NOW - timedelta(seconds=1)).isoformat(), orderTimestamp=ordered
    )
    book.apply(batch([earlier, first], notices=[first]))
    book.apply(batch([earlier, first], notices=[earlier]))
    assert book.snapshot()["executions"] == 2
    assert book.snapshot()["balance"] == "999996.00000000"


def test_capacity_bounds_and_cash_overflow_do_not_commit_partial_result(tmp_path):
    book = create(tmp_path, balance="1000000000000000000")
    close = execution(
        executionId=601,
        orderId=301,
        rootOrderId=301,
        clientOrderId="DemoClose",
        settleType="CLOSE",
        side="SELL",
        orderSize="400",
        orderExecutedSize="400",
        lossGain="40",
        settledSwap="0",
        amount="38",
    )
    with pytest.raises(CashBookError, match="cash_book_money_out_of_range"):
        book.apply(batch([close]))
    source = batch()
    with pytest.raises(CashBookError, match="cash_book_batch_capacity"):
        book.apply(source.model_copy(update={"events": source.events * 2001}))
    assert book.snapshot()["executions"] == 0


def test_unsupported_sql_identity_cannot_overflow_sqlite_integer_column(tmp_path):
    book = create(tmp_path)
    with pytest.raises(CashBookError, match="cash_book_identity_out_of_range"):
        book.apply(batch([execution(executionId=2**63)]))
    assert book.snapshot()["executions"] == 0


def test_failed_initializer_removes_only_its_new_storage_files(tmp_path, monkeypatch):
    connect = sqlite3.connect
    monkeypatch.setattr(
        "trading.execution_cash_book.sqlite3.connect",
        lambda *a, **k: (_ for _ in ()).throw(sqlite3.OperationalError("fixture-secret")),
    )
    with pytest.raises(CashBookError, match="cash_book_initialization_failed"):
        create(tmp_path)
    assert not (tmp_path / "cash").exists()
    monkeypatch.setattr("trading.execution_cash_book.sqlite3.connect", connect)
    assert create(tmp_path).snapshot()["executions"] == 0


def test_offline_demo_book_and_report_are_repeatable_and_output_is_protected(tmp_path):
    directory = tmp_path / "demo"
    result = demo(directory)
    assert result["snapshot"]["balance"] == "1000039.00000000"
    assert result["duplicate"]["applied_execution_ids"] == ()
    assert result["restart_duplicate"]["applied_execution_ids"] == ()
    assert result["balance_match"]["balance_match"]
    assert result["unexplained_cash"]["difference"] == "100.00000000"
    assert not result["complete"] and not result["live_enabled"]
    saved = (directory / "report.json").read_bytes()
    assert json.loads(saved) == json.loads(json.dumps(result))
    assert ExecutionCashBook(directory, "synthetic-cash").snapshot() == result["snapshot"]
    with pytest.raises(FileExistsError):
        demo(directory)
    assert (directory / "report.json").read_bytes() == saved
