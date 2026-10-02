"""Cash integration fences, durable ambiguity and journal epoch ownership."""

import socket
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from decimal import Decimal

import pytest
from test_account_events import NOW, execution, raw
from test_account_sync import Clock, report
from test_execution_reconciliation import read_order
from test_private_stream import setup as stream_setup
from test_private_stream import start as stream_start

from trading.account_sync import SyncError
from trading.event_capture import JournaledEventCapture
from trading.event_journal import EventJournal, JournalError
from trading.execution_cash_book import ExecutionCashBook, OpeningCash
from trading.execution_cash_sync_lab import demo


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("network attempted"))


def capture_for(journal, clock):
    capture = JournaledEventCapture(
        journal, clock=lambda: clock.wall, monotonic_ns=lambda: int(clock.mono * 1e9)
    )
    capture.start_session(expected_head=journal.inspect()["head"])
    return capture


@pytest.fixture
def setup(tmp_path):
    clock = Clock()
    journal = EventJournal.create(tmp_path / "journal", "synthetic")
    capture = capture_for(journal, clock)
    book = ExecutionCashBook.create(
        tmp_path / "cash",
        "synthetic",
        OpeningCash(balance="1000000", cutoff=NOW - timedelta(seconds=1)),
    )
    capture.ingest(1, raw(execution()))
    return clock, journal, capture, book


def resync(clock, capture, book=None, *, rows=None, **changes):
    return capture.resync(
        lambda: report(clock, **changes),
        collect_orders=lambda ids: (read_order(clock, rows),),
        cash_book=book,
    )


def test_opt_in_books_individuals_and_repeated_resync_is_idempotent(setup):
    clock, journal, capture, book = setup
    assessment = resync(clock, capture)
    assert assessment.execution_cash is None and book.snapshot()["executions"] == 0
    for index in range(2):
        assessment = resync(clock, capture, book)
        cash = assessment.execution_cash
        assert cash["applied_execution_ids"] == ((501,) if index == 0 else ())
        assert cash["already_applied_execution_ids"] == (() if index == 0 else (501,))
        assert Decimal(cash["balance"]) == 999_998
        assert cash["revision"] == assessment.revision
        assert cash["accounting_applied"] and not cash["live_enabled"]
        assert not assessment.complete and not assessment.live_enabled
        assert not assessment.execution_reconciliation.accounting_applied
        assert "execution_accounting_not_applied" in assessment.blockers
    assert book.snapshot()["proofs"] == 1
    assert not journal.inspect()["unacknowledged_records"]


def test_restart_requires_new_epoch_and_fresh_reads_but_never_rebooks(setup):
    clock, journal, capture, book = setup
    resync(clock, capture, book)
    reopened = capture_for(EventJournal(journal.path.parent, "synthetic"), clock)
    restored_book = ExecutionCashBook(book.path.parent, "synthetic")
    with pytest.raises(SyncError):
        reopened.apply_execution_cash(restored_book, expected_revision=1)
    # A refused stale application closes that capture, so explicitly start anew.
    reopened = capture_for(EventJournal(journal.path.parent, "synthetic"), clock)
    reopened.ingest(1, raw(execution()))
    assessment = resync(clock, reopened, restored_book)
    assert assessment.execution_cash["already_applied_execution_ids"] == (501,)
    assert not assessment.execution_cash["applied_execution_ids"]
    assert "journal_epoch_gap_not_repaired" in assessment.blockers
    assert restored_book.snapshot()["proofs"] == 1


def test_later_cash_movement_does_not_block_matching_fills_or_clear_account_uncertainty(setup):
    clock, _, capture, book = setup
    resync(clock, capture, book)
    second = execution(
        executionId=502, executionSize="200", orderExecutedSize="600", fee="-3", amount="-3"
    )
    capture.ingest(2, raw(second))
    assessment = resync(clock, capture, book, rows=[execution(), second], balance="999995")
    assert assessment.mismatches == ("balance_change_unverified",)
    assert not assessment.structural_match
    assert assessment.execution_cash["applied_execution_ids"] == (502,)
    assert Decimal(book.snapshot()["balance"]) == 999_995
    assert capture.status()["resync_required"]
    assert "execution_accounting_not_applied" in assessment.blockers


@pytest.mark.parametrize("kind", ["execution", "position"])
def test_discrepancies_never_book_partial_or_structurally_mismatched_batches(setup, kind):
    clock, _, capture, book = setup
    if kind == "execution":
        result = capture.resync(
            lambda: report(clock),
            collect_orders=lambda ids: (
                read_order(clock, fill_changes={"fee": "-3", "amount": "-3"}),
            ),
            cash_book=book,
        )
    else:
        # A position update must agree as well as the individual execution.
        from test_account_sync import position

        capture.ingest(2, position(units=500))
        result = resync(clock, capture, book)
    assert result.mismatches and result.execution_cash is None
    assert book.snapshot()["executions"] == 0


def test_unknown_cash_flow_is_not_converted_to_an_execution_or_deposit(setup):
    clock, _, capture, book = setup
    result = resync(clock, capture, book, balance="1234567")
    assert Decimal(book.snapshot()["balance"]) == 999_998
    comparison = book.compare_balance(result.report)
    assert not comparison["balance_match"]
    assert "cash_balance_difference_unexplained" in comparison["blockers"]


@pytest.mark.parametrize(
    "change", ["event", "duplicate", "disconnect", "expired", "clock", "resync"]
)
def test_old_accepted_revision_cannot_book_after_invalidation(setup, change):
    clock, _, capture, book = setup
    accepted = resync(clock, capture)
    if change in {"event", "duplicate"}:
        row = execution() if change == "duplicate" else execution(executionId=502)
        capture.ingest(2, raw(row))
    elif change == "disconnect":
        capture.disconnect()
    elif change == "expired":
        clock.advance(31)
    elif change == "clock":
        clock.wall -= timedelta(seconds=1)
    else:
        newer = resync(clock, capture)
        assert newer.revision != accepted.revision
    with pytest.raises((SyncError, JournalError)):
        capture.apply_execution_cash(book, expected_revision=accepted.revision)
    assert book.snapshot()["executions"] == 0


def test_balance_only_batch_also_expires(setup):
    clock, _, capture, book = setup
    resync(clock, capture)
    accepted = resync(clock, capture, balance="999999")
    assert not accepted.structural_match
    clock.advance(31)
    with pytest.raises(SyncError):
        capture.apply_execution_cash(book, expected_revision=accepted.revision)
    assert book.snapshot()["executions"] == 0


def test_no_execution_is_a_read_only_observation(setup):
    clock, journal, _, book = setup
    capture = capture_for(EventJournal(journal.path.parent, "synthetic"), clock)
    result = capture.resync(lambda: report(clock), collect_orders=lambda ids: (), cash_book=book)
    assert result.execution_cash is None
    assert book.snapshot()["executions"] == 0


def test_scope_and_order_collector_are_required_before_any_collection(setup, tmp_path):
    clock, _, capture, book = setup
    other = ExecutionCashBook.create(
        tmp_path / "other", "different", OpeningCash(balance="0", cutoff=NOW)
    )
    with pytest.raises(SyncError, match="execution_cash_scope_mismatch"):
        capture.resync(lambda: pytest.fail("wrong scope reached REST"), cash_book=other)
    with pytest.raises(SyncError, match="execution_cash_requires_order_collection"):
        capture.resync(lambda: pytest.fail("missing collector reached REST"), cash_book=book)
    assert not capture.status()["capture_failed"]
    assert resync(clock, capture, book).execution_cash["applied_execution_ids"] == (501,)


def test_event_during_rest_does_not_wait_and_prevents_posting(setup):
    clock, _, capture, book = setup
    entered, release = threading.Event(), threading.Event()

    def orders(ids):
        entered.set()
        assert release.wait(5)
        return (read_order(clock),)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            capture.resync, lambda: report(clock), collect_orders=orders, cash_book=book
        )
        assert entered.wait(5)
        try:
            capture.ingest(2, raw(execution()))
        finally:
            release.set()
        with pytest.raises(SyncError):
            future.result(timeout=5)
    assert book.snapshot()["executions"] == 0


def test_journal_takeover_after_collection_cannot_post(setup, monkeypatch):
    clock, journal, capture, book = setup
    original = capture._monitor.resync

    def takeover(*args, **kwargs):
        assessment = original(*args, **kwargs)
        capture_for(EventJournal(journal.path.parent, "synthetic"), clock)
        return assessment

    monkeypatch.setattr(capture._monitor, "resync", takeover)
    with pytest.raises(JournalError):
        resync(clock, capture, book)
    assert capture.status()["capture_failed"]
    assert book.snapshot()["executions"] == 0


def test_journal_epoch_is_reserved_through_cash_commit(setup, monkeypatch):
    clock, journal, capture, book = setup
    monkeypatch.setattr("trading.event_journal.BUSY_TIMEOUT_SECONDS", 0.05)
    original = book.apply
    head = journal.inspect()["head"]

    def checked(batch):
        peer = EventJournal(journal.path.parent, "synthetic")
        with pytest.raises(JournalError, match="journal_busy"):
            peer.start_session(
                expected_head=head, at=clock.wall, monotonic_ns=int(clock.mono * 1e9)
            )
        assert journal.inspect()["head"] == head
        return original(batch)

    monkeypatch.setattr(book, "apply", checked)
    assert resync(clock, capture, book).execution_cash["applied_execution_ids"] == (501,)
    assert journal.inspect()["epoch"] == 1


def test_unknown_delivery_from_prior_epoch_blocks_cash_even_with_new_matching_notice(setup):
    clock, journal, capture, book = setup
    journal.record(
        capture._session,
        "EVENT",
        at=clock.wall,
        monotonic_ns=0,
        sequence=2,
        payload=raw(execution()),
    )
    newer = capture_for(EventJournal(journal.path.parent, "synthetic"), clock)
    newer.ingest(1, raw(execution()))
    with pytest.raises(JournalError, match="capture_delivery_unresolved"):
        resync(clock, newer, book)
    assert newer.status()["capture_failed"]
    assert book.snapshot()["executions"] == 0


@pytest.mark.parametrize("after_commit", [False, True])
def test_ambiguous_failure_stops_capture_and_fresh_retry_preserves_once_only_booking(
    setup, monkeypatch, after_commit
):
    clock, journal, capture, book = setup
    original = book.apply

    def failed(batch):
        if after_commit:
            original(batch)
        raise RuntimeError("secret-not-for-errors")

    monkeypatch.setattr(book, "apply", failed)
    with pytest.raises(SyncError, match="^execution_cash_posting_failed$"):
        resync(clock, capture, book)
    assert capture.status()["capture_failed"]
    assert book.snapshot()["executions"] == int(after_commit)
    reopened = capture_for(EventJournal(journal.path.parent, "synthetic"), clock)
    reopened.ingest(1, raw(execution()))
    restored = ExecutionCashBook(book.path.parent, "synthetic")
    result = resync(clock, reopened, restored)
    assert result.execution_cash["applied_execution_ids"] == (() if after_commit else (501,))
    assert restored.snapshot()["executions"] == restored.snapshot()["proofs"] == 1


def test_expiry_during_commit_never_reports_a_current_observation(setup, monkeypatch):
    clock, _, capture, book = setup
    original = book.apply

    def slow(batch):
        result = original(batch)
        clock.advance(31)
        return result

    monkeypatch.setattr(book, "apply", slow)
    with pytest.raises(SyncError, match="execution_cash_posting_failed"):
        resync(clock, capture, book)
    assert capture.status()["capture_failed"]
    # Valid historical postings are never reversed to make a stale report fresh.
    assert book.snapshot()["execution_ids"] == (501,)


@pytest.mark.parametrize("fail", [False, True])
def test_receiver_forwards_explicit_book_and_closes_on_cash_failure(tmp_path, fail):
    clock, journal, capture, _, sock, receiver, _ = stream_setup(tmp_path)
    book = ExecutionCashBook.create(
        tmp_path / "cash",
        "synthetic",
        OpeningCash(balance="1000000", cutoff=clock.wall - timedelta(seconds=1)),
    )
    stream_start(journal, receiver)
    row = execution(
        orderTimestamp=clock.wall.isoformat(), executionTimestamp=clock.wall.isoformat()
    )
    sock.messages.append(raw(row))
    receiver.step()
    try:
        if fail:
            with pytest.raises(SyncError):
                receiver.resync(lambda: report(clock), cash_book=book)
            assert sock.closed and receiver.status()["stream_closed"]
            assert book.snapshot()["executions"] == 0
        else:
            result = receiver.resync(
                lambda: report(clock),
                collect_orders=lambda ids: (read_order(clock, [row]),),
                cash_book=book,
            )
            assert result.execution_cash["applied_execution_ids"] == (501,)
            assert receiver.status()["stream_running"]
    finally:
        receiver.close()


def test_offline_demo_preserves_unexplained_balance_and_does_not_overwrite(tmp_path):
    directory = tmp_path / "demo"
    result = demo(directory)
    assert result["first_post"]["execution_cash"]["applied_execution_ids"] == [501]
    assert result["duplicate"]["execution_cash"]["already_applied_execution_ids"] == [501]
    assert result["restart_duplicate"]["execution_cash"]["already_applied_execution_ids"] == [501]
    assert result["unexplained_cash"]["mismatches"] == ["balance_change_unverified"]
    assert Decimal(result["cash_book"]["balance"]) == 999_998
    assert Decimal(result["balance_comparison"]["difference"]) == 100
    assert not result["complete"] and not result["live_enabled"]
    assert (directory / "report.json").is_file()
    with pytest.raises(FileExistsError):
        demo(directory)


def test_process_exit_after_cash_commit_is_reconciled_without_rebooking(tmp_path):
    directory = tmp_path / "crash"
    script = """
import os, sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from trading.account_read_lab import replay
from trading.account_sync_lab import demo_execution_transcript
from trading.event_capture import JournaledEventCapture
from trading.event_journal import EventJournal
from trading.execution_cash_book import ExecutionCashBook, OpeningCash
from trading.execution_cash_lab import synthetic_batch
root = Path(sys.argv[1])
now = datetime(2026, 10, 2, tzinfo=UTC)
journal = EventJournal.create(root / 'journal', 'synthetic')
book = ExecutionCashBook.create(root / 'cash', 'synthetic',
    OpeningCash(balance='1000000', cutoff=now-timedelta(seconds=1)))
capture = JournaledEventCapture(journal, clock=lambda: now, monotonic_ns=lambda: 0)
capture.start_session(expected_head=journal.inspect()['head'])
scenario = demo_execution_transcript(now)
capture.ingest(1, scenario.steps[1].payload.encode())
original = book.apply
def interrupted(batch):
    original(batch)
    os._exit(17)
book.apply = interrupted
capture.resync(lambda: replay(scenario.steps[2].transcript),
    collect_orders=lambda ids: synthetic_batch(now).reports, cash_book=book)
"""
    process = subprocess.run(
        [sys.executable, "-c", script, str(directory)], capture_output=True, timeout=15
    )
    assert process.returncode == 17, process.stderr.decode()
    book = ExecutionCashBook(directory / "cash", "synthetic")
    assert book.snapshot()["execution_ids"] == (501,)
    from trading.account_read_lab import replay
    from trading.account_sync_lab import demo_execution_transcript
    from trading.execution_cash_lab import synthetic_batch

    now = NOW.replace(month=10, day=2)
    clock = Clock()
    clock.wall = now
    journal = EventJournal(directory / "journal", "synthetic")
    assert not journal.inspect()["unacknowledged_records"]
    capture = capture_for(journal, clock)
    scenario = demo_execution_transcript(now)
    capture.ingest(1, scenario.steps[1].payload.encode())
    result = capture.resync(
        lambda: replay(scenario.steps[2].transcript),
        collect_orders=lambda ids: synthetic_batch(now).reports,
        cash_book=book,
    )
    assert result.execution_cash["already_applied_execution_ids"] == (501,)
    assert not result.execution_cash["applied_execution_ids"]
    assert book.snapshot()["proofs"] == book.snapshot()["executions"] == 1
