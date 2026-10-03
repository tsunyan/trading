"""Durable receipts reduce REST work without converting history into fresh proof."""

import sqlite3
from datetime import timedelta
from decimal import Decimal

import pytest
from test_account_events import NOW, execution, raw
from test_account_sync import Clock, report
from test_execution_reconciliation import read_order

from trading.account_events import parse_event
from trading.account_sync import SyncError
from trading.event_capture import JournaledEventCapture
from trading.event_journal import EventJournal
from trading.execution_cash_book import CashBookError, ExecutionCashBook, OpeningCash


def completed(index=0, **changes):
    return execution(
        orderId=201 + index,
        rootOrderId=201 + index,
        clientOrderId=f"Complete{index}",
        executionId=501 + index,
        positionId=401 + index,
        executionSize="1000",
        orderExecutedSize="1000",
        **changes,
    )


@pytest.fixture
def setup(tmp_path):
    clock = Clock()
    journal = EventJournal.create(tmp_path / "journal", "synthetic")
    capture = JournaledEventCapture(
        journal, clock=lambda: clock.wall, monotonic_ns=lambda: int(clock.mono * 1e9)
    )
    capture.start_session(expected_head=journal.head())
    book = ExecutionCashBook.create(
        tmp_path / "cash",
        "synthetic",
        OpeningCash(balance="1000000", cutoff=NOW - timedelta(seconds=1)),
    )
    return clock, journal, capture, book


def sync(setup, rows, *, incremental=True, on_collect=None, status="EXECUTED"):
    clock, _, capture, book = setup
    requested = []

    def collect_orders(ids):
        requested.append(ids)
        if on_collect:
            on_collect()
        return tuple(
            read_order(
                clock,
                [r for r in rows if r["orderId"] == order_id],
                order_changes={"status": status},
            )
            for order_id in ids
        )

    result = capture.resync(
        lambda: report(clock, units=None, orders=False, balance=str(1000000 - 2 * len(rows))),
        collect_orders=collect_orders,
        cash_book=book,
        incremental_cash=incremental,
    )
    return result, requested


def test_fully_booked_order_is_not_retrieved_or_counted_as_fresh(setup):
    clock, _, capture, book = setup
    row = completed()
    capture.ingest(1, raw(row))
    first, requested = sync(setup, [row])
    assert requested == [(201,)]
    assert first.execution_cash["applied_execution_ids"] == (501,)
    head = book.snapshot()["head"]
    clock.advance(31)  # Prior REST proof has expired for current observation purposes.
    capture.heartbeat()
    second, requested = sync(setup, [row])
    assert requested == []
    assert second.execution_reconciliation is None and second.execution_cash is None
    assert second.previously_booked_execution_ids == (501,)
    assert second.booked_cash_head == head
    assert not second.unverified_execution_ids
    assert not second.complete and not second.live_enabled
    assert book.snapshot()["head"] == head
    view = capture.status()
    assert view["reconciled_executions"] == 0
    assert view["previously_booked_executions"] == 1


def test_40_orders_in_one_epoch_never_repeat_the_39_old_requests(setup):
    clock, journal, capture, book = setup
    rows = []
    for index in range(40):
        row = completed(index)
        rows.append(row)
        capture.ingest(index + 1, raw(row))
        result, requests = sync(setup, rows, on_collect=lambda: clock.advance(1))
        assert requests == [(row["orderId"],)]
        assert result.execution_cash["applied_execution_ids"] == (row["executionId"],)
        assert len(result.previously_booked_execution_ids) == index
        assert result.execution_reconciliation.matched_execution_ids == (row["executionId"],)
    assert Decimal(book.snapshot()["balance"]) == 999920
    assert book.snapshot()["executions"] == 40
    assert not journal.inspect()["unacknowledged_records"]
    assert clock.mono == 40


def test_partial_orders_are_retrieved_until_final_complete_report(setup):
    clock, _, capture, book = setup
    first = execution()
    capture.ingest(1, raw(first))
    result, requests = sync(setup, [first], status="ORDERED")
    assert requests == [(201,)] and result.execution_cash["applied_execution_ids"] == (501,)
    result, requests = sync(setup, [first], status="ORDERED")
    assert requests == [(201,)]
    assert result.previously_booked_execution_ids == ()
    second = execution(executionId=502, executionSize="600", orderExecutedSize="1000")
    capture.ingest(2, raw(second))
    result, requests = sync(setup, [first, second])
    assert requests == [(201,)]
    assert result.execution_cash["applied_execution_ids"] == (502,)
    assert result.execution_cash["already_applied_execution_ids"] == (501,)
    result, requests = sync(setup, [first, second])
    assert requests == [] and result.previously_booked_execution_ids == (501, 502)
    assert book.snapshot()["executions"] == 2


def test_default_full_collection_remains_available(setup):
    _, _, capture, _ = setup
    row = completed()
    capture.ingest(1, raw(row))
    sync(setup, [row])
    result, requests = sync(setup, [row], incremental=False)
    assert requests == [(201,)]
    assert result.previously_booked_execution_ids == ()
    assert result.execution_cash["already_applied_execution_ids"] == (501,)


def test_restart_matches_against_durable_receipts_without_restoring_account_proof(setup):
    clock, journal, capture, book = setup
    row = completed()
    capture.ingest(1, raw(row))
    sync(setup, [row])
    reopened = JournaledEventCapture(
        EventJournal(journal.path.parent, "synthetic"),
        clock=lambda: clock.wall,
        monotonic_ns=lambda: int(clock.mono * 1e9),
    )
    reopened.start_session(expected_head=journal.head())
    reopened.ingest(1, raw(row))
    restored = ExecutionCashBook(book.path.parent, "synthetic")
    result, requests = sync((clock, journal, reopened, restored), [row])
    assert requests == [] and result.previously_booked_execution_ids == (501,)
    assert "journal_epoch_gap_not_repaired" in result.blockers
    assert not result.complete and not result.live_enabled
    assert restored.snapshot()["proofs"] == 1


@pytest.mark.parametrize(
    "change",
    [
        {"executionPrice": "151"},
        {"fee": "-3", "amount": "-3"},
        {"rootOrderId": 202},
        {"clientOrderId": "AnotherOrder"},
        {"positionId": 402},
    ],
)
def test_conflicting_known_notice_persists_stop_before_network(setup, change):
    clock, _, capture, book = setup
    row = completed()
    capture.ingest(1, raw(row))
    sync(setup, [row])
    before = book.snapshot()
    changed = parse_event(raw({**row, **change}), clock.wall)
    with pytest.raises(CashBookError, match="identity_conflict"):
        book.match_booked_events((changed,))
    after = ExecutionCashBook(book.path.parent, "synthetic").snapshot()
    assert after["halted"] and after["reason"] == "cash_book_identity_conflict"
    assert after["balance"] == before["balance"] and after["head"] == before["head"]


def test_equivalent_numeric_wire_spelling_is_not_a_conflict(setup):
    clock, _, capture, book = setup
    row = completed()
    capture.ingest(1, raw(row))
    sync(setup, [row])
    equivalent = parse_event(raw({**row, "executionPrice": "150.0"}), clock.wall)
    lookup = book.match_booked_events((equivalent,))
    assert lookup["booked_execution_ids"] == (501,)
    assert lookup["fully_booked_order_ids"] == (201,)
    assert lookup["historical_evidence_only"] and not lookup["live_enabled"]


def test_extra_notice_for_fully_booked_order_is_never_dropped(setup):
    clock, _, capture, book = setup
    first = completed()
    capture.ingest(1, raw(first))
    sync(setup, [first])
    extra = {**first, "executionId": 999}
    capture.ingest(2, raw(extra))
    requests = []

    def collect_orders(ids):
        requests.append(ids)
        return (read_order(clock, [first], order_changes={"status": "EXECUTED"}),)

    result = capture.resync(
        lambda: report(clock, units=None, orders=False),
        collect_orders=collect_orders,
        cash_book=book,
        incremental_cash=True,
    )
    assert requests == [(201,)]
    assert result.execution_reconciliation.unverified_execution_ids == (999,)
    assert result.execution_cash is None
    assert book.snapshot()["executions"] == 1


def test_event_arrival_during_incremental_collection_still_invalidates_it(setup):
    _, _, capture, book = setup
    first = completed()
    capture.ingest(1, raw(first))
    sync(setup, [first])
    second = completed(1)
    capture.ingest(2, raw(second))
    with pytest.raises(SyncError):
        sync(setup, [first, second], on_collect=lambda: capture.ingest(3, raw(second)))
    assert book.snapshot()["executions"] == 1


def test_external_book_change_during_collection_refuses_prior_lookup_head(setup):
    clock, _, capture, book = setup
    first = completed()
    capture.ingest(1, raw(first))
    sync(setup, [first])
    second = completed(1)
    from trading.execution_cash_book import ExecutionCashBatch

    def alter_book():
        book.apply(
            ExecutionCashBatch(
                events=(parse_event(raw(second), clock.wall),),
                reports=(read_order(clock, [second], order_changes={"status": "EXECUTED"}),),
            )
        )
        return report(clock, units=None, orders=False)

    with pytest.raises(SyncError, match="cash_book_changed_during_collection"):
        capture.resync(
            alter_book, collect_orders=lambda _: (), cash_book=book, incremental_cash=True
        )
    assert book.snapshot()["executions"] == 2


def test_corruption_in_saved_proof_is_not_used_to_skip_network(setup):
    _, _, capture, book = setup
    first = completed()
    capture.ingest(1, raw(first))
    sync(setup, [first])
    with sqlite3.connect(book.path) as conn:
        conn.execute("UPDATE proofs SET body='{}'")
    with pytest.raises(SyncError):
        capture.resync(
            lambda: pytest.fail("corrupt proof reached network"),
            collect_orders=lambda _: (),
            cash_book=book,
            incremental_cash=True,
        )


@pytest.mark.parametrize("flag", [True, 1, "yes"])
def test_incremental_mode_requires_explicit_book_before_collection(setup, flag):
    _, _, capture, _ = setup
    with pytest.raises(SyncError, match="incremental_cash_requires_explicit_book"):
        capture.resync(lambda: pytest.fail("invalid option reached network"), incremental_cash=flag)
