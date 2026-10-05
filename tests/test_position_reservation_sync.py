"""Current reservation evidence is fenced by capture revision, clock and journal epoch."""

import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from decimal import Decimal

import pytest
from test_account_events import execution, raw
from test_account_sync import Clock
from test_execution_cash_sync import capture_for
from test_execution_positions import batch
from test_execution_reconciliation import read_order
from test_position_reservations import account, create, order, partial_row
from test_private_stream import setup as stream_setup
from test_private_stream import start as stream_start

from trading.account_sync import AccountSyncMonitor, SyncError
from trading.event_journal import EventJournal, JournalError
from trading.execution_cash_book import ExecutionCashBook, OpeningCash
from trading.execution_positions import OpeningPosition, PositionBasis
from trading.position_reservation_sync_lab import demo


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("network attempted"))


@pytest.fixture
def setup(tmp_path):
    clock = Clock()
    journal = EventJournal.create(tmp_path / "journal", "synthetic")
    capture = capture_for(journal, clock)
    book = create(tmp_path)
    return clock, journal, capture, book


def resync(clock, capture, book=None, *, ordered=300, **changes):
    return capture.resync(
        lambda: account(order(now=clock.wall), now=clock.wall, ordered=ordered),
        collect_reservations=lambda: (order(now=clock.wall),),
        reservation_book=book,
        **changes,
    )


def test_opt_in_comparison_collects_orders_before_account_and_never_writes(setup):
    clock, journal, capture, book = setup
    phases = []
    before = book.snapshot()

    def collect_orders():
        phases.append("orders")
        return (order(now=clock.wall),)

    def collect_account():
        phases.append("account")
        return account(order(now=clock.wall), now=clock.wall)

    result = capture.resync(
        collect_account, collect_reservations=collect_orders, reservation_book=book
    )
    assert phases == ["orders", "account"]
    comparison = result.position_reservations
    assert comparison["reservation_match"] and comparison["revision"] == result.revision
    assert comparison["epoch"] == result.epoch and comparison["received_sequence"] == 0
    assert comparison["head"] == before["head"]
    assert result.structural_match and not result.complete and not result.live_enabled
    assert "reservation_intents_not_authenticated" in result.blockers
    assert book.snapshot() == before and not journal.inspect()["unacknowledged_records"]


def test_omission_is_diagnostic_only_and_separate_comparison_uses_internal_evidence(setup):
    clock, _, capture, book = setup
    result = resync(clock, capture)
    assert result.position_reservations is None
    altered = result.model_copy(update={"report": account(ordered=0)})
    assert altered.report != result.report
    comparison = capture.compare_position_reservations(book, expected_revision=result.revision)
    assert comparison["reservation_match"]
    assert comparison["reservations"][0]["observed_ordered_units"] == 300


def test_reservation_difference_is_returned_without_promotion_or_correction(setup):
    clock, _, capture, book = setup
    before = book.snapshot()
    result = resync(clock, capture, book, ordered=100)
    assert result.structural_match  # Event/REST structure is a separate diagnostic.
    assert not result.position_reservations["reservation_match"]
    assert "position_reservation_difference_unexplained" in result.blockers
    assert not result.complete and not result.live_enabled and book.snapshot() == before


@pytest.mark.parametrize("kind", ["scope", "missing_collector", "different_book"])
def test_configuration_is_rejected_before_collection(setup, tmp_path, kind):
    _, _, capture, book = setup
    if kind == "scope":
        other = ExecutionCashBook.create(
            tmp_path / "wrong",
            "other",
            OpeningCash(balance="1000000", cutoff=book.snapshot()["opening"]["cutoff"]),
        )
        changes = {"reservation_book": other, "collect_reservations": lambda: ()}
        code = "execution_cash_scope_mismatch"
    elif kind == "missing_collector":
        changes, code = {"reservation_book": book}, "reservations_require_order_collection"
    else:
        other = create(tmp_path / "other")
        changes = {
            "reservation_book": book,
            "cash_book": other,
            "collect_orders": lambda ids: (),
            "collect_reservations": lambda: (),
        }
        code = "reservation_cash_book_mismatch"
    with pytest.raises(SyncError, match=code):
        capture.resync(lambda: pytest.fail("invalid configuration reached REST"), **changes)
    assert not capture.status()["capture_failed"]


@pytest.mark.parametrize(
    "kind", ["event", "disconnect", "expired", "clock", "resync", "collector_omitted"]
)
def test_old_revision_or_invalidated_evidence_cannot_be_compared(setup, kind):
    clock, _, capture, book = setup
    accepted = resync(clock, capture)
    if kind == "event":
        capture.ingest(1, raw(execution()))
    elif kind == "disconnect":
        capture.disconnect()
    elif kind == "expired":
        clock.advance(31)
    elif kind == "clock":
        clock.wall -= timedelta(seconds=1)
    elif kind == "resync":
        resync(clock, capture)
    else:
        capture.resync(lambda: account(order(now=clock.wall), now=clock.wall))
    with pytest.raises((SyncError, JournalError)):
        capture.compare_position_reservations(book, expected_revision=accepted.revision)


def test_balance_difference_does_not_disable_reservation_evidence_expiry(setup):
    clock, _, capture, book = setup
    resync(clock, capture)
    result = capture.resync(
        lambda: account(order(now=clock.wall), now=clock.wall).model_copy(
            update={"assets": account().assets.model_copy(update={"balance": Decimal("1000001")})}
        ),
        collect_reservations=lambda: (order(now=clock.wall),),
    )
    assert result.mismatches == ("balance_change_unverified",)
    assert capture.compare_position_reservations(book, expected_revision=result.revision)[
        "reservation_match"
    ]
    clock.advance(31)
    with pytest.raises(SyncError):
        capture.compare_position_reservations(book, expected_revision=result.revision)


@pytest.mark.parametrize("kind", ["old", "future", "list", "oversized", "exception", "interrupted"])
def test_collector_failures_are_bounded_and_sanitized(setup, kind):
    clock, _, capture, _ = setup

    def collect():
        if kind == "exception":
            raise RuntimeError("secret")
        if kind == "interrupted":
            raise KeyboardInterrupt
        item = order(
            now=clock.wall + timedelta(seconds=(-1 if kind == "old" else 1))
            if kind in {"old", "future"}
            else clock.wall
        )
        return [item] if kind == "list" else (item,) * 1001 if kind == "oversized" else (item,)

    with pytest.raises(KeyboardInterrupt if kind == "interrupted" else SyncError):
        capture.resync(
            lambda: account(order(now=clock.wall), now=clock.wall), collect_reservations=collect
        )
    assert capture.status()["resync_required"]


@pytest.mark.parametrize("phase", ["orders", "account"])
def test_event_during_collection_is_delivered_and_invalidates_attempt(setup, phase):
    clock, _, capture, book = setup
    entered, release = threading.Event(), threading.Event()

    def wait_if(name):
        if phase == name:
            entered.set()
            assert release.wait(5)

    def orders():
        wait_if("orders")
        return (order(now=clock.wall),)

    def read():
        wait_if("account")
        return account(order(now=clock.wall), now=clock.wall)

    before = book.snapshot()
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            capture.resync, read, collect_reservations=orders, reservation_book=book
        )
        assert entered.wait(5)
        try:
            capture.ingest(1, raw(execution()))
        finally:
            release.set()
        with pytest.raises(SyncError):
            future.result(timeout=5)
    assert capture.status()["received_sequence"] == 1 and book.snapshot() == before


def test_new_journal_epoch_or_unresolved_delivery_cannot_reuse_comparison(setup):
    clock, journal, capture, book = setup
    accepted = resync(clock, capture)
    newer = capture_for(EventJournal(journal.path.parent, "synthetic"), clock)
    with pytest.raises(JournalError):
        capture.compare_position_reservations(book, expected_revision=accepted.revision)
    with pytest.raises(SyncError, match="reservations_require_current_collection"):
        newer.compare_position_reservations(book, expected_revision=newer.status()["revision"])


def test_journal_takeover_during_collection_prevents_comparison(setup, monkeypatch):
    clock, journal, capture, book = setup
    original = capture._monitor.resync

    def takeover(*args, **kwargs):
        result = original(*args, **kwargs)
        capture_for(EventJournal(journal.path.parent, "synthetic"), clock)
        return result

    monkeypatch.setattr(capture._monitor, "resync", takeover)
    with pytest.raises(JournalError):
        resync(clock, capture, book)
    assert capture.status()["capture_failed"]


def test_journal_epoch_is_reserved_during_comparison(setup, monkeypatch):
    clock, journal, capture, book = setup
    monkeypatch.setattr("trading.event_journal.BUSY_TIMEOUT_SECONDS", 0)
    original = book.compare_reservations

    def checked(*args, **kwargs):
        peer = EventJournal(journal.path.parent, "synthetic")
        with pytest.raises(JournalError, match="journal_busy"):
            peer.start_session(
                expected_head=journal.inspect()["head"],
                at=clock.wall,
                monotonic_ns=int(clock.mono * 1e9),
            )
        return original(*args, **kwargs)

    monkeypatch.setattr(book, "compare_reservations", checked)
    assert resync(clock, capture, book).position_reservations["reservation_match"]


@pytest.mark.parametrize("kind", ["exception", "expired", "interrupt"])
def test_comparison_failure_or_expiry_stops_capture_without_writes(setup, monkeypatch, kind):
    clock, _, capture, book = setup
    before = book.snapshot()
    original = book.compare_reservations

    def failed(*args, **kwargs):
        if kind == "expired":
            result = original(*args, **kwargs)
            clock.advance(31)
            return result
        if kind == "interrupt":
            raise KeyboardInterrupt
        raise RuntimeError("secret-not-for-output")

    monkeypatch.setattr(book, "compare_reservations", failed)
    with pytest.raises(
        KeyboardInterrupt if kind == "interrupt" else SyncError,
        match=None if kind == "interrupt" else "^position_reservation_comparison_failed$",
    ):
        resync(clock, capture, book)
    assert capture.status()["capture_failed"] and book.snapshot() == before


def test_monitor_reentrant_event_during_comparison_is_rejected(tmp_path, monkeypatch):
    clock = Clock()
    monitor = AccountSyncMonitor(**clock.args())
    session = monitor.start_session()
    book = create(tmp_path)
    result = monitor.resync(
        session, lambda: account(order()), collect_reservations=lambda: (order(),)
    )
    original = book.compare_reservations

    def changed(*args, **kwargs):
        comparison = original(*args, **kwargs)
        monitor.ingest(session, 1, raw(execution()))
        return comparison

    monkeypatch.setattr(book, "compare_reservations", changed)
    with pytest.raises(SyncError, match="position_reservation_comparison_failed"):
        monitor.compare_position_reservations(session, book, expected_revision=result.revision)


def test_post_then_compare_partial_close_uses_same_book_head_once(setup):
    clock, _, capture, book = setup
    row = partial_row()
    capture.ingest(1, raw(row))
    evidence = batch(row).reports[0]
    for index in range(2):
        result = capture.resync(
            lambda: account(evidence, units=300, ordered=200),
            collect_reservations=lambda: (evidence,),
            collect_orders=lambda ids: (read_order(clock, [row]),),
            cash_book=book,
            reservation_book=book,
        )
        assert result.execution_cash["applied_execution_ids"] == ((601,) if index == 0 else ())
        assert result.position_reservations["reservation_match"]
        assert result.position_reservations["head"] == result.execution_cash["head"]
    assert book.snapshot()["execution_ids"] == (601,)


def test_peer_book_update_between_posting_and_comparison_is_rejected(setup, monkeypatch):
    clock, _, capture, book = setup
    row = partial_row()
    capture.ingest(1, raw(row))
    original = book.compare_reservations

    def changed(*args, **kwargs):
        from trading.execution_cash_lab import synthetic_batch

        peer = ExecutionCashBook(book.path.parent, "synthetic")
        peer.apply(synthetic_batch(clock.wall - timedelta(microseconds=1)))
        return original(*args, **kwargs)

    monkeypatch.setattr(book, "compare_reservations", changed)
    evidence = batch(row).reports[0]
    with pytest.raises(SyncError, match="cash_book_changed_during_reservation_comparison"):
        capture.resync(
            lambda: account(evidence, units=300, ordered=200),
            collect_reservations=lambda: (evidence,),
            collect_orders=lambda ids: (read_order(clock, [row]),),
            cash_book=book,
            reservation_book=book,
        )
    assert capture.status()["capture_failed"]
    assert book.snapshot()["execution_ids"] == (501, 601)


@pytest.mark.parametrize("fail", [False, True])
def test_receiver_forwards_comparison_and_closes_on_failure(tmp_path, fail):
    clock, journal, _, _, sock, receiver, _ = stream_setup(tmp_path)
    book = ExecutionCashBook.create(
        tmp_path / "book",
        "synthetic",
        OpeningCash(
            balance="1000000",
            cutoff=clock.wall - timedelta(seconds=1),
            position_basis=PositionBasis(
                positions=(
                    OpeningPosition(position_id=401, side="BUY", units=400, average_price="150"),
                )
            ),
        ),
    )
    stream_start(journal, receiver)
    try:
        if fail:
            with pytest.raises(SyncError, match="reservations_require_order_collection"):
                receiver.resync(lambda: account(now=clock.wall), reservation_book=book)
            assert sock.closed and receiver.status()["stream_closed"]
        else:
            result = receiver.resync(
                lambda: account(order(now=clock.wall), now=clock.wall),
                collect_reservations=lambda: (order(now=clock.wall),),
                reservation_book=book,
            )
            assert result.position_reservations["reservation_match"]
            assert receiver.status()["stream_running"]
    finally:
        receiver.close()


def test_current_revision_without_collector_has_no_restored_reservation_proof(setup):
    clock, _, capture, book = setup
    resync(clock, capture)
    current = capture.resync(lambda: account(order(now=clock.wall), now=clock.wall))
    with pytest.raises(SyncError, match="reservations_require_current_collection"):
        capture.compare_position_reservations(book, expected_revision=current.revision)


def test_unresolved_prior_delivery_blocks_even_a_fresh_reservation_comparison(setup):
    clock, journal, capture, book = setup
    before = book.snapshot()
    journal.record(
        capture._session,
        "EVENT",
        at=clock.wall,
        monotonic_ns=0,
        sequence=1,
        payload=raw(execution()),
    )
    newer = capture_for(EventJournal(journal.path.parent, "synthetic"), clock)
    with pytest.raises(JournalError, match="capture_delivery_unresolved"):
        resync(clock, newer, book)
    assert newer.status()["capture_failed"] and book.snapshot() == before


def test_collection_deadline_covers_reservation_collector(setup):
    clock, _, capture, book = setup

    def slow():
        clock.advance(31)
        return (order(now=clock.wall),)

    with pytest.raises(SyncError, match="sync_collection_expired"):
        capture.resync(
            lambda: account(order(now=clock.wall), now=clock.wall),
            collect_reservations=slow,
            reservation_book=book,
        )
    assert capture.status()["resync_required"]


def test_offline_demo_preserves_mismatch_expiry_and_new_epoch(tmp_path):
    result = demo(tmp_path / "demo")
    for name in ("initial", "partial", "restart"):
        assert result[name]["position_reservations"]["reservation_match"]
    assert not result["unexplained_reservation"]["position_reservations"]["reservation_match"]
    assert result["expired_comparison"] == "stale_reservation_sync_revision"
    assert "journal_epoch_gap_not_repaired" in result["restart"]["blockers"]
    assert result["cash_book"]["balance"] == "1000008.00000000"
    assert result["cash_book"]["executions"] == 1
    assert not result["complete"] and not result["live_enabled"]
    with pytest.raises(FileExistsError):
        demo(tmp_path / "demo")


def test_explicit_empty_collection_is_valid_and_distinct_from_omission(setup):
    clock, _, capture, book = setup
    result = capture.resync(
        lambda: account(ordered=0, now=clock.wall),
        collect_reservations=lambda: (),
        reservation_book=book,
    )
    assert result.position_reservations["reservation_match"]
    assert result.position_reservations["orders"] == ()


def test_monitor_passes_clock_skew_to_book_without_loosening_receipt_freshness(tmp_path):
    clock = Clock()
    monitor = AccountSyncMonitor(**clock.args(), clock_skew_ms=10)
    session = monitor.start_session()
    book = create(tmp_path)
    evidence = order()
    observed_at = clock.wall + timedelta(milliseconds=10)
    evidence = evidence.model_copy(
        update={
            "evidence": evidence.evidence.model_copy(update={"observed_at": observed_at}),
            "observations": tuple(
                o.model_copy(update={"response_at": observed_at}) for o in evidence.observations
            ),
        }
    )
    source = account(order())
    source = source.model_copy(
        update={
            "observations": tuple(
                o.model_copy(update={"response_at": observed_at}) for o in source.observations
            )
        }
    )
    result = monitor.resync(session, lambda: source, collect_reservations=lambda: (evidence,))
    assert monitor.compare_position_reservations(session, book, expected_revision=result.revision)[
        "reservation_match"
    ]


def test_comparison_failure_after_cash_commit_requires_fresh_retry_and_never_rebooks(
    setup, monkeypatch
):
    clock, journal, capture, book = setup
    row = partial_row()
    evidence = batch(row).reports[0]
    capture.ingest(1, raw(row))

    def run(current):
        return current.resync(
            lambda: account(evidence, units=300, ordered=200),
            collect_reservations=lambda: (evidence,),
            collect_orders=lambda ids: (read_order(clock, [row]),),
            cash_book=book,
            reservation_book=book,
        )

    with monkeypatch.context() as context:

        def failed(*args, **kwargs):
            raise RuntimeError("private comparison failure")

        context.setattr(book, "compare_reservations", failed)
        with pytest.raises(SyncError, match="position_reservation_comparison_failed"):
            run(capture)
    assert capture.status()["capture_failed"] and book.snapshot()["execution_ids"] == (601,)
    newer = capture_for(EventJournal(journal.path.parent, "synthetic"), clock)
    newer.ingest(1, raw(row))
    result = run(newer)
    assert result.execution_cash["already_applied_execution_ids"] == (601,)
    assert not result.execution_cash["applied_execution_ids"]
    assert result.position_reservations["reservation_match"]
    assert book.snapshot()["balance"] == "1000008.00000000"
