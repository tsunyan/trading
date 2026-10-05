"""Current inventory evidence, expiry and stable journal/book fences."""

from datetime import timedelta
from decimal import Decimal

import pytest
from test_account_events import execution, raw
from test_account_sync import Clock
from test_execution_cash_sync import capture_for
from test_execution_positions import batch
from test_position_reservations import account, create

from trading.account_sync import AccountSyncMonitor, SyncError
from trading.event_journal import EventJournal, JournalError
from trading.execution_cash_book import ExecutionCashBook, OpeningCash
from trading.execution_positions import PositionBasis


@pytest.fixture
def setup(tmp_path):
    clock = Clock()
    journal = EventJournal.create(tmp_path / "journal", "synthetic")
    return clock, journal, capture_for(journal, clock), create(tmp_path)


def compare(capture, book, result):
    return capture.compare_account_inventory(book, expected_revision=result.revision)


def test_matching_inventory_uses_retained_report_and_never_writes(setup):
    clock, _, capture, book = setup
    before = book.snapshot()
    result = capture.resync(lambda: account(now=clock.wall))
    # Even forced mutation of a returned frozen model cannot alter retained proof.
    object.__setattr__(result.report.assets, "balance", Decimal("1"))
    object.__setattr__(result.report, "positions", ())
    diagnostic = compare(capture, book, result)
    assert diagnostic["balance"]["balance_match"] and diagnostic["positions"]["position_match"]
    assert diagnostic["head"] == before["head"]
    assert diagnostic["revision"] == result.revision and diagnostic["epoch"] == result.epoch
    assert not diagnostic["complete"] and not diagnostic["live_enabled"]
    assert book.snapshot() == before


@pytest.mark.parametrize("kind", ["missing", "units", "side", "price", "unexpected"])
def test_position_difference_with_equal_balance_is_diagnostic(setup, kind):
    clock, _, capture, book = setup
    source = account(now=clock.wall)
    changes = (
        {"units": 300}
        if kind == "units"
        else {"side": "SELL"}
        if kind == "side"
        else {"price": Decimal("151")}
    )
    positions = () if kind == "missing" else (source.positions[0].model_copy(update=changes),)
    if kind == "unexpected":
        positions = (*source.positions, source.positions[0].model_copy(update={"position_id": 402}))
    result = capture.resync(lambda: source.model_copy(update={"positions": positions}))
    diagnostic = compare(capture, book, result)
    assert diagnostic["balance"]["balance_match"]
    assert not diagnostic["positions"]["position_match"]


def test_no_basis_is_unknown_and_empty_basis_is_declared_flat(setup, tmp_path):
    clock, _, capture, _ = setup
    legacy = ExecutionCashBook.create(
        tmp_path / "legacy",
        "synthetic",
        OpeningCash(balance="1000000", cutoff=clock.wall - timedelta(seconds=1)),
    )
    flat = ExecutionCashBook.create(
        tmp_path / "flat",
        "synthetic",
        OpeningCash(
            balance="1000000",
            cutoff=clock.wall - timedelta(seconds=1),
            position_basis=PositionBasis(positions=()),
        ),
    )
    result = capture.resync(lambda: account(now=clock.wall))
    diagnostic = compare(capture, legacy, result)
    assert diagnostic["positions"] is None
    assert "cash_book_position_basis_required" in diagnostic["blockers"]
    assert not compare(capture, flat, result)["positions"]["position_match"]


@pytest.mark.parametrize(
    "kind", ["event", "duplicate", "disconnect", "expiry", "resync", "clock", "takeover"]
)
def test_old_evidence_cannot_survive_invalidation(setup, kind):
    clock, journal, capture, book = setup
    if kind == "duplicate":
        capture.ingest(1, raw(execution()))
    result = capture.resync(lambda: account(now=clock.wall))
    if kind in {"event", "duplicate"}:
        capture.ingest(2 if kind == "duplicate" else 1, raw(execution()))
    elif kind == "disconnect":
        capture.disconnect()
    elif kind == "expiry":
        clock.advance(31)
    elif kind == "resync":
        capture.resync(lambda: account(now=clock.wall))
    elif kind == "clock":
        clock.wall -= timedelta(seconds=1)
    else:
        capture_for(EventJournal(journal.path.parent, "synthetic"), clock)
    with pytest.raises((SyncError, JournalError)):
        compare(capture, book, result)


def test_mismatched_baseline_does_not_disable_inventory_expiry(setup):
    clock, _, capture, book = setup
    capture.resync(lambda: account(now=clock.wall))
    source = account(now=clock.wall)
    source = source.model_copy(
        update={"assets": source.assets.model_copy(update={"balance": Decimal("1000001")})}
    )
    result = capture.resync(lambda: source)
    assert result.mismatches == ("balance_change_unverified",)
    assert not compare(capture, book, result)["balance"]["balance_match"]
    clock.advance(31)
    with pytest.raises(SyncError):
        compare(capture, book, result)


@pytest.mark.parametrize("kind", ["head", "expiry", "interrupt", "exception"])
def test_changes_during_comparison_fail_without_undoing_cash(setup, monkeypatch, kind):
    clock, _, capture, book = setup
    result = capture.resync(lambda: account(now=clock.wall))
    original = book.compare_balance

    def changed(*args, **kwargs):
        diagnostic = original(*args, **kwargs)
        if kind == "head":
            ExecutionCashBook(book.path.parent, "synthetic").apply(batch(execution()))
        elif kind == "expiry":
            clock.advance(31)
        elif kind == "interrupt":
            raise KeyboardInterrupt
        else:
            raise RuntimeError("secret")
        return diagnostic

    monkeypatch.setattr(book, "compare_balance", changed)
    with pytest.raises(KeyboardInterrupt if kind == "interrupt" else SyncError):
        compare(capture, book, result)
    assert capture.status()["capture_failed"]
    assert book.snapshot()["execution_ids"] == ((501,) if kind == "head" else ())


def test_journal_is_reserved_while_inventory_is_compared(setup, monkeypatch):
    clock, journal, capture, book = setup
    monkeypatch.setattr("trading.event_journal.BUSY_TIMEOUT_SECONDS", 0)
    result = capture.resync(lambda: account(now=clock.wall))
    original = book.compare_positions

    def checked(*args, **kwargs):
        peer = EventJournal(journal.path.parent, "synthetic")
        with pytest.raises(JournalError, match="journal_busy"):
            peer.start_session(expected_head=journal.head(), at=clock.wall, monotonic_ns=0)
        return original(*args, **kwargs)

    monkeypatch.setattr(book, "compare_positions", checked)
    assert compare(capture, book, result)["positions"]["position_match"]


def test_reentrant_event_invalidates_inventory_during_comparison(tmp_path, monkeypatch):
    clock = Clock()
    monitor = AccountSyncMonitor(**clock.args())
    session = monitor.start_session()
    book = create(tmp_path)
    result = monitor.resync(session, lambda: account(now=clock.wall))
    original = book.compare_balance

    def changed(*args, **kwargs):
        diagnostic = original(*args, **kwargs)
        monitor.ingest(session, 1, raw(execution()))
        return diagnostic

    monkeypatch.setattr(book, "compare_balance", changed)
    with pytest.raises(SyncError, match="account_inventory_comparison_failed"):
        monitor.compare_account_inventory(session, book, expected_revision=result.revision)
