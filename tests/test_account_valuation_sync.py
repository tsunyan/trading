"""Fresh valuation evidence, journal fencing and optional stream integration."""

import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from decimal import Decimal

import pytest
from test_account_events import NOW, execution, raw
from test_account_sync import Clock
from test_execution_cash_sync import capture_for
from test_execution_positions import close
from test_execution_reconciliation import read_order
from test_position_reservations import create
from test_private_stream import setup as stream_setup
from test_private_stream import start as stream_start

from trading.account_sync import AccountSyncMonitor, SyncError
from trading.account_valuation import ValuationQuote
from trading.account_valuation_lab import synthetic_account, synthetic_policy
from trading.account_valuation_sync_lab import demo
from trading.cash_transfer_lab import synthetic_match
from trading.event_journal import EventJournal, JournalError
from trading.execution_cash_book import ExecutionCashBook, OpeningCash
from trading.execution_positions import OpeningPosition, PositionBasis


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("network attempted"))


@pytest.fixture
def setup(tmp_path):
    clock = Clock()
    journal = EventJournal.create(tmp_path / "journal", "synthetic")
    return clock, journal, capture_for(journal, clock), create(tmp_path, transfers=True)


def quote(now=NOW):
    return ValuationQuote(bid="149.9", ask="150.1", observed_at=now)


def resync(clock, capture, book=None, **changes):
    return capture.resync(
        lambda: synthetic_account(clock.wall),
        collect_quote=lambda: quote(clock.wall),
        valuation_policy=synthetic_policy(),
        valuation_book=book,
        **changes,
    )


def test_opt_in_read_only_comparison_and_collection_order(setup):
    clock, journal, capture, book = setup
    phases = []

    def account():
        phases.append("account")
        return synthetic_account(clock.wall)

    def price():
        phases.append("quote")
        return quote(clock.wall)

    before = book.snapshot()
    result = capture.resync(
        account,
        collect_quote=price,
        valuation_policy=synthetic_policy(),
        valuation_book=book,
    )
    assert phases == ["account", "quote"]
    diagnostic = result.account_valuation
    assert diagnostic["diagnostics_match"] and diagnostic["head"] == before["head"]
    assert diagnostic["revision"] == result.revision and diagnostic["epoch"] == result.epoch
    assert diagnostic["received_sequence"] == 0
    assert not result.complete and not result.live_enabled
    assert "local_valuation_model_not_broker_verified" in result.blockers
    assert book.snapshot() == before and not journal.inspect()["unacknowledged_records"]


def test_explicit_retention_uses_internal_report_quote_and_policy(setup):
    clock, _, capture, book = setup
    result = resync(clock, capture)
    assert result.account_valuation is None
    altered = result.model_copy(update={"report": synthetic_account(clock.wall, equity="1")})
    assert altered.report != result.report
    diagnostic = capture.compare_account_valuation(book, expected_revision=result.revision)
    assert diagnostic["diagnostics_match"]
    plain = capture.resync(lambda: synthetic_account(clock.wall))
    assert plain.account_valuation is None
    with pytest.raises(SyncError, match="valuation_requires_current_collection"):
        capture.compare_account_valuation(book, expected_revision=plain.revision)


def test_difference_is_separate_from_structure_and_never_corrected(setup):
    clock, _, capture, book = setup
    before = book.snapshot()
    result = capture.resync(
        lambda: synthetic_account(clock.wall, equity="1"),
        collect_quote=lambda: quote(clock.wall),
        valuation_policy=synthetic_policy(),
        valuation_book=book,
    )
    assert result.structural_match and not result.account_valuation["diagnostics_match"]
    assert "local_valuation_difference_unexplained" in result.blockers
    assert book.snapshot() == before and not result.complete and not result.live_enabled


@pytest.mark.parametrize(
    "kind", ["scope", "collector", "policy", "invalid_policy", "different_book"]
)
def test_configuration_fails_before_any_collection(setup, tmp_path, kind):
    _, _, capture, book = setup
    options = {
        "collect_quote": quote,
        "valuation_policy": synthetic_policy(),
        "valuation_book": book,
    }
    if kind == "scope":
        options["valuation_book"] = ExecutionCashBook.create(
            tmp_path / "wrong",
            "other",
            OpeningCash(balance="1000000", cutoff=NOW),
        )
    elif kind == "collector":
        options["collect_quote"] = None
    elif kind == "policy":
        options["valuation_policy"] = None
    elif kind == "invalid_policy":
        options["valuation_policy"] = synthetic_policy().model_copy(
            update={"margin_rate": Decimal(2)}
        )
    else:
        options.update(reservation_book=create(tmp_path / "other"), collect_reservations=lambda: ())
    with pytest.raises(SyncError):
        capture.resync(lambda: pytest.fail("invalid configuration reached REST"), **options)
    assert not capture.status()["capture_failed"]


@pytest.mark.parametrize("kind", ["event", "disconnect", "expired", "clock", "resync", "omitted"])
def test_invalidated_inputs_and_old_revision_are_rejected(setup, kind):
    clock, _, capture, book = setup
    result = resync(clock, capture)
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
        capture.resync(lambda: synthetic_account(clock.wall))
    with pytest.raises((SyncError, JournalError)):
        capture.compare_account_valuation(book, expected_revision=result.revision)


def test_structural_mismatch_does_not_prevent_retained_evidence_expiry(setup):
    clock, _, capture, book = setup
    resync(clock, capture)
    original = synthetic_account(clock.wall)
    source = original.model_copy(
        update={
            "assets": original.assets.model_copy(update={"balance": Decimal("1000001")}),
        }
    )
    result = capture.resync(
        lambda: source,
        collect_quote=lambda: quote(clock.wall),
        valuation_policy=synthetic_policy(),
    )
    assert result.mismatches == ("balance_change_unverified",)
    assert not capture.compare_account_valuation(book, expected_revision=result.revision)[
        "diagnostics_match"
    ]
    clock.advance(31)
    with pytest.raises(SyncError, match="stale_valuation_sync_revision"):
        capture.compare_account_valuation(book, expected_revision=result.revision)


@pytest.mark.parametrize("kind", ["old", "future", "malformed", "exception", "interrupt", "slow"])
def test_quote_collection_bounds_and_sanitized_failures(setup, kind):
    clock, _, capture, book = setup

    def collect():
        if kind == "exception":
            raise RuntimeError("private-secret")
        if kind == "interrupt":
            raise KeyboardInterrupt
        if kind == "slow":
            clock.advance(31)
        if kind == "malformed":
            return quote().model_copy(update={"bid": Decimal("NaN")})
        return quote(
            clock.wall + timedelta(seconds=-1 if kind == "old" else 1 if kind == "future" else 0)
        )

    with pytest.raises(KeyboardInterrupt if kind == "interrupt" else SyncError) as error:
        capture.resync(
            lambda: synthetic_account(clock.wall),
            collect_quote=collect,
            valuation_policy=synthetic_policy(),
            valuation_book=book,
        )
    assert "private-secret" not in str(error.value)
    assert capture.status()["resync_required"]


@pytest.mark.parametrize("phase", ["account", "quote"])
def test_event_during_collection_is_delivered_and_invalidates_attempt(setup, phase):
    clock, _, capture, book = setup
    entered, release = threading.Event(), threading.Event()

    def collect(name, value):
        if phase == name:
            entered.set()
            assert release.wait(5)
        return value

    before = book.snapshot()
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            capture.resync,
            lambda: collect("account", synthetic_account(clock.wall)),
            collect_quote=lambda: collect("quote", quote(clock.wall)),
            valuation_policy=synthetic_policy(),
            valuation_book=book,
        )
        assert entered.wait(5)
        try:
            capture.ingest(1, raw(execution()))
        finally:
            release.set()
        with pytest.raises(SyncError):
            future.result(timeout=5)
    assert capture.status()["received_sequence"] == 1 and book.snapshot() == before


def test_journal_epoch_takeover_and_restart_require_fresh_evidence(setup):
    clock, journal, capture, book = setup
    result = resync(clock, capture)
    newer = capture_for(EventJournal(journal.path.parent, "synthetic"), clock)
    with pytest.raises(JournalError):
        capture.compare_account_valuation(book, expected_revision=result.revision)
    with pytest.raises(SyncError, match="valuation_requires_current_collection"):
        newer.compare_account_valuation(book, expected_revision=newer.status()["revision"])


def test_journal_takeover_after_collection_rejects_valuation(setup, monkeypatch):
    clock, journal, capture, book = setup
    original = capture._monitor.resync

    def takeover(*args, **kwargs):
        result = original(*args, **kwargs)
        capture_for(EventJournal(journal.path.parent, "synthetic"), clock)
        return result

    before = book.snapshot()
    monkeypatch.setattr(capture._monitor, "resync", takeover)
    with pytest.raises(JournalError):
        resync(clock, capture, book)
    assert capture.status()["capture_failed"] and book.snapshot() == before


@pytest.mark.parametrize("revision", [None, True, -1])
def test_revision_must_be_the_exact_current_integer(setup, revision):
    clock, _, capture, book = setup
    resync(clock, capture)
    with pytest.raises(SyncError, match="stale_valuation_sync_revision"):
        capture.compare_account_valuation(book, expected_revision=revision)


def test_declared_quote_age_can_expire_before_monitor_retention_ttl(setup):
    clock, _, capture, book = setup
    result = capture.resync(
        lambda: synthetic_account(clock.wall),
        collect_quote=lambda: quote(clock.wall),
        valuation_policy=synthetic_policy().model_copy(update={"max_quote_age_seconds": 1}),
    )
    clock.advance(2)
    with pytest.raises(SyncError, match="account_valuation_comparison_failed"):
        capture.compare_account_valuation(book, expected_revision=result.revision)


def test_unresolved_delivery_blocks_even_newly_collected_valuation(setup):
    clock, journal, capture, book = setup
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
    assert newer.status()["capture_failed"]


def test_journal_guard_prevents_epoch_takeover_during_comparison(setup, monkeypatch):
    clock, journal, capture, book = setup
    monkeypatch.setattr("trading.event_journal.BUSY_TIMEOUT_SECONDS", 0.05)
    original = book.compare_valuation

    def checked(*args, **kwargs):
        peer = EventJournal(journal.path.parent, "synthetic")
        with pytest.raises(JournalError, match="journal_busy"):
            peer.start_session(
                expected_head=journal.inspect()["head"], at=clock.wall, monotonic_ns=0
            )
        return original(*args, **kwargs)

    monkeypatch.setattr(book, "compare_valuation", checked)
    assert resync(clock, capture, book).account_valuation["diagnostics_match"]


@pytest.mark.parametrize("kind", ["exception", "interrupt", "ttl", "quote_age", "report_age"])
def test_comparison_failure_and_expiry_after_work_stop_capture(setup, monkeypatch, kind):
    clock, _, capture, book = setup
    original = book.compare_valuation
    policy = synthetic_policy().model_copy(
        update={
            "max_quote_age_seconds": 1 if kind == "quote_age" else 60,
            "max_report_age_seconds": 1 if kind == "report_age" else 60,
        }
    )

    def failed(*args, **kwargs):
        if kind == "exception":
            raise RuntimeError("private-secret")
        if kind == "interrupt":
            raise KeyboardInterrupt
        result = original(*args, **kwargs)
        clock.advance(31 if kind == "ttl" else 2)
        return result

    before = book.snapshot()
    monkeypatch.setattr(book, "compare_valuation", failed)
    with pytest.raises(KeyboardInterrupt if kind == "interrupt" else SyncError) as error:
        capture.resync(
            lambda: synthetic_account(clock.wall),
            collect_quote=lambda: quote(clock.wall),
            valuation_policy=policy,
            valuation_book=book,
        )
    if kind != "interrupt":
        assert str(error.value) == "account_valuation_comparison_failed"
    assert capture.status()["capture_failed"] and book.snapshot() == before


def test_reentrant_event_during_monitor_comparison_is_rejected(tmp_path, monkeypatch):
    clock = Clock()
    monitor = AccountSyncMonitor(**clock.args())
    session = monitor.start_session()
    book = create(tmp_path)
    result = monitor.resync(
        session,
        lambda: synthetic_account(clock.wall),
        collect_quote=lambda: quote(clock.wall),
        valuation_policy=synthetic_policy(),
    )
    original = book.compare_valuation

    def changed(*args, **kwargs):
        diagnostic = original(*args, **kwargs)
        monitor.ingest(session, 1, raw(execution()))
        return diagnostic

    monkeypatch.setattr(book, "compare_valuation", changed)
    with pytest.raises(SyncError, match="account_valuation_comparison_failed"):
        monitor.compare_account_valuation(session, book, expected_revision=result.revision)


def partial_account(now):
    source = synthetic_account(now)
    return source.model_copy(
        update={
            "positions": (
                source.positions[0].model_copy(update={"units": 300, "loss_gain": Decimal("-30")}),
            ),
            "assets": source.assets.model_copy(
                update={
                    "balance": Decimal("1000008"),
                    "equity": Decimal("999978"),
                    "position_loss_gain": Decimal("-30"),
                    "margin": Decimal("1802"),
                    "available_amount": Decimal("998176"),
                }
            ),
        }
    )


def post_and_compare(clock, capture, book):
    row = close(units=100, pnl="10", seconds=0)
    return capture.resync(
        lambda: partial_account(clock.wall),
        collect_orders=lambda ids: (read_order(clock, [row]),),
        collect_reservations=lambda: (),
        collect_quote=lambda: quote(clock.wall),
        valuation_policy=synthetic_policy(),
        cash_book=book,
        reservation_book=book,
        valuation_book=book,
    )


def test_post_reserve_and_value_share_head_and_retry_never_rebooks(setup):
    clock, _, capture, book = setup
    capture.ingest(1, raw(close(units=100, pnl="10", seconds=0)))
    for index in range(2):
        result = post_and_compare(clock, capture, book)
        assert result.execution_cash["applied_execution_ids"] == ((601,) if index == 0 else ())
        assert result.account_valuation["diagnostics_match"]
        assert result.position_reservations["reservation_match"]
        assert (
            result.execution_cash["head"]
            == result.account_valuation["head"]
            == result.position_reservations["head"]
        )
    assert book.snapshot()["balance"] == "1000008.00000000"


def test_full_collection_order_places_quote_after_execution_evidence(setup):
    clock, _, capture, book = setup
    row = close(units=100, pnl="10", seconds=0)
    capture.ingest(1, raw(row))
    phases = []

    def collect(name, factory):
        phases.append(name)
        return factory()

    result = capture.resync(
        lambda: collect("account", lambda: partial_account(clock.wall)),
        collect_reservations=lambda: collect("reservations", lambda: ()),
        collect_orders=lambda ids: collect("executions", lambda: (read_order(clock, [row]),)),
        collect_quote=lambda: collect("quote", lambda: quote(clock.wall)),
        valuation_policy=synthetic_policy(),
        cash_book=book,
        reservation_book=book,
        valuation_book=book,
    )
    assert phases == ["reservations", "account", "executions", "quote"]
    assert result.account_valuation["diagnostics_match"]


@pytest.mark.parametrize("cash", [False, True])
def test_peer_transfer_between_diagnostics_rejects_combined_heads(setup, monkeypatch, cash):
    clock, _, capture, book = setup
    original = book.compare_valuation

    def changed(*args, **kwargs):
        peer = ExecutionCashBook(book.path.parent, "synthetic")
        peer.apply_transfers((synthetic_match(clock.wall, amount="100"),))
        return original(*args, **kwargs)

    monkeypatch.setattr(book, "compare_valuation", changed)
    with pytest.raises(SyncError, match="cash_book_changed_during_valuation_comparison"):
        if cash:
            capture.ingest(1, raw(close(units=100, pnl="10", seconds=0)))
            post_and_compare(clock, capture, book)
        else:
            resync(clock, capture, book, reservation_book=book, collect_reservations=lambda: ())
    assert capture.status()["capture_failed"]


def test_failure_after_cash_commit_requires_new_capture_and_deduplicates_retry(setup, monkeypatch):
    clock, journal, capture, book = setup
    payload = raw(close(units=100, pnl="10", seconds=0))
    capture.ingest(1, payload)
    with monkeypatch.context() as context:

        def failed(*args, **kwargs):
            raise RuntimeError("private-secret")

        context.setattr(book, "compare_valuation", failed)
        with pytest.raises(SyncError, match="account_valuation_comparison_failed"):
            post_and_compare(clock, capture, book)
    assert book.snapshot()["execution_ids"] == (601,)
    newer = capture_for(EventJournal(journal.path.parent, "synthetic"), clock)
    newer.ingest(1, payload)
    result = post_and_compare(clock, newer, book)
    assert not result.execution_cash["applied_execution_ids"]
    assert result.execution_cash["already_applied_execution_ids"] == (601,)
    assert result.account_valuation["diagnostics_match"]


@pytest.mark.parametrize("fail", [False, True])
def test_private_receiver_forwards_valuation_and_closes_on_failure(tmp_path, fail):
    clock, journal, _, _, sock, receiver, _ = stream_setup(tmp_path)
    book = ExecutionCashBook.create(
        tmp_path / "book",
        "synthetic",
        OpeningCash(
            balance="1000000",
            cutoff=clock.wall - timedelta(seconds=1),
            position_basis=PositionBasis(
                positions=(
                    OpeningPosition(
                        position_id=401,
                        side="BUY",
                        units=400,
                        average_price="150",
                    ),
                )
            ),
        ),
    )
    stream_start(journal, receiver)
    try:
        if fail:
            with pytest.raises(SyncError, match="valuation_requires_quote_and_policy"):
                receiver.resync(lambda: synthetic_account(clock.wall), valuation_book=book)
            assert sock.closed and receiver.status()["stream_closed"]
        else:
            result = receiver.resync(
                lambda: synthetic_account(clock.wall),
                collect_quote=lambda: quote(clock.wall),
                valuation_policy=synthetic_policy(),
                valuation_book=book,
            )
            assert result.account_valuation["diagnostics_match"]
            assert receiver.status()["stream_running"]
    finally:
        receiver.close()


def test_offline_demo_requires_fresh_restart_and_preserves_difference(tmp_path):
    result = demo(tmp_path / "demo")
    assert result["initial"]["account_valuation"]["diagnostics_match"]
    assert result["restart"]["account_valuation"]["diagnostics_match"]
    assert not result["equity_difference"]["account_valuation"]["diagnostics_match"]
    assert result["expired_comparison"] == "stale_valuation_sync_revision"
    assert "journal_epoch_gap_not_repaired" in result["restart"]["blockers"]
    assert result["cash_book"]["executions"] == 0
    assert not result["complete"] and not result["live_enabled"]
    with pytest.raises(FileExistsError):
        demo(tmp_path / "demo")


def test_clock_skew_is_forwarded_without_allowing_future_quote(tmp_path):
    clock = Clock()
    monitor = AccountSyncMonitor(**clock.args(), clock_skew_ms=10)
    session = monitor.start_session()
    source = synthetic_account(clock.wall)
    source = source.model_copy(
        update={
            "observations": tuple(
                o.model_copy(update={"response_at": clock.wall + timedelta(milliseconds=10)})
                for o in source.observations
            )
        }
    )
    result = monitor.resync(
        session, lambda: source, collect_quote=quote, valuation_policy=synthetic_policy()
    )
    assert monitor.compare_account_valuation(
        session, create(tmp_path), expected_revision=result.revision
    )["diagnostics_match"]
    with pytest.raises(SyncError, match="stale_or_invalid_valuation_quote"):
        monitor.resync(
            session,
            lambda: source,
            collect_quote=lambda: quote(clock.wall + timedelta(milliseconds=1)),
            valuation_policy=synthetic_policy(),
        )
