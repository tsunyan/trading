"""An operator-reviewed close of unknown delivery or a FAULT end; booking stays mandatory."""

import pytest
from test_account_events import execution, raw
from test_account_sync import NOW
from test_execution_reconciliation import read_order
from test_stream_control import begin, capture, recovery
from test_stream_control import setup as control_setup

from trading.event_journal import Entry, JournalError
from trading.execution_cash_book import ExecutionCashBatch
from trading.stream_control import StreamControlError


@pytest.fixture
def setup(tmp_path):
    return control_setup.__wrapped__(tmp_path)


def review(control, journal, book, **changes):
    state = control.snapshot()
    return control.review_delivery_uncertainty(
        journal,
        book,
        **{
            "expected_revision": state["revision"],
            "expected_reason": state["reason"],
            "expected_head": journal.head(),
            "at": NOW,
            **changes,
        },
    )


def unacknowledged_execution(setup):
    clock, journal, book, control = setup
    with control.ownership():
        begin(control, journal)
        current = capture(clock, journal)
        row = execution()
        journal.record(
            current._session, "EVENT", at=NOW, monotonic_ns=0, sequence=1, payload=raw(row)
        )
    return row


def book_stored(setup, row):
    """What reconcile-stopped does after its GETs: match and book every stored execution."""
    clock, journal, book, control = setup
    with control.ownership():
        events = journal.recovery_events(expected_head=journal.head())
        batch = ExecutionCashBatch(
            events=events,
            reports=(read_order(clock, [row], order_changes={"status": "ORDERED"}),),
        )
        with journal.guard_recovery_events(expected_head=journal.head()):
            return book.apply(batch)


def test_reviewed_unknown_delivery_still_requires_booking_before_recovery(setup):
    clock, journal, book, control = setup
    row = unacknowledged_execution(setup)
    # Before the review, neither booking nor recovery can read the segment.
    with pytest.raises(JournalError, match="capture_delivery_unresolved"):
        journal.recovery_events(expected_head=journal.head())
    before = control.snapshot()
    reviewed = review(control, journal, book)
    assert reviewed["revision"] == before["revision"] + 1
    # The abandoned RUNNING owner is gone (OS ownership); the phase itself is unchanged.
    assert reviewed["phase"] == before["phase"] and reviewed["journal_head"] == journal.head()
    view = journal.inspect()
    assert view["unacknowledged_records"] == () and view["reviewed_unknown_records"] == (2,)
    assert not view["session_open"]
    # The review is a decision, never a delivery: replay still reports the outcome unknown.
    outcomes = journal.replay()["outcomes"]
    assert [o["error"] for o in outcomes if o["kind"] == "EVENT"] == ["delivery_outcome_unknown"]
    # Recovery stays strict until every stored execution is matched and booked.
    with pytest.raises(JournalError, match="recovery_execution_not_booked"):
        recovery(control, journal, book, acknowledge_token_uncertainty=True)
    book_stored(setup, row)
    assert book.snapshot()["executions"] == 1
    journal = recovery(control, journal, book, acknowledge_token_uncertainty=True)
    assert control.snapshot()["phase"] == "READY"
    assert book.snapshot()["executions"] == 1
    assert journal.audit_history()["archived_segments"] == 1


def test_fault_end_is_reviewable_and_then_recovers(setup):
    clock, journal, book, control = setup
    with control.ownership():
        begin(control, journal)
        current = capture(clock, journal)
        current.heartbeat()
        session = current._session
    with journal._transaction(write=True) as conn:
        meta, entries, state = journal._verify(conn)
        journal._append(
            conn,
            meta,
            Entry(
                kind="FAULT",
                epoch=state["epoch"],
                session=session,
                at=NOW,
                monotonic_ns=state["mono"],
                reason="delivery_failed",
            ),
        )
    with pytest.raises(JournalError, match="recovery_fault_requires_review"):
        recovery(control, journal, book, acknowledge_token_uncertainty=True)
    review(control, journal, book)
    journal = recovery(control, journal, book, acknowledge_token_uncertainty=True)
    assert control.snapshot()["phase"] == "READY"


def test_review_is_refused_when_nothing_is_uncertain_or_the_checkpoint_moved(setup):
    clock, journal, book, control = setup
    with control.ownership():
        begin(control, journal)
        capture(clock, journal)
    # An open session with every record acknowledged ends normally by recovery.
    with pytest.raises(JournalError, match="journal_review_not_required"):
        review(control, journal, book)
    with pytest.raises(StreamControlError, match="state_changed"):
        review(control, journal, book, expected_reason="closed")
    with pytest.raises(JournalError, match="journal_head_changed"):
        review(control, journal, book, expected_head="0" * 64)


def test_review_needs_a_stopped_control_and_os_ownership(setup):
    clock, journal, book, control = setup
    with pytest.raises(StreamControlError, match="recovery_not_required"):
        review(control, journal, book)
    unacknowledged_execution(setup)
    with control.ownership():
        with pytest.raises(StreamControlError):
            # A second owner (even in the same process) is refused while one holds the lock.
            review(control, journal, book)


def test_reviewed_record_is_part_of_the_hash_chain(setup):
    clock, journal, book, control = setup
    unacknowledged_execution(setup)
    review(control, journal, book)
    with journal._transaction(write=True) as conn:
        meta, entries, state = journal._verify(conn)
        assert entries[-1].kind == "REVIEWED"
        # A second REVIEWED has nothing uncertain left to close and fails verification.
        journal._append(
            conn,
            meta,
            Entry(
                kind="REVIEWED",
                epoch=state["epoch"],
                session=state["session"],
                at=NOW,
                monotonic_ns=state["mono"],
            ),
        )
    with pytest.raises(JournalError, match="integrity"):
        journal.inspect()
