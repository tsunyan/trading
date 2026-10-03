"""Persistent stops, real OS ownership, and receipt-checked explicit recovery."""

import json
import sqlite3
import subprocess
import sys
from contextlib import closing
from datetime import timedelta

import pytest
from test_account_events import execution, raw
from test_account_sync import NOW, Clock, report
from test_execution_reconciliation import read_order

from trading.event_capture import JournaledEventCapture
from trading.event_journal import JournalError
from trading.execution_cash_book import ExecutionCashBook, OpeningCash
from trading.segmented_journal import SegmentedEventJournal
from trading.stream_control import StreamControl, StreamControlError


@pytest.fixture
def setup(tmp_path):
    clock = Clock()
    journal = SegmentedEventJournal.create(tmp_path / "journal", "synthetic", max_records=8)
    book = ExecutionCashBook.create(
        tmp_path / "cash",
        "synthetic",
        OpeningCash(balance="1000000", cutoff=NOW - timedelta(seconds=1)),
    )
    control = StreamControl.create(
        tmp_path / "control", journal, book, wall_ns=lambda: int(clock.wall.timestamp() * 1e9)
    )
    return clock, journal, book, control


def begin(control, journal):
    return control.begin(
        journal, expected_revision=control.snapshot()["revision"], expected_head=journal.head()
    )


def capture(clock, journal):
    result = JournaledEventCapture(
        journal, clock=lambda: clock.wall, monotonic_ns=lambda: int(clock.mono * 1e9)
    )
    result.start_session(expected_head=journal.head())
    return result


def recovery(control, journal, book, **kwargs):
    state = control.snapshot()
    return control.recover(
        journal,
        book,
        expected_revision=state["revision"],
        expected_reason=state["reason"],
        expected_head=journal.head(),
        at=NOW,
        **kwargs,
    )


def test_reopen_is_read_only_and_clean_close_allows_only_a_new_owner(setup):
    _, journal, book, control = setup
    before = control.path.read_bytes()
    peer = StreamControl(control.path.parent)
    assert peer.snapshot() == control.snapshot() and control.path.read_bytes() == before
    with control.ownership():
        first = begin(control, journal)
        control.update(first["owner"], success=True)
        state = control.snapshot()
        assert state["phase"] == "RUNNING" and state["cleanup_unknown"]
        assert state["sync_successes"] == 1
        control.finish(first["owner"], journal)
    assert control.snapshot()["phase"] == "READY"
    with control.ownership():
        next_run = begin(control, journal)
        assert next_run["owner"] != first["owner"] and next_run["generation"] == 2
        with pytest.raises(StreamControlError, match="owner_fenced"):
            control.update(first["owner"], success=True)
        control.finish(next_run["owner"], journal)
    assert not control.snapshot()["complete"] and not control.snapshot()["live_enabled"]
    control.check_binding(journal, book)


def test_running_record_never_expires_and_requires_token_uncertainty_ack(setup):
    clock, journal, book, control = setup
    with control.ownership():
        begin(control, journal)
    clock.advance(10_000_000)
    peer = StreamControl(control.path.parent, wall_ns=control._wall)
    with peer.ownership(), pytest.raises(StreamControlError, match="recovery_required"):
        begin(peer, journal)
    with pytest.raises(StreamControlError, match="uncertainty_requires_acknowledgment"):
        recovery(peer, journal, book)
    journal = recovery(peer, journal, book, acknowledge_token_uncertainty=True)
    assert peer.snapshot()["phase"] == "READY" and peer.snapshot()["generation"] == 2
    assert journal.inspect()["records"] == 0


def test_live_owner_blocks_recovery_even_in_the_same_process(setup):
    _, journal, book, control = setup
    with control.ownership():
        begin(control, journal)
        peer = StreamControl(control.path.parent)
        with pytest.raises(StreamControlError, match="owner_busy"):
            recovery(peer, journal, book, acknowledge_token_uncertainty=True)
    journal = recovery(control, journal, book, acknowledge_token_uncertainty=True)
    assert control.snapshot()["phase"] == "READY"


def test_explicit_stop_does_not_reset_by_clean_finish_or_reopening(setup):
    _, journal, book, control = setup
    with control.ownership():
        owner = begin(control, journal)["owner"]
        control.finish(owner, journal, reason="sync_failed")
        control.finish(owner, journal)
    assert (
        control.snapshot()["phase"] == "STOPPED" and control.snapshot()["reason"] == "sync_failed"
    )
    peer = StreamControl(control.path.parent, wall_ns=control._wall)
    journal = recovery(peer, journal, book)
    assert peer.snapshot()["phase"] == "READY"
    assert peer.snapshot()["reason"] == "recovery_approved"


@pytest.mark.parametrize("field", ["revision", "head", "reason"])
def test_recovery_rejects_changed_operator_snapshot_before_any_journal_write(setup, field):
    _, journal, book, control = setup
    with control.ownership():
        begin(control, journal)
    state = control.snapshot()
    kwargs = dict(
        expected_revision=state["revision"],
        expected_head=journal.head(),
        expected_reason=state["reason"],
    )
    kwargs[
        {"revision": "expected_revision", "head": "expected_head", "reason": "expected_reason"}[
            field
        ]
    ] = state["revision"] - 1 if field == "revision" else "mismatch"
    before = journal.path.read_bytes()
    with pytest.raises(ValueError):
        control.recover(journal, book, acknowledge_token_uncertainty=True, at=NOW, **kwargs)
    assert journal.path.read_bytes() == before and control.snapshot()["phase"] == "RUNNING"


@pytest.mark.parametrize("case", ["pending_ack", "unbooked", "booked"])
def test_orphan_epoch_is_retired_only_with_known_delivery_and_durable_receipts(setup, case):
    clock, journal, book, control = setup
    with control.ownership():
        begin(control, journal)
        current = capture(clock, journal)
        row = execution()
        if case == "pending_ack":
            journal.record(
                current._session, "EVENT", at=NOW, monotonic_ns=0, sequence=1, payload=raw(row)
            )
        else:
            current.ingest(1, raw(row))
            if case == "booked":
                current.resync(
                    lambda: report(clock, units=None, orders=False),
                    collect_orders=lambda _: (
                        read_order(clock, [row], order_changes={"status": "ORDERED"}),
                    ),
                    cash_book=book,
                )
    if case != "booked":
        before = journal.path.read_bytes()
        with pytest.raises(JournalError, match="unresolved|not_booked"):
            recovery(control, journal, book, acknowledge_token_uncertainty=True)
        assert journal.path.read_bytes() == before
    else:
        journal = recovery(control, journal, book, acknowledge_token_uncertainty=True)
        assert journal.audit_history()["archived_records"] == 4
        assert book.snapshot()["executions"] == 1
        fresh = capture(clock, journal)
        assert fresh.status()["phase"] == "NEEDS_RESYNC"
        assert "journal_rollover_gap_not_repaired" in fresh.status()["blockers"]
        with pytest.raises(JournalError):
            current.heartbeat()


@pytest.mark.parametrize("case", ["missing", "replaced", "contents"])
def test_owner_file_identity_is_never_recreated_or_silently_replaced(setup, case):
    _, _, _, control = setup
    if case == "missing":
        control.lock_path.unlink()
    elif case == "replaced":
        saved = control.lock_path.with_suffix(".saved")
        control.lock_path.rename(saved)
        control.lock_path.write_bytes(saved.read_bytes())
    else:
        control.lock_path.write_bytes(b"x" * 32)
    with pytest.raises(StreamControlError):
        with control.ownership():
            pytest.fail("unsafe owner accepted")
    if case == "missing":
        assert not control.lock_path.exists()


def test_state_digest_or_missing_database_rejects_reopen_without_creation(setup):
    _, _, _, control = setup
    with closing(sqlite3.connect(control.path)) as conn:
        body = json.loads(conn.execute("SELECT body FROM control").fetchone()[0])
        body["phase"] = "RUNNING"
        conn.execute("UPDATE control SET body=?", (json.dumps(body),))
        conn.commit()
    with pytest.raises(StreamControlError, match="integrity_failed"):
        StreamControl(control.path.parent)
    control.path.rename(control.path.with_suffix(".saved"))
    with pytest.raises(StreamControlError, match="storage_failed"):
        StreamControl(control.path.parent)
    assert not control.path.exists()


def test_rotation_checkpoint_and_wrong_cash_book_binding(setup, tmp_path):
    clock, journal, book, control = setup
    with control.ownership():
        owner = begin(control, journal)["owner"]
        current = capture(clock, journal)
        current.heartbeat()
        head = current.end_for_rollover(book)
        journal = journal.rotate(expected_head=head)
        control.update(owner, journal=journal)
        control.finish(owner, journal)
    assert control.snapshot()["rotations"] == 1
    assert control.snapshot()["journal_instance"] == journal.inspect()["instance"]
    other = ExecutionCashBook.create(
        tmp_path / "other", "synthetic", OpeningCash(balance="1000000", cutoff=NOW)
    )
    with pytest.raises(StreamControlError, match="binding_mismatch"):
        control.check_binding(journal, other)


def test_binding_does_not_repeat_history_scan_but_refuses_changed_series(setup, monkeypatch):
    _, journal, book, control = setup
    monkeypatch.setattr(journal, "audit_history", lambda: pytest.fail("duplicate history scan"))
    control.check_binding(journal, book)
    with sqlite3.connect(journal.path) as conn:
        conn.execute("UPDATE series SET instance=?", ("b" * 32,))
    with pytest.raises(JournalError, match="archive_integrity_failed"):
        control.check_binding(journal, book)


def test_regular_sync_counters_do_not_consume_transition_capacity(setup):
    _, journal, _, control = setup
    with control.ownership():
        owner = begin(control, journal)["owner"]
        for _ in range(30):
            control.update(owner, success=True)
        with sqlite3.connect(control.path) as conn:
            assert conn.execute("SELECT COUNT(*) FROM transitions").fetchone()[0] == 2
    assert control.snapshot()["sync_successes"] == 30


def test_real_process_exit_releases_os_owner_but_leaves_persistent_stop_requirement(setup):
    _, journal, book, control = setup
    code = """
import os, sys
from trading.stream_control import StreamControl
from trading.segmented_journal import SegmentedEventJournal
c = StreamControl(sys.argv[1])
j = SegmentedEventJournal(sys.argv[2], 'synthetic')
with c.ownership():
    c.begin(j, expected_revision=c.snapshot()['revision'], expected_head=j.head())
    os._exit(23)
"""
    result = subprocess.run(
        [sys.executable, "-c", code, str(control.path.parent), str(journal.path.parent)],
        capture_output=True,
        timeout=20,
    )
    assert result.returncode == 23, result.stderr.decode()
    peer = StreamControl(control.path.parent)
    assert peer.snapshot()["phase"] == "RUNNING" and peer.snapshot()["cleanup_unknown"]
    journal = recovery(peer, journal, book, acknowledge_token_uncertainty=True)
    assert peer.snapshot()["phase"] == "READY"


@pytest.mark.parametrize("change", ["backward", "invalid"])
def test_bad_clock_still_persists_stop_and_never_reports_ready(setup, change):
    clock, journal, _, control = setup
    with control.ownership():
        owner = begin(control, journal)["owner"]
        if change == "backward":
            clock.wall -= timedelta(seconds=1)
        else:
            control._wall = lambda: float("nan")
        with pytest.raises(ValueError):
            control.update(owner)
        control.finish(owner, journal, reason="clock_invalid")
    assert (
        control.snapshot()["phase"] == "STOPPED" and control.snapshot()["reason"] == "clock_invalid"
    )


def test_mutating_without_os_ownership_is_rejected(setup):
    _, journal, _, control = setup
    with pytest.raises(StreamControlError, match="owner_required"):
        begin(control, journal)
    assert control.snapshot()["phase"] == "READY"
