"""No history deletion, no uncertain-delivery bypass, and no old-segment writes."""

import hashlib
import json
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Event

import pytest
from test_account_events import execution, raw
from test_account_sync import Clock, report
from test_event_journal import NOW, event, start
from test_execution_reconciliation import read_order

from trading.account_sync import SyncError
from trading.event_capture import JournaledEventCapture
from trading.event_journal import ZERO, EventJournal, JournalError
from trading.execution_cash_book import ExecutionCashBook, OpeningCash
from trading.segmented_journal import (
    Segment,
    SegmentedEventJournal,
    _segment_body,
    _segment_digest,
    _unpack_archive,
)


@pytest.fixture
def journal(tmp_path):
    return SegmentedEventJournal.create(tmp_path / "journal", "synthetic", max_records=8)


def close_segment(journal):
    session = start(journal)
    record = event(journal, session)
    journal.acknowledge(session, record)
    journal.record(session, "END", at=NOW, monotonic_ns=0)
    return session


def stored(journal, table="records", *, segment=None):
    with sqlite3.connect(journal.path) as conn:
        if segment is None:
            return conn.execute(f"SELECT id,body,digest FROM {table} ORDER BY id").fetchall()
        descriptor = Segment.model_validate_json(
            conn.execute("SELECT body FROM segments WHERE id=?", (segment,)).fetchone()[0]
        )
        return journal._archive_records(conn, descriptor)


def legacy_archive(journal):
    """Represent the first rotation in the original row format, without resealing it."""
    records = stored(journal, segment=1)
    with sqlite3.connect(journal.path) as conn:
        segment = Segment.model_validate_json(
            conn.execute("SELECT body FROM segments").fetchone()[0]
        )
        segment = segment.model_copy(update={"archive_sha256": None})
        body = _segment_body(segment)
        digest = _segment_digest(segment.series, 1, ZERO, body)
        conn.executemany("INSERT INTO archived_records VALUES(1,?,?,?)", records)
        conn.execute("UPDATE segments SET body=?,digest=? WHERE id=1", (body, digest))
        conn.execute("UPDATE series SET head=?", (digest,))
        conn.execute("DROP TABLE sealed_archives")
    return body, digest


def test_rotation_preserves_exact_records_link_and_replay_and_starts_no_session(journal):
    close_segment(journal)
    original = stored(journal)
    replay = journal.replay()
    old = journal.inspect()
    next_journal = journal.rotate(expected_head=old["head"])
    new = next_journal.inspect()
    assert new["records"] == new["epoch"] == 0 and not new["session_open"]
    assert new["head"] == ZERO and new["instance"] != old["instance"]
    assert new["archived_segments"] == 1
    assert stored(next_journal, segment=1) == original
    archived = next_journal.replay_archive(1)
    assert {k: archived[k] for k in replay} == replay
    audit = next_journal.audit_history()
    assert audit["archived_records"] == 4 and audit["active_records"] == 0
    assert audit["history_gap_unproven"] and audit["resync_required"]
    assert not audit["complete"] and not audit["live_enabled"]


def test_many_segments_keep_active_work_bounded_and_reopen_audits_every_archive(journal):
    all_records = []
    for _ in range(30):
        close_segment(journal)
        all_records.append(stored(journal))
        journal = journal.rotate(expected_head=journal.head())
        assert journal.inspect()["records"] == 0
    reopened = SegmentedEventJournal(journal.path.parent, "synthetic")
    assert reopened.audit_history()["archived_records"] == 120
    for index, original in enumerate(all_records, 1):
        assert stored(reopened, segment=index) == original
    assert reopened.inspect()["archived_segments"] == 30
    assert reopened.inspect()["max_records"] == 8


@pytest.mark.parametrize("method", ["start", "record", "ack", "guard", "rotate"])
def test_retired_objects_and_sessions_are_fenced(journal, method):
    old = close_segment(journal)
    peer = SegmentedEventJournal(journal.path.parent, "synthetic")
    expected = journal.head()
    new = journal.rotate(expected_head=expected)
    new_session = start(new)
    before = stored(new)
    with pytest.raises(JournalError):
        if method == "start":
            peer.start_session(expected_head=ZERO, at=NOW, monotonic_ns=0)
        elif method == "record":
            event(peer, old)
        elif method == "ack":
            peer.acknowledge(old, 2)
        elif method == "guard":
            with peer.guard_session(old):
                pytest.fail("retired cash guard granted")
        else:
            peer.rotate(expected_head=expected)
    assert stored(new) == before and new.current(new_session)["archived_segments"] == 1
    with pytest.raises(JournalError):
        EventJournal(new.path.parent, "synthetic")  # v1 cannot bypass v2 retirement.


@pytest.mark.parametrize("state", ["empty", "active", "pending", "fault", "prior_pending"])
def test_rotation_refuses_any_unclosed_or_unknown_delivery_without_mutation(journal, state):
    if state != "empty":
        session = start(journal)
        if state in {"pending", "prior_pending"}:
            event(journal, session)
        if state == "prior_pending":
            session = start(journal)
            journal.record(session, "END", at=NOW, monotonic_ns=0)
        elif state == "fault":
            journal.fail_delivery(session)
    before = journal.path.read_bytes()
    with pytest.raises(JournalError):
        journal.rotate(expected_head=journal.head())
    assert journal.path.read_bytes() == before
    assert journal.inspect()["archived_segments"] == 0


def test_changed_head_and_concurrent_rotations_have_one_winner(journal):
    close_segment(journal)
    with pytest.raises(JournalError, match="head_changed"):
        journal.rotate(expected_head=ZERO)
    peers = [SegmentedEventJournal(journal.path.parent, "synthetic") for _ in range(2)]
    expected = journal.head()

    def rotate(peer):
        try:
            return peer.rotate(expected_head=expected)
        except JournalError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(rotate, peers))
    assert sum(result is not None for result in results) == 1
    next_journal = next(result for result in results if result is not None)
    assert next_journal.audit_history()["archived_records"] == 4


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE archived_records SET body='{}' WHERE segment=1 AND id=2",
        "DELETE FROM archived_records WHERE segment=1 AND id=1",
        "UPDATE segments SET digest='bad' WHERE id=1",
        "DELETE FROM segments WHERE id=1",
        "UPDATE series SET count=0",
        "UPDATE series SET head='bad'",
        "UPDATE series SET origin='bad'",
        "UPDATE journal SET instance='00000000000000000000000000000000'",
        "INSERT INTO archived_records VALUES(999,1,'{}','bad')",
    ],
)
@pytest.mark.parametrize("legacy", [False, True])
def test_archive_corruption_is_rejected_on_audit_reopen_and_rotation(journal, sql, legacy):
    close_segment(journal)
    journal = journal.rotate(expected_head=journal.head())
    if legacy:
        legacy_archive(journal)
    elif sql.startswith("UPDATE archived_records SET body"):
        sql = "UPDATE sealed_archives SET body=zeroblob(length(body)) WHERE id=1"
    elif sql.startswith("DELETE FROM archived_records"):
        sql = "DELETE FROM sealed_archives WHERE id=1"
    close_segment(journal)
    with sqlite3.connect(journal.path) as conn:
        conn.execute(sql)
    before = journal.path.read_bytes()
    with pytest.raises(JournalError):
        journal.audit_history()
    with pytest.raises(JournalError):
        SegmentedEventJournal(journal.path.parent, "synthetic")
    with pytest.raises(JournalError):
        journal.rotate(expected_head=journal.head())
    assert journal.path.read_bytes() == before


def test_active_operations_do_not_reaudit_archived_bodies_per_event(journal, monkeypatch):
    close_segment(journal)
    journal = journal.rotate(expected_head=journal.head())
    monkeypatch.setattr(journal, "_audit", lambda *a: pytest.fail("unexpected full archive scan"))
    session = start(journal)
    record = event(journal, session)
    journal.acknowledge(session, record)
    journal.record(session, "END", at=NOW, monotonic_ns=0)
    assert journal.inspect()["records"] == 4


def test_rotation_write_failure_rolls_back_all_copies_and_active_reset(journal):
    close_segment(journal)
    before = stored(journal)
    with sqlite3.connect(journal.path) as conn:
        conn.execute(
            "CREATE TRIGGER fail_rotation BEFORE DELETE ON records "
            "BEGIN SELECT RAISE(ABORT, 'fixture'); END"
        )
    with pytest.raises(JournalError, match="storage_failed"):
        journal.rotate(expected_head=journal.head())
    reopened = SegmentedEventJournal(journal.path.parent, "synthetic")
    assert stored(reopened) == before
    assert reopened.audit_history()["archived_records"] == 0


@pytest.mark.parametrize("after_commit", [False, True])
def test_process_exit_at_rotation_commit_is_atomic_and_recoverable(journal, after_commit):
    close_segment(journal)
    original = stored(journal)
    code = """
import os, sys
from contextlib import contextmanager
from trading.segmented_journal import SegmentedEventJournal
j = SegmentedEventJournal(sys.argv[1], 'synthetic')
original = j._transaction
@contextmanager
def interrupted(*, write=False):
    with original(write=write) as conn:
        yield conn
        if write and sys.argv[2] == 'before':
            os._exit(23)
    if write and sys.argv[2] == 'after':
        os._exit(23)
j._transaction = interrupted
j.rotate(expected_head=j.head())
"""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            code,
            str(journal.path.parent),
            "after" if after_commit else "before",
        ],
        capture_output=True,
        timeout=20,
    )
    assert result.returncode == 23, result.stderr.decode()
    reopened = SegmentedEventJournal(journal.path.parent, "synthetic")
    if after_commit:
        assert reopened.inspect()["records"] == 0
        assert stored(reopened, segment=1) == original
        assert reopened.audit_history()["archived_segments"] == 1
    else:
        assert stored(reopened) == original
        assert reopened.audit_history()["archived_segments"] == 0


def capture(journal, clock):
    result = JournaledEventCapture(
        journal, clock=lambda: clock.wall, monotonic_ns=lambda: int(clock.mono * 1e9)
    )
    result.start_session(expected_head=journal.head())
    return result


@pytest.mark.parametrize("finished", [False, True])
def test_rollover_discards_no_unbooked_notice_and_preserves_cash_idempotency(
    tmp_path, journal, finished
):
    clock = Clock()
    current = capture(journal, clock)
    row = execution(executionSize="1000", orderExecutedSize="1000") if finished else execution()
    order_status = "EXECUTED" if finished else "ORDERED"
    current.ingest(1, raw(row))
    with pytest.raises(SyncError, match="cash_book_required"):
        current.end_for_rollover()
    book = ExecutionCashBook.create(
        tmp_path / "cash",
        "synthetic",
        OpeningCash(balance="1000000", cutoff=NOW - timedelta(seconds=1)),
    )
    with pytest.raises(SyncError, match="not_booked"):
        current.end_for_rollover(book)
    result = current.resync(
        lambda: report(clock, units=None, orders=False),
        collect_orders=lambda _: (
            read_order(clock, [row], order_changes={"status": order_status}),
        ),
        cash_book=book,
    )
    assert result.execution_cash["applied_execution_ids"] == (501,)
    head = current.end_for_rollover(book)
    assert current.status()["phase"] == "DISCONNECTED"
    journal = journal.rotate(expected_head=head)
    next_capture = capture(journal, clock)
    status = next_capture.status()
    assert status["resync_required"] and status["phase"] == "NEEDS_RESYNC"
    assert "journal_rollover_gap_not_repaired" in status["blockers"]
    next_capture.ingest(1, raw(row))
    requests = []

    def collect_orders(ids):
        requests.append(ids)
        return (read_order(clock, [row], order_changes={"status": order_status}),)

    result = next_capture.resync(
        lambda: report(clock, units=None, orders=False),
        collect_orders=collect_orders,
        cash_book=book,
    )
    assert requests == ([] if finished else [(201,)])
    assert result.previously_booked_execution_ids == ((501,) if finished else ())
    assert not result.complete and not result.live_enabled
    assert "journal_rollover_gap_not_repaired" in result.blockers
    assert book.snapshot()["executions"] == 1


def test_pending_collection_prevents_clean_rollover(journal):
    clock = Clock()
    current = capture(journal, clock)

    def collect():
        with pytest.raises(SyncError, match="collection_in_progress"):
            current.end_for_rollover()
        return report(clock)

    current.resync(collect)
    assert journal.inspect()["session_open"]


def test_rollover_end_rejects_a_foreign_write_after_the_receipt_check(journal, monkeypatch):
    clock = Clock()
    current = capture(journal, clock)
    original = current._monitor.assert_rollover_ready

    def changed(*args):
        original(*args)
        record = journal.record(current._session, "HEARTBEAT", at=NOW, monotonic_ns=0)
        journal.acknowledge(current._session, record)

    monkeypatch.setattr(current._monitor, "assert_rollover_ready", changed)
    with pytest.raises(JournalError, match="head_changed"):
        current.end_for_rollover()
    assert current.status()["phase"] == "DISCONNECTED"
    assert journal.inspect()["archived_segments"] == 0


@pytest.mark.parametrize("index", [0, True, "1", 10_001])
def test_invalid_archive_replay_indices(journal, index):
    with pytest.raises(JournalError, match="invalid_archive_index"):
        journal.replay_archive(index)


def test_unknown_archive_and_existing_directory_are_not_created(journal):
    with pytest.raises(JournalError, match="archive_not_found"):
        journal.replay_archive(1)
    with pytest.raises(FileExistsError):
        SegmentedEventJournal.create(journal.path.parent, "synthetic")
    assert journal.inspect()["records"] == 0


def test_capacity_boundary_can_end_then_rotate_without_unknown_outcome(tmp_path):
    journal = SegmentedEventJournal.create(tmp_path / "small", "synthetic", max_records=4)
    close_segment(journal)  # BEGIN + EVENT + ACK + END exhausts this segment.
    assert journal.inspect()["records"] == 4
    journal = journal.rotate(expected_head=journal.head())
    close_segment(journal)
    assert journal.audit_history()["archived_records"] == 4


def test_saved_segment_chain_is_explicitly_linked(journal):
    instances = []
    for _ in range(3):
        instances.append(journal.inspect()["instance"])
        close_segment(journal)
        journal = journal.rotate(expected_head=journal.head())
    with sqlite3.connect(journal.path) as conn:
        rows = conn.execute("SELECT body,digest FROM segments ORDER BY id").fetchall()
    previous = ZERO
    for index, (body, digest) in enumerate(rows):
        segment = json.loads(body)
        assert segment["previous"] == previous
        assert segment["instance"] == instances[index]
        expected_next = instances[index + 1] if index < 2 else journal.inspect()["instance"]
        assert segment["next_instance"] == expected_next
        previous = digest
    assert journal.audit_history()["archive_head"] == previous


def test_first_begin_after_rotation_cannot_reverse_the_archived_wall_clock(journal):
    close_segment(journal)
    journal = journal.rotate(expected_head=journal.head())
    with pytest.raises(JournalError, match="invalid_capture_clock"):
        start(journal, at=NOW - timedelta(seconds=1))
    assert journal.inspect()["records"] == 0
    assert start(journal, at=NOW, mono=0)


def test_mixed_history_keeps_original_legacy_descriptor_and_rows(journal):
    close_segment(journal)
    journal = journal.rotate(expected_head=journal.head())
    original = stored(journal, segment=1)
    descriptor = legacy_archive(journal)
    reopened = SegmentedEventJournal(journal.path.parent, "synthetic")
    assert reopened.check_history()["sealed_segments"] == 0
    close_segment(reopened)
    reopened = reopened.rotate(expected_head=reopened.head())
    with sqlite3.connect(reopened.path) as conn:
        assert conn.execute("SELECT body,digest FROM segments WHERE id=1").fetchone() == descriptor
        assert conn.execute("SELECT COUNT(*) FROM sealed_archives").fetchone()[0] == 1
    assert stored(reopened, segment=1) == original
    assert reopened.audit_history()["archived_records"] == 8
    assert reopened.check_history()["sealed_segments"] == 1


def test_byte_check_skips_archived_semantic_parsing_but_explicit_audit_retains_it(
    journal, monkeypatch
):
    close_segment(journal)
    retired = journal.inspect()["instance"]
    journal = journal.rotate(expected_head=journal.head())
    checked = []
    original = SegmentedEventJournal._check

    def check(self, meta, rows):
        checked.append(meta["instance"])
        return original(self, meta, rows)

    monkeypatch.setattr(SegmentedEventJournal, "_check", check)
    reopened = SegmentedEventJournal(journal.path.parent, "synthetic")
    assert retired not in checked
    reopened.check_history()
    assert retired not in checked
    reopened.audit_history()
    assert retired in checked


@pytest.mark.parametrize("offset", [0, 8, 12, 16, 20, 52, -1])
def test_every_part_of_sealed_bytes_is_rechecked_without_process_cache(journal, offset):
    close_segment(journal)
    journal = journal.rotate(expected_head=journal.head())
    journal.check_history()
    with sqlite3.connect(journal.path) as conn:
        body = bytearray(conn.execute("SELECT body FROM sealed_archives").fetchone()[0])
        body[offset] ^= 1
        conn.execute("UPDATE sealed_archives SET body=?", (bytes(body),))
    with pytest.raises(JournalError):
        journal.check_history()
    with pytest.raises(JournalError):
        SegmentedEventJournal(journal.path.parent, "synthetic")


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE sealed_archives SET body=substr(body,1,length(body)-1)",
        "UPDATE sealed_archives SET body=CAST(body AS TEXT)",
        "INSERT INTO sealed_archives VALUES(999,x'00')",
        "DROP TABLE sealed_archives",
        "DROP TABLE archived_records",
    ],
)
def test_sealed_storage_shape_and_orphans_fail_closed(journal, sql):
    close_segment(journal)
    journal = journal.rotate(expected_head=journal.head())
    with sqlite3.connect(journal.path) as conn:
        conn.execute(sql)
    with pytest.raises(JournalError):
        SegmentedEventJournal(journal.path.parent, "synthetic")


@pytest.mark.parametrize("offset", [8, 12, 16])
def test_explicit_decoder_rejects_invalid_framing(journal, offset):
    close_segment(journal)
    journal = journal.rotate(expected_head=journal.head())
    with sqlite3.connect(journal.path) as conn:
        body = bytearray(conn.execute("SELECT body FROM sealed_archives").fetchone()[0])
    body[offset : offset + 4] = (0).to_bytes(4, "big")
    with pytest.raises(ValueError):
        _unpack_archive(bytes(body))


def test_explicit_audit_detects_invalid_semantics_even_with_recomputed_seal(journal):
    close_segment(journal)
    journal = journal.rotate(expected_head=journal.head())
    with sqlite3.connect(journal.path) as conn:
        body = conn.execute("SELECT body FROM sealed_archives").fetchone()[0]
        body = body.replace(b'"kind":"END"', b'"kind":"ACK"')
        segment = Segment.model_validate_json(
            conn.execute("SELECT body FROM segments").fetchone()[0]
        )
        segment = segment.model_copy(update={"archive_sha256": hashlib.sha256(body).hexdigest()})
        descriptor = _segment_body(segment)
        digest = _segment_digest(segment.series, 1, ZERO, descriptor)
        conn.execute("UPDATE sealed_archives SET body=?", (body,))
        conn.execute("UPDATE segments SET body=?,digest=?", (descriptor, digest))
        conn.execute("UPDATE series SET head=?", (digest,))
    # Byte checks deliberately do not promise protection from hostile rehashed SQL edits.
    assert journal.check_history()["archived_records"] == 4
    with pytest.raises(JournalError):
        journal.audit_history()


def test_altered_archive_insert_rolls_back_before_active_rows_are_deleted(journal):
    close_segment(journal)
    original = stored(journal)
    with sqlite3.connect(journal.path) as conn:
        conn.execute(
            "CREATE TRIGGER alter_seal AFTER INSERT ON sealed_archives "
            "BEGIN UPDATE sealed_archives SET body=zeroblob(length(body)); END"
        )
    with pytest.raises(JournalError):
        journal.rotate(expected_head=journal.head())
    reopened = SegmentedEventJournal(journal.path.parent, "synthetic")
    assert stored(reopened) == original
    assert reopened.check_history()["archived_records"] == 0


def test_byte_scan_and_writer_contention_remain_recoverable(journal, monkeypatch):
    close_segment(journal)
    journal = journal.rotate(expected_head=journal.head())
    reader = SegmentedEventJournal(journal.path.parent, "synthetic")
    entered, release = Event(), Event()
    original = reader._sealed_body

    def paused(*args):
        result = original(*args)
        entered.set()
        assert release.wait(5)
        return result

    monkeypatch.setattr(reader, "_sealed_body", paused)
    with ThreadPoolExecutor(max_workers=2) as pool:
        scan = pool.submit(reader.check_history)
        assert entered.wait(5)
        writer = pool.submit(start, journal)
        try:
            assert not writer.done()
        finally:
            release.set()
        assert scan.result(timeout=5)["archived_records"] == 4
        assert writer.result(timeout=5)
    assert journal.inspect()["session_open"]
