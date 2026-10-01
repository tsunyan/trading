import json
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest

from trading.event_journal import ZERO, EventJournal, JournalError, _canonical, _digest

NOW = datetime(2026, 9, 30, tzinfo=UTC)


def payload(**changes):
    return json.dumps(
        {
            "channel": "positionEvents",
            "positionId": 401,
            "symbol": "USD_JPY",
            "side": "BUY",
            "size": "400",
            "orderdSize": "0",
            "price": "150",
            "lossGain": "-40",
            "totalSwap": "0",
            "timestamp": NOW.isoformat(),
            "msgType": "UPR",
            **changes,
        }
    ).encode()


@pytest.fixture
def journal(tmp_path):
    return EventJournal.create(tmp_path / "journal", "synthetic")


def start(journal, at=NOW, mono=0):
    return journal.start_session(expected_head=journal.inspect()["head"], at=at, monotonic_ns=mono)


def event(journal, session, sequence=1, data=None, at=NOW, mono=0):
    return journal.record(
        session,
        "EVENT",
        sequence=sequence,
        payload=payload() if data is None else data,
        at=at,
        monotonic_ns=mono,
    )


def test_record_ack_reopen_and_readonly_replay(journal):
    session = start(journal)
    record_id = event(journal, session)
    assert journal.inspect()["unacknowledged_records"] == (record_id,)
    assert journal.replay()["outcomes"][-1]["error"] == "delivery_outcome_unknown"
    journal.acknowledge(session, record_id)
    journal.record(session, "END", at=NOW, monotonic_ns=0)
    before = journal.path.read_bytes()
    reopened = EventJournal(journal.path.parent, "synthetic")
    assert reopened.inspect() == journal.inspect()
    replay = reopened.replay()
    assert replay == journal.replay() and replay["historical_only"]
    assert not replay["complete"] and not replay["live_enabled"] and replay["resync_required"]
    assert replay["outcomes"][-1]["phase"] == "DISCONNECTED"
    assert not replay["unacknowledged_records"]
    assert before == journal.path.read_bytes()


def test_missing_database_is_not_recreated(tmp_path, journal):
    with pytest.raises(JournalError, match="storage_failed"):
        EventJournal(tmp_path / "absent", "synthetic")
    assert not (tmp_path / "absent").exists()
    saved = journal.path.with_suffix(".saved")
    journal.path.rename(saved)
    with pytest.raises(JournalError):
        journal.inspect()
    assert not journal.path.exists()


def test_scope_and_existing_directory_cannot_be_replaced(journal):
    with pytest.raises(JournalError, match="integrity"):
        EventJournal(journal.path.parent, "wrong")
    with pytest.raises(FileExistsError):
        EventJournal.create(journal.path.parent, "synthetic")
    assert journal.inspect()["records"] == 0


@pytest.mark.parametrize("scope", ["", "a/b", "x" * 65, None])
def test_invalid_scopes_do_not_create_files(tmp_path, scope):
    with pytest.raises(JournalError):
        EventJournal.create(tmp_path / "invalid", scope)
    assert not (tmp_path / "invalid").exists()


@pytest.mark.parametrize("sequence", [0, 2, True, "1"])
def test_sequence_rejection_is_durable_and_payload_redacted(journal, sequence):
    session = start(journal)
    with pytest.raises(JournalError, match="sequence_gap"):
        event(journal, session, sequence, b"credential-do-not-store")
    assert b"credential-do-not-store" not in journal.path.read_bytes()
    status = journal.inspect()
    assert status["rejected_frames"] == 1 and not status["session_open"]
    with pytest.raises(JournalError, match="fenced"):
        event(journal, session)


@pytest.mark.parametrize("case", ["unknown", "oversized", "duplicate", "wrong_type"])
def test_invalid_frames_are_never_stored_verbatim(journal, case):
    session = start(journal)
    data = {
        "unknown": b'{"API-SECRET":"do-not-store"}',
        "oversized": b"x" * 16_385,
        "duplicate": b'{"symbol":"USD_JPY","symbol":"USD_JPY"}',
        "wrong_type": "do-not-store",
    }[case]
    with pytest.raises(JournalError, match="frame_rejected"):
        event(journal, session, data=data)
    assert journal.inspect()["rejected_frames"] == 1
    assert b"do-not-store" not in journal.path.read_bytes()
    with sqlite3.connect(journal.path) as conn:
        body = json.loads(
            conn.execute("SELECT body FROM records ORDER BY id DESC LIMIT 1").fetchone()[0]
        )
    assert body["kind"] == "REJECTED" and body["payload"] is None


def test_no_more_delivery_before_ack_and_no_duplicate_ack(journal):
    session = start(journal)
    record_id = event(journal, session)
    with pytest.raises(JournalError, match="unresolved"):
        event(journal, session, 2)
    with pytest.raises(JournalError, match="receipt_mismatch"):
        journal.acknowledge(session, record_id + 1)
    journal.acknowledge(session, record_id)
    with pytest.raises(JournalError, match="receipt_mismatch"):
        journal.acknowledge(session, record_id)


def test_explicit_new_epoch_fences_old_writer_and_keeps_unknown_delivery(journal):
    old = start(journal)
    record_id = event(journal, old)
    reopened = EventJournal(journal.path.parent, "synthetic")
    assert reopened.inspect()["unacknowledged_records"] == (record_id,)
    new = start(reopened, mono=0)
    assert new != old
    with pytest.raises(JournalError, match="fenced"):
        journal.acknowledge(old, record_id)
    new_id = event(reopened, new)
    reopened.acknowledge(new, new_id)
    assert reopened.inspect()["unacknowledged_records"] == (record_id,)
    assert reopened.inspect()["epoch"] == 2


def test_compare_and_swap_epoch_race_has_one_winner(journal):
    expected = journal.inspect()["head"]
    peers = [EventJournal(journal.path.parent, "synthetic") for _ in range(2)]

    def attempt(peer):
        try:
            peer.start_session(expected_head=expected, at=NOW, monotonic_ns=0)
            return True
        except JournalError:
            return False

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(attempt, peers)) == [False, True]
    assert journal.inspect()["epoch"] == 1


def test_parallel_capture_has_only_one_committed_pending_delivery(journal):
    session = start(journal)
    peers = [EventJournal(journal.path.parent, "synthetic") for _ in range(2)]

    def attempt(peer):
        try:
            return event(peer, session)
        except JournalError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, peers))
    assert results.count(2) == 1 and results.count(None) == 1
    assert journal.inspect()["captured_events"] == 1


@pytest.mark.parametrize(
    "change", ["body", "hash", "tail", "middle", "head", "version", "instance", "count"]
)
def test_corruption_rejected_without_writes(journal, change):
    session = start(journal)
    journal.acknowledge(session, event(journal, session))
    with sqlite3.connect(journal.path) as conn:
        conn.execute(
            {
                "body": "UPDATE records SET body='{}' WHERE id=2",
                "hash": "UPDATE records SET digest='bad' WHERE id=2",
                "tail": "DELETE FROM records WHERE id=3",
                "middle": "DELETE FROM records WHERE id=2",
                "head": "UPDATE journal SET head='bad'",
                "version": "UPDATE journal SET version=99",
                "instance": "UPDATE journal SET instance='ffffffffffffffffffffffffffffffff'",
                "count": "UPDATE journal SET count=0",
            }[change]
        )
    before = journal.path.read_bytes()
    with pytest.raises(JournalError, match="integrity"):
        journal.inspect()
    assert before == journal.path.read_bytes()
    with pytest.raises(JournalError):
        EventJournal(journal.path.parent, "synthetic")


@pytest.mark.parametrize("clock", ["wall", "mono"])
def test_clock_reversal_is_durably_closed(journal, clock):
    session = start(journal, mono=100)
    with pytest.raises(JournalError, match="clock"):
        event(
            journal,
            session,
            at=NOW - timedelta(seconds=1) if clock == "wall" else NOW,
            mono=99 if clock == "mono" else 100,
        )
    assert not journal.inspect()["session_open"]
    assert journal.replay()["outcomes"][-1]["kind"] == "FAULT"


def test_capacity_does_not_prune_or_ack_silently(tmp_path):
    journal = EventJournal.create(tmp_path / "journal", "synthetic", max_records=4)
    session = start(journal)
    journal.acknowledge(session, event(journal, session))
    pending = event(journal, session, 2)
    with pytest.raises(JournalError, match="capacity"):
        journal.acknowledge(session, pending)
    assert journal.inspect()["records"] == 4
    assert journal.inspect()["unacknowledged_records"] == (pending,)


def test_sql_failure_rolls_back_capture_and_head(journal):
    session = start(journal)
    before = journal.inspect()
    with sqlite3.connect(journal.path) as conn:
        conn.executescript(
            "CREATE TRIGGER reject_head BEFORE UPDATE ON journal "
            "BEGIN SELECT RAISE(ABORT,'private OS error'); END;"
        )
    with pytest.raises(JournalError, match="storage_failed"):
        event(journal, session)
    with sqlite3.connect(journal.path) as conn:
        conn.execute("DROP TRIGGER reject_head")
    reopened = EventJournal(journal.path.parent, "synthetic")
    assert reopened.inspect() == before


def test_process_exit_between_capture_and_ack_preserves_ambiguity(journal):
    script = """
import os, sys
from pathlib import Path
from datetime import datetime, UTC
from trading.event_journal import EventJournal
j = EventJournal(Path(sys.argv[1]), 'synthetic')
now = datetime(2026, 9, 30, tzinfo=UTC)
s = j.start_session(expected_head=j.inspect()['head'], at=now, monotonic_ns=0)
j.record(s, 'HEARTBEAT', at=now, monotonic_ns=0)
os._exit(23)
"""
    process = subprocess.run(
        [sys.executable, "-c", script, str(journal.path.parent)], capture_output=True, timeout=10
    )
    assert process.returncode == 23
    reopened = EventJournal(journal.path.parent, "synthetic")
    assert reopened.inspect()["unacknowledged_records"] == (2,)
    assert reopened.replay()["outcomes"][-1]["error"] == "delivery_outcome_unknown"


@pytest.mark.parametrize("change", ["bad_receipt", "sequence", "clock", "extra_field"])
def test_semantic_inconsistency_rejected_even_when_hashes_match(journal, change):
    session = start(journal)
    journal.acknowledge(session, event(journal, session))
    with sqlite3.connect(journal.path) as conn:
        instance, scope = conn.execute("SELECT instance,scope FROM journal").fetchone()
        entries = [
            json.loads(row[0]) for row in conn.execute("SELECT body FROM records ORDER BY id")
        ]
        if change == "bad_receipt":
            entries[2]["target"] = 1
        elif change == "sequence":
            entries[1]["sequence"] = 2
        elif change == "clock":
            entries[1]["at"] = "2026-09-29T00:00:00Z"
        else:
            entries[0]["target"] = 1
        previous, size = ZERO, 0
        for index, entry in enumerate(entries, 1):
            body = _canonical(entry)
            previous = _digest(instance, scope, index, previous, body)
            size += len(body.encode())
            conn.execute("UPDATE records SET body=?,digest=? WHERE id=?", (body, previous, index))
        conn.execute("UPDATE journal SET head=?,bytes=?", (previous, size))
    with pytest.raises(JournalError, match="integrity"):
        journal.replay()


def test_new_epoch_refuses_old_process_ack(journal):
    session = start(journal)
    pending = event(journal, session)
    start(journal)
    script = """
import sys
from pathlib import Path
from trading.event_journal import EventJournal, JournalError
j = EventJournal(Path(sys.argv[1]), 'synthetic')
try:
    j.acknowledge(sys.argv[2], int(sys.argv[3]))
except JournalError as e:
    print(str(e))
else:
    raise SystemExit(4)
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(journal.path.parent), session, str(pending)],
        capture_output=True,
        timeout=10,
    )
    assert result.returncode == 0
    assert result.stdout.decode().strip() == "capture_session_fenced"
    assert journal.inspect()["unacknowledged_records"] == (pending,)


@pytest.mark.parametrize("operation", ["open", "inspect", "current", "replay"])
def test_read_only_verification_never_blocks_a_writer_commit(journal, monkeypatch, operation):
    session = start(journal)
    writer = EventJournal(journal.path.parent, "synthetic")
    reader = EventJournal(journal.path.parent, "synthetic")
    original, written = EventJournal._check, []

    def check(self, meta, rows):
        # A rollback-journal COMMIT needs every shared lock released. Verifying
        # while still holding one would make this write time out as busy.
        if self is not writer and not written:
            written.append(event(writer, session))
        return original(self, meta, rows)

    monkeypatch.setattr(EventJournal, "_check", check)
    if operation == "open":
        EventJournal(journal.path.parent, "synthetic")
    elif operation == "current":
        assert reader.current(session)["unacknowledged_records"] == ()
    else:
        getattr(reader, operation)()
    assert written == [2]
    assert not writer._failed and not reader._failed
    assert reader.inspect()["unacknowledged_records"] == (2,)


def test_busy_write_is_retried_once_without_duplicate(journal, monkeypatch):
    monkeypatch.setattr("trading.event_journal.BUSY_TIMEOUT_SECONDS", 0.05)
    session = start(journal)
    lock = sqlite3.connect(journal.path)
    waits = []

    def wait(seconds):
        waits.append(seconds)
        lock.rollback()

    journal._wait = wait
    try:
        lock.execute("BEGIN IMMEDIATE")
        assert event(journal, session) == 2
    finally:
        lock.close()
    assert waits == [0.05]
    view = journal.inspect()
    assert view["records"] == 2 and view["unacknowledged_records"] == (2,)


def test_busy_exhaustion_is_not_treated_as_corruption(journal, monkeypatch):
    monkeypatch.setattr("trading.event_journal.BUSY_TIMEOUT_SECONDS", 0.05)
    session = start(journal)
    before = journal.inspect()
    waits = []
    journal._wait = waits.append
    with sqlite3.connect(journal.path) as lock:
        lock.execute("BEGIN IMMEDIATE")
        with pytest.raises(JournalError, match="journal_busy"):
            event(journal, session)
        lock.rollback()
    assert waits == [0.05, 0.05]
    assert not journal._failed
    assert journal.inspect() == before
    assert event(journal, session) == 2
