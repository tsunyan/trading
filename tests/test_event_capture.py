import json
import socket
import sqlite3
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest

from trading.account_read_lab import demo_transcript, replay
from trading.account_sync import SyncError
from trading.event_capture import JournaledEventCapture
from trading.event_journal import EventJournal, JournalError

NOW = datetime(2026, 9, 30, tzinfo=UTC)


class Clock:
    def __init__(self):
        self.wall, self.mono = NOW, 0

    def advance(self, seconds):
        self.wall += timedelta(seconds=seconds)
        self.mono += seconds * 1_000_000_000

    def args(self):
        return {"clock": lambda: self.wall, "monotonic_ns": lambda: self.mono}


def frame(**changes):
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
def setup(tmp_path, monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("network attempted"))
    clock = Clock()
    journal = EventJournal.create(tmp_path / "journal", "synthetic")
    capture = JournaledEventCapture(journal, **clock.args())
    return clock, journal, capture


def begin(journal, capture):
    capture.start_session(expected_head=journal.inspect()["head"])


def test_commit_precedes_delivery_and_ack_follows(setup, monkeypatch):
    _, journal, capture = setup
    assert not capture.status()["capture_failed"]
    begin(journal, capture)
    calls = []
    ingest = capture._monitor.ingest

    def checked(*args):
        reopened = EventJournal(journal.path.parent, "synthetic")
        assert reopened.inspect()["unacknowledged_records"] == (2,)
        calls.append(True)
        ingest(*args)

    monkeypatch.setattr(capture._monitor, "ingest", checked)
    capture.ingest(1, frame())
    assert calls == [True]
    assert journal.inspect()["unacknowledged_records"] == ()
    assert capture.status()["phase"] == "NEEDS_RESYNC"
    capture.disconnect()
    assert not capture.status()["capture_failed"]
    assert not journal.inspect()["session_open"]


def test_reopening_never_resumes_observation_or_pending_delivery(setup):
    clock, journal, capture = setup
    begin(journal, capture)
    capture.ingest(1, frame())
    result = capture.resync(lambda: replay(demo_transcript(clock.wall)))
    assert result.structural_match and "journal_is_not_broker_history_proof" in result.blockers
    assert not capture.status()["resync_required"]
    reopened = JournaledEventCapture(EventJournal(journal.path.parent, "synthetic"), **clock.args())
    assert reopened.status()["resync_required"] and reopened.status()["phase"] == "DISCONNECTED"
    assert journal.inspect()["resync_required"]
    begin(journal, reopened)
    assert reopened.status()["phase"] == "NEEDS_RESYNC"
    assert "journal_epoch_gap_not_repaired" in reopened.status()["blockers"]
    assert capture.status()["capture_failed"]
    with pytest.raises(JournalError):
        capture.ingest(2, frame())


def test_unknown_old_delivery_remains_a_blocker_after_new_epoch_resync(setup):
    clock, journal, capture = setup
    old = journal.start_session(expected_head=journal.inspect()["head"], at=NOW, monotonic_ns=0)
    record_id = journal.record(old, "EVENT", at=NOW, monotonic_ns=0, sequence=1, payload=frame())
    begin(journal, capture)
    result = capture.resync(lambda: replay(demo_transcript(clock.wall)))
    assert result.structural_match and not result.complete and not result.live_enabled
    assert "journal_delivery_outcome_unknown" in result.blockers
    assert "journal_epoch_gap_not_repaired" in result.blockers
    assert capture.status()["journal_unacknowledged_records"] == (record_id,)
    assert capture.status()["journal_epoch"] == 2


@pytest.mark.parametrize("kind", ["insert", "ack"])
def test_storage_failure_never_leaves_observation_usable(setup, kind):
    clock, journal, capture = setup
    begin(journal, capture)
    capture.resync(lambda: replay(demo_transcript(clock.wall)))
    with sqlite3.connect(journal.path) as conn:
        target = 2 if kind == "insert" else 3
        conn.executescript(f"""
            CREATE TRIGGER reject_write BEFORE INSERT ON records WHEN NEW.id={target}
            BEGIN SELECT RAISE(ABORT, 'sensitive storage details'); END;
        """)
    with pytest.raises(JournalError) as caught:
        capture.ingest(1, frame())
    assert "sensitive" not in str(caught.value)
    assert capture.status()["capture_failed"] and capture.status()["phase"] == "DISCONNECTED"
    with sqlite3.connect(journal.path) as conn:
        conn.execute("DROP TRIGGER reject_write")
    reopened = EventJournal(journal.path.parent, "synthetic")
    assert reopened.inspect()["unacknowledged_records"] == (() if kind == "insert" else (2,))


def test_consumer_failure_records_fixed_fault_and_keeps_pending(setup, monkeypatch):
    _, journal, capture = setup
    begin(journal, capture)

    def broken(*args):
        raise RuntimeError("private consumer details")

    monkeypatch.setattr(capture._monitor, "ingest", broken)
    with pytest.raises(JournalError, match="capture_delivery_failed"):
        capture.ingest(1, frame())
    assert journal.inspect()["unacknowledged_records"] == (2,)
    assert not journal.inspect()["session_open"]
    assert b"private consumer details" not in journal.path.read_bytes()


def test_bad_frame_ends_capture_without_logging_secret(setup):
    _, journal, capture = setup
    begin(journal, capture)
    with pytest.raises(JournalError, match="frame_rejected"):
        capture.ingest(1, b'{"authorization":"secret-do-not-log"}')
    assert b"secret-do-not-log" not in journal.path.read_bytes()
    assert journal.inspect()["rejected_frames"] == 1
    assert capture.status()["phase"] == "DISCONNECTED"


def test_notification_can_arrive_during_resync(setup):
    clock, journal, capture = setup
    begin(journal, capture)
    entered, release = threading.Event(), threading.Event()

    def collect():
        entered.set()
        assert release.wait(5)
        return replay(demo_transcript(clock.wall))

    with ThreadPoolExecutor(max_workers=1) as pool:
        result = pool.submit(capture.resync, collect)
        try:
            assert entered.wait(5)
            capture.ingest(1, frame())
        finally:
            release.set()
        with pytest.raises(SyncError, match="changed_during_collection"):
            result.result(timeout=5)
    assert journal.inspect()["captured_events"] == 1
    assert journal.inspect()["unacknowledged_records"] == ()


def test_new_writer_fences_collection_after_callback(setup):
    clock, journal, capture = setup
    begin(journal, capture)

    def collect():
        other = JournaledEventCapture(
            EventJournal(journal.path.parent, "synthetic"), **clock.args()
        )
        begin(journal, other)
        return replay(demo_transcript(clock.wall))

    with pytest.raises(JournalError, match="fenced"):
        capture.resync(collect)
    assert capture.status()["capture_failed"]


def test_heartbeat_and_stale_liveness_are_persisted(setup):
    clock, journal, capture = setup
    begin(journal, capture)
    clock.advance(60)
    capture.heartbeat()
    assert journal.inspect()["unacknowledged_records"] == ()
    clock.advance(76)
    with pytest.raises(JournalError, match="delivery_failed"):
        capture.heartbeat()
    assert not journal.inspect()["session_open"]
    assert journal.inspect()["unacknowledged_records"] == (4,)


@pytest.mark.parametrize("kind", ["wall", "callback", "mono"])
def test_invalid_clock_disconnects(setup, kind):
    clock, journal, capture = setup
    begin(journal, capture)
    if kind == "wall":
        clock.wall -= timedelta(seconds=1)
    elif kind == "mono":
        clock.mono = True
    else:
        capture._clock = lambda: (_ for _ in ()).throw(RuntimeError("private time source"))
    with pytest.raises(JournalError, match="clock"):
        capture.ingest(1, frame())
    assert capture.status()["phase"] == "DISCONNECTED"


def test_crash_at_consumer_boundary_leaves_durable_pending(setup):
    _, journal, _ = setup
    script = """
import os, sys
from pathlib import Path
from datetime import datetime, UTC
from trading.event_capture import JournaledEventCapture
from trading.event_journal import EventJournal
j = EventJournal(Path(sys.argv[1]), 'synthetic')
c = JournaledEventCapture(j, clock=lambda: datetime(2026,9,30,tzinfo=UTC), monotonic_ns=lambda: 0)
c.start_session(expected_head=j.inspect()['head'])
c._monitor.heartbeat = lambda *a: os._exit(23)
c.heartbeat()
"""
    process = subprocess.run(
        [sys.executable, "-c", script, str(journal.path.parent)], capture_output=True, timeout=10
    )
    assert process.returncode == 23
    assert journal.inspect()["unacknowledged_records"] == (2,)
    assert journal.replay()["outcomes"][-1]["error"] == "delivery_outcome_unknown"


def test_failed_capture_status_preserves_unknown_delivery(setup, monkeypatch):
    _, journal, capture = setup
    begin(journal, capture)

    def broken(*args):
        raise RuntimeError("private consumer details")

    monkeypatch.setattr(capture._monitor, "ingest", broken)
    with pytest.raises(JournalError, match="delivery_failed"):
        capture.ingest(1, frame())
    status = capture.status()
    assert status["capture_failed"]
    assert status["journal_epoch"] == 1
    assert status["journal_unacknowledged_records"] == (2,)
    assert "journal_delivery_outcome_unknown" in status["blockers"]


def test_delivery_verifies_twice_and_stale_session_still_fenced(setup, monkeypatch):
    _, journal, capture = setup
    begin(journal, capture)
    original, checks = journal._verify, []

    def verify(conn):
        checks.append(True)
        return original(conn)

    monkeypatch.setattr(journal, "_verify", verify)
    capture.ingest(1, frame())
    assert len(checks) == 2
    journal.start_session(expected_head=journal.inspect()["head"], at=NOW, monotonic_ns=0)
    with pytest.raises(JournalError, match="fenced"):
        capture.ingest(2, frame())
    assert capture.status()["capture_failed"]


def test_clock_tolerance_is_persisted_for_verify_reopen_and_replay(setup):
    clock, journal, _ = setup
    capture = JournaledEventCapture(journal, **clock.args(), clock_skew_ms=100)
    begin(journal, capture)
    capture.ingest(1, frame(timestamp=(NOW + timedelta(milliseconds=50)).isoformat()))
    assert capture.status()["phase"] == "NEEDS_RESYNC"
    reopened = EventJournal(journal.path.parent, "synthetic")
    assert reopened.inspect()["captured_events"] == 1
    assert all(item["error"] is None for item in reopened.replay()["outcomes"])
    # A new epoch does not inherit the preceding epoch's clock tolerance.
    strict = JournaledEventCapture(reopened, **clock.args())
    begin(reopened, strict)
    with pytest.raises(JournalError, match="frame_rejected"):
        strict.ingest(1, frame(timestamp=(NOW + timedelta(milliseconds=50)).isoformat()))


def test_diagnostic_reader_during_capture_does_not_poison_delivery(setup, monkeypatch):
    _, journal, capture = setup
    begin(journal, capture)
    original, delivered = EventJournal._check, []

    def check(self, meta, rows):
        # Deliver while another process-style reader is still verifying.
        if self is not journal and not delivered:
            capture.ingest(1, frame())
            delivered.append(True)
        return original(self, meta, rows)

    monkeypatch.setattr(EventJournal, "_check", check)
    EventJournal(journal.path.parent, "synthetic").inspect()
    status = capture.status()
    assert delivered and not status["capture_failed"]
    assert status["journal_unacknowledged_records"] == ()


def test_busy_status_read_reports_unknown_state_without_stopping_capture(setup, monkeypatch):
    monkeypatch.setattr("trading.event_journal.BUSY_TIMEOUT_SECONDS", 0)
    _, journal, capture = setup
    journal._wait = lambda seconds: None
    with sqlite3.connect(journal.path) as lock:
        lock.execute("BEGIN EXCLUSIVE")
        # Before start: a diagnostic status() must not consume the new object.
        status = capture.status()
        assert not status["capture_failed"]
        assert "journal_busy_state_unknown" in status["blockers"]
        lock.rollback()
    begin(journal, capture)
    with sqlite3.connect(journal.path) as lock:
        lock.execute("BEGIN EXCLUSIVE")
        status = capture.status()
        assert not status["capture_failed"] and status["journal_epoch"] is None
        assert "journal_busy_state_unknown" in status["blockers"]
        lock.rollback()
    capture.ingest(1, frame())
    status = capture.status()
    assert not status["capture_failed"] and status["journal_epoch"] == 1
    assert "journal_busy_state_unknown" not in status["blockers"]


def test_transient_busy_during_delivery_is_retried(setup, monkeypatch):
    monkeypatch.setattr("trading.event_journal.BUSY_TIMEOUT_SECONDS", 0)
    _, journal, capture = setup
    begin(journal, capture)
    lock = sqlite3.connect(journal.path)
    journal._wait = lambda seconds: lock.rollback()
    try:
        lock.execute("BEGIN IMMEDIATE")
        capture.ingest(1, frame())
    finally:
        lock.close()
    status = capture.status()
    assert not status["capture_failed"]
    assert status["journal_unacknowledged_records"] == ()
    assert journal.inspect()["captured_events"] == 1
