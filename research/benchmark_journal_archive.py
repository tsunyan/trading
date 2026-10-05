"""Synthetic archive measurements, using temporary stores and fully checked bulk fixtures."""

import argparse
import hashlib
import json
import platform
import sqlite3
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from threading import Event

from trading.event_journal import ZERO, Entry, _digest, _entry_body
from trading.segmented_journal import (
    Segment,
    SegmentedEventJournal,
    _pack_archive,
    _segment_body,
    _segment_digest,
)

NOW = datetime(2026, 10, 4, tzinfo=UTC)


def build(directory, segments, records, *, existing=None):
    journal = existing or SegmentedEventJournal.create(directory, "synthetic", max_records=records)
    if journal.path.parent != Path(directory).resolve() or journal.scope != "synthetic":
        raise ValueError("synthetic_archive_directory_required")
    view = journal.inspect()
    if view["records"] or journal.archive_identity()["archived_segments"]:
        raise ValueError("synthetic_archive_must_be_empty")
    with closing(sqlite3.connect(journal.path)) as conn, conn:
        series, instance = conn.execute("SELECT instance,origin FROM series").fetchone()
        previous = ZERO
        for index in range(1, segments + 1):
            session, rows, head = uuid.uuid4().hex, [], ZERO
            for identity in range(1, records + 1):
                kind = (
                    "BEGIN"
                    if identity == 1
                    else "END"
                    if identity == records
                    else "HEARTBEAT"
                    if identity % 2 == 0
                    else "ACK"
                )
                entry = Entry(
                    kind=kind,
                    epoch=1,
                    session=session,
                    at=NOW,
                    monotonic_ns=identity,
                    target=identity - 1 if kind == "ACK" else None,
                )
                body = _entry_body(entry)
                head = _digest(instance, journal.scope, identity, head, body)
                rows.append((identity, body, head))
            # Avoid quadratic append checks; validate every generated record before sealing.
            _, entries, state = journal._check({"instance": instance, "head": head}, rows)
            assert len(entries) == records and not state["active"] and not state["unacknowledged"]
            sealed = _pack_archive(rows)
            next_instance = uuid.uuid4().hex
            segment = Segment(
                series=series,
                index=index,
                previous=previous,
                instance=instance,
                next_instance=next_instance,
                scope=journal.scope,
                records=records,
                bytes=sum(len(row[1].encode("ascii")) for row in rows),
                max_records=records,
                head=head,
                started_at=NOW,
                ended_at=NOW,
                archive_sha256=hashlib.sha256(sealed).hexdigest(),
            )
            descriptor = _segment_body(segment)
            previous = _segment_digest(series, index, previous, descriptor)
            conn.execute("INSERT INTO segments VALUES(?,?,?)", (index, descriptor, previous))
            conn.execute("INSERT INTO sealed_archives VALUES(?,?)", (index, sealed))
            instance = next_instance
        conn.execute("UPDATE series SET count=?,head=?", (segments, previous))
        conn.execute("UPDATE journal SET instance=?", (instance,))
    return SegmentedEventJournal(directory, "synthetic")


def timed(operation):
    started = time.perf_counter()
    result = operation()
    return time.perf_counter() - started, result


def measure(segments, records):
    with tempfile.TemporaryDirectory(prefix="trading-archive-bench-") as directory:
        journal = build(Path(directory) / "journal", segments, records)
        full_seconds, audit = timed(journal.audit_history)
        check_seconds, check = timed(journal.check_history)
        open_seconds, _ = timed(lambda: SegmentedEventJournal(journal.path.parent, "synthetic"))
        assert audit["archive_head"] == check["archive_head"]
        reader = SegmentedEventJournal(journal.path.parent, "synthetic")
        entered = Event()
        original = reader._sealed_body

        def signaled(*args):
            result = original(*args)
            entered.set()
            return result

        reader._sealed_body = signaled
        with ThreadPoolExecutor(max_workers=2) as pool:
            scan = pool.submit(timed, reader.check_history)
            assert entered.wait(5)
            write_seconds, session = timed(
                lambda: journal.start_session(expected_head=ZERO, at=NOW, monotonic_ns=0)
            )
            concurrent_scan_seconds, _ = scan.result(timeout=30)
        journal.current(session)  # Confirms the writer's live session is still valid.
        assert journal.inspect()["session_open"]
        return {
            "segments": segments,
            "records": audit["archived_records"],
            "body_bytes": audit["archived_bytes"],
            "sqlite_bytes": journal.path.stat().st_size,
            "full_audit_seconds": full_seconds,
            "byte_check_seconds": check_seconds,
            "open_seconds": open_seconds,
            "concurrent_scan_seconds": concurrent_scan_seconds,
            "concurrent_writer_seconds": write_seconds,
            "writer_committed": True,
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--segments", nargs="+", type=int, default=[100, 500])
    parser.add_argument("--records", type=int, default=1024)
    args = parser.parse_args()
    if (
        not 4 <= args.records <= 20_000
        or args.records % 2
        or any(not 1 <= count <= 10_000 for count in args.segments)
    ):
        parser.error("bounded segment counts and even record capacity required")
    result = {
        "date": NOW.date().isoformat(),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "sqlite": sqlite3.sqlite_version,
        "fixture": "BEGIN, HEARTBEAT/ACK pairs, END; fully validated before sealing",
        "limitations": "Warm local reads; one sample per size; byte scans and SQLite read locks "
        "still grow with retained bytes; no production latency guarantee",
        "measurements": [measure(count, args.records) for count in args.segments],
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
