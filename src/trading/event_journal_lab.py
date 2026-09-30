"""Local event journal demo, integrity inspection and historical replay. No network."""

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from trading.event_capture import JournaledEventCapture
from trading.event_journal import EventJournal, JournalError


def demo(directory: Path, scope: str):
    journal = EventJournal.create(directory, scope)
    now = datetime.now(UTC)
    frame = json.dumps(
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
            "timestamp": now.isoformat(),
            "msgType": "UPR",
        }
    ).encode()
    # Model an interrupted delivery without killing a process in the user's workspace.
    old = journal.start_session(expected_head=journal.inspect()["head"], at=now, monotonic_ns=0)
    record_id = journal.record(old, "EVENT", at=now, monotonic_ns=0, sequence=1, payload=frame)
    journal.acknowledge(old, record_id)
    journal.record(old, "EVENT", at=now, monotonic_ns=0, sequence=2, payload=frame)
    reopened = EventJournal(directory, scope)
    capture = JournaledEventCapture(
        reopened, clock=lambda: now + timedelta(seconds=1), monotonic_ns=lambda: 0
    )
    capture.start_session(expected_head=reopened.inspect()["head"])
    capture.ingest(1, frame)
    capture.disconnect()
    result = {
        "synthetic_only": True,
        "live_enabled": False,
        "journal": reopened.inspect(),
        "replay": reopened.replay(),
    }
    with (directory / "report.json").open("x", encoding="utf-8") as output:
        output.write(json.dumps(result, indent=2, ensure_ascii=False))
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("init", "status", "replay", "demo"))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--scope", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "demo":
            result = demo(args.directory, args.scope)
        else:
            journal = (
                EventJournal.create(args.directory, args.scope)
                if args.command == "init"
                else EventJournal(args.directory, args.scope)
            )
            result = journal.replay() if args.command == "replay" else journal.inspect()
        print(json.dumps(result, ensure_ascii=False))
    except (JournalError, OSError, ValueError):
        parser.exit(2, "Event journal failed; check scope, integrity and output directory.\n")


if __name__ == "__main__":
    main()
