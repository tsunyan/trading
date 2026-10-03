"""Offline current-revision reservation comparison through journaled capture."""

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from trading.account_sync import SyncError
from trading.event_capture import JournaledEventCapture
from trading.event_journal import EventJournal
from trading.execution_cash_book import ExecutionCashBook, OpeningCash
from trading.execution_positions import OpeningPosition, PositionBasis
from trading.position_reservation_lab import synthetic_account, synthetic_close


def demo(directory: Path):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    origin = datetime(2026, 10, 2, tzinfo=UTC)
    now = origin
    scope = "synthetic-reservation-sync"
    journal = EventJournal.create(directory / "journal", scope)
    book = ExecutionCashBook.create(
        directory / "cash",
        scope,
        OpeningCash(
            balance="1000000",
            cutoff=now - timedelta(seconds=1),
            position_basis=PositionBasis(
                positions=(
                    OpeningPosition(position_id=401, side="BUY", units=400, average_price="150"),
                )
            ),
        ),
    )

    def capture():
        result = JournaledEventCapture(
            journal,
            clock=lambda: now,
            monotonic_ns=lambda: int((now - origin).total_seconds() * 1e9),
        )
        result.start_session(expected_head=journal.inspect()["head"])
        return result

    current = capture()
    initial = current.resync(
        lambda: synthetic_account(now),
        collect_reservations=lambda: synthetic_close(now, filled=False).reports,
        reservation_book=book,
    )
    now += timedelta(seconds=1)
    executed_at = now
    # Synthetic locally matched accounting is explicit; comparison never books it.
    book.apply(synthetic_close(now))
    current.ingest(
        1,
        json.dumps(
            {
                "channel": "positionEvents",
                "positionId": 401,
                "symbol": "USD_JPY",
                "side": "BUY",
                "size": "300",
                "orderdSize": "200",
                "price": "150",
                "lossGain": "-40",
                "totalSwap": "0",
                "timestamp": now.isoformat(),
                "msgType": "UPR",
            }
        ).encode(),
    )
    partial = current.resync(
        lambda: synthetic_account(now, units=300, ordered=200, balance="1000008"),
        collect_reservations=lambda: synthetic_close(now, executed_at=executed_at).reports,
        reservation_book=book,
    )
    mismatch = current.resync(
        lambda: synthetic_account(now, units=300, ordered=100, balance="1000008"),
        collect_reservations=lambda: synthetic_close(now, executed_at=executed_at).reports,
        reservation_book=book,
    )
    now += timedelta(seconds=31)
    expired = None
    try:
        current.compare_position_reservations(book, expected_revision=mismatch.revision)
    except SyncError as error:
        expired = str(error)
    current = capture()  # Explicit new epoch and fresh reads, no restored evidence.
    restarted = current.resync(
        lambda: synthetic_account(now, units=300, ordered=200, balance="1000008"),
        collect_reservations=lambda: synthetic_close(now, executed_at=executed_at).reports,
        reservation_book=book,
    )
    current.disconnect()
    result = {
        "synthetic_only": True,
        "complete": False,
        "live_enabled": False,
        "initial": initial.model_dump(mode="json"),
        "partial": partial.model_dump(mode="json"),
        "unexplained_reservation": mismatch.model_dump(mode="json"),
        "expired_comparison": expired,
        "restart": restarted.model_dump(mode="json"),
        "cash_book": book.snapshot(),
        "journal": journal.inspect(),
    }
    with (directory / "report.json").open("x", encoding="utf-8") as output:
        json.dump(result, output, ensure_ascii=False, indent=2)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("demo",))
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args(argv)
    print(json.dumps(demo(args.directory), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
