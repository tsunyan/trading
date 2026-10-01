"""Offline current-revision valuation through journaled capture, never a trade gate."""

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from trading.account_sync import SyncError
from trading.account_valuation import ValuationQuote
from trading.account_valuation_lab import synthetic_account, synthetic_policy
from trading.event_capture import JournaledEventCapture
from trading.event_journal import EventJournal
from trading.execution_cash_book import ExecutionCashBook, OpeningCash
from trading.execution_positions import OpeningPosition, PositionBasis


def demo(directory: Path):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    origin = datetime(2026, 10, 2, tzinfo=UTC)
    now = origin
    scope = "synthetic-valuation-sync"
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

    def observe(current, *, equity="999960"):
        return current.resync(
            lambda: synthetic_account(now, equity=equity),
            collect_quote=lambda: ValuationQuote(bid="149.9", ask="150.1", observed_at=now),
            valuation_policy=synthetic_policy(),
            valuation_book=book,
        )

    current = capture()
    initial = observe(current)
    mismatch = observe(current, equity="999950")
    now += timedelta(seconds=31)
    expired = None
    try:
        current.compare_account_valuation(book, expected_revision=mismatch.revision)
    except SyncError as error:
        expired = str(error)
    current = capture()
    restarted = observe(current)
    current.disconnect()
    result = {
        "synthetic_only": True,
        "complete": False,
        "live_enabled": False,
        "initial": initial.model_dump(mode="json"),
        "equity_difference": mismatch.model_dump(mode="json"),
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
