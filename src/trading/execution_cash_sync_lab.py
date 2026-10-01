"""Offline journal / REST / cash integration demo, without keys or network."""

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from trading.account_read_lab import Transcript
from trading.account_read_lab import replay as replay_account
from trading.account_sync_lab import demo_execution_transcript
from trading.event_capture import JournaledEventCapture
from trading.event_journal import EventJournal
from trading.execution_cash_book import ExecutionCashBook, OpeningCash
from trading.execution_cash_lab import synthetic_batch


def demo(directory: Path):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    now = datetime(2026, 10, 2, tzinfo=UTC)
    scope = "synthetic-cash-sync"
    journal = EventJournal.create(directory / "journal", scope)
    book = ExecutionCashBook.create(
        directory / "cash",
        scope,
        OpeningCash(balance="1000000", cutoff=now - timedelta(seconds=1)),
    )
    scenario = demo_execution_transcript(now)
    payload = scenario.steps[1].payload.encode()
    batch = synthetic_batch(now)

    def capture():
        result = JournaledEventCapture(journal, clock=lambda: now, monotonic_ns=lambda: 0)
        result.start_session(expected_head=journal.inspect()["head"])
        result.ingest(1, payload)
        return result

    def account(balance="999998"):
        data = scenario.steps[2].transcript.model_dump(mode="json")
        for exchange in data["exchanges"]:
            if exchange["path"] == "/v1/account/assets":
                exchange["response"]["data"][0]["balance"] = balance
        return replay_account(Transcript.model_validate(data))

    first = capture()
    posted = first.resync(account, collect_orders=lambda ids: batch.reports, cash_book=book)
    duplicate = first.resync(account, collect_orders=lambda ids: batch.reports, cash_book=book)
    first.disconnect()
    journal = EventJournal(directory / "journal", scope)
    book = ExecutionCashBook(directory / "cash", scope)
    restarted = capture()
    repeated = restarted.resync(account, collect_orders=lambda ids: batch.reports, cash_book=book)
    unexplained = restarted.resync(
        lambda: account("1000098"), collect_orders=lambda ids: batch.reports, cash_book=book
    )
    comparison = book.compare_balance(unexplained.report)
    restarted.disconnect()
    result = {
        "synthetic_only": True,
        "complete": False,
        "live_enabled": False,
        "first_post": posted.model_dump(mode="json"),
        "duplicate": duplicate.model_dump(mode="json"),
        "restart_duplicate": repeated.model_dump(mode="json"),
        "unexplained_cash": unexplained.model_dump(mode="json"),
        "balance_comparison": comparison,
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
