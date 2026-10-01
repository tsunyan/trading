"""Offline statement matches, repeat-safe external cash postings and residual differences."""

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from trading.account_read_lab import Transcript
from trading.account_read_lab import replay as replay_account
from trading.account_sync_lab import demo_execution_transcript
from trading.cash_transfers import (
    CashTransferEvidence,
    CashTransferMatch,
    CashTransferPolicy,
    CashTransferRecord,
)
from trading.execution_cash_book import ExecutionCashBook, OpeningCash
from trading.execution_cash_lab import synthetic_batch
from trading.execution_positions import PositionBasis


def synthetic_match(now, *, identity="Deposit1", kind="DEPOSIT", amount="10000", fee="0"):
    record = CashTransferRecord(
        transfer_id=identity, kind=kind, amount=amount, fee_debit=fee, occurred_at=now
    )
    return CashTransferMatch(
        primary=CashTransferEvidence(
            source="synthetic-broker",
            reference=f"B-{identity}",
            document_sha256="1" * 64,
            observed_at=now,
            record=record,
        ),
        confirmation=CashTransferEvidence(
            source="synthetic-bank",
            reference=f"K-{identity}",
            document_sha256="2" * 64,
            observed_at=now,
            record=record,
        ),
    )


def demo(directory: Path):
    now = datetime(2026, 10, 2, tzinfo=UTC)
    book = ExecutionCashBook.create(
        directory,
        "synthetic-transfers",
        OpeningCash(
            balance="1000000",
            cutoff=now - timedelta(seconds=1),
            position_basis=PositionBasis(positions=()),
            transfer_policy=CashTransferPolicy(
                primary_source="synthetic-broker", confirmation_source="synthetic-bank"
            ),
        ),
    )
    deposit = synthetic_match(now)
    posted = book.apply_transfers((deposit,))
    duplicate = book.apply_transfers((deposit,))
    book = ExecutionCashBook(directory, "synthetic-transfers")
    restarted = book.apply_transfers((deposit,))
    withdrawal = book.apply_transfers(
        (
            synthetic_match(
                now + timedelta(seconds=1),
                identity="Withdrawal1",
                kind="WITHDRAWAL",
                amount="2500",
                fee="3",
            ),
        )
    )
    execution = book.apply(synthetic_batch(now + timedelta(seconds=2)))
    read = demo_execution_transcript(now + timedelta(seconds=3)).steps[2].transcript

    def account(balance):
        data = read.model_dump(mode="json")
        for exchange in data["exchanges"]:
            if exchange["path"] == "/v1/account/assets":
                exchange["response"]["data"][0]["balance"] = balance
        return replay_account(Transcript.model_validate(data))

    result = {
        "synthetic_only": True,
        "complete": False,
        "live_enabled": False,
        "deposit": posted,
        "duplicate": duplicate,
        "restart_duplicate": restarted,
        "withdrawal": withdrawal,
        "execution": execution,
        "snapshot": book.snapshot(),
        "balance_match": book.compare_balance(account("1007495")),
        "position_match": book.compare_positions(account("1007495")),
        "unexplained_cash": book.compare_balance(account("1007595")),
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
