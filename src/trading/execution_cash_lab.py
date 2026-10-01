"""Offline matched execution cash postings, duplicate / restart / balance demo."""

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from trading.account_events import parse_event
from trading.account_read_lab import ReplayTransport, Transcript
from trading.account_read_lab import replay as replay_account
from trading.account_reader import AccountReader
from trading.account_sync_lab import demo_execution_transcript
from trading.broker_contracts import OrderIntent
from trading.execution_cash_book import ExecutionCashBatch, ExecutionCashBook, OpeningCash


def synthetic_batch(now: datetime, *, closing=False):
    """Parse synthetic wire notices and collect exact known-order GET exchanges."""
    scenario = demo_execution_transcript(now)
    notice = json.loads(scenario.steps[1].payload)
    read = scenario.steps[2].order_reads[0]
    intent, order_id, transcript = read.intent, read.order_id, read.transcript
    if closing:
        notice.update(
            executionId=601,
            orderId=301,
            rootOrderId=301,
            clientOrderId="DemoClose",
            settleType="CLOSE",
            side="SELL",
            orderSize="400",
            orderExecutedSize="400",
            executionPrice="150.1",
            orderPrice="150.1",
            lossGain="40",
            settledSwap="3",
            amount="41",
        )
        intent = OrderIntent(
            client_id="DemoClose",
            side="SELL",
            effect="CLOSE",
            units=400,
            kind="LIMIT",
            price="150.1",
            positions=({"position_id": 401, "units": 400},),
        )
        order_id = 301
        data = transcript.model_dump(mode="json")
        for exchange in data["exchanges"]:
            exchange["query"] = (("orderId", "301"),)
            row = exchange["response"]["data"]["list"][0]
            row.update(
                orderId=301,
                clientOrderId="DemoClose",
                settleType="CLOSE",
                side="SELL",
                size="400",
                price="150.1",
            )
            if exchange["path"] == "/v1/orders":
                row.update(rootOrderId=301, status="EXECUTED")
            else:
                row.update(executionId=601, lossGain="40", settledSwap="3", amount="41")
        transcript = Transcript.model_validate(data)
    transport = ReplayTransport(transcript)
    report = AccountReader(transport, clock=lambda: transport.now).collect_order(intent, order_id)
    if transport.index != len(transcript.exchanges):
        raise ValueError("cash_lab_unused_exchanges")
    return ExecutionCashBatch(
        events=(parse_event(json.dumps(notice).encode(), now),), reports=(report,)
    )


def demo(directory: Path, scope="synthetic-cash"):
    now = datetime(2026, 10, 2, tzinfo=UTC)
    book = ExecutionCashBook.create(
        directory, scope, OpeningCash(balance="1000000", cutoff=now - timedelta(seconds=1))
    )
    opening = book.snapshot()
    first = synthetic_batch(now)
    posted = book.apply(first)
    duplicate = book.apply(first)
    reopened = ExecutionCashBook(directory, scope)
    restarted_duplicate = reopened.apply(first)
    close = reopened.apply(synthetic_batch(now + timedelta(seconds=1), closing=True))
    state = reopened.snapshot()
    # The account response is independently synthetic, including a matching
    # balance followed by an unexplained +100. Neither comparison changes cash.
    read = demo_execution_transcript(now + timedelta(seconds=2)).steps[2].transcript
    data = read.model_dump(mode="json")
    for exchange in data["exchanges"]:
        if exchange["path"] == "/v1/account/assets":
            exchange["response"]["data"][0]["balance"] = state["balance"]
    account = replay_account(Transcript.model_validate(data))
    matched = reopened.compare_balance(account)
    for exchange in data["exchanges"]:
        if exchange["path"] == "/v1/account/assets":
            exchange["response"]["data"][0]["balance"] = "1000139"
    mismatch = reopened.compare_balance(replay_account(Transcript.model_validate(data)))
    result = {
        "synthetic_only": True,
        "complete": False,
        "live_enabled": False,
        "opening": opening,
        "first_post": posted,
        "duplicate": duplicate,
        "restart_duplicate": restarted_duplicate,
        "close_post": close,
        "snapshot": state,
        "balance_match": matched,
        "unexplained_cash": mismatch,
    }
    with (directory / "report.json").open("x", encoding="utf-8") as output:
        json.dump(result, output, ensure_ascii=False, indent=2)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("demo", "status"))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--scope", default="synthetic-cash")
    args = parser.parse_args(argv)
    result = (
        demo(args.directory, args.scope)
        if args.command == "demo"
        else (ExecutionCashBook(args.directory, args.scope).snapshot())
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
