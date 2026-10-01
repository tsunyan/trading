"""Offline close-order reservation, partial fill, restart and cancellation demo."""

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from trading.account_events import parse_event
from trading.account_read_lab import ReplayTransport, Transcript, demo_transcript, replay
from trading.account_reader import AccountReader
from trading.account_sync_lab import demo_execution_transcript
from trading.broker_contracts import OrderIntent
from trading.execution_cash_book import ExecutionCashBatch, ExecutionCashBook, OpeningCash
from trading.execution_positions import OpeningPosition, PositionBasis


def synthetic_close(now, *, filled=True, status="ORDERED", executed_at=None):
    executed_at = executed_at or now
    scenario = demo_execution_transcript(now)
    notice = json.loads(scenario.steps[1].payload)
    notice.update(
        executionId=601,
        orderId=301,
        rootOrderId=301,
        clientOrderId="DemoClose",
        settleType="CLOSE",
        side="SELL",
        orderSize="300",
        orderExecutedSize="100",
        executionSize="100",
        executionPrice="150.1",
        orderPrice="150.1",
        lossGain="10",
        settledSwap="0",
        amount="8",
        executionTimestamp=executed_at.isoformat(),
        orderTimestamp=executed_at.isoformat(),
    )
    intent = OrderIntent(
        client_id="DemoClose",
        side="SELL",
        effect="CLOSE",
        units=300,
        kind="LIMIT",
        price="150.1",
        positions=({"position_id": 401, "units": 300},),
    )
    data = scenario.steps[2].order_reads[0].transcript.model_dump(mode="json")
    for exchange in data["exchanges"]:
        exchange["query"] = (("orderId", "301"),)
        row = exchange["response"]["data"]["list"][0]
        row.update(
            orderId=301, clientOrderId="DemoClose", settleType="CLOSE", side="SELL", price="150.1"
        )
        if exchange["path"] == "/v1/orders":
            row.update(rootOrderId=301, size="300", status=status)
        else:
            row.update(
                executionId=601,
                size="100",
                lossGain="10",
                settledSwap="0",
                amount="8",
                timestamp=executed_at.isoformat(),
            )
            if not filled:
                exchange["response"]["data"]["list"] = []
    transport = ReplayTransport(Transcript.model_validate(data))
    order = AccountReader(transport, clock=lambda: transport.now).collect_order(intent, 301)
    return ExecutionCashBatch(
        events=(parse_event(json.dumps(notice).encode(), now),) if filled else (), reports=(order,)
    )


def synthetic_account(now, *, units=400, ordered=300, active=True, balance="1000000"):
    data = demo_transcript(now).model_dump(mode="json")
    exchanges = []
    for exchange in data["exchanges"]:
        path = exchange["path"]
        if path == "/v1/account/assets":
            exchange["response"]["data"][0]["balance"] = balance
        elif path == "/v1/openPositions":
            for row in exchange["response"]["data"]["list"]:
                row.update(size=str(units), orderedSize=str(ordered))
        else:
            if not active:
                if len(exchange["query"]) > 1:
                    continue
                exchange["response"]["data"]["list"] = []
            else:
                exchange["query"] = tuple(
                    (k, "301" if k == "prevId" else v) for k, v in exchange["query"]
                )
                for row in exchange["response"]["data"]["list"]:
                    row.update(
                        orderId=301,
                        rootOrderId=301,
                        clientOrderId="DemoClose",
                        settleType="CLOSE",
                        side="SELL",
                        size="300",
                        price="150.1",
                    )
        exchanges.append(exchange)
    data["exchanges"] = exchanges
    return replay(Transcript.model_validate(data))


def demo(directory: Path):
    now = datetime(2026, 10, 2, tzinfo=UTC)
    book = ExecutionCashBook.create(
        directory,
        "synthetic-reservations",
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
    initial = book.compare_reservations(
        synthetic_account(now), synthetic_close(now, filled=False).reports
    )
    fill = synthetic_close(now + timedelta(seconds=1))
    book.apply(fill)
    later = now + timedelta(seconds=2)
    evidence = synthetic_close(later, executed_at=now + timedelta(seconds=1)).reports
    partial = book.compare_reservations(
        synthetic_account(later, units=300, ordered=200, balance="1000008"), evidence
    )
    book = ExecutionCashBook(directory, "synthetic-reservations")
    restarted = book.compare_reservations(
        synthetic_account(later, units=300, ordered=200), evidence
    )
    cancelled = book.compare_reservations(
        synthetic_account(later, units=300, ordered=0, active=False),
        synthetic_close(later, status="CANCELED", executed_at=now + timedelta(seconds=1)).reports,
    )
    mismatch = book.compare_reservations(synthetic_account(later, units=300, ordered=100), evidence)
    result = {
        "synthetic_only": True,
        "complete": False,
        "live_enabled": False,
        "initial": initial,
        "partial": partial,
        "restart": restarted,
        "cancelled": cancelled,
        "unexplained_reservation": mismatch,
        "snapshot": book.snapshot(),
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
