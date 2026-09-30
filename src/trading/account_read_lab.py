"""Explicit file replay for the account reader. Never connects to a broker."""

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import AwareDatetime

from trading.account_reader import ASSETS, ORDERS, POSITIONS, AccountReader, CollectionError
from trading.broker_contracts import Contract, RequestPlan


class Exchange(Contract):
    method: Literal["GET"]
    path: str
    query: tuple[tuple[str, str], ...] = ()
    received_at: AwareDatetime
    response: dict


class Transcript(Contract):
    started_at: AwareDatetime
    exchanges: tuple[Exchange, ...]


class ReplayTransport:
    def __init__(self, transcript: Transcript):
        self.transcript = Transcript.model_validate(transcript.model_dump())
        self.index = 0
        self.now = transcript.started_at

    def get(self, request: RequestPlan) -> dict:
        if self.index >= len(self.transcript.exchanges):
            raise CollectionError("replay_exhausted")
        exchange = self.transcript.exchanges[self.index]
        if request != RequestPlan(exchange.method, exchange.path, query=exchange.query):
            raise CollectionError("replay_request_mismatch")
        self.index += 1
        self.now = exchange.received_at
        return exchange.response


def replay(transcript: Transcript):
    transport = ReplayTransport(transcript)
    report = AccountReader(transport, clock=lambda: transport.now).collect_account()
    if transport.index != len(transcript.exchanges):
        raise CollectionError("unused_replay_exchanges")
    return report


def demo_transcript(now: datetime) -> Transcript:
    """Synthetic partial-open position, outstanding order, and signed P&L."""
    assets = [
        {
            "balance": "999998",
            "equity": "999958",
            "availableAmount": "993958",
            "margin": "6000",
            "estimatedTradeFee": "3",
            "positionLossGain": "-40",
            "totalSwap": "0",
            "transferableAmount": "993958",
        }
    ]
    position = {
        "positionId": 401,
        "symbol": "USD_JPY",
        "side": "BUY",
        "size": "400",
        "orderedSize": "0",
        "price": "150",
        "lossGain": "-40",
        "totalSwap": "0",
        "timestamp": now.isoformat(),
    }
    order = {
        "rootOrderId": 201,
        "orderId": 201,
        "clientOrderId": "DemoOpen",
        "symbol": "USD_JPY",
        "side": "BUY",
        "orderType": "NORMAL",
        "executionType": "LIMIT",
        "settleType": "OPEN",
        "size": "1000",
        "price": "150",
        "status": "ORDERED",
        "timestamp": now.isoformat(),
    }
    exchanges = []
    for _ in range(2):
        for path, query, data in (
            (ASSETS, (), assets),
            (POSITIONS, (("count", "100"),), {"list": [position]}),
            (POSITIONS, (("count", "100"), ("prevId", "401")), {"list": []}),
            (ORDERS, (("count", "100"),), {"list": [order]}),
            (ORDERS, (("count", "100"), ("prevId", "201")), {"list": []}),
            (ASSETS, (), assets),
        ):
            exchanges.append(
                Exchange(
                    method="GET",
                    path=path,
                    query=query,
                    received_at=now,
                    response={"status": 0, "data": data, "responsetime": now.isoformat()},
                )
            )
    return Transcript(started_at=now, exchanges=tuple(exchanges))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo", help="write synthetic transcript and report")
    demo.add_argument("--directory", required=True, type=Path)
    replay_command = commands.add_parser("replay", help="replay local JSON; print diagnostics")
    replay_command.add_argument("--input", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "demo":
            transcript = demo_transcript(datetime.now(UTC))
            report = replay(transcript)
            # Existing directories, including paper accounts, are never overwritten.
            args.directory.mkdir(parents=True, exist_ok=False)
            (args.directory / "transcript.json").write_text(
                transcript.model_dump_json(indent=2), encoding="utf-8"
            )
            (args.directory / "report.json").write_text(
                report.model_dump_json(indent=2), encoding="utf-8"
            )
            print(
                json.dumps(
                    {
                        "synthetic_only": True,
                        "live_enabled": False,
                        "requests": len(report.observations),
                        "positions": len(report.positions),
                        "active_orders": len(report.active_orders),
                        "blockers": report.blockers,
                    }
                )
            )
        else:
            # Bounded input. Never reads an env file, key store, or network address.
            with args.input.open("rb") as stream:
                payload = stream.read(2_000_001)
            if len(payload) > 2_000_000:
                raise CollectionError("replay_file_too_large")
            transcript = Transcript.model_validate_json(payload)
            print(replay(transcript).model_dump_json(indent=2))
    except (ValueError, OSError):
        # Validation exceptions can include raw financial data; keep stderr generic.
        parser.exit(2, "Account replay failed; check input, timing and unused output directory.\n")


if __name__ == "__main__":
    main()
