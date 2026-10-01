"""Offline declared valuation model, differences and freshness demo."""

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from trading.account_read_lab import Transcript, demo_transcript, replay
from trading.account_valuation import ValuationPolicy, ValuationQuote
from trading.execution_cash_book import ExecutionCashBook, OpeningCash
from trading.execution_positions import OpeningPosition, PositionBasis


def synthetic_account(now, *, equity="999960"):
    data = demo_transcript(now).model_dump(mode="json")
    exchanges = []
    for exchange in data["exchanges"]:
        if exchange["path"] == "/v1/account/assets":
            exchange["response"]["data"][0].update(
                balance="1000000",
                equity=equity,
                margin="2402",
                availableAmount="997558",
                positionLossGain="-40",
                totalSwap="0",
                estimatedTradeFee="0",
            )
        elif exchange["path"] == "/v1/activeOrders":
            if len(exchange["query"]) > 1:
                continue
            exchange["response"]["data"]["list"] = []
        exchanges.append(exchange)
    data["exchanges"] = exchanges
    return replay(Transcript.model_validate(data))


def synthetic_policy():
    return ValuationPolicy(
        margin_rate="0.04",
        margin_quantum_jpy="1",
        margin_rounding="CEILING",
        margin_rounding_scope="ACCOUNT",
        include_reported_swap=False,
        subtract_reported_estimated_fee=False,
        tolerance_jpy="0",
    )


def demo(directory: Path):
    now = datetime(2026, 10, 2, tzinfo=UTC)
    book = ExecutionCashBook.create(
        directory,
        "synthetic-valuation",
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
    quote = ValuationQuote(bid="149.9", ask="150.1", observed_at=now)
    policy = synthetic_policy()
    matched = book.compare_valuation(synthetic_account(now), quote, policy, evaluated_at=now)
    reopened = ExecutionCashBook(directory, "synthetic-valuation")
    restart = reopened.compare_valuation(synthetic_account(now), quote, policy, evaluated_at=now)
    mismatch = book.compare_valuation(
        synthetic_account(now, equity="999950"), quote, policy, evaluated_at=now
    )
    result = {
        "synthetic_only": True,
        "complete": False,
        "live_enabled": False,
        "matched": matched,
        "restart": restart,
        "equity_difference": mismatch,
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
