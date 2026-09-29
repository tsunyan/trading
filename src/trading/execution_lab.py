"""Scripted offline failure rehearsal. No broker connection or live CLI flag."""

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from trading.account_guard import AccountPolicy, AccountQuote, AccountSnapshot, Position
from trading.broker_contracts import OrderIntent, OrderLimits, Settlement, parse_evidence
from trading.order_journal import OrderBlocked, OrderJournal


def fixture_evidence(intent, root_id, order_id, status, fills, observed_at):
    """Synthetic broker-shaped data for the demo only, not historical/live evidence."""
    identity = {
        "clientOrderId": intent.client_id,
        "symbol": intent.symbol,
        "side": intent.side,
        "settleType": intent.effect,
    }
    order = {
        **identity,
        "rootOrderId": root_id,
        "orderId": order_id,
        "orderType": "NORMAL",
        "executionType": intent.kind,
        "size": str(intent.units),
        "status": status,
    }
    if intent.price is not None:
        order["price"] = str(intent.price)
    executions = [{**identity, "orderId": order_id, **fill} for fill in fills]
    return parse_evidence(
        intent,
        {"status": 0, "data": [order]},
        {"status": 0, "data": executions},
        observed_at,
        executions_complete=True,
    )


def demo(directory: Path) -> dict:
    limits = OrderLimits(
        min_units=100,
        max_units=1000,
        unit_step=100,
        price_tick="0.001",
        max_reference_notional="200000",
    )
    journal = OrderJournal.create(directory, limits)
    now = datetime.now(UTC)
    intent = OrderIntent(
        client_id="DemoOpen001",
        side="BUY",
        effect="OPEN",
        units=1000,
        kind="LIMIT",
        price="150.000",
    )
    journal.prepare(intent)
    journal.begin_submission(intent.client_id)
    journal.unknown(intent.client_id)  # Simulated lost response; NO request was sent.
    journal = OrderJournal(directory)  # Reopen the persisted DB as after a restart.
    try:
        journal.begin_submission(intent.client_id)
    except OrderBlocked:
        resend_blocked = True
    else:
        raise AssertionError("unsafe duplicate submission")
    fill1 = {
        "executionId": 301,
        "positionId": 401,
        "size": "500",
        "price": "150.000",
        "fee": "1.5",
        "lossGain": "0",
        "settledSwap": "0",
        "timestamp": now.isoformat(),
    }
    journal.reconcile(fixture_evidence(intent, 101, 201, "ORDERED", [fill1], now))
    journal.begin_cancel(intent.client_id)
    journal.cancellation_response(
        intent.client_id,
        {
            "status": 0,
            "data": {
                "success": [
                    {
                        "rootOrderId": 101,
                        "clientOrderId": intent.client_id,
                    }
                ]
            },
        },
    )
    fill2 = {**fill1, "executionId": 302}
    journal.reconcile(
        fixture_evidence(
            intent,
            101,
            201,
            "EXECUTED",
            [fill1, fill2],
            now + timedelta(seconds=1),
        )
    )  # Fill raced with cancellation; do NOT report it as canceled.
    close = OrderIntent(
        client_id="DemoClose001",
        side="SELL",
        effect="CLOSE",
        units=1000,
        kind="MARKET",
        bound="149.900",
        positions=(Settlement(position_id=401, units=1000),),
    )
    journal.prepare(close)
    journal.begin_submission(close.client_id)
    closed_fill = {
        **fill1,
        "executionId": 303,
        "size": "1000",
        "price": "150.100",
        "fee": "3",
        "lossGain": "100",
    }
    journal.reconcile(
        fixture_evidence(
            close,
            102,
            202,
            "EXECUTED",
            [closed_fill],
            now + timedelta(seconds=2),
        )
    )
    report = journal.snapshot()
    report["synthetic_only"] = True
    report["resend_blocked_after_restart"] = resend_blocked
    report["scenario"] = "lost response -> restart -> partial fill -> cancel race -> fill -> close"
    (directory / "report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return report


def guard_demo(directory: Path) -> dict:
    """Synthetic account risk rehearsal: loss blocks entries but permits a close."""
    now = datetime.now(UTC)
    policy = AccountPolicy(
        account_id="synthetic-account",
        bootstrap_at=now,
        starting_balance="1000000",
        max_order_notional="200000",
        max_gross_notional="400000",
        max_leverage="1",
        max_loss_jpy="100",
        max_drawdown="0.05",
        margin_rate="0.04",
        min_margin_ratio="2",
        min_available_margin="100000",
        fee_buffer_rate="0.00002",
        max_spread="0.05",
    )
    limits = OrderLimits(
        min_units=100,
        max_units=1000,
        unit_step=100,
        price_tick="0.001",
        max_reference_notional="200000",
    )
    journal = OrderJournal.create(directory, limits, account_policy=policy)
    initial = AccountSnapshot(
        account_id=policy.account_id,
        observed_at=now,
        complete=True,
        balance="1000000",
        equity="1000000",
        required_margin="0",
        available_margin="1000000",
    )
    quote = AccountQuote(bid="150", ask="150.01", observed_at=now, market_open=True)
    journal.update_account(initial, quote, now=now)
    order = OrderIntent(
        client_id="GuardOpen", side="BUY", effect="OPEN", units=1000, kind="LIMIT", price="150.01"
    )
    journal.prepare(order)
    journal.begin_submission(order.client_id, quote=quote, now=now)
    fill = {
        "executionId": 301,
        "positionId": 401,
        "size": "1000",
        "price": "150",
        "fee": "3",
        "lossGain": "0",
        "settledSwap": "0",
        "timestamp": now.isoformat(),
    }
    journal.reconcile(fixture_evidence(order, 101, 201, "EXECUTED", [fill], now))
    later = now + timedelta(seconds=1)
    falling = AccountQuote(bid="149.8", ask="149.81", observed_at=later, market_open=True)
    account = AccountSnapshot(
        account_id=policy.account_id,
        observed_at=later,
        complete=True,
        balance="999997",
        equity="999797",
        required_margin="6000",
        available_margin="993797",
        positions=(Position(position_id=401, side="BUY", units=1000, average_price="150"),),
    )
    journal.update_account(account, falling, now=later)
    journal = OrderJournal(directory)
    another = order.model_copy(update={"client_id": "GuardDenied"})
    journal.prepare(another)
    try:
        journal.begin_submission(another.client_id, quote=falling, now=later)
    except OrderBlocked as exc:
        if "entry_loss_halt" not in str(exc):
            raise
    else:
        raise AssertionError("loss gate did not block new entry")
    journal.abandon(another.client_id)
    close = OrderIntent(
        client_id="GuardClose",
        side="SELL",
        effect="CLOSE",
        units=1000,
        kind="MARKET",
        bound="149.79",
        positions=(Settlement(position_id=401, units=1000),),
    )
    journal.prepare(close)
    journal.begin_submission(close.client_id, quote=falling, now=later)
    closed_fill = {
        **fill,
        "executionId": 302,
        "price": "149.8",
        "lossGain": "-200",
        "timestamp": later.isoformat(),
    }
    journal.reconcile(fixture_evidence(close, 102, 202, "EXECUTED", [closed_fill], later))
    after = later + timedelta(seconds=1)
    journal.update_account(
        AccountSnapshot(
            account_id=policy.account_id,
            observed_at=after,
            complete=True,
            balance="999794",
            equity="999794",
            required_margin="0",
            available_margin="999794",
        ),
        AccountQuote(bid="149.8", ask="149.81", observed_at=after, market_open=True),
        now=after,
    )
    report = journal.snapshot()
    report.update(
        synthetic_only=True,
        new_entry_blocked=True,
        reducing_close_allowed=True,
        scenario="reconcile -> open -> loss halt -> restart -> reject entry -> close",
    )
    (directory / "report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("demo", "guard-demo", "status"))
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "demo":
        result = demo(args.directory)
    elif args.command == "guard-demo":
        result = guard_demo(args.directory)
    else:
        result = OrderJournal(args.directory).snapshot()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
