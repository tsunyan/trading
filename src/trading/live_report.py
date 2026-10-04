"""Read-only profit and risk report of the dedicated live journal. No credentials or HTTP.

Figures come from the journal's own reconciled evidence: the last account proof, the
saved order evidence and the ACCOUNT_RECONCILED history. Nothing here is broker-verified
beyond what those records already claim, and nothing here changes the journal.
"""

import argparse
import csv
import json
from decimal import Decimal
from pathlib import Path

from trading.account_guard import AccountPolicy, AccountSnapshot
from trading.broker_contracts import OrderEvidence, OrderIntent
from trading.private_order_recovery import PrivateOrderRecovery


def _money(value):
    return format(Decimal(value).normalize(), "f")


def read_dispatches(path):
    """client_id -> reviewed quote from an order_runtime dispatch log; bad lines are skipped."""
    quotes = {}
    if path is None or not Path(path).is_file():
        return quotes
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        try:
            item = json.loads(line)
            if item.get("operation") == "submit" and item.get("quote"):
                quotes[item["client_id"]] = item["quote"]
        except (ValueError, AttributeError, KeyError):
            continue
    return quotes


def _trade_stats(outcomes):
    """Closing-order outcomes net of their own fees and swaps; opening fees are excluded."""
    wins = [o for o in outcomes if o > 0]
    losses = [o for o in outcomes if o < 0]
    gross_loss = -sum(losses, Decimal(0))
    return {
        "count": len(outcomes),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": _money(Decimal(len(wins)) / len(outcomes)) if outcomes else None,
        "profit_factor": (
            _money((sum(wins, Decimal(0)) / gross_loss).quantize(Decimal("0.0001")))
            if gross_loss
            else None
        ),
    }


def report(journal, *, history=24, dispatches=None):
    if type(history) is not int or not 0 <= history <= 1000:
        raise ValueError("invalid_history_length")
    with journal._transaction() as conn:
        gate = journal._gate(conn)
        rows = [dict(r) for r in conn.execute("SELECT * FROM orders ORDER BY client_id")]
        events = [
            json.loads(r[0])
            for r in conn.execute(
                "SELECT payload_json FROM events WHERE kind='ACCOUNT_RECONCILED' "
                "ORDER BY id DESC LIMIT ?",
                (history,),
            )
        ]
    policy = AccountPolicy.model_validate_json(gate["policy_json"])
    peak = Decimal(gate["peak"])
    orders, realized, fees, swaps = [], Decimal(0), Decimal(0), Decimal(0)
    cost, measured = Decimal(0), 0
    outcomes = []
    dispatches = dispatches or {}
    for row in rows:
        intent = OrderIntent.model_validate_json(row["intent_json"])
        item = {
            "client_id": row["client_id"],
            "state": row["state"],
            "side": intent.side,
            "effect": intent.effect,
            "units": intent.units,
            "filled_units": 0,
        }
        if row["evidence_json"]:
            evidence = OrderEvidence.model_validate_json(row["evidence_json"])
            fills = evidence.executions
            filled = sum(e.units for e in fills)
            item.update(
                broker_status=evidence.status,
                executions_complete=evidence.executions_complete,
                filled_units=filled,
                average_price=(
                    _money(sum(e.price * e.units for e in fills) / filled) if filled else None
                ),
                fee=_money(sum((e.fee for e in fills), Decimal(0))),
                realized=_money(sum((e.loss_gain for e in fills), Decimal(0))),
                settled_swap=_money(sum((e.settled_swap for e in fills), Decimal(0))),
            )
            sent = dispatches.get(row["client_id"])
            if filled and sent:
                # Per unit against the reviewed quote; positive means worse than that quote.
                average = sum(e.price * e.units for e in fills) / filled
                slip = (
                    average - Decimal(sent["ask"])
                    if intent.side == "BUY"
                    else Decimal(sent["bid"]) - average
                )
                item["slippage"] = _money(slip)
                cost += slip * filled
                measured += 1
            if intent.effect == "CLOSE":
                # One closing order is one realized trade outcome, net of its own costs.
                outcomes.append(
                    sum((e.loss_gain + e.settled_swap - e.fee for e in fills), Decimal(0))
                )
            realized += sum((e.loss_gain for e in fills), Decimal(0))
            fees += sum((e.fee for e in fills), Decimal(0))
            swaps += sum((e.settled_swap for e in fills), Decimal(0))
        orders.append(item)
    account = None
    if gate["proof_json"]:
        snapshot = AccountSnapshot.model_validate(json.loads(gate["proof_json"])["snapshot"])
        loss = policy.starting_balance - snapshot.equity
        drawdown = (peak - snapshot.equity) / peak if peak > 0 else Decimal(0)
        account = {
            "observed_at": snapshot.model_dump(mode="json")["observed_at"],
            "balance": _money(snapshot.balance),
            "equity": _money(snapshot.equity),
            "unrealized_swap": _money(snapshot.unrealized_swap),
            "required_margin": _money(snapshot.required_margin),
            "available_margin": _money(snapshot.available_margin),
            "positions": [p.model_dump(mode="json") for p in snapshot.positions],
            "loss_from_start": _money(loss),
            "loss_limit_remaining": _money(policy.max_loss_jpy - loss),
            "drawdown_from_peak": _money(drawdown.quantize(Decimal("0.000001"))),
            "drawdown_limit": _money(policy.max_drawdown),
        }
    return {
        "starting_balance": _money(policy.starting_balance),
        "peak_equity": _money(peak),
        "entry_halted": bool(gate["entry_halted"]),
        "account": account,
        "totals": {
            "realized": _money(realized),
            "fees": _money(fees),
            "settled_swap": _money(swaps),
            "net": _money(realized + swaps - fees),
        },
        "closed_trades": _trade_stats(outcomes),
        "orders": orders,
        "execution": {"orders_measured": measured, "slippage_cost": _money(cost)},
        "equity_history": [
            {
                "observed_at": event["snapshot"]["observed_at"],
                "equity": _money(event["snapshot"]["equity"]),
            }
            for event in reversed(events)
        ],
        "broker_verified": False,
    }


CSV_COLUMNS = (
    "timestamp",
    "client_id",
    "order_id",
    "execution_id",
    "position_id",
    "side",
    "effect",
    "units",
    "price",
    "fee",
    "loss_gain",
    "settled_swap",
)


def executions(journal):
    """Every reconciled execution, oldest first, for comparison with the broker history."""
    with journal._transaction() as conn:
        rows = [
            dict(r) for r in conn.execute("SELECT * FROM orders WHERE evidence_json IS NOT NULL")
        ]
    items = []
    for row in rows:
        evidence = OrderEvidence.model_validate_json(row["evidence_json"])
        for fill in evidence.executions:
            items.append(
                {
                    "timestamp": fill.timestamp.isoformat(),
                    "client_id": row["client_id"],
                    "order_id": evidence.order_id,
                    "execution_id": fill.execution_id,
                    "position_id": fill.position_id,
                    "side": evidence.intent.side,
                    "effect": evidence.intent.effect,
                    "units": fill.units,
                    "price": _money(fill.price),
                    "fee": _money(fill.fee),
                    "loss_gain": _money(fill.loss_gain),
                    "settled_swap": _money(fill.settled_swap),
                }
            )
    return sorted(items, key=lambda item: (item["timestamp"], item["execution_id"]))


def write_csv(items, path):
    with Path(path).open("x", encoding="utf-8", newline="") as output:  # Never overwrite.
        writer = csv.DictWriter(output, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(items)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--read-control-directory", type=Path, required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--history", type=int, default=24)
    parser.add_argument("--dispatch-log", type=Path)
    parser.add_argument("--executions-csv", type=Path)
    args = parser.parse_args(argv)
    try:
        journal = PrivateOrderRecovery(
            args.directory, args.read_control_directory, args.scope
        ).journal
        result = report(
            journal, history=args.history, dispatches=read_dispatches(args.dispatch_log)
        )
        if args.executions_csv is not None:
            items = executions(journal)
            write_csv(items, args.executions_csv)
            result["executions_csv"] = {"path": str(args.executions_csv), "rows": len(items)}
    except Exception as error:
        parser.exit(2, f"live_report_failed: {type(error).__name__}\n")
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
