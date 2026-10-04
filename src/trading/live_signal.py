"""Propose one live order intent from the configured strategy. Never prepares or sends.

The signal uses the same completed-bar and freshness rules as forward paper trading.
Holdings come from the live journal's last reconciled account proof, not from the paper
account. Closing a position comes first; a reversal opens on a later run after the close
has been reconciled. The proposal goes through `live_setup prepare` and the reviewed send.
"""

import argparse
import json
import os
import tempfile
from datetime import UTC, datetime, timedelta
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from pathlib import Path

import httpx
import pandas as pd

from trading.account_guard import AccountQuote, AccountSnapshot, fresh
from trading.broker_contracts import OrderIntent, Settlement
from trading.config import Settings, load_settings
from trading.data import validate_bars
from trading.gmo import GmoPublic
from trading.order_runtime import _quote
from trading.private_order_recovery import PrivateOrderRecovery
from trading.strategy import signal_direction

SETTLED = {"FILLED", "CANCELED", "EXPIRED", "ABANDONED"}


class LiveSignalError(ValueError):
    """Fixed local reasons only."""


def _tick(value, tick, rounding):
    return (value / tick).to_integral_value(rounding=rounding) * tick


def decide(
    frame,
    quote,
    cfg,
    *,
    positions,
    pending,
    units,
    max_slippage,
    limits,
    now,
    flatten=False,
    entry_halted=False,
):
    """A hold, open or close decision with its exact intent. Pure; no I/O.

    `flatten` ignores the strategy and bars and proposes closing every held lot.
    """
    if cfg.market != "fx" or cfg.symbol != "USD_JPY":
        raise LiveSignalError("live_signal_supports_usd_jpy_only")
    if not isinstance(quote, AccountQuote) or now.tzinfo is None:
        raise LiveSignalError("invalid_live_signal_inputs")
    max_slippage = Decimal(max_slippage)
    if not max_slippage > 0:
        raise LiveSignalError("positive_slippage_bound_required")
    age = (now - quote.observed_at).total_seconds()
    if not -cfg.max_future_quote_seconds <= age <= cfg.max_quote_age_seconds:
        raise LiveSignalError("stale_or_future_quote")
    if type(flatten) is not bool:
        raise LiveSignalError("invalid_flatten_option")
    if flatten:
        signal_time, target = None, 0
    else:
        frame = validate_bars(frame, cfg)
        boundary = min(pd.Timestamp(now), pd.Timestamp(quote.observed_at))
        completed = frame.loc[frame.timestamp + pd.Timedelta(seconds=cfg.bar_seconds) <= boundary]
        if len(completed) < cfg.warmup_bars:
            raise LiveSignalError("not_enough_completed_bars")
        signal_time = completed.timestamp.iloc[-1] + pd.Timedelta(seconds=cfg.bar_seconds)
        latest = max(pd.Timestamp(now), pd.Timestamp(quote.observed_at))
        if (latest - signal_time).total_seconds() > cfg.max_signal_age_seconds:
            raise LiveSignalError("stale_signal_data")
        target = signal_direction(completed.close.tolist(), cfg)
    held = {side: [p for p in positions if p.side == side] for side in ("BUY", "SELL")}
    if held["BUY"] and held["SELL"]:
        raise LiveSignalError("both_sides_held")
    current = 1 if held["BUY"] else -1 if held["SELL"] else 0
    decision = {
        "signal_time": None if signal_time is None else signal_time.isoformat(),
        "flatten": flatten,
        "target": target,
        "current": current,
        "intent": None,
    }
    if pending:
        return {**decision, "action": "hold", "reason": "unsettled_local_order"}
    if current == target:
        return {**decision, "action": "hold", "reason": "at_target"}
    # One proposal per signal bar (or flatten minute) and direction; repeats are refused.
    prefix, stamp = (
        ("F", pd.Timestamp(now).tz_convert("UTC").strftime("%Y%m%d%H%M"))
        if flatten
        else ("S", signal_time.strftime("%Y%m%d%H"))
    )
    if current:
        lots = held["BUY" if current > 0 else "SELL"]
        if len(lots) > 10:
            raise LiveSignalError("too_many_positions_to_close")
        side = "SELL" if current > 0 else "BUY"
        effect, size = "CLOSE", sum(p.units for p in lots)
        settlements = tuple(Settlement(position_id=p.position_id, units=p.units) for p in lots)
        action = "close"
    else:
        if entry_halted:
            # The loss stop admits closes only; an open would be refused at the send gate.
            return {**decision, "action": "hold", "reason": "entry_loss_halt"}
        if quote.ask - quote.bid > Decimal(str(cfg.max_spread)):
            return {**decision, "action": "hold", "reason": "spread_exceeds_entry_limit"}
        if (
            type(units) is not int
            or not limits.min_units <= units <= limits.max_units
            or units % limits.unit_step
        ):
            raise LiveSignalError("units_outside_order_limits")
        side = "BUY" if target > 0 else "SELL"
        effect, size, settlements, action = "OPEN", units, (), "open"
    bound = (
        _tick(quote.ask + max_slippage, limits.price_tick, ROUND_FLOOR)
        if side == "BUY"
        else _tick(quote.bid - max_slippage, limits.price_tick, ROUND_CEILING)
    )
    if bound <= 0:
        raise LiveSignalError("slippage_exceeds_quote")
    intent = OrderIntent(
        client_id=f"{prefix}{stamp}{effect[0]}{side[0]}",
        side=side,
        effect=effect,
        units=size,
        kind="MARKET",
        bound=bound,
        positions=settlements,
    )
    reason = "flatten_requested" if flatten else "signal_changed"
    return {**decision, "action": action, "reason": reason, "intent": intent}


def entry_halted(journal):
    return bool((journal.snapshot()["account_guard"] or {}).get("entry_halted"))


def journal_state(journal, now):
    """Positions from the last reconciled proof and whether any local order is unsettled."""
    snapshot = journal.snapshot()
    guard = snapshot["account_guard"]
    proof = (guard or {}).get("last_proof")
    if not proof:
        raise LiveSignalError("account_proof_required")
    account = AccountSnapshot.model_validate(proof["snapshot"])
    if not fresh(account.observed_at, now, guard["policy"]["max_snapshot_age_seconds"]):
        raise LiveSignalError("stale_account_proof")
    pending = any(row["state"] not in SETTLED for row in snapshot["orders"])
    return account.positions, pending, journal.limits


def write_intent(intent, path):
    path = Path(path)
    handle, temporary = tempfile.mkstemp(dir=path.parent, prefix=".intent-", suffix=".tmp")
    try:
        with os.fdopen(handle, "wb") as output:
            output.write(intent.model_dump_json().encode())
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def recent_bars(cfg: Settings, now, *, days=10, client=None):
    with client or httpx.Client(follow_redirects=False, trust_env=False) as http:
        end = now.date()
        return GmoPublic(http).candles(cfg, end - timedelta(days=days), end, now)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--read-control-directory", type=Path, required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--quote", type=Path, required=True)
    parser.add_argument("--units", type=int, required=True)
    parser.add_argument("--max-slippage", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--flatten", action="store_true")
    args = parser.parse_args(argv)
    try:
        now = datetime.now(UTC)
        cfg = load_settings(args.config)
        journal = PrivateOrderRecovery(args.directory, args.read_control_directory, args.scope)
        positions, pending, limits = journal_state(journal.journal, now)
        decision = decide(
            None if args.flatten else recent_bars(cfg, now),
            _quote(args.quote),
            cfg,
            positions=positions,
            pending=pending,
            units=args.units,
            max_slippage=args.max_slippage,
            limits=limits,
            now=now,
            flatten=args.flatten,
            entry_halted=entry_halted(journal.journal),
        )
        if decision["intent"] is not None:
            write_intent(decision["intent"], args.output)
        intent = decision["intent"]
        print(
            json.dumps(
                {
                    **decision,
                    "intent": None if intent is None else intent.model_dump(mode="json"),
                    "prepared": False,
                    "orders_sent": False,
                }
            )
        )
    except Exception as error:
        reason = str(error) if isinstance(error, LiveSignalError) else "live_signal_failed"
        parser.exit(2, f"{reason}\n")


if __name__ == "__main__":
    main()
