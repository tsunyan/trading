"""One operator cycle up to, never including, the order POST.

Reconcile accepted orders, refresh the account proof, take one public quote, ask the
strategy for a proposal and optionally prepare it, then print the reviewed execution
context. Sending stays a separate `order_runtime submit` with that context's SHA-256.
"""

import argparse
import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from trading.config import load_settings
from trading.live_account import ACCOUNT_CONFIRMATIONS, LiveAccountError, LiveAccountRefresh
from trading.live_order_sync import (
    ACCEPTED,
    HISTORY_CONFIRMATIONS,
    LiveOrderSync,
    LiveOrderSyncError,
)
from trading.live_quote import LiveQuoteError, fetch_quote, write_quote
from trading.live_signal import (
    LiveSignalError,
    decide,
    journal_state,
    recent_bars,
    write_intent,
)
from trading.windows_notify import send_toast

CYCLE_CONFIRMATIONS = ACCOUNT_CONFIRMATIONS | HISTORY_CONFIRMATIONS


class LiveCycleError(ValueError):
    """Fixed local reasons only."""


class LiveCycle:
    def __init__(self, directory, read_control_directory, scope, **options):
        self.stores = (directory, read_control_directory, scope)
        self.options = options
        self.sync = LiveOrderSync(*self.stores, **options)
        self.journal, self.clock = self.sync.journal, self.sync.clock

    def run(
        self,
        credential_reference,
        *,
        confirmations,
        cfg,
        units,
        max_slippage,
        prepare=False,
        flatten=False,
        bars=None,
        quote=None,
        vault=None,
        transport=None,
        quote_transport=None,
        quote_output=None,
        intent_output=None,
        valuation_tolerance=None,
    ):
        if not isinstance(confirmations, (set, frozenset, tuple, list)) or set(
            confirmations
        ) != set(CYCLE_CONFIRMATIONS):
            raise LiveCycleError("cycle_confirmations_required")
        if type(prepare) is not bool:
            raise LiveCycleError("invalid_prepare_option")
        result = {"reconciled_orders": [], "orders_sent": False}
        rows = self.journal.snapshot()["orders"]
        for row in rows:
            if row["state"] in ACCEPTED:
                synced = self.sync.reconcile(
                    row["client_id"],
                    credential_reference=credential_reference,
                    confirmations=HISTORY_CONFIRMATIONS,
                    vault=vault,
                    transport=transport,
                )
                result["reconciled_orders"].append(
                    {"client_id": synced["client_id"], "state": synced["state"]}
                )
        if quote is None:
            quote = fetch_quote(transport=quote_transport, clock=self.clock)
        if quote_output is not None:
            write_quote(quote, quote_output)
        account = LiveAccountRefresh(*self.stores, **self.options).refresh(
            credential_reference,
            confirmations=ACCOUNT_CONFIRMATIONS,
            quote=quote,
            vault=vault,
            transport=transport,
            valuation_tolerance=valuation_tolerance,
        )
        result["account"] = {
            "observed_at": account["observed_at"],
            "positions": account["positions"],
            "working_orders": account["working_orders"],
            "entry_halted": account["entry_halted"],
            "valuation_adjusted": account["valuation_adjusted"],
        }
        now = self.clock()
        positions, pending, limits = journal_state(self.journal, now)
        decision = decide(
            None if flatten else bars if bars is not None else recent_bars(cfg, now),
            quote,
            cfg,
            positions=positions,
            pending=pending,
            units=units,
            max_slippage=max_slippage,
            limits=limits,
            now=now,
            flatten=flatten,
        )
        intent = decision["intent"]
        result["decision"] = {
            **decision,
            "intent": None if intent is None else intent.model_dump(mode="json"),
        }
        result["prepared"] = False
        if intent is None:
            return result
        if intent_output is not None:
            write_intent(intent, intent_output)
        if not prepare:
            return result
        state = self.journal.prepare(intent)
        if state != "PREPARED":
            # The same signal bar and direction was already used by an earlier order.
            raise LiveCycleError("signal_client_id_already_used")
        context = self.journal.execution_context(intent.client_id, quote=quote)
        result.update(
            prepared=True,
            client_id=intent.client_id,
            checkpoint_sha256=context["checkpoint_sha256"],
            request={"path": context["path"], "body": context["body"]},
            risk=context["risk"],
        )
        return result


def notify(kind, reference, *, send=None):
    """Best effort desktop notice; a failed toast never changes the cycle outcome."""
    try:
        (send or send_toast)({"kind": kind, "id": reference}, "live-cycle")
        return True
    except Exception:
        return False


def write_result(result, path):
    path = Path(path)
    handle, temporary = tempfile.mkstemp(dir=path.parent, prefix=".cycle-", suffix=".tmp")
    try:
        with os.fdopen(handle, "wb") as output:
            output.write(json.dumps(result, default=str, ensure_ascii=False).encode())
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def main(argv=None, *, send=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--read-control-directory", type=Path, required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--credential-reference", required=True)
    parser.add_argument("--units", type=int, required=True)
    parser.add_argument("--max-slippage", required=True)
    parser.add_argument("--quote-output", type=Path, required=True)
    parser.add_argument("--intent-output", type=Path)
    parser.add_argument("--result-output", type=Path)
    parser.add_argument("--prepare", action="store_true")
    parser.add_argument("--flatten", action="store_true")
    parser.add_argument("--notify", action="store_true")
    parser.add_argument("--valuation-tolerance")
    parser.add_argument("--confirm", action="append", default=[])
    args = parser.parse_args(argv)
    finished = lambda: datetime.now(UTC).isoformat()  # noqa: E731
    try:
        cycle = LiveCycle(args.directory, args.read_control_directory, args.scope)
        result = cycle.run(
            args.credential_reference,
            confirmations=args.confirm,
            cfg=load_settings(args.config),
            units=args.units,
            max_slippage=args.max_slippage,
            prepare=args.prepare,
            flatten=args.flatten,
            valuation_tolerance=args.valuation_tolerance,
            quote_output=args.quote_output,
            intent_output=args.intent_output,
        )
        result = {**result, "ok": True, "finished_at": finished()}
    except Exception as error:
        fixed = (
            LiveCycleError,
            LiveAccountError,
            LiveOrderSyncError,
            LiveSignalError,
            LiveQuoteError,
        )
        reason = str(error) if isinstance(error, fixed) else type(error).__name__
        result = {"ok": False, "reason": reason, "orders_sent": False, "finished_at": finished()}
    if args.notify:
        decision = result.get("decision") or {}
        if not result["ok"]:
            result["notified"] = notify("live_cycle_failed", result["reason"][:16], send=send)
        elif decision.get("action") in {"open", "close"}:
            reference = (decision.get("intent") or {}).get("client_id", "proposal")
            result["notified"] = notify("live_cycle_proposal", reference[-16:], send=send)
    if args.result_output is not None:
        try:
            write_result(result, args.result_output)
        except OSError:
            result["result_written"] = False
    if not result["ok"]:
        parser.exit(2, f"live_cycle_failed: {result['reason']}\n")
    print(json.dumps(result, default=str))


if __name__ == "__main__":
    main()
