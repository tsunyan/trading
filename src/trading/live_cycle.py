"""One operator cycle up to, never including, the order POST.

Reconcile accepted orders, refresh the account proof, take one public quote, ask the
strategy for a proposal and optionally prepare it, then print the reviewed execution
context. Sending stays a separate `order_runtime submit` with that context's SHA-256.
"""

import argparse
import hashlib
import json
import os
import re
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from trading.config import load_settings
from trading.live_account import ACCOUNT_CONFIRMATIONS, LiveAccountError, LiveAccountRefresh
from trading.live_attestation import LiveAttestationError
from trading.live_attestation import load as load_attestation
from trading.live_attestation import require as require_attestation
from trading.live_order_sync import (
    ACCEPTED,
    HISTORY_CONFIRMATIONS,
    LiveOrderSync,
    LiveOrderSyncError,
)
from trading.live_quote import LiveQuoteError, fetch_quote, fetch_status, write_quote
from trading.live_signal import (
    LiveSignalError,
    clear_intent,
    decide,
    entry_halted,
    journal_state,
    recent_bars,
    resolve_units,
    write_intent,
)
from trading.order_journal import OrderBlocked
from trading.promotion import PromotionError, require_live
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
        candidate=None,
        service_status=None,
    ):
        if not isinstance(confirmations, (set, frozenset, tuple, list)) or set(
            confirmations
        ) != set(CYCLE_CONFIRMATIONS):
            raise LiveCycleError("cycle_confirmations_required")
        if type(prepare) is not bool:
            raise LiveCycleError("invalid_prepare_option")
        if not flatten:
            # Only the frozen candidate promoted to live may propose; flattening stays available.
            if candidate is None:
                raise LiveCycleError("promoted_candidate_required")
            ledger, hypothesis = candidate
            require_live(ledger, hypothesis, cfg)
        if intent_output is not None:
            clear_intent(intent_output)  # Only this run's intent, if any, may be left.
        result = {"reconciled_orders": [], "orders_sent": False}
        if service_status is None and quote is None:
            # Live mode: during broker maintenance every private GET fails; skip quietly.
            service_status = fetch_status(transport=quote_transport)
        if service_status == "MAINTENANCE":
            result.update(
                decision={"action": "hold", "reason": "broker_maintenance", "intent": None},
                prepared=False,
                service_status="MAINTENANCE",
            )
            return result
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
            units=resolve_units(units, cfg, self.journal, quote, limits),
            max_slippage=max_slippage,
            limits=limits,
            now=now,
            flatten=flatten,
            entry_halted=entry_halted(self.journal),
        )
        intent = decision["intent"]
        result["decision"] = {
            **decision,
            "intent": None if intent is None else intent.model_dump(mode="json"),
        }
        result["prepared"] = False
        result["waiting_prepared"] = sorted(
            row["client_id"]
            for row in self.journal.snapshot()["orders"]
            if row["state"] == "PREPARED"
        )
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
        try:
            context = self.journal.execution_context(intent.client_id, quote=quote)
        except Exception as error:
            # Prepared in this run and never claimed: abandon it so it cannot block later
            # proposals. A fixed live code (e.g. a risk refusal) is reported as is.
            self.journal.abandon(intent.client_id)
            code = str(error)
            if not isinstance(error, OrderBlocked) or not re.fullmatch(r"[a-z0-9_]{1,64}", code):
                code = "context_refused"
            raise LiveCycleError(f"prepared_order_abandoned:{code}") from None
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


def append_history(result, path):
    """One JSON line per run; existing lines are never rewritten."""
    line = json.dumps(result, default=str, ensure_ascii=False, separators=(",", ":"))
    with Path(path).open("a", encoding="utf-8", newline="\n") as output:
        output.write(line + "\n")
        output.flush()
        os.fsync(output.fileno())


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


APPROVAL_NOTICE_SECONDS = 24 * 3600


def attestation_expiry(path):
    if path is None:
        return None
    try:
        return load_attestation(path).expires_at
    except LiveAttestationError:
        return None


def approval_expiry(cycle):
    if cycle is None:
        return None
    try:
        approval = cycle.journal.snapshot()["live_control"]["approval"]
        return datetime.fromisoformat(approval["expires_at"]) if approval else None
    except Exception:
        return None


def proposal_key(intent, current):
    """Identity of a standing proposal: every order term except the per-bar client ID and
    the limit/bound price, which follows the quote every hour and is re-reviewed at send."""
    terms = {k: v for k, v in intent.items() if k not in {"client_id", "price", "bound"}}
    body = json.dumps({"intent": terms, "current": current}, sort_keys=True, default=str)
    return hashlib.sha256(body.encode()).hexdigest()


def read_result(path):
    if path is None:
        return {}
    try:
        with Path(path).open("rb") as handle:
            data = json.loads(handle.read(1_000_000))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _candidate(args):
    if (args.ledger is None) != (args.hypothesis is None):
        raise LiveCycleError("ledger_and_hypothesis_required_together")
    return None if args.ledger is None else (args.ledger, args.hypothesis)


def main(argv=None, *, send=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--read-control-directory", type=Path, required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--credential-reference", required=True)
    parser.add_argument("--units", required=True, help="lot size, or auto")
    parser.add_argument("--max-slippage", required=True)
    parser.add_argument("--quote-output", type=Path, required=True)
    parser.add_argument("--intent-output", type=Path)
    parser.add_argument("--result-output", type=Path)
    parser.add_argument("--history-output", type=Path)
    parser.add_argument("--dashboard-output", type=Path)
    parser.add_argument("--prepare", action="store_true")
    parser.add_argument("--flatten", action="store_true")
    parser.add_argument("--notify", action="store_true")
    parser.add_argument("--valuation-tolerance")
    parser.add_argument("--ledger", type=Path)
    parser.add_argument("--hypothesis")
    parser.add_argument("--confirm", action="append", default=[])
    parser.add_argument("--attestation", type=Path)
    args = parser.parse_args(argv)
    finished = lambda: datetime.now(UTC).isoformat()  # noqa: E731
    cycle = None
    try:
        cycle = LiveCycle(args.directory, args.read_control_directory, args.scope)
        confirmations = args.confirm
        if args.attestation is not None:
            # Unattended runs: the operator's expiring statement replaces --confirm.
            if args.confirm:
                raise LiveCycleError("confirm_or_attestation_not_both")
            confirmations = require_attestation(
                args.attestation, cycle.journal, required=CYCLE_CONFIRMATIONS, now=datetime.now(UTC)
            ).confirmations
        result = cycle.run(
            args.credential_reference,
            confirmations=confirmations,
            cfg=load_settings(args.config),
            units=args.units,
            max_slippage=args.max_slippage,
            prepare=args.prepare,
            flatten=args.flatten,
            valuation_tolerance=args.valuation_tolerance,
            candidate=_candidate(args),
            quote_output=args.quote_output,
            intent_output=args.intent_output,
        )
        result = {**result, "ok": True, "finished_at": finished()}
        if result.get("prepared"):
            # Ready to review and paste; only the order key reference is left to the operator.
            result["submit_command"] = subprocess.list2cmdline(
                [
                    "uv",
                    "run",
                    "python",
                    "-m",
                    "trading.order_runtime",
                    "submit",
                    "--directory",
                    str(args.directory),
                    "--read-control-directory",
                    str(args.read_control_directory),
                    "--scope",
                    args.scope,
                    "--client-id",
                    result["client_id"],
                    "--quote",
                    str(args.quote_output),
                    "--expected-sha256",
                    result["checkpoint_sha256"],
                    "--credential-reference",
                    "<order_reference>",
                    "--order-permission-confirmed",
                    "--dispatch-log",
                    str(Path(args.quote_output).with_name("dispatch.jsonl")),
                ]
            )
    except Exception as error:
        fixed = (
            LiveCycleError,
            LiveAccountError,
            LiveOrderSyncError,
            LiveSignalError,
            LiveQuoteError,
            PromotionError,
            LiveAttestationError,
        )
        reason = str(error) if isinstance(error, fixed) else type(error).__name__
        result = {"ok": False, "reason": reason, "orders_sent": False, "finished_at": finished()}
    expiry = approval_expiry(cycle)
    if expiry is not None:
        result["approval_expires_at"] = expiry.isoformat()
    # The result file is rewritten every run; carry the expiry notice marker forward.
    previous = read_result(args.result_output)
    if expiry is not None and previous.get("approval_notice_for") == expiry.isoformat():
        result["approval_notice_for"] = previous["approval_notice_for"]
    attested = attestation_expiry(args.attestation)
    if attested is not None:
        result["attestation_expires_at"] = attested.isoformat()
        if previous.get("attestation_notice_for") == attested.isoformat():
            result["attestation_notice_for"] = previous["attestation_notice_for"]
    if args.notify:
        remaining = (attested - datetime.now(UTC)).total_seconds() if attested else None
        if (
            remaining is not None
            and 0 < remaining < APPROVAL_NOTICE_SECONDS
            and "attestation_notice_for" not in result
        ):
            hours = f"{int(remaining // 3600)}h left"
            if notify("live_cycle_attestation_expiring", hours, send=send):
                result["attestation_notice_for"] = attested.isoformat()
        left = (expiry - datetime.now(UTC)).total_seconds() if expiry is not None else None
        if (
            left is not None
            and 0 < left < APPROVAL_NOTICE_SECONDS
            and "approval_notice_for" not in result
        ):
            hours = f"{int(left // 3600)}h left"
            if notify("live_cycle_approval_expiring", hours, send=send):
                result["approval_notice_for"] = expiry.isoformat()
        decision = result.get("decision") or {}
        # Each order reaches a final state once, so these notices never repeat.
        for order in result.get("reconciled_orders") or []:
            if order["state"] in {"FILLED", "CANCELED", "EXPIRED"}:
                notify(
                    "live_cycle_order_settled", f"{order['client_id']} {order['state']}", send=send
                )
        waiting = result.get("waiting_prepared") or []
        if waiting and previous.get("prepared_notice_for") == waiting[0]:
            result["prepared_notice_for"] = waiting[0]
        elif waiting and notify("live_cycle_prepared_waiting", waiting[0], send=send):
            # An unsent prepared order holds every later proposal; say so once per order.
            result["prepared_notice_for"] = waiting[0]
        if not result["ok"]:
            # A persisting failure is announced once, again only when its reason changes.
            if (
                previous.get("ok") is False
                and previous.get("reason") == result["reason"]
                and previous.get("notified") is True
            ):
                result["notified"] = True
            else:
                result["notified"] = notify("live_cycle_failed", result["reason"], send=send)
        elif decision.get("action") in {"open", "close"}:
            intent = decision.get("intent") or {}
            reference = " ".join(
                str(intent[key])
                for key in ("client_id", "side", "effect", "units")
                if intent.get(key) is not None
            )
            key = proposal_key(intent, decision.get("current"))
            if previous.get("proposal_notice_for") == key:
                result["proposal_notice_for"] = key
            elif notify("live_cycle_proposal", reference or "proposal", send=send):
                result["notified"] = True
                result["proposal_notice_for"] = key
            else:
                result["notified"] = False
    if args.history_output is not None:
        try:
            append_history(result, args.history_output)
        except OSError:
            result["history_written"] = False
    if args.dashboard_output is not None and cycle is not None:
        try:
            from trading.live_dashboard import read_history, render, write_page
            from trading.live_doctor import diagnose
            from trading.live_report import read_dispatches, report

            now = datetime.now(UTC)
            candidate = _candidate(args)
            doctor = diagnose(
                cycle.journal,
                now,
                candidate=None if candidate is None else (*candidate, load_settings(args.config)),
                cycle=result,
            )
            # The submit command writes its dispatch log next to the quote file.
            dispatches = read_dispatches(Path(args.quote_output).with_name("dispatch.jsonl"))
            page = render(
                doctor,
                report(cycle.journal, dispatches=dispatches),
                result,
                generated_at=now.isoformat(),
                history=read_history(args.history_output),
            )
            write_page(page, args.dashboard_output)
        except Exception:
            result["dashboard_written"] = False  # A page never changes the cycle outcome.
    # Written last, so the canonical result records every output that failed above.
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
