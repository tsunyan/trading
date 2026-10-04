"""Explicitly promote one stable read-only account collection into the live account gate."""

import json
from decimal import Decimal
from pathlib import Path

from trading.account_guard import (
    AccountPolicy,
    AccountSnapshot,
    Position,
    WorkingOrder,
    marked_equity,
    reconcile_account,
)
from trading.account_reader import AccountReader, AccountReadReport
from trading.broker_contracts import OrderEvidence, OrderIntent
from trading.credential_store import CredentialParser, CredentialVault
from trading.live_quote import fetch_quote
from trading.order_runtime import OrderRuntime, _quote
from trading.swap_check import read_schedule, swap_check

ACCOUNT_CONFIRMATIONS = frozenset(
    {"complete-account", "account-identity", "external-writers-paused"}
)


AWAITING_RECONCILIATION = frozenset({"SUBMITTING", "UNKNOWN", "RECONCILING"})


class LiveAccountError(ValueError):
    """Fixed local reasons only; account values are never echoed."""


class AccountDiscrepancy(LiveAccountError):
    """The broker shows an order the local journal cannot explain. The journal is halted."""


def snapshot_from_report(report, *, account_id, rows):
    """A complete snapshot by explicit operator declaration, never by inference alone.

    The two-sweep report proves traversal and stability, not identity or history; those
    come from the confirmations and the activation acceptance evidence. The guard then
    reconciles balances, positions and working orders against local execution evidence.
    """
    if not isinstance(report, AccountReadReport):
        raise LiveAccountError("account_read_report_required")
    report = AccountReadReport.model_validate(report.model_dump())
    if not report.observations:
        raise LiveAccountError("account_observations_required")
    assets = report.assets
    if sum((p.total_swap for p in report.positions), Decimal(0)) != assets.total_swap:
        raise LiveAccountError("position_swap_total_mismatch")
    known = {row["client_id"]: row for row in rows}
    working = []
    for active in report.active_orders:
        row = known.get(active.client_id)
        if row is not None and row["state"] in AWAITING_RECONCILIATION:
            # Our own sent order, not yet reconciled from its GET evidence: not a discrepancy.
            raise LiveAccountError("local_order_reconciliation_required")
        if (
            row is None
            or row["state"] not in {"WORKING", "PARTIAL", "CANCEL_PENDING"}
            or not row["evidence_json"]
        ):
            raise AccountDiscrepancy("unexplained_active_order")
        intent = OrderIntent.model_validate_json(row["intent_json"])
        evidence = OrderEvidence.model_validate_json(row["evidence_json"])
        if (
            (active.root_order_id, active.order_id) != (evidence.root_order_id, evidence.order_id)
            or (active.symbol, active.side, active.effect, active.kind)
            != (intent.symbol, intent.side, intent.effect, intent.kind)
            or active.units != intent.units
            or active.price != intent.price
        ):
            raise AccountDiscrepancy("active_order_differs_from_journal")
        # Wire size is total quantity; the unfilled part comes from local execution evidence.
        working.append(
            WorkingOrder(
                client_id=active.client_id,
                order_id=evidence.order_id,
                remaining_units=intent.units - sum(e.units for e in evidence.executions),
            )
        )
    return AccountSnapshot(
        account_id=account_id,
        observed_at=min(o.response_at for o in report.observations),
        complete=True,
        balance=assets.balance,
        equity=assets.equity,
        unrealized_swap=assets.total_swap,
        required_margin=assets.margin,
        available_margin=assets.available_amount,
        positions=tuple(
            Position(position_id=p.position_id, side=p.side, units=p.units, average_price=p.price)
            for p in report.positions
        ),
        working_orders=tuple(working),
    )


def _valuation_tolerance(value):
    if value is None:
        return None
    try:
        tolerance = Decimal(str(value))
    except Exception:
        raise LiveAccountError("invalid_valuation_tolerance") from None
    if not tolerance.is_finite() or not 0 < tolerance <= 1:
        raise LiveAccountError("invalid_valuation_tolerance")
    return tolerance


def revalue(snapshot, quote, *, tolerance, policy):
    """Mark open positions at the reviewed ticker instead of the broker's valuation instant.

    Only when the broker's equity is within `tolerance` JPY per held unit of the ticker
    valuation. Available margin never rises above what the lower equity supports.
    """
    local = marked_equity(snapshot, quote)
    units = sum(p.units for p in snapshot.positions)
    if abs(snapshot.equity - local) > tolerance * units + policy.tolerance_jpy:
        raise LiveAccountError("valuation_outside_tolerance")
    available = min(snapshot.available_margin, max(local - snapshot.required_margin, Decimal(0)))
    return snapshot.model_copy(update={"equity": local, "available_margin": available})


class LiveAccountRefresh:
    """Read-only key, two GET sweeps, one public ticker, one local reconciliation. No POST."""

    def __init__(self, directory, read_control_directory, scope, **options):
        self.runtime = OrderRuntime(directory, read_control_directory, scope, **options)
        self.journal, self.reads, self.clock = (
            self.runtime.journal,
            self.runtime.reads,
            self.runtime.clock,
        )

    def _local(self):
        with self.journal._transaction() as conn:
            policy = AccountPolicy.model_validate_json(self.journal._gate(conn)["policy_json"])
            rows = [dict(row) for row in conn.execute("SELECT * FROM orders ORDER BY client_id")]
        return policy, rows

    def refresh(
        self,
        credential_reference,
        *,
        confirmations,
        quote=None,
        vault=None,
        transport=None,
        quote_transport=None,
        valuation_tolerance=None,
        absent_order=None,
        swap_schedule=None,
        swap_tolerance="1",
    ):
        if not isinstance(confirmations, (set, frozenset, tuple, list)) or set(
            confirmations
        ) != set(ACCOUNT_CONFIRMATIONS):
            raise LiveAccountError("account_confirmations_required")
        tolerance = _valuation_tolerance(valuation_tolerance)
        binding = self.journal.credential_binding()
        if self.reads.status()["blocked"]:
            raise LiveAccountError("read_control_blocked")
        vault = vault if vault is not None else CredentialVault()
        try:
            with vault.open_client(
                self.reads, credential_reference, transport=transport, clock=self.clock
            ) as client:
                report = AccountReader(client, clock=self.clock).collect_account()
        except Exception:
            raise LiveAccountError("account_collection_failed") from None
        if self.journal.credential_binding() != binding:
            raise LiveAccountError("live_binding_changed")
        if absent_order is not None and any(
            o.client_id == absent_order for o in report.active_orders
        ):
            # Found, not absent: reconcile it by its broker ID instead (order_discovery).
            raise LiveAccountError("unknown_order_is_active")
        try:
            policy, rows = self._local()
            if absent_order is not None:
                rows = [
                    {**r, "state": "ABANDONED"} if r["client_id"] == absent_order else r
                    for r in rows
                ]
            snapshot = snapshot_from_report(report, account_id=policy.account_id, rows=rows)
        except AccountDiscrepancy:
            self.journal.halt()
            raise
        except LiveAccountError:
            raise
        except Exception:
            raise LiveAccountError("account_snapshot_invalid") from None
        if quote is None:
            quote = fetch_quote(transport=quote_transport, clock=self.clock)
        broker_equity, adjusted = snapshot.equity, False
        if tolerance is not None:
            snapshot = revalue(snapshot, quote, tolerance=tolerance, policy=policy)
            adjusted = snapshot.equity != broker_equity
        now = self.clock()
        try:
            errors = set(reconcile_account(policy, rows, snapshot, quote, now))
        except ValueError:
            errors = set()  # The journal records and halts on structural failures.
        if errors == {"equity_mismatch"}:
            # Ledger contents agree; only the broker's valuation instant differs from the
            # ticker. Do not halt for that, and do not substitute a locally marked equity.
            raise LiveAccountError("valuation_time_mismatch")
        if absent_order is not None:
            # The account must reconcile as if the unknown order never existed.
            return self.journal.record_absence_account(absent_order, snapshot, quote, now=now)
        # A diagnostic computed before the journal decides; it never halts or refuses.
        swaps = (
            swap_check(report, swap_schedule, tolerance_jpy=swap_tolerance)
            if swap_schedule is not None
            else None
        )
        result = self.journal.update_account(snapshot, quote, now=now)
        return {
            **result,
            "swap_check": swaps,
            "observed_at": snapshot.model_dump(mode="json")["observed_at"],
            "positions": len(snapshot.positions),
            "working_orders": len(snapshot.working_orders),
            "quote_observed_at": quote.model_dump(mode="json")["observed_at"],
            "broker_equity": str(broker_equity),
            "valuation_adjusted": adjusted,
        }


def main(argv=None):
    parser = CredentialParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--read-control-directory", type=Path, required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--credential-reference", required=True)
    parser.add_argument("--quote", type=Path)
    parser.add_argument("--confirm", action="append", default=[])
    parser.add_argument("--valuation-tolerance")
    parser.add_argument("--absent-order", metavar="CLIENT_ID")
    parser.add_argument("--swap-schedule", type=Path)
    parser.add_argument("--swap-tolerance", default="1")
    args = parser.parse_args(argv)
    try:
        refresh = LiveAccountRefresh(args.directory, args.read_control_directory, args.scope)
        result = refresh.refresh(
            args.credential_reference,
            confirmations=args.confirm,
            quote=_quote(args.quote) if args.quote is not None else None,
            valuation_tolerance=args.valuation_tolerance,
            absent_order=args.absent_order,
            swap_schedule=(
                read_schedule(args.swap_schedule) if args.swap_schedule is not None else None
            ),
            swap_tolerance=args.swap_tolerance,
        )
        print(json.dumps({**result, "network_used": True, "orders_sent": False}))
    except Exception as error:
        reason = (
            str(error) if isinstance(error, LiveAccountError) else "account_reconciliation_failed"
        )
        parser.exit(2, f"Account refresh failed: {reason}; inspect the live journal events.\n")


if __name__ == "__main__":
    main()
