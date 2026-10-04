"""Reconcile an accepted live order from two GET reads of its saved broker ID. No POST.

After a submission receipt the journal holds the order in RECONCILING; until GET evidence
arrives it blocks the next order and keeps the account proof from moving. Unknown
submissions keep their own recovery path (`private_order_recovery`); this command only
reads orders whose broker ID the journal already saved.
"""

import json
from pathlib import Path

from trading.account_reader import AccountReader, OrderReadReport
from trading.broker_contracts import OrderEvidence, OrderIntent
from trading.credential_store import CredentialParser, CredentialVault
from trading.order_runtime import OrderRuntime

HISTORY_CONFIRMATIONS = frozenset({"complete-history", "external-writers-paused"})
ACCEPTED = {"RECONCILING", "WORKING", "PARTIAL", "CANCEL_PENDING"}


class LiveOrderSyncError(ValueError):
    """Fixed local reasons only; broker values are never echoed."""


class LiveOrderSync:
    def __init__(self, directory, read_control_directory, scope, **options):
        runtime = OrderRuntime(directory, read_control_directory, scope, **options)
        self.journal, self.reads, self.clock = runtime.journal, runtime.reads, runtime.clock

    def _target(self, client_id):
        """The intent and the broker order ID the journal itself saved for it."""
        with self.journal._transaction() as conn:
            row = self.journal._row(conn, client_id)
            if row["state"] not in ACCEPTED:
                raise LiveOrderSyncError("accepted_order_required")
            intent = OrderIntent.model_validate_json(row["intent_json"])
            receipt = self.journal._receipt(conn, client_id)
            if row["evidence_json"]:
                order_id = OrderEvidence.model_validate_json(row["evidence_json"]).order_id
            elif receipt is not None:
                order_id = receipt.order_id
            else:
                raise LiveOrderSyncError("saved_broker_order_id_required")
            if receipt is not None and receipt.order_id != order_id:
                raise LiveOrderSyncError("saved_broker_order_id_conflict")
            return intent, order_id, dict(row)

    def reconcile(
        self, client_id, *, credential_reference, confirmations, vault=None, transport=None
    ):
        if not isinstance(confirmations, (set, frozenset, tuple, list)) or set(
            confirmations
        ) != set(HISTORY_CONFIRMATIONS):
            raise LiveOrderSyncError("history_confirmations_required")
        self.journal.credential_binding()  # Registered original stores only.
        intent, order_id, before = self._target(client_id)
        if self.journal.posts.snapshot()["claim"] is not None:
            raise LiveOrderSyncError("post_claim_in_flight")
        if self.reads.status()["blocked"]:
            raise LiveOrderSyncError("read_control_blocked")
        vault = vault if vault is not None else CredentialVault()
        try:
            with vault.open_client(
                self.reads, credential_reference, transport=transport, clock=self.clock
            ) as client:
                report = AccountReader(client, clock=self.clock).collect_order(intent, order_id)
        except Exception:
            raise LiveOrderSyncError("order_collection_failed") from None
        if not isinstance(report, OrderReadReport) or report.evidence.intent != intent:
            raise LiveOrderSyncError("order_evidence_mismatch")
        if self._target(client_id)[2] != before:
            raise LiveOrderSyncError("order_changed_during_collection")
        # Two equal reads of the order's own execution list, declared complete by the
        # operator. The journal still checks receipt, identity and execution history.
        evidence = report.evidence.model_copy(update={"executions_complete": True})
        state = self.journal.reconcile(evidence)
        return {
            "client_id": client_id,
            "order_id": order_id,
            "broker_status": evidence.status,
            "state": state,
            "executions": len(evidence.executions),
            "orders_sent": False,
        }


def main(argv=None):
    parser = CredentialParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--read-control-directory", type=Path, required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--credential-reference", required=True)
    parser.add_argument("--confirm", action="append", default=[])
    args = parser.parse_args(argv)
    try:
        sync = LiveOrderSync(args.directory, args.read_control_directory, args.scope)
        result = sync.reconcile(
            args.client_id,
            credential_reference=args.credential_reference,
            confirmations=args.confirm,
        )
        print(json.dumps({**result, "network_used": True}))
    except Exception as error:
        reason = str(error) if isinstance(error, LiveOrderSyncError) else "live_order_sync_failed"
        parser.exit(2, f"{reason}\n")


if __name__ == "__main__":
    main()
