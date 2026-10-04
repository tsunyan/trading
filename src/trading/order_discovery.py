"""Find broker IDs of an unknown live order among active orders. Read-only, no journal change.

GMO order GETs require an order ID and cannot search by client order ID. The active-order
list does carry the client order ID, so a still-active order can be found there. Absence
proves nothing: the order may have filled, expired, been rejected or never arrived.
"""

import json
import time
from datetime import UTC, datetime
from pathlib import Path

from trading.account_reader import AccountReader
from trading.broker_contracts import OrderIntent
from trading.credential_store import CredentialParser, CredentialVault
from trading.private_order_recovery import PrivateOrderRecovery


class OrderDiscoveryError(ValueError):
    """Fixed local reasons only; broker values are never echoed."""


def match_active_order(report, intent):
    """One active order with the same client ID and matching terms, or None."""
    matches = [o for o in report.active_orders if o.client_id == intent.client_id]
    if len(matches) > 1:
        raise OrderDiscoveryError("duplicate_active_client_order")
    if not matches:
        return None
    order = matches[0]
    if (order.symbol, order.side, order.effect, order.kind, order.units, order.price) != (
        intent.symbol,
        intent.side,
        intent.effect,
        intent.kind,
        intent.units,
        intent.price,
    ):
        raise OrderDiscoveryError("active_order_terms_differ")
    return order


class OrderDiscovery:
    def __init__(
        self,
        directory,
        read_control_directory,
        scope,
        *,
        clock=lambda: datetime.now(UTC),
        monotonic=time.monotonic,
        read_clocks=None,
    ):
        self.recovery = PrivateOrderRecovery(
            directory,
            read_control_directory,
            scope,
            clock=clock,
            monotonic=monotonic,
            read_clocks=read_clocks,
        )
        self.clock = clock

    def discover(
        self,
        client_id,
        *,
        credential_reference,
        read_only_confirmed=False,
        vault=None,
        transport=None,
    ):
        if read_only_confirmed is not True:
            raise OrderDiscoveryError("order_discovery_read_confirmation_required")
        journal, reads = self.recovery.journal, self.recovery.reads
        # The same local consistency checks as GET recovery, before any credential read.
        context = journal.order_recovery_context(client_id)
        if reads.status()["blocked"]:
            raise OrderDiscoveryError("read_control_blocked")
        with journal._transaction() as conn:
            intent = OrderIntent.model_validate_json(journal._row(conn, client_id)["intent_json"])
        vault = vault if vault is not None else CredentialVault()
        try:
            with vault.open_client(
                reads, credential_reference, transport=transport, clock=self.clock
            ) as client:
                report = AccountReader(client, clock=self.clock).collect_account()
        except Exception:
            raise OrderDiscoveryError("order_discovery_collection_failed") from None
        # The answer is only about the checkpoint read before the GETs; if the order was
        # resolved or otherwise advanced meanwhile, report nothing rather than stale IDs.
        try:
            after = journal.order_recovery_context(client_id)["checkpoint_sha256"]
        except Exception:
            after = None
        if after != context["checkpoint_sha256"]:
            raise OrderDiscoveryError("journal_changed_during_discovery")
        order = match_active_order(report, intent)
        result = {
            "client_id": client_id,
            "recovery_checkpoint_sha256": context["checkpoint_sha256"],
            "found": order is not None,
            "absence_proven": False,
            "journal_changed": False,
        }
        if order is not None:
            result.update(
                root_order_id=order.root_order_id, order_id=order.order_id, status=order.status
            )
        return result


def main(argv=None):
    parser = CredentialParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--read-control-directory", type=Path, required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--credential-reference", required=True)
    parser.add_argument("--read-only-confirmed", action="store_true")
    args = parser.parse_args(argv)
    try:
        result = OrderDiscovery(args.directory, args.read_control_directory, args.scope).discover(
            args.client_id,
            credential_reference=args.credential_reference,
            read_only_confirmed=args.read_only_confirmed,
        )
        print(json.dumps({**result, "network_used": True}))
    except Exception as error:
        reason = str(error) if isinstance(error, OrderDiscoveryError) else "order_discovery_failed"
        parser.exit(2, f"{reason}\n")


if __name__ == "__main__":
    main()
