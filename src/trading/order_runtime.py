"""Review an existing live order, then explicitly load bound credentials and dispatch once."""

import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

from trading.account_guard import AccountQuote
from trading.credential_store import CredentialParser, _object
from trading.live_execution import require_checkpoint
from trading.live_journal import LiveOrderJournal
from trading.order_credentials import OrderCredentialVault
from trading.post_control import PersistentPostLimiter
from trading.private_order import PrivateOrderClient
from trading.read_control import PersistentReadLimiter


class OrderRuntimeError(ValueError):
    """Fixed local reasons only, including failures at injected secret boundaries."""


class OrderRuntime:
    def __init__(
        self,
        directory,
        read_control_directory,
        scope,
        *,
        clock=lambda: datetime.now(UTC),
        monotonic=time.monotonic,
        read_clocks=None,
        post_sleep=time.sleep,
    ):
        self.clock, self.monotonic = clock, monotonic
        self.reads = PersistentReadLimiter(read_control_directory, scope, **(read_clocks or {}))
        binding = self.reads.post_binding()
        if binding is None:
            raise OrderRuntimeError("order_runtime_post_binding_required")
        self.posts = PersistentPostLimiter(
            binding["path"],
            self.reads,
            wall_ns=lambda: int(clock().timestamp() * 1e9),
            monotonic=monotonic,
            sleep=post_sleep,
        )
        self.journal = LiveOrderJournal(directory, self.posts, clock=clock)
        self.journal.credential_binding()  # Operational entry points require registered originals.

    def context(self, client_id, *, quote=None, operation="submit", authorization_sha256=None):
        try:
            return self.journal.execution_context(
                client_id,
                operation=operation,
                quote=quote,
                authorization_sha256=authorization_sha256,
            )
        except Exception:
            raise OrderRuntimeError("order_runtime_preflight_refused") from None

    def dispatch(
        self,
        client_id,
        *,
        expected_sha256,
        credential_reference,
        operation="submit",
        quote=None,
        authorization_sha256=None,
        order_permission_confirmed=False,
        vault=None,
        transport=None,
    ):
        if order_permission_confirmed is not True:
            raise OrderRuntimeError("order_permission_declaration_required")
        try:
            # A caller-supplied vault never bypasses local operation-specific preflight.
            require_checkpoint(
                self.context(
                    client_id,
                    quote=quote,
                    operation=operation,
                    authorization_sha256=authorization_sha256,
                ),
                expected_sha256,
            )
            credentials = (vault if vault is not None else OrderCredentialVault()).load(
                self.journal,
                credential_reference,
                client_id,
                expected_sha256=expected_sha256,
                operation=operation,
                quote=quote,
                authorization_sha256=authorization_sha256,
                order_permission_confirmed=True,
            )
            require_checkpoint(
                self.context(
                    client_id,
                    quote=quote,
                    operation=operation,
                    authorization_sha256=authorization_sha256,
                ),
                expected_sha256,
            )
            with PrivateOrderClient(
                credentials.api_key,
                credentials.secret,
                journal=self.journal,
                transport=transport,
                clock=self.clock,
                monotonic=self.monotonic,
            ) as client:
                if operation == "submit":
                    return client.submit(
                        client_id, quote=quote, expected_execution_sha256=expected_sha256
                    )
                return client.cancel(
                    client_id,
                    authorization_sha256=authorization_sha256,
                    expected_execution_sha256=expected_sha256,
                )
        except Exception:
            # HTTP/claim ambiguities are preserved by the existing transport, never replayed here.
            raise OrderRuntimeError("order_runtime_dispatch_failed") from None


def _quote(path):
    if path is None:
        raise OrderRuntimeError("execution_quote_required")
    with path.open("rb") as handle:
        payload = handle.read(4097)
    if len(payload) > 4096:
        raise OrderRuntimeError("execution_quote_too_large")
    return AccountQuote.model_validate(json.loads(payload, object_pairs_hook=_object))


def append_dispatch(path, client_id, operation, checkpoint_sha256, quote):
    """One JSON line per accepted send with the reviewed quote, for execution-cost review."""
    line = {
        "client_id": client_id,
        "operation": operation,
        "checkpoint_sha256": checkpoint_sha256,
        "sent_at": datetime.now(UTC).isoformat(),
        "quote": None if quote is None else quote.model_dump(mode="json"),
    }
    with Path(path).open("a", encoding="utf-8", newline="\n") as output:
        output.write(json.dumps(line, separators=(",", ":")) + "\n")
        output.flush()
        os.fsync(output.fileno())


def main(argv=None):
    parser = CredentialParser(description=__doc__)
    parser.add_argument("command", choices=("context", "submit", "cancel-context", "cancel"))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--read-control-directory", type=Path, required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--quote", type=Path)
    parser.add_argument("--authorization-sha256")
    parser.add_argument("--expected-sha256")
    parser.add_argument("--credential-reference")
    parser.add_argument("--order-permission-confirmed", action="store_true")
    parser.add_argument("--dispatch-log", type=Path)
    args = parser.parse_args(argv)
    try:
        inspection = args.command in {"context", "cancel-context"}
        operation = "cancel" if args.command.startswith("cancel") else "submit"
        if inspection and (
            args.expected_sha256 is not None
            or args.credential_reference is not None
            or args.order_permission_confirmed
        ):
            raise OrderRuntimeError("invalid_execution_context_options")
        if not inspection and (not args.credential_reference or not args.expected_sha256):
            raise OrderRuntimeError("explicit_execution_checkpoint_required")
        if (operation == "cancel" and args.quote is not None) or (
            operation == "submit" and args.authorization_sha256 is not None
        ):
            raise OrderRuntimeError("invalid_execution_options")
        quote = _quote(args.quote) if operation == "submit" else None
        runtime = OrderRuntime(args.directory, args.read_control_directory, args.scope)
        options = dict(
            quote=quote, operation=operation, authorization_sha256=args.authorization_sha256
        )
        if inspection:
            result = {**runtime.context(args.client_id, **options), "network_used": False}
        else:
            receipt = runtime.dispatch(
                args.client_id,
                **options,
                expected_sha256=args.expected_sha256,
                credential_reference=args.credential_reference,
                order_permission_confirmed=args.order_permission_confirmed,
            )
            result = {
                "operation": operation,
                "client_id": args.client_id,
                "root_order_id": receipt.root_order_id,
                "accepted": True,
                "network_used": True,
                "account_complete": False,
                "reconciliation_required": True,
            }
            if args.dispatch_log is not None:
                # After acceptance: a failed log line never hides that the order was sent.
                try:
                    append_dispatch(
                        args.dispatch_log, args.client_id, operation, args.expected_sha256, quote
                    )
                except OSError:
                    result["dispatch_logged"] = False
        print(json.dumps(result, ensure_ascii=False))
    except Exception:
        parser.exit(
            2, "Order runtime failed; inspect original order and POST records before retry.\n"
        )


if __name__ == "__main__":
    main()
