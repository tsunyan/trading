"""Explicit order credentials in a separate Windows namespace, bound to original live stores."""

import getpass
import json
import sys
import uuid
import warnings
from dataclasses import dataclass, field
from pathlib import Path

from pydantic import SecretStr

from trading.credential_store import (
    MAX_BLOB,
    ORDER_PREFIX,
    CredentialError,
    CredentialParser,
    WindowsCredentialBackend,
    _object,
    _secret,
    _target,
)
from trading.live_execution import require_checkpoint
from trading.live_journal import LiveOrderJournal
from trading.private_order_recovery import PrivateOrderRecovery


@dataclass(frozen=True)
class OrderCredentials:
    api_key: SecretStr = field(repr=False)
    secret: SecretStr = field(repr=False)


class OrderCredentialVault:
    """A local declaration, never proof of account identity or broker permissions.

    Save/rotation changes only an explicit credential reference, never stops,
    acceptance, risk evidence or orders. Loading requires an exact eligible
    operation and rechecks it after the native read. Injected backends are trusted.
    """

    def __init__(self, backend=None):
        if isinstance(backend, WindowsCredentialBackend) and backend._namespace != ORDER_PREFIX:
            raise CredentialError("order_credential_namespace_required")
        self._backend = (
            backend if backend is not None else WindowsCredentialBackend(namespace=ORDER_PREFIX)
        )

    @staticmethod
    def binding(journal):
        if not isinstance(journal, LiveOrderJournal):
            raise CredentialError("dedicated_live_journal_required")
        return journal.credential_binding()

    def save(self, journal, api_key, secret, *, order_permission_confirmed=False):
        if order_permission_confirmed is not True:
            raise CredentialError("order_permission_declaration_required")
        try:
            binding = self.binding(journal)
            reference = uuid.uuid4().hex
            payload = {
                "version": 1,
                "purpose": "orders",
                "reference": reference,
                "binding": binding,
                "order_permission_declared": True,
                "api_key": _secret(api_key),
                "secret": _secret(secret),
            }
            blob = json.dumps(payload, separators=(",", ":")).encode("ascii")
            if len(blob) > MAX_BLOB:
                raise CredentialError("credential_blob_too_large")
            # Recheck the original local binding before entering the native write.
            if self.binding(journal) != binding:
                raise CredentialError("credential_binding_mismatch")
            self._backend.write_new(reference, blob)
            return reference
        except CredentialError:
            raise
        except Exception:
            raise CredentialError("order_credential_save_failed") from None

    def load(
        self,
        journal,
        reference,
        client_id,
        *,
        expected_sha256,
        operation="submit",
        quote=None,
        authorization_sha256=None,
        order_permission_confirmed=False,
    ):
        if order_permission_confirmed is not True:
            raise CredentialError("order_permission_declaration_required")
        _target(reference, ORDER_PREFIX)
        try:
            binding = self.binding(journal)

            def check():
                context = journal.execution_context(
                    client_id,
                    operation=operation,
                    quote=quote,
                    authorization_sha256=authorization_sha256,
                )
                require_checkpoint(context, expected_sha256)
                if context["binding"] != binding:
                    raise CredentialError("credential_binding_mismatch")

            check()  # Refused operations never enter the native credential boundary.
            blob = self._backend.read(reference)
            if blob is None:
                raise CredentialError("credential_not_found")
            if type(blob) is not bytes or not 0 < len(blob) <= MAX_BLOB:
                raise CredentialError("invalid_credential_blob")
            data = json.loads(blob, object_pairs_hook=_object)
            if (
                not isinstance(data, dict)
                or set(data)
                != {
                    "version",
                    "purpose",
                    "reference",
                    "binding",
                    "order_permission_declared",
                    "api_key",
                    "secret",
                }
                or type(data["version"]) is not int
                or data["version"] != 1
                or data["purpose"] != "orders"
                or data["reference"] != reference
                or data["binding"] != binding
                or data["order_permission_declared"] is not True
                or not isinstance(data["api_key"], str)
                or not isinstance(data["secret"], str)
            ):
                raise CredentialError("credential_binding_mismatch")
            credentials = OrderCredentials(SecretStr(data["api_key"]), SecretStr(data["secret"]))
            _secret(credentials.api_key)
            _secret(credentials.secret)
            check()
            return credentials
        except CredentialError:
            raise
        except Exception:
            raise CredentialError("order_credential_load_failed") from None


def main(argv=None):
    parser = CredentialParser(description=__doc__)
    parser.add_argument("command", choices=("binding", "save"))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--read-control-directory", type=Path, required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--order-permission-confirmed", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.command == "save" and (
            not args.order_permission_confirmed or not sys.stdin.isatty()
        ):
            raise CredentialError("interactive_order_save_required")
        if args.command == "binding" and args.order_permission_confirmed:
            raise CredentialError("invalid_binding_options")
        journal = PrivateOrderRecovery(
            args.directory, args.read_control_directory, args.scope
        ).journal
        vault = OrderCredentialVault()
        binding = vault.binding(journal)
        result = {"binding": binding, "network_used": False, "broker_identity_verified": False}
        if args.command == "save":
            with warnings.catch_warnings():
                warnings.simplefilter("error", getpass.GetPassWarning)
                key = SecretStr(getpass.getpass("Order API key: "))
                secret = SecretStr(getpass.getpass("API secret: "))
            reference = vault.save(journal, key, secret, order_permission_confirmed=True)
            result.update(stored=True, reference=reference)
        print(json.dumps(result))
    except Exception:
        parser.exit(2, "Order credential operation failed; no key material is displayed.\n")


if __name__ == "__main__":
    main()
