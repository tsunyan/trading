"""Explicit GET investigation and accepted terminal claim resolution; never resend or resume."""

import argparse
import json
import math
import re
import sqlite3
import time
from datetime import UTC, datetime
from pathlib import Path

from trading.account_reader import AccountReader
from trading.credential_store import CredentialVault
from trading.live_journal import LiveOrderJournal, OrderResolutionApproval
from trading.post_control import PersistentPostLimiter
from trading.private_read import PrivateReadClient
from trading.read_control import PersistentReadLimiter


class OrderRecoveryError(ValueError):
    """Fixed reason codes; no credentials or remote text."""


class PrivateOrderRecovery:
    """Existing permanently bound stores, with credentials loaded only for GETs."""

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
        self.clock, self.monotonic = clock, monotonic
        self.reads = PersistentReadLimiter(read_control_directory, scope, **(read_clocks or {}))
        binding = self.reads.post_binding()
        if binding is None:
            raise OrderRecoveryError("order_recovery_post_binding_required")
        self.posts = PersistentPostLimiter(
            binding["path"],
            self.reads,
            wall_ns=lambda: int(clock().timestamp() * 1e9),
            monotonic=monotonic,
        )
        self.journal = LiveOrderJournal(directory, self.posts, clock=clock)

    def context(self, client_id):
        return self.journal.order_recovery_context(client_id)

    def resolution_context(self, client_id):
        return self.journal.order_resolution_context(client_id)

    def active_cancel_context(self, client_id):
        return self.journal.active_cancel_context(client_id)

    def resolve(self, client_id, approval, *, confirmations):
        return self.journal.resolve_order_claim(client_id, approval, confirmations=confirmations)

    def absence_context(self, client_id):
        return self.journal.order_absence_context(client_id)

    def resolve_absence(self, client_id, approval, *, confirmations):
        return self.journal.resolve_absent_order(client_id, approval, confirmations=confirmations)

    def reconcile(
        self,
        client_id,
        order_id,
        *,
        expected_sha256,
        credential_reference,
        read_only_confirmed=False,
        vault=None,
        transport=None,
    ):
        if read_only_confirmed is not True:
            raise OrderRecoveryError("order_recovery_read_confirmation_required")
        if (
            type(order_id) is not int
            or not 0 < order_id < 2**63
            or not isinstance(credential_reference, str)
            or not re.fullmatch(r"[a-f0-9]{32}", credential_reference)
            or not isinstance(expected_sha256, str)
            or not re.fullmatch(r"[a-f0-9]{64}", expected_sha256)
        ):
            raise OrderRecoveryError("invalid_order_recovery_input")

        def collect(intent):
            # Journal checkpoint, POST owner and GET dependencies are checked
            # before this callback may enter the native credential boundary.
            credentials = (vault if vault is not None else CredentialVault()).load(
                self.reads, credential_reference
            )
            with PrivateReadClient(
                credentials.api_key,
                credentials.secret,
                limiter=self.reads,
                transport=transport,
                clock=self.clock,
                monotonic=self.monotonic,
            ) as client:
                started = self.monotonic()
                if type(started) not in {int, float} or not math.isfinite(started) or started < 0:
                    raise OrderRecoveryError("order_recovery_clock_invalid")
                last = started

                def check_deadline():
                    nonlocal last
                    now = self.monotonic()
                    if (
                        type(now) not in {int, float}
                        or not math.isfinite(now)
                        or now < last
                        or now - started >= 30
                    ):
                        raise OrderRecoveryError("order_recovery_deadline")
                    last = now

                class BoundedReader:
                    def get(self, request):
                        check_deadline()
                        response = client.get(request)
                        check_deadline()
                        return response

                report = AccountReader(BoundedReader(), clock=self.clock).collect_order(
                    intent, order_id
                )
                check_deadline()
            # A failed close must not persist a successful observation.
            return report

        return self.journal.reconcile_unknown_order(
            client_id, expected_sha256=expected_sha256, collect=collect
        )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=(
            "context",
            "reconcile",
            "resolution-context",
            "active-cancel-context",
            "resolve",
            "absence-context",
            "resolve-absence",
        ),
    )
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--read-control-directory", type=Path, required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--order-id", type=int)
    parser.add_argument("--expected-sha256")
    parser.add_argument("--credential-reference")
    parser.add_argument("--read-only-confirmed", action="store_true")
    parser.add_argument("--approval", type=Path)
    parser.add_argument("--confirm", action="append", default=[])
    args = parser.parse_args(argv)
    try:
        recovery = PrivateOrderRecovery(args.directory, args.read_control_directory, args.scope)
        if args.command == "context":
            result = recovery.context(args.client_id)
        elif args.command == "resolution-context":
            result = recovery.resolution_context(args.client_id)
        elif args.command == "active-cancel-context":
            result = recovery.active_cancel_context(args.client_id)
        elif args.command == "absence-context":
            result = recovery.absence_context(args.client_id)
        elif args.command in {"resolve", "resolve-absence"}:
            if args.approval is None:
                raise OrderRecoveryError("order_resolution_approval_required")
            with args.approval.open("rb") as handle:
                payload = handle.read(64_001)
            if len(payload) > 64_000:
                raise OrderRecoveryError("order_resolution_approval_too_large")
            approval = OrderResolutionApproval.model_validate_json(payload)
            resolve = recovery.resolve if args.command == "resolve" else recovery.resolve_absence
            result = resolve(args.client_id, approval, confirmations=args.confirm)
        else:
            result = recovery.reconcile(
                args.client_id,
                args.order_id,
                expected_sha256=args.expected_sha256,
                credential_reference=args.credential_reference,
                read_only_confirmed=args.read_only_confirmed,
            )
        print(json.dumps(result, ensure_ascii=False))
    except (
        ValueError,
        OSError,
        sqlite3.Error,
        KeyError,
        TypeError,
        AttributeError,
        ArithmeticError,
        RecursionError,
    ):
        parser.exit(2, "private_order_recovery_failed\n")


if __name__ == "__main__":
    main()
