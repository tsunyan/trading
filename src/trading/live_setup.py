"""Operator CLI for the dedicated live journal: create, prepare, activate, status and stop.

None of these commands loads credentials or sends HTTP. Activation stays an explicit,
expiring operator acceptance; this module only moves validated files into the journal.
"""

import argparse
import hashlib
import json
import sqlite3
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from trading.account_guard import AccountPolicy
from trading.broker_contracts import OrderIntent, OrderLimits
from trading.live_journal import LiveApproval, LiveOrderError, LiveOrderJournal
from trading.post_control import PersistentPostLimiter
from trading.private_order_recovery import PrivateOrderRecovery
from trading.read_control import PersistentReadLimiter
from trading.wire_validation import unique_object

MAX_FILE = 64_000


class LiveSetupError(ValueError):
    """Fixed local reasons only; file contents are never echoed."""


class LiveConfiguration(BaseModel):
    """Order limits and account risk policy, fixed into the journal at creation."""

    model_config = ConfigDict(frozen=True, extra="forbid")
    limits: OrderLimits
    policy: AccountPolicy


def _load(path, model):
    if path is None:
        raise LiveSetupError("input_file_required")
    with Path(path).open("rb") as handle:
        payload = handle.read(MAX_FILE + 1)
    if len(payload) > MAX_FILE:
        raise LiveSetupError("input_file_too_large")
    try:
        json.loads(payload, object_pairs_hook=unique_object)  # Refuse duplicate keys first.
        # JSON mode: strict timestamps accept ISO strings exactly as saved approvals do.
        return model.model_validate_json(payload)
    except Exception:
        raise LiveSetupError("invalid_input_file") from None


def create(directory, read_control_directory, scope, configuration):
    """A new DISABLED journal permanently bound to the existing GET/POST controls."""
    reads = PersistentReadLimiter(read_control_directory, scope)
    binding = reads.post_binding()
    if binding is None:
        raise LiveSetupError("post_control_binding_required")
    posts = PersistentPostLimiter(binding["path"], reads)
    return LiveOrderJournal.create(directory, posts, configuration.limits, configuration.policy)


def activate(journal, approval, *, expected_revision, confirmations):
    """Operational activation also requires registered sync/watchdog prerequisites."""
    if journal.snapshot()["live_control"].get("operations") is None:
        raise LiveSetupError("register_operations_before_activation")
    journal.activate(approval, expected_revision=expected_revision, confirmations=confirmations)
    return status(journal)


def backup(journal, output):
    """A consistent read-only copy for audit. Restoring a copy is not a supported recovery."""
    if output is None:
        raise LiveSetupError("output_required")
    output = Path(output)
    if output.exists():
        raise LiveSetupError("output_exists")
    with journal._transaction():  # Validates the live state before copying.
        pass
    with sqlite3.connect(f"file:{journal.path}?mode=ro", uri=True) as source:
        with sqlite3.connect(output) as target:
            source.backup(target)
    with sqlite3.connect(output) as copy:
        check = copy.execute("PRAGMA integrity_check").fetchone()[0]
        events = copy.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    if check != "ok":
        raise LiveSetupError("backup_integrity_failed")
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    return {"output": str(output), "events": events, "sha256": digest, "restorable": False}


def status(journal):
    """A compact view without account values, order bodies or event payloads."""
    snapshot = journal.snapshot()
    control = snapshot["live_control"]
    guard = snapshot["account_guard"] or {}
    proof = guard.get("last_proof") or {}
    return {
        "phase": control["phase"],
        "revision": control["revision"],
        "live_enabled": snapshot["live_enabled"],
        "implementation_matches": snapshot["implementation_matches"],
        "halted": snapshot["halted"],
        "operations_registered": control.get("operations") is not None,
        "approval_expires_at": (control.get("approval") or {}).get("expires_at"),
        "entry_halted": guard.get("entry_halted"),
        "account_observed_at": (proof.get("snapshot") or {}).get("observed_at"),
        "orders": [
            {"client_id": row["client_id"], "state": row["state"]} for row in snapshot["orders"]
        ],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=(
            "create",
            "status",
            "prepare",
            "abandon",
            "activation-context",
            "activate",
            "stop",
            "backup",
        ),
    )
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--read-control-directory", type=Path, required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--intent", type=Path)
    parser.add_argument("--approval", type=Path)
    parser.add_argument("--expected-revision", type=int)
    parser.add_argument("--confirm", action="append", default=[])
    parser.add_argument("--confirm-stop", action="store_true")
    parser.add_argument("--client-id")
    parser.add_argument("--confirm-abandon", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "create":
            journal = create(
                args.directory,
                args.read_control_directory,
                args.scope,
                _load(args.config, LiveConfiguration),
            )
            result = {**status(journal), **journal.activation_context()}
        else:
            journal = PrivateOrderRecovery(
                args.directory, args.read_control_directory, args.scope
            ).journal
            if args.command == "status":
                result = status(journal)
            elif args.command == "prepare":
                intent = _load(args.intent, OrderIntent)
                journal.prepare(intent)
                result = {"client_id": intent.client_id, "state": "PREPARED"}
            elif args.command == "abandon":
                # Only a never-claimed PREPARED intent; anything possibly sent is refused.
                if not args.confirm_abandon or not args.client_id:
                    raise LiveSetupError("explicit_abandon_confirmation_required")
                journal.abandon(args.client_id)
                result = {"client_id": args.client_id, "state": "ABANDONED"}
            elif args.command == "backup":
                result = backup(journal, args.output)
            elif args.command == "activation-context":
                result = journal.activation_context()
            elif args.command == "activate":
                if args.expected_revision is None:
                    raise LiveSetupError("expected_revision_required")
                result = activate(
                    journal,
                    _load(args.approval, LiveApproval),
                    expected_revision=args.expected_revision,
                    confirmations=args.confirm,
                )
            else:
                if not args.confirm_stop:
                    raise LiveSetupError("explicit_stop_confirmation_required")
                journal.halt()
                result = status(journal)
        print(json.dumps({**result, "network_used": False}, ensure_ascii=False))
    except (
        ValueError,
        OSError,
        sqlite3.Error,
        KeyError,
        TypeError,
        AttributeError,
        ArithmeticError,
        RecursionError,
    ) as error:
        # Validation errors can quote file contents; only fixed local codes are shown.
        fixed = isinstance(error, (LiveSetupError, LiveOrderError))
        reason = str(error) if fixed else "live_setup_failed"
        parser.exit(2, f"{reason}\n")


if __name__ == "__main__":
    main()
