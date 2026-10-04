"""Explicit restart of stopped live operations; no HTTP, credentials, or order submission."""

import argparse
import json
import sqlite3
from pathlib import Path

from trading.live_journal import LiveRestartApproval
from trading.private_order_recovery import PrivateOrderRecovery


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("context", "restart"))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--read-control-directory", type=Path, required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--approval", type=Path)
    parser.add_argument("--confirm", action="append", default=[])
    args = parser.parse_args(argv)
    try:
        operations = PrivateOrderRecovery(args.directory, args.read_control_directory, args.scope)
        if args.command == "context":
            result = operations.journal.restart_context()
        else:
            if args.approval is None:
                raise ValueError("live_restart_approval_required")
            with args.approval.open("rb") as handle:
                payload = handle.read(64_001)
            if len(payload) > 64_000:
                raise ValueError("live_restart_approval_too_large")
            result = operations.journal.restart(
                LiveRestartApproval.model_validate_json(payload), confirmations=args.confirm
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
        parser.exit(2, "private_order_restart_failed\n")


if __name__ == "__main__":
    main()
