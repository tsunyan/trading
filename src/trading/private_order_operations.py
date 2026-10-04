"""Register original sync/watchdog dispatch prerequisites; no keys, HTTP or activation."""

import argparse
import json
import sqlite3
from pathlib import Path

from trading.private_order_recovery import PrivateOrderRecovery


class OperationsParser(argparse.ArgumentParser):
    def error(self, message):
        self.exit(2, "Invalid live operations arguments.\n")


def main(argv=None):
    parser = OperationsParser(description=__doc__)
    parser.add_argument("command", choices=("context", "bind"))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--read-control-directory", type=Path, required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--sync-directory", type=Path)
    parser.add_argument("--expected-revision", type=int)
    parser.add_argument("--expected-plan-sha256")
    parser.add_argument("--expected-monitor-instance")
    parser.add_argument("--max-sync-age-seconds", type=int, default=120)
    parser.add_argument("--max-watchdog-age-seconds", type=int, default=120)
    parser.add_argument("--confirm-operations", action="store_true")
    parser.add_argument("--confirm-migration", action="store_true")
    args = parser.parse_args(argv)
    try:
        journal = PrivateOrderRecovery(
            args.directory, args.read_control_directory, args.scope
        ).journal
        if args.command == "context":
            state = journal.snapshot()["live_control"]
            result = {**journal.activation_context(), "operations": state["operations"]}
        else:
            if args.sync_directory is None:
                raise ValueError("sync_directory_required")
            result = journal.bind_operations(
                args.sync_directory,
                expected_revision=args.expected_revision,
                expected_plan_sha256=args.expected_plan_sha256,
                expected_monitor_instance=args.expected_monitor_instance,
                max_sync_age_seconds=args.max_sync_age_seconds,
                max_watchdog_age_seconds=args.max_watchdog_age_seconds,
                operations_confirmed=args.confirm_operations,
                migration_confirmed=args.confirm_migration,
            )
        print(json.dumps({**result, "complete": False, "live_enabled": False}, default=str))
    except (ValueError, OSError, KeyError, TypeError, sqlite3.Error):
        parser.exit(2, "private_order_operations_failed\n")


if __name__ == "__main__":
    main()
