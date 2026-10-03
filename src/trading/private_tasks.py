"""Reproducible Windows actions for the local Private-sync watchdog."""

import json
import subprocess
from pathlib import Path

from trading.paper_runner import scheduled_process_args
from trading.private_operations import OperationsError, OperationsParser, PrivateOperations
from trading.private_sync import PrivateSyncWorkspace


def task_plan(directory, *, interval_seconds=60):
    directory = Path(directory).resolve()
    monitor = PrivateOperations(directory).status()
    workspace = PrivateSyncWorkspace(directory).status()
    if (
        monitor["control_instance"] != workspace["control"]["instance"]
        or monitor["plan_sha256"] != workspace["plan_sha256"]
    ):
        raise OperationsError("operations_task_binding_mismatch")
    if type(interval_seconds) is not int or not 30 <= interval_seconds <= monitor["stale_seconds"]:
        raise OperationsError("operations_task_interval_invalid")
    argv = scheduled_process_args(
        "from trading.private_operations import main; raise SystemExit(main(sys.argv[1:]))",
        "watchdog",
        "--directory",
        directory,
    )
    return {
        "directory": str(directory),
        "monitor_instance": monitor["monitor_instance"],
        "control_instance": monitor["control_instance"],
        "plan_sha256": monitor["plan_sha256"],
        "interval_seconds": interval_seconds,
        "tasks": [
            {
                "name": f"TradingLab-Private-{monitor['control_instance'][:12]}-Watchdog",
                "description": (
                    f"Trading Lab Private monitor {monitor['monitor_instance']} "
                    f"control {monitor['control_instance']}"
                ),
                "executable": argv[0],
                "arguments": subprocess.list2cmdline(argv[1:]),
                "working_directory": str(Path.cwd()),
                "execution_limit_seconds": 90,
            }
        ],
        "complete": False,
        "live_enabled": False,
    }


def main(argv=None):
    parser = OperationsParser(description=__doc__)
    parser.add_argument("command", choices=("plan",))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--interval-seconds", type=int, default=60)
    args = parser.parse_args(argv)
    try:
        print(
            json.dumps(
                {"ok": True, **task_plan(args.directory, interval_seconds=args.interval_seconds)}
            )
        )
        return 0
    except Exception:
        print(json.dumps({"ok": False, "reason": "private_task_plan_failed"}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
