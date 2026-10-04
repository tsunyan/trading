"""Reproducible Windows actions for the local Private-sync watchdog."""

import json
import subprocess
from pathlib import Path

from trading.paper_runner import scheduled_process_args
from trading.private_operations import OperationsError, OperationsParser, PrivateOperations
from trading.private_sync import PrivateSyncWorkspace


def task_plan(directory, *, interval_seconds=60, sync_duration_seconds=None):
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
    if sync_duration_seconds is not None and (
        type(sync_duration_seconds) is not int or not 3600 <= sync_duration_seconds <= 604_800
    ):
        raise OperationsError("operations_sync_duration_invalid")
    argv = scheduled_process_args(
        "from trading.private_operations import main; raise SystemExit(main(sys.argv[1:]))",
        "watchdog",
        "--directory",
        directory,
    )
    tasks = [
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
    ]
    if sync_duration_seconds is not None:
        # Continue only from a clean READY end; STOPPED keeps failing fast without keys.
        sync = scheduled_process_args(
            "from trading.private_sync import main; raise SystemExit(main(sys.argv[1:]))",
            "continue",
            "--directory",
            directory,
            "--duration-seconds",
            str(sync_duration_seconds),
            "--read-only-confirmed",
        )
        tasks.append(
            {
                "name": f"TradingLab-Private-{monitor['control_instance'][:12]}-Sync",
                "description": (
                    f"Trading Lab Private sync continue control {monitor['control_instance']}"
                ),
                "executable": sync[0],
                "arguments": subprocess.list2cmdline(sync[1:]),
                "working_directory": str(Path.cwd()),
                "execution_limit_seconds": sync_duration_seconds + 600,
            }
        )
    return {
        "directory": str(directory),
        "monitor_instance": monitor["monitor_instance"],
        "control_instance": monitor["control_instance"],
        "plan_sha256": monitor["plan_sha256"],
        "interval_seconds": interval_seconds,
        "tasks": tasks,
        "complete": False,
        "live_enabled": False,
    }


def main(argv=None):
    parser = OperationsParser(description=__doc__)
    parser.add_argument("command", choices=("plan",))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--interval-seconds", type=int, default=60)
    parser.add_argument("--sync-duration-seconds", type=int)
    args = parser.parse_args(argv)
    try:
        print(
            json.dumps(
                {
                    "ok": True,
                    **task_plan(
                        args.directory,
                        interval_seconds=args.interval_seconds,
                        sync_duration_seconds=args.sync_duration_seconds,
                    ),
                }
            )
        )
        return 0
    except Exception:
        print(json.dumps({"ok": False, "reason": "private_task_plan_failed"}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
