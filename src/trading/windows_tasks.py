"""Build reproducible Windows task actions and run the independent paper watchdog."""

import argparse
import json
import sqlite3
import subprocess
from pathlib import Path

from trading.config import Settings
from trading.observer import code_hashes, load_manifest
from trading.paper_runner import PaperRunner, RunnerPolicy, scheduled_process_args
from trading.provenance import runtime_versions
from trading.windows_notify import deliver_alerts


def task_plan(directory, policy_path=None):
    directory = Path(directory).resolve()
    manifest = load_manifest(directory)
    if manifest.get("mode") != "exploratory_forward_paper":
        raise ValueError("scheduled tasks require an exploratory FX paper account")
    cfg = Settings.model_validate(manifest["config"])
    if (
        cfg.market != "fx"
        or cfg.fingerprint != manifest["config_sha256"]
        or code_hashes() != manifest["core_sha256"]
    ):
        raise ValueError("frozen observer configuration or code changed")
    if runtime_versions() != manifest["runtime"]:
        raise ValueError("frozen observer runtime changed")
    policy = (
        RunnerPolicy.model_validate_json(Path(policy_path).read_text(encoding="utf-8"))
        if policy_path
        else RunnerPolicy()
    )
    arguments = ["--directory", str(directory)]
    if policy_path:
        arguments += ["--policy", str(Path(policy_path).resolve())]
    tasks = []
    for kind, module, command, timeout in (
        ("Observation", "trading.paper_runner", "step", policy.cycle_timeout_seconds + 30),
        ("Watchdog", "trading.windows_tasks", "watchdog", 90),
    ):
        argv = scheduled_process_args(
            f"from {module} import main; raise SystemExit(main(sys.argv[1:]))",
            command,
            *arguments,
        )
        tasks.append(
            {
                "name": f"TradingLab-Paper-{manifest['observer_id'][:12]}-{kind}",
                "description": f"Trading Lab observer {manifest['observer_id']} {kind}",
                "executable": argv[0],
                "arguments": subprocess.list2cmdline(argv[1:]),
                "working_directory": str(Path.cwd()),
                "execution_limit_seconds": timeout,
            }
        )
    return {
        "observer_id": manifest["observer_id"],
        "directory": str(directory),
        "interval_seconds": policy.interval_seconds,
        "tasks": tasks,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "watchdog"))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--policy", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            result = task_plan(args.directory, args.policy)
        else:
            policy = (
                RunnerPolicy.model_validate_json(args.policy.read_text(encoding="utf-8"))
                if args.policy
                else RunnerPolicy()
            )
            health = PaperRunner(args.directory, policy).check()
            notifications = deliver_alerts(args.directory)
            result = {"health": health, "notifications": notifications}
        print(json.dumps(result, ensure_ascii=True, indent=2))
        if args.command == "watchdog":
            return int(health.get("stale", False) or notifications["status"] == "error")
        return 0
    except (ValueError, OSError, sqlite3.Error) as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "error": str(exc)}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
