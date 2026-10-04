"""Reproducible Windows action for an hourly read-only live cycle. It never prepares or sends.

The task runs `live_cycle` without `--prepare` a minute after each hourly bar closes,
writes the result file and raises a desktop notice for a proposal or a failure. The
operator then prepares and submits by hand within the quote freshness window.
"""

import argparse
import json
import re
import subprocess
from decimal import Decimal
from pathlib import Path

from trading.config import load_settings
from trading.live_account import LiveAccountError, _valuation_tolerance
from trading.live_cycle import CYCLE_CONFIRMATIONS
from trading.order_runtime import OrderRuntime
from trading.paper_runner import scheduled_process_args
from trading.promotion import PromotionError, require_live


class LiveTaskError(ValueError):
    """Fixed local reasons only."""


def task_plan(
    directory,
    read_control_directory,
    scope,
    *,
    credential_reference,
    config,
    units,
    max_slippage,
    quote_output,
    result_output,
    confirmations,
    valuation_tolerance=None,
    ledger=None,
    hypothesis=None,
    history_output=None,
    dashboard_output=None,
    doctor_interval_seconds=None,
):
    if not isinstance(confirmations, (set, frozenset, tuple, list)) or set(confirmations) != set(
        CYCLE_CONFIRMATIONS
    ):
        raise LiveTaskError("cycle_confirmations_required")
    if not isinstance(credential_reference, str) or not re.fullmatch(
        r"[a-f0-9]{32}", credential_reference
    ):
        raise LiveTaskError("invalid_credential_reference")
    if units != "auto" and (type(units) is not int or units <= 0):
        raise LiveTaskError("invalid_units")
    for value in (max_slippage, valuation_tolerance):
        if value is not None and not Decimal(str(value)).is_finite():
            raise LiveTaskError("invalid_decimal_option")
    # Static inputs the hourly run would refuse every time are refused once, here.
    if not Decimal(str(max_slippage)) > 0:
        raise LiveTaskError("invalid_max_slippage")
    try:
        _valuation_tolerance(valuation_tolerance)
    except LiveAccountError:
        raise LiveTaskError("invalid_valuation_tolerance") from None
    if ledger is None or hypothesis is None:
        # The scheduled cycle proposes entries, so it needs the live-promoted candidate.
        raise LiveTaskError("promoted_candidate_required")
    directory, read_control_directory, config = (
        Path(directory).resolve(),
        Path(read_control_directory).resolve(),
        Path(config).resolve(),
    )
    load_settings(config)  # A broken strategy config fails here, not every hour.
    # Only a registered original journal (sync/watchdog prerequisites) can be scheduled.
    journal = OrderRuntime(directory, read_control_directory, scope).journal
    binding = journal.credential_binding()
    limits = journal.limits
    if units != "auto" and (
        not limits.min_units <= units <= limits.max_units or units % limits.unit_step
    ):
        raise LiveTaskError("units_outside_journal_limits")
    quote_output, result_output = Path(quote_output).resolve(), Path(result_output).resolve()
    for path in (quote_output, result_output):
        if not path.parent.is_dir():
            raise LiveTaskError("output_directory_required")
    args = [
        "--config",
        config,
        "--directory",
        directory,
        "--read-control-directory",
        read_control_directory,
        "--scope",
        scope,
        "--credential-reference",
        credential_reference,
        "--units",
        str(units),
        "--max-slippage",
        str(max_slippage),
        "--quote-output",
        quote_output,
        "--result-output",
        result_output,
        "--notify",
    ]
    if valuation_tolerance is not None:
        args += ["--valuation-tolerance", str(valuation_tolerance)]
    if history_output is not None:
        history_output = Path(history_output).resolve()
        if not history_output.parent.is_dir():
            raise LiveTaskError("output_directory_required")
        args += ["--history-output", history_output]
    if dashboard_output is not None:
        dashboard_output = Path(dashboard_output).resolve()
        if not dashboard_output.parent.is_dir():
            raise LiveTaskError("output_directory_required")
        args += ["--dashboard-output", dashboard_output]
    # Fail at planning, not every hour, if the candidate is not promoted for live.
    require_live(Path(ledger), hypothesis, load_settings(config))
    args += ["--ledger", Path(ledger).resolve(), "--hypothesis", hypothesis]
    for item in sorted(CYCLE_CONFIRMATIONS):
        args += ["--confirm", item]
    if doctor_interval_seconds is not None and (
        type(doctor_interval_seconds) is not int or not 300 <= doctor_interval_seconds <= 3600
    ):
        raise LiveTaskError("invalid_doctor_interval")
    argv = scheduled_process_args(
        "from trading.live_cycle import main; raise SystemExit(main(sys.argv[1:]))", *args
    )
    tasks = [
        {
            "name": f"TradingLab-Live-{binding['live_instance'][:12]}-Cycle",
            "description": (
                f"Trading Lab live cycle {binding['live_instance']} (no prepare, no send)"
            ),
            "executable": argv[0],
            "arguments": subprocess.list2cmdline(argv[1:]),
            "working_directory": str(Path.cwd()),
            "execution_limit_seconds": 300,
        }
    ]
    if doctor_interval_seconds is not None:
        # Notices when the hourly cycle stops or any send gate starts or stops blocking.
        doctor = [
            "--directory",
            directory,
            "--read-control-directory",
            read_control_directory,
            "--scope",
            scope,
            "--cycle-result",
            result_output,
            "--notify-state",
            result_output.with_name("doctor-state.json"),
        ]
        doctor += ["--ledger", Path(ledger).resolve(), "--hypothesis", hypothesis]
        doctor += ["--config", config]
        doctor_argv = scheduled_process_args(
            "from trading.live_doctor import main; raise SystemExit(main(sys.argv[1:]))", *doctor
        )
        tasks.append(
            {
                "name": f"TradingLab-Live-{binding['live_instance'][:12]}-Doctor",
                "description": f"Trading Lab live doctor {binding['live_instance']} (read only)",
                "executable": doctor_argv[0],
                "arguments": subprocess.list2cmdline(doctor_argv[1:]),
                "working_directory": str(Path.cwd()),
                "execution_limit_seconds": 120,
                "interval_seconds": doctor_interval_seconds,
            }
        )
    return {
        "live_instance": binding["live_instance"],
        "scope": binding["scope"],
        "start_minute": 1,
        "interval_seconds": 3600,
        "tasks": tasks,
        "prepares_orders": False,
        "sends_orders": False,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan",))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--read-control-directory", type=Path, required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--credential-reference", required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--units", required=True, help="lot size, or auto")
    parser.add_argument("--max-slippage", required=True)
    parser.add_argument("--quote-output", type=Path, required=True)
    parser.add_argument("--result-output", type=Path, required=True)
    parser.add_argument("--valuation-tolerance")
    parser.add_argument("--history-output", type=Path)
    parser.add_argument("--dashboard-output", type=Path)
    parser.add_argument("--doctor-interval-seconds", type=int)
    parser.add_argument("--ledger", type=Path)
    parser.add_argument("--hypothesis")
    parser.add_argument("--confirm", action="append", default=[])
    args = parser.parse_args(argv)
    try:
        plan = task_plan(
            args.directory,
            args.read_control_directory,
            args.scope,
            credential_reference=args.credential_reference,
            config=args.config,
            units=args.units if args.units == "auto" else int(args.units),
            max_slippage=args.max_slippage,
            quote_output=args.quote_output,
            result_output=args.result_output,
            confirmations=args.confirm,
            valuation_tolerance=args.valuation_tolerance,
            ledger=args.ledger,
            hypothesis=args.hypothesis,
            history_output=args.history_output,
            dashboard_output=args.dashboard_output,
            doctor_interval_seconds=args.doctor_interval_seconds,
        )
        print(json.dumps({"ok": True, **plan}))
        return 0
    except Exception as error:
        fixed = isinstance(error, (LiveTaskError, PromotionError))
        reason = str(error) if fixed else "live_task_plan_failed"
        print(json.dumps({"ok": False, "reason": reason}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
