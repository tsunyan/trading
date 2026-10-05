"""Repeat synthetic live path timings with fixed transport refusal reasons recorded."""

import hashlib
import json
import re
from pathlib import Path
from unittest.mock import patch

import benchmark_live_paths as benchmark
from benchmark_live_paths import measure as measure_paths

from trading import private_order


def recheck(segments, alerts, clock_mode):
    original_measured = benchmark.measured
    original_reason = private_order._reason
    stages, events = [], []

    def measured(operation):
        stage = "watchdog" if not stages else "dispatch"
        stages.append(stage)

        def reason(error):
            code = original_reason(error)
            if not isinstance(code, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", code):
                raise RuntimeError("recheck_fixed_reason_invalid")
            events.append({"stage": stage, "reason": code})
            return code

        with patch.object(private_order, "_reason", reason):
            return original_measured(operation)

    with patch.object(benchmark, "measured", measured):
        result = measure_paths(segments, alerts, clock_mode)
    if stages != ["watchdog", "dispatch"]:
        raise RuntimeError("recheck_stage_count_invalid")
    print(
        json.dumps(
            {
                "segments": segments,
                "alerts": alerts,
                "clock_mode": clock_mode,
                "dispatch_seconds": result["dispatch_seconds"],
                "mock_http_requests": result["mock_http_requests"],
                "fixed_reason_events": events,
            }
        ),
        flush=True,
    )
    return {
        **result,
        "fixed_reason_events": events,
        "recheck_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }


def main(argv=None):
    with patch.object(benchmark, "measure", recheck):
        return benchmark.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
