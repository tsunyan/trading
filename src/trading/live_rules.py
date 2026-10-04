"""Check live order limits against GMO's public USD_JPY trading rules. No credentials or orders.

`check` compares a live configuration file (or an existing journal's fixed limits) with
the public `symbols` rules and lists every conflict. `--output` saves the fetched rules as
an immutable evidence file that `live_acceptance file-evidence --kind rules` can fingerprint.
"""

import argparse
import json
import os
import tempfile
import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import httpx

from trading.gmo import PUBLIC_URL
from trading.wire_validation import decimal_string, unique_object

MAX_SYMBOLS_BYTES = 65536
SYMBOL = "USD_JPY"


class LiveRulesError(ValueError):
    """Fixed local reasons only; response bodies are never echoed."""


def parse_rules(content):
    try:
        payload = json.loads(content, object_pairs_hook=unique_object)
        if payload.get("status") != 0 or not isinstance(payload.get("data"), list):
            raise ValueError
        rows = [r for r in payload["data"] if isinstance(r, dict) and r.get("symbol") == SYMBOL]
        if len(rows) != 1:
            raise ValueError
        row = rows[0]
        rules = {
            "min_open_order_size": decimal_string(row["minOpenOrderSize"]),
            "max_order_size": decimal_string(row["maxOrderSize"]),
            "size_step": decimal_string(row["sizeStep"]),
            "tick_size": decimal_string(row["tickSize"]),
        }
    except Exception:
        raise LiveRulesError("invalid_public_symbols") from None
    if any(value <= 0 for value in rules.values()):
        raise LiveRulesError("invalid_public_symbols")
    return rules


def fetch_rules(*, transport=None, monotonic=time.monotonic):
    started = monotonic()
    try:
        with httpx.Client(
            transport=transport, timeout=5.0, follow_redirects=False, trust_env=False
        ) as client:
            with client.stream("GET", f"{PUBLIC_URL}/symbols") as response:
                if response.status_code != 200:
                    raise LiveRulesError("unexpected_public_symbols_status")
                kind = response.headers.get("content-type", "").split(";", 1)[0].strip()
                if kind != "application/json":
                    raise LiveRulesError("expected_public_symbols_json")
                content = bytearray()
                for chunk in response.iter_bytes():
                    if len(content) + len(chunk) > MAX_SYMBOLS_BYTES:
                        raise LiveRulesError("public_symbols_too_large")
                    content.extend(chunk)
                    if monotonic() - started > 5:
                        raise LiveRulesError("public_symbols_deadline_exceeded")
    except LiveRulesError:
        raise
    except Exception:
        raise LiveRulesError("public_symbols_unavailable") from None
    return parse_rules(bytes(content))


def conflicts(limits, rules):
    """Every way the fixed limits could produce an order the broker refuses."""
    problems = []
    if Decimal(limits.min_units) < rules["min_open_order_size"]:
        problems.append("min_units_below_broker_minimum")
    if Decimal(limits.max_units) > rules["max_order_size"]:
        problems.append("max_units_above_broker_maximum")
    if Decimal(limits.unit_step) % rules["size_step"]:
        problems.append("unit_step_not_multiple_of_broker_step")
    if Decimal(limits.min_units) % rules["size_step"]:
        problems.append("min_units_not_multiple_of_broker_step")
    if limits.price_tick % rules["tick_size"]:
        problems.append("price_tick_not_multiple_of_broker_tick")
    return problems


def _write_new(path, body):
    """Publish complete evidence only: write and fsync a private temporary file, then link it
    into place. Linking fails instead of replacing a file that exists meanwhile, and a
    crash never leaves a partial file at the final path."""
    path = Path(path)
    if path.exists():
        raise FileExistsError(path.name)
    handle, temporary = tempfile.mkstemp(dir=path.parent, prefix=".rules-", suffix=".tmp")
    try:
        with os.fdopen(handle, "wb") as output:
            output.write(body)
            output.flush()
            os.fsync(output.fileno())
        os.link(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("check",))
    parser.add_argument("--config", type=Path)
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--read-control-directory", type=Path)
    parser.add_argument("--scope")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        if (args.config is None) == (args.directory is None):
            raise LiveRulesError("config_or_journal_required")
        if args.config is not None:
            from trading.live_setup import LiveConfiguration, _load

            limits = _load(args.config, LiveConfiguration).limits
        else:
            from trading.private_order_recovery import PrivateOrderRecovery

            limits = PrivateOrderRecovery(
                args.directory, args.read_control_directory, args.scope
            ).journal.limits
        rules = fetch_rules()
        problems = conflicts(limits, rules)
        result = {
            "symbol": SYMBOL,
            "fetched_at": datetime.now(UTC).isoformat(),
            "rules": {k: format(v, "f") for k, v in rules.items()},
            "limits": limits.model_dump(mode="json"),
            "conflicts": problems,
            "credentials_used": False,
        }
        if args.output is not None:
            _write_new(args.output, json.dumps(result, sort_keys=True).encode())
    except Exception as error:
        reason = str(error) if isinstance(error, LiveRulesError) else type(error).__name__
        parser.exit(2, f"live_rules_failed: {reason}\n")
    print(json.dumps(result))
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
