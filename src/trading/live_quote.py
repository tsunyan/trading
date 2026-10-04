"""Fetch one public USD_JPY ticker as an exact order-review quote. No credentials, no orders."""

import argparse
import json
import os
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx

from trading.account_guard import AccountQuote
from trading.gmo import PUBLIC_URL
from trading.wire_validation import decimal_string, timestamp_string, unique_object

MAX_TICKER_BYTES = 16384
MAX_FUTURE_SKEW = timedelta(seconds=2)
SYMBOL = "USD_JPY"


class LiveQuoteError(ValueError):
    """Fixed local reasons only; ticker bodies are never echoed."""


def parse_ticker(content, *, received_at):
    """The exact USD_JPY row. Missing, duplicated or malformed rows are refused, not repaired."""
    try:
        payload = json.loads(content, object_pairs_hook=unique_object)
        if (
            not isinstance(payload, dict)
            or payload.get("status") != 0
            or not isinstance(payload.get("data"), list)
        ):
            raise ValueError
        rows = [
            row for row in payload["data"] if isinstance(row, dict) and row.get("symbol") == SYMBOL
        ]
        if len(rows) != 1:
            raise ValueError
        row = rows[0]
        if not {"symbol", "bid", "ask", "timestamp", "status"} <= set(row):
            raise ValueError
        if row["status"] not in {"OPEN", "CLOSE"}:
            raise ValueError
        observed_at = timestamp_string(row["timestamp"]).astimezone(UTC)
        quote = AccountQuote(
            bid=decimal_string(row["bid"]),
            ask=decimal_string(row["ask"]),
            observed_at=observed_at,
            market_open=row["status"] == "OPEN",
        )
    except Exception:
        raise LiveQuoteError("invalid_public_ticker") from None
    if observed_at > received_at + MAX_FUTURE_SKEW:
        raise LiveQuoteError("public_ticker_from_future")
    return quote


STATUSES = frozenset({"OPEN", "CLOSE", "MAINTENANCE"})


def _get_json(path, *, transport, monotonic, prefix):
    started = monotonic()
    try:
        with httpx.Client(
            transport=transport, timeout=5.0, follow_redirects=False, trust_env=False
        ) as client:
            with client.stream("GET", f"{PUBLIC_URL}/{path}") as response:
                if response.status_code != 200:
                    raise LiveQuoteError(f"unexpected_public_{prefix}_status")
                kind = response.headers.get("content-type", "").split(";", 1)[0].strip()
                if kind != "application/json":
                    raise LiveQuoteError(f"expected_public_{prefix}_json")
                content = bytearray()
                for chunk in response.iter_bytes():
                    if len(content) + len(chunk) > MAX_TICKER_BYTES:
                        raise LiveQuoteError(f"public_{prefix}_too_large")
                    content.extend(chunk)
                    if monotonic() - started > 5:
                        raise LiveQuoteError(f"public_{prefix}_deadline_exceeded")
    except LiveQuoteError:
        raise
    except Exception:
        raise LiveQuoteError(f"public_{prefix}_unavailable") from None
    return bytes(content)


def parse_status(content):
    """GMO FX service state: OPEN, CLOSE or MAINTENANCE (the ticker never says MAINTENANCE)."""
    try:
        payload = json.loads(content, object_pairs_hook=unique_object)
        if payload.get("status") != 0 or not isinstance(payload.get("data"), dict):
            raise ValueError
        state = payload["data"]["status"]
        if state not in STATUSES:
            raise ValueError
        return state
    except Exception:
        raise LiveQuoteError("invalid_public_service_status") from None


def fetch_status(*, transport=None, monotonic=time.monotonic):
    return parse_status(
        _get_json("status", transport=transport, monotonic=monotonic, prefix="service")
    )


def fetch_quote(*, transport=None, clock=lambda: datetime.now(UTC), monotonic=time.monotonic):
    """One unauthenticated GET to the fixed public host; staleness stays with the risk gate."""
    started = monotonic()
    try:
        with httpx.Client(
            transport=transport, timeout=5.0, follow_redirects=False, trust_env=False
        ) as client:
            with client.stream("GET", f"{PUBLIC_URL}/ticker") as response:
                if response.status_code != 200:
                    raise LiveQuoteError("unexpected_public_ticker_status")
                kind = response.headers.get("content-type", "").split(";", 1)[0].strip()
                if kind != "application/json":
                    raise LiveQuoteError("expected_public_ticker_json")
                content = bytearray()
                for chunk in response.iter_bytes():
                    if len(content) + len(chunk) > MAX_TICKER_BYTES:
                        raise LiveQuoteError("public_ticker_too_large")
                    content.extend(chunk)
                    if monotonic() - started > 5:
                        raise LiveQuoteError("public_ticker_deadline_exceeded")
    except LiveQuoteError:
        raise
    except Exception:
        raise LiveQuoteError("public_ticker_unavailable") from None
    return parse_ticker(bytes(content), received_at=clock())


def write_quote(quote, path):
    """Atomic replace; a reader sees the previous file or the new one, never a partial quote."""
    path = Path(path)
    body = json.dumps(quote.model_dump(mode="json"), sort_keys=True).encode()
    handle, temporary = tempfile.mkstemp(dir=path.parent, prefix=".quote-", suffix=".tmp")
    try:
        with os.fdopen(handle, "wb") as output:
            output.write(body)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise
    return body


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        quote = fetch_quote()
        write_quote(quote, args.output)
        print(
            json.dumps(
                {**quote.model_dump(mode="json"), "network_used": True, "credentials_used": False}
            )
        )
    except Exception as error:
        reason = str(error) if isinstance(error, LiveQuoteError) else "quote_write_failed"
        parser.exit(2, f"Public quote failed: {reason}\n")


if __name__ == "__main__":
    main()
