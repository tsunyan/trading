"""Expiring operator attestations for unattended live cycles. Never reads the broker.

The cycle's confirmations (complete account and history, account identity, external
writers paused) describe the broker account at a point in time; software cannot observe
them. An unattended task therefore never carries them as permanent arguments. The
operator writes an attestation bound to one live journal with a short expiry (at most
72 hours); the scheduled cycle uses it only while it is valid and fails closed after
that until the operator renews it.
"""

import argparse
import json
import os
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

from pydantic import AwareDatetime

from trading.broker_contracts import Contract
from trading.wire_validation import unique_object

FORMAT = "trading.live-attestation/1"
MAX_HOURS = 72
MAX_BYTES = 64 * 1024


class LiveAttestationError(ValueError):
    """Fixed local reasons only."""


class Attestation(Contract):
    format: Literal["trading.live-attestation/1"]
    live_instance: str
    confirmations: tuple[str, ...]
    attested_at: AwareDatetime
    expires_at: AwareDatetime


def attest(journal, path, *, confirmations, required, hours, now):
    """Write (or renew) the attestation for this journal; it changes nothing else."""
    if not isinstance(confirmations, (set, frozenset, tuple, list)) or set(confirmations) != set(
        required
    ):
        raise LiveAttestationError("attestation_confirmations_required")
    if type(hours) is not int or not 1 <= hours <= MAX_HOURS:
        raise LiveAttestationError("attestation_hours_out_of_range")
    built = Attestation(
        format=FORMAT,
        live_instance=journal.credential_binding()["live_instance"],
        confirmations=tuple(sorted(confirmations)),
        attested_at=now,
        expires_at=now + timedelta(hours=hours),
    )
    path = Path(path)
    handle, temporary = tempfile.mkstemp(dir=path.parent, prefix=".attest-", suffix=".tmp")
    try:
        with os.fdopen(handle, "wb") as output:
            output.write(built.model_dump_json(indent=2).encode())
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)  # Renewal replaces the previous attestation.
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise
    return built


def load(path):
    try:
        with Path(path).open("rb") as handle:
            content = handle.read(MAX_BYTES + 1)
        if len(content) > MAX_BYTES:
            raise ValueError
        json.loads(content, object_pairs_hook=unique_object)
        return Attestation.model_validate_json(content)
    except Exception:
        raise LiveAttestationError("attestation_unavailable") from None


def require(path, journal, *, required, now):
    """The attested confirmations, only while valid for exactly this journal."""
    saved = load(path)
    if saved.live_instance != journal.credential_binding()["live_instance"]:
        raise LiveAttestationError("attestation_not_bound_to_journal")
    if set(saved.confirmations) != set(required):
        raise LiveAttestationError("attestation_confirmations_required")
    if saved.expires_at - saved.attested_at > timedelta(hours=MAX_HOURS):
        raise LiveAttestationError("attestation_hours_out_of_range")
    if not saved.attested_at <= now < saved.expires_at:
        raise LiveAttestationError("attestation_expired")
    return saved


def main(argv=None):
    from trading.live_cycle import CYCLE_CONFIRMATIONS
    from trading.order_runtime import OrderRuntime

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("attest", "status"))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--read-control-directory", type=Path, required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--hours", type=int)
    parser.add_argument("--confirm", action="append", default=[])
    args = parser.parse_args(argv)
    try:
        journal = OrderRuntime(args.directory, args.read_control_directory, args.scope).journal
        now = datetime.now(UTC)
        if args.command == "attest":
            built = attest(
                journal,
                args.output,
                confirmations=args.confirm,
                required=CYCLE_CONFIRMATIONS,
                hours=args.hours,
                now=now,
            )
        else:
            built = require(args.output, journal, required=CYCLE_CONFIRMATIONS, now=now)
        print(
            json.dumps(
                {
                    "attestation": str(args.output),
                    "confirmations": list(built.confirmations),
                    "expires_at": built.expires_at.isoformat(),
                }
            )
        )
    except LiveAttestationError as error:
        parser.exit(2, f"live_attestation_failed: {error}\n")
    except Exception as error:
        parser.exit(2, f"live_attestation_failed: {type(error).__name__}\n")


if __name__ == "__main__":
    main()
