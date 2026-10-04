"""Acceptance evidence files and the LiveApproval built from them. Never activates.

`read-evidence` saves one read-only two-sweep account collection as an evidence file.
`file-evidence` fingerprints an operator document (broker rules, identity, history).
`approval` writes a LiveApproval for the journal's current activation context from
those fingerprints. Activation itself stays `live_setup activate` with confirmations.
"""

import hashlib
import json
import os
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

from trading.account_reader import AccountReader
from trading.credential_store import CredentialParser, CredentialVault
from trading.live_journal import EVIDENCE_KINDS, AcceptanceEvidence, LiveApproval
from trading.private_order_recovery import PrivateOrderRecovery

READ_KINDS = frozenset({"read_acceptance", "account_baseline"})
MAX_DOCUMENT = 64 * 1024 * 1024


class LiveAcceptanceError(ValueError):
    """Fixed local reasons only; evidence contents are never echoed."""


def _write_new(path, body):
    """Evidence is immutable: never overwrite an existing file."""
    path = Path(path)
    if path.exists():
        raise LiveAcceptanceError("evidence_file_exists")
    handle, temporary = tempfile.mkstemp(dir=path.parent, prefix=".evidence-", suffix=".tmp")
    try:
        with os.fdopen(handle, "wb") as output:
            output.write(body)
            output.flush()
            os.fsync(output.fileno())
        os.link(temporary, path)  # Fails instead of replacing a file created meanwhile.
    finally:
        Path(temporary).unlink(missing_ok=True)


def fingerprint(kind, path):
    if kind not in EVIDENCE_KINDS:
        raise LiveAcceptanceError("invalid_evidence_kind")
    path = Path(path)
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            size += len(chunk)
            if size > MAX_DOCUMENT:
                raise LiveAcceptanceError("evidence_document_too_large")
            digest.update(chunk)
    if size == 0:
        raise LiveAcceptanceError("empty_evidence_document")
    return AcceptanceEvidence(kind=kind, reference=path.name, sha256=digest.hexdigest())


def read_evidence(
    journal, reads, kind, credential_reference, output, *, clock, vault=None, transport=None
):
    """One read-only account collection bound to this journal's stores, saved immutably."""
    if kind not in READ_KINDS:
        raise LiveAcceptanceError("invalid_read_evidence_kind")
    context = journal.activation_context()
    vault = vault if vault is not None else CredentialVault()
    try:
        with vault.open_client(
            reads, credential_reference, transport=transport, clock=clock
        ) as client:
            report = AccountReader(client, clock=clock).collect_account()
    except Exception:
        raise LiveAcceptanceError("account_collection_failed") from None
    body = json.dumps(
        {
            "kind": kind,
            "collected_at": clock().isoformat(),
            "account_id": context["account_id"],
            "configuration_sha256": context["configuration_sha256"],
            "report": report.model_dump(mode="json"),
            "account_identity_verified": False,
        },
        sort_keys=True,
        ensure_ascii=False,
    ).encode()
    _write_new(output, body)
    return fingerprint(kind, output)


def approval(journal, evidence, *, hours, now):
    """A LiveApproval for the journal's current fingerprints; it does not activate."""
    if type(hours) is not int or not 1 <= hours <= 7 * 24:
        raise LiveAcceptanceError("approval_hours_out_of_range")
    items = tuple(evidence)
    if sorted(e.kind for e in items) != sorted(EVIDENCE_KINDS):
        raise LiveAcceptanceError("one_evidence_per_kind_required")
    context = journal.activation_context()
    return LiveApproval(
        account_id=context["account_id"],
        configuration_sha256=context["configuration_sha256"],
        implementation_sha256=context["implementation_sha256"],
        accepted_at=now,
        expires_at=now + timedelta(hours=hours),
        evidence=tuple(sorted(items, key=lambda e: e.kind)),
    ), context["revision"]


def main(argv=None):
    parser = CredentialParser(description=__doc__)
    parser.add_argument("command", choices=("read-evidence", "file-evidence", "approval"))
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--read-control-directory", type=Path)
    parser.add_argument("--scope")
    parser.add_argument("--kind")
    parser.add_argument("--file", type=Path)
    parser.add_argument("--credential-reference")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--evidence", action="append", default=[])
    parser.add_argument("--hours", type=int)
    args = parser.parse_args(argv)
    try:
        if args.command == "file-evidence":
            result = fingerprint(args.kind, args.file).model_dump(mode="json")
        else:
            recovery = PrivateOrderRecovery(args.directory, args.read_control_directory, args.scope)
            journal, now = recovery.journal, datetime.now(UTC)
            if args.command == "read-evidence":
                result = read_evidence(
                    journal,
                    recovery.reads,
                    args.kind,
                    args.credential_reference,
                    args.output,
                    clock=lambda: datetime.now(UTC),
                ).model_dump(mode="json")
            else:
                items = []
                for item in args.evidence:
                    kind, _, path = item.partition("=")
                    items.append(fingerprint(kind, path))
                built, revision = approval(journal, items, hours=args.hours, now=now)
                _write_new(args.output, built.model_dump_json(indent=2).encode())
                result = {
                    "approval": str(args.output),
                    "expected_revision": revision,
                    "expires_at": built.expires_at.isoformat(),
                    "activated": False,
                }
        print(json.dumps(result, ensure_ascii=False))
    except Exception as error:
        reason = str(error) if isinstance(error, LiveAcceptanceError) else type(error).__name__
        parser.exit(2, f"live_acceptance_failed: {reason}\n")


if __name__ == "__main__":
    main()
