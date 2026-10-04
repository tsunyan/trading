"""Acceptance evidence files and the LiveApproval built from them. Never activates.

`read-evidence` saves one read-only two-sweep account collection as an evidence file.
`file-evidence` fingerprints an operator document (broker rules, identity, history); the
two read kinds are refused there because only `read-evidence` may produce them.
`approval` writes a LiveApproval for the journal's current activation context. Read
evidence is parsed and must be bound to this journal's account and configuration, and
each read kind must come from its own collection; every kind must be a distinct document
(the approval models enforce that too). Activation itself stays `live_setup activate`.

Threat model: these checks catch mislabelled, stale, foreign, duplicated or reused
evidence. They cannot authenticate a document against the local operator, who controls
the files, the journal and the code; a hand-built file in the exact format with a fresh
collection ID is indistinguishable from a collected one.
"""

import hashlib
import json
import os
import tempfile
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

from pydantic import AwareDatetime, Field

from trading.account_reader import AccountReader, AccountReadReport
from trading.broker_contracts import Contract
from trading.credential_store import CredentialParser, CredentialVault
from trading.live_journal import (
    CANCEL_EVIDENCE_KINDS,
    EVIDENCE_KINDS,
    AcceptanceEvidence,
    CancelApproval,
    LiveApproval,
    LiveRestartApproval,
    OrderResolutionApproval,
)
from trading.private_order_recovery import PrivateOrderRecovery
from trading.wire_validation import unique_object

READ_KINDS = frozenset({"read_acceptance", "account_baseline"})
READ_FORMAT = "trading.read-evidence/2"
MAX_DOCUMENT = 64 * 1024 * 1024
MAX_READ_AGE = timedelta(hours=24)


class ReadEvidence(Contract):
    """The only accepted shape of read_acceptance/account_baseline evidence."""

    format: Literal["trading.read-evidence/2"]
    kind: Literal["read_acceptance", "account_baseline"]
    collection_id: str = Field(pattern=r"^[a-f0-9]{32}$")
    collected_at: AwareDatetime
    account_id: str
    configuration_sha256: str
    report: AccountReadReport
    account_identity_verified: Literal[False]


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
    """An operator document. Read evidence must come from `read_evidence`/`read_document`."""
    if kind not in EVIDENCE_KINDS:
        raise LiveAcceptanceError("invalid_evidence_kind")
    if kind in READ_KINDS:
        raise LiveAcceptanceError("read_evidence_requires_collection")
    return _digest(kind, path)


def _digest(kind, path):
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
    if Path(output).exists():
        raise LiveAcceptanceError("evidence_file_exists")
    context = journal.activation_context()
    vault = vault if vault is not None else CredentialVault()
    try:
        with vault.open_client(
            reads, credential_reference, transport=transport, clock=clock
        ) as client:
            report = AccountReader(client, clock=clock).collect_account()
    except Exception:
        raise LiveAcceptanceError("account_collection_failed") from None
    if journal.activation_context() != context:
        # The binding written below must still be the journal's after the network reads.
        raise LiveAcceptanceError("journal_changed_during_collection")
    _write_new(output, read_body(kind, context, report, clock()))
    return _digest(kind, output)


def read_body(kind, context, report, collected_at, *, collection_id=None):
    """One collection's evidence. Its random ID lets an approval refuse one collection
    copied into both read kinds (changing `kind` alone changes the bytes and SHA-256)."""
    return (
        ReadEvidence(
            format=READ_FORMAT,
            kind=kind,
            collection_id=collection_id or uuid.uuid4().hex,
            collected_at=collected_at,
            account_id=context["account_id"],
            configuration_sha256=context["configuration_sha256"],
            report=report,
            account_identity_verified=False,
        )
        .model_dump_json(indent=2)
        .encode()
    )


def read_document(kind, path, journal, *, now):
    """Validate saved read evidence against this journal before fingerprinting it."""
    return _read_document(kind, path, journal, now=now)[0]


def _read_document(kind, path, journal, *, now):
    if kind not in READ_KINDS:
        raise LiveAcceptanceError("invalid_read_evidence_kind")
    evidence = _digest(kind, path)
    try:
        content = Path(path).read_bytes()
        json.loads(content, object_pairs_hook=unique_object)
        saved = ReadEvidence.model_validate_json(content)
    except Exception:
        raise LiveAcceptanceError("invalid_read_evidence_document") from None
    if hashlib.sha256(content).hexdigest() != evidence.sha256:
        raise LiveAcceptanceError("read_evidence_changed")
    context = journal.activation_context()
    if saved.kind != kind:
        raise LiveAcceptanceError("read_evidence_kind_mismatch")
    if (
        saved.account_id != context["account_id"]
        or saved.configuration_sha256 != context["configuration_sha256"]
    ):
        raise LiveAcceptanceError("read_evidence_not_bound_to_journal")
    if not timedelta(0) <= now - saved.collected_at <= MAX_READ_AGE:
        raise LiveAcceptanceError("read_evidence_not_recent")
    return evidence, saved.collection_id


def document(kind, path, journal, *, now):
    if kind in READ_KINDS:
        return read_document(kind, path, journal, now=now)
    return fingerprint(kind, path)


def documents(pairs, journal, *, now):
    """Fingerprint (kind, path) pairs; the read kinds must come from distinct collections."""
    items, collections = [], []
    for kind, path in pairs:
        if kind in READ_KINDS:
            item, collection = _read_document(kind, path, journal, now=now)
            collections.append(collection)
        else:
            item = fingerprint(kind, path)
        items.append(item)
    if len(set(collections)) != len(collections):
        raise LiveAcceptanceError("read_evidence_reused_collection")
    return items


def approval(journal, evidence, *, hours, now):
    """A LiveApproval for the journal's current fingerprints; it does not activate."""
    if type(hours) is not int or not 1 <= hours <= 7 * 24:
        raise LiveAcceptanceError("approval_hours_out_of_range")
    items = _evidence(evidence, EVIDENCE_KINDS)
    context = journal.activation_context()
    return LiveApproval(
        account_id=context["account_id"],
        configuration_sha256=context["configuration_sha256"],
        implementation_sha256=context["implementation_sha256"],
        accepted_at=now,
        expires_at=now + timedelta(hours=hours),
        evidence=items,
    ), context["revision"]


def _evidence(items, kinds):
    items = tuple(items)
    if sorted(e.kind for e in items) != sorted(kinds):
        raise LiveAcceptanceError("one_evidence_per_kind_required")
    if len({e.sha256 for e in items}) != len(items):
        raise LiveAcceptanceError("evidence_documents_must_differ")
    return tuple(sorted(items, key=lambda e: e.kind))


def checkpoint_approval(journal, kind, client_id, evidence, *, minutes, now):
    """A short-lived acceptance of one exact resolution or restricted cancel checkpoint."""
    if type(minutes) is not int or not 1 <= minutes <= 10:
        raise LiveAcceptanceError("approval_minutes_out_of_range")
    if kind == "resolution":
        context, model, kinds = (
            journal.order_resolution_context(client_id),
            OrderResolutionApproval,
            EVIDENCE_KINDS,
        )
    elif kind == "cancel":
        context, model, kinds = (
            journal.cancel_context(client_id),
            CancelApproval,
            CANCEL_EVIDENCE_KINDS,
        )
    else:
        raise LiveAcceptanceError("invalid_checkpoint_approval_kind")
    return model(
        account_id=context["account_id"],
        checkpoint_sha256=context["checkpoint_sha256"],
        accepted_at=now,
        expires_at=now + timedelta(minutes=minutes),
        evidence=_evidence(evidence, kinds),
    )


def restart_approval(journal, evidence, stop_review, *, hours, now):
    """A new activation for the current code plus the reviewed stop cause; never restarts."""
    if type(hours) is not int or not 1 <= hours <= 7 * 24:
        raise LiveAcceptanceError("approval_hours_out_of_range")
    context = journal.restart_context()
    review = _digest("history", stop_review)  # Any non-empty reviewed document.
    built = LiveRestartApproval(
        checkpoint_sha256=context["checkpoint_sha256"],
        approval=LiveApproval(
            account_id=context["account_id"],
            configuration_sha256=context["configuration_sha256"],
            implementation_sha256=context["implementation_sha256"],
            accepted_at=now,
            expires_at=now + timedelta(hours=hours),
            evidence=_evidence(evidence, EVIDENCE_KINDS),
        ),
        stop_review_reference=review.reference,
        stop_review_sha256=review.sha256,
    )
    return built, context["confirmations"]


def main(argv=None):
    parser = CredentialParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=(
            "read-evidence",
            "file-evidence",
            "approval",
            "restart-approval",
            "resolution-approval",
            "cancel-approval",
        ),
    )
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--read-control-directory", type=Path)
    parser.add_argument("--scope")
    parser.add_argument("--kind")
    parser.add_argument("--file", type=Path)
    parser.add_argument("--credential-reference")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--evidence", action="append", default=[])
    parser.add_argument("--hours", type=int)
    parser.add_argument("--minutes", type=int)
    parser.add_argument("--client-id")
    parser.add_argument("--stop-review", type=Path)
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
                items = documents(
                    [item.partition("=")[::2] for item in args.evidence], journal, now=now
                )
                if args.command in {"resolution-approval", "cancel-approval"}:
                    built = checkpoint_approval(
                        journal,
                        args.command.split("-")[0],
                        args.client_id,
                        items,
                        minutes=args.minutes,
                        now=now,
                    )
                    _write_new(args.output, built.model_dump_json(indent=2).encode())
                    print(
                        json.dumps(
                            {
                                "approval": str(args.output),
                                "expires_at": built.expires_at.isoformat(),
                                "applied": False,
                            }
                        )
                    )
                    return
                if args.command == "restart-approval":
                    built, confirmations = restart_approval(
                        journal, items, args.stop_review, hours=args.hours, now=now
                    )
                    _write_new(args.output, built.model_dump_json(indent=2).encode())
                    print(
                        json.dumps(
                            {
                                "approval": str(args.output),
                                "confirmations": confirmations,
                                "expires_at": built.approval.expires_at.isoformat(),
                                "restarted": False,
                            }
                        )
                    )
                    return
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
