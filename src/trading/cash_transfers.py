"""Declared, normalized statement evidence; no broker API or funds transfer."""

from decimal import Decimal
from typing import Annotated, Literal

from pydantic import AwareDatetime, Field, model_validator

from trading.broker_contracts import Contract

Source = Annotated[str, Field(pattern=r"^[A-Za-z0-9_-]{1,64}$", max_length=64)]
Reference = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$", max_length=128)]


class CashTransferPolicy(Contract):
    primary_source: Source
    confirmation_source: Source

    @model_validator(mode="after")
    def distinct_sources(self):
        if self.primary_source == self.confirmation_source:
            raise ValueError("cash_transfer_sources_must_differ")
        return self


class CashTransferRecord(Contract):
    transfer_id: Reference
    kind: Literal["DEPOSIT", "WITHDRAWAL"]
    amount: Decimal = Field(gt=0)
    fee_debit: Decimal = Field(default=Decimal(0), ge=0)
    occurred_at: AwareDatetime
    currency: Literal["JPY"] = "JPY"
    status: Literal["SETTLED"] = "SETTLED"


class CashTransferEvidence(Contract):
    source: Source
    reference: Reference
    document_sha256: str = Field(pattern=r"^[a-f0-9]{64}$", max_length=64)
    observed_at: AwareDatetime
    record: CashTransferRecord


class CashTransferMatch(Contract):
    primary: CashTransferEvidence
    confirmation: CashTransferEvidence


def transfer_identity(match):
    # Document versions / observation dates may change on a repeated reading.
    # Native transaction references and the economic record may not change.
    return (match.primary.record, match.primary.reference, match.confirmation.reference)


def check_transfer(match, policy, cutoff):
    primary, confirmation = match.primary, match.confirmation
    if (
        primary.source != policy.primary_source
        or confirmation.source != policy.confirmation_source
        or primary.document_sha256 == confirmation.document_sha256
        or primary.record != confirmation.record
        or primary.record.occurred_at <= cutoff
        or primary.observed_at < primary.record.occurred_at
        or confirmation.observed_at < primary.record.occurred_at
    ):
        raise ValueError("cash_transfer_evidence_not_matched")
