"""Dedicated, explicitly enabled live journal. Offline stores are never adopted."""

import hashlib
import json
import sqlite3
import sys
import threading
import uuid
from contextlib import closing, contextmanager
from datetime import UTC, datetime, timedelta
from importlib.metadata import version
from pathlib import Path
from typing import Literal

from pydantic import AwareDatetime, Field, model_validator

from trading.account_guard import AccountPolicy
from trading.account_reader import OrderReadReport
from trading.broker_contracts import (
    Contract,
    OrderEvidence,
    OrderIntent,
    OrderLimits,
    cancel_request,
    order_request,
    validate_evidence,
)
from trading.order_journal import SCHEMA, OrderBlocked, OrderJournal
from trading.order_receipts import CancellationReceipt
from trading.post_control import PersistentPostLimiter
from trading.storage_init import new_storage_directory

MODE = "live-execution-v1"
CONFIRMATIONS = frozenset(
    {
        "live-orders",
        "account-identity",
        "broker-rules",
        "read-acceptance",
        "complete-account",
        "external-writers-paused",
    }
)
EVIDENCE_KINDS = frozenset({"identity", "rules", "read_acceptance", "account_baseline", "history"})
CODE_FILES = (
    "live_journal.py",
    "private_order.py",
    "post_control.py",
    "order_journal.py",
    "order_receipts.py",
    "account_guard.py",
    "broker_contracts.py",
    "wire_validation.py",
    "order_states.py",
    "private_stream_token.py",
    "read_control.py",
    "storage_init.py",
)


class LiveOrderError(OrderBlocked):
    """Fixed reason codes; no credentials or remote responses."""


def _body(value):
    return json.dumps(value.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def _hash(value):
    return hashlib.sha256(value.encode()).hexdigest()


def implementation_sha256():
    """Freeze the code actually used to sign, claim, check risk and bind receipts."""
    return _hash(
        json.dumps(
            {
                "files": [
                    [name, hashlib.sha256((Path(__file__).parent / name).read_bytes()).hexdigest()]
                    for name in CODE_FILES
                ],
                "runtime": {
                    "python": sys.version,
                    "httpx": version("httpx"),
                    "pydantic": version("pydantic"),
                },
            },
            separators=(",", ":"),
        )
    )


class AcceptanceEvidence(Contract):
    kind: Literal["identity", "rules", "read_acceptance", "account_baseline", "history"]
    reference: str = Field(min_length=1, max_length=256)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def printable(self):
        if any(ord(c) < 32 or ord(c) == 127 for c in self.reference):
            raise ValueError("invalid_acceptance_reference")
        return self


class LiveApproval(Contract):
    """Operator supplied acceptance references, not an automatic broker attestation."""

    account_id: str = Field(min_length=1, max_length=100)
    configuration_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    implementation_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    accepted_at: AwareDatetime = Field(strict=True)
    expires_at: AwareDatetime = Field(strict=True)
    evidence: tuple[AcceptanceEvidence, ...]

    @model_validator(mode="after")
    def coherent(self):
        if (
            not self.accepted_at < self.expires_at <= self.accepted_at + timedelta(days=7)
            or len(self.evidence) != len(EVIDENCE_KINDS)
            or {e.kind for e in self.evidence} != EVIDENCE_KINDS
        ):
            raise ValueError("invalid_live_approval")
        return self


class LiveState(Contract):
    version: Literal[1] = 1
    mode: Literal["live-execution-v1"] = MODE
    instance: str = Field(pattern=r"^[a-f0-9]{32}$")
    post_instance: str = Field(pattern=r"^[a-f0-9]{32}$")
    read_instance: str = Field(pattern=r"^[a-f0-9]{32}$")
    scope: str = Field(pattern=r"^[A-Za-z0-9_-]{1,64}$")
    post_path: str
    limits: OrderLimits
    policy: AccountPolicy
    implementation_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    configuration_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    phase: Literal["DISABLED", "ENABLED", "STOPPED"] = "DISABLED"
    approval: LiveApproval | None = None
    revision: int = Field(default=0, strict=True, ge=0)


def _configuration(state):
    return _hash(
        json.dumps(
            {
                key: state.model_dump(mode="json")[key]
                for key in (
                    "mode",
                    "instance",
                    "post_instance",
                    "read_instance",
                    "scope",
                    "post_path",
                    "limits",
                    "policy",
                    "implementation_sha256",
                )
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )


class LiveOrderJournal(OrderJournal):
    """Reuse lifecycle/risk calculations with a separate mode, file and bound owner.

    Approval and complete normalized account inputs are trusted operator/provider
    evidence. A read-only diagnostic is never promoted to that input here.
    """

    def __init__(self, directory, posts, *, clock=lambda: datetime.now(UTC)):
        if not isinstance(posts, PersistentPostLimiter):
            raise LiveOrderError("explicit_persistent_posts_required")
        self.path = Path(directory).resolve() / "live-orders.sqlite"
        self.posts, self.clock = posts, clock
        self._lock = threading.RLock()
        self._instance = None
        with self._transaction() as conn:
            state = self._live_state(conn)
        self._instance = state.instance
        self.limits = state.limits

    @classmethod
    def create(cls, directory, posts, limits, policy, *, clock=lambda: datetime.now(UTC)):
        if not isinstance(posts, PersistentPostLimiter):
            raise LiveOrderError("explicit_persistent_posts_required")
        posts.check()
        if posts.execution_binding() is not None or posts.snapshot()["blocked"]:
            raise LiveOrderError("live_journal_binding_unavailable")
        limits = OrderLimits.model_validate(limits.model_dump())
        policy = AccountPolicy.model_validate(policy.model_dump())
        post = posts.snapshot()
        state = LiveState(
            instance=uuid.uuid4().hex,
            post_instance=post["instance"],
            read_instance=post["read_instance"],
            scope=post["scope"],
            post_path=str(posts.path),
            limits=limits,
            policy=policy,
            implementation_sha256=implementation_sha256(),
            configuration_sha256="0" * 64,
        )
        state = state.model_copy(update={"configuration_sha256": _configuration(state)})
        directory = Path(directory).resolve()
        with new_storage_directory(directory, ("live-orders.sqlite-journal", "live-orders.sqlite")):
            with closing(sqlite3.connect(directory / "live-orders.sqlite")) as conn:
                conn.execute("PRAGMA synchronous=FULL")
                conn.executescript(SCHEMA)
                conn.execute("CREATE INDEX events_client_kind ON events(client_id,kind)")
                conn.execute(
                    "CREATE TABLE live_control(id INTEGER PRIMARY KEY CHECK(id=1),"
                    "body TEXT,digest TEXT)"
                )
                conn.execute(
                    "INSERT INTO metadata VALUES(1,?,?,0)", (MODE, limits.model_dump_json())
                )
                conn.execute(
                    "INSERT INTO account_gate VALUES(1,?,NULL,?,0)",
                    (policy.model_dump_json(), str(policy.starting_balance)),
                )
                conn.execute(
                    "INSERT INTO live_control VALUES(1,?,?)", (_body(state), _hash(_body(state)))
                )
                conn.commit()
        # Interrupted/unbound stores are retained; never adopt or recreate them.
        posts.bind_execution(state.instance, directory)
        return cls(directory, posts, clock=clock)

    def _live_state(self, conn):
        rows = conn.execute("SELECT id,body,digest FROM live_control LIMIT 2").fetchall()
        try:
            if len(rows) != 1 or rows[0]["id"] != 1:
                raise ValueError
            state = LiveState.model_validate_json(rows[0]["body"])
            meta = conn.execute("SELECT * FROM metadata").fetchall()
            gate = conn.execute("SELECT * FROM account_gate").fetchall()
            if (
                _body(state) != rows[0]["body"]
                or _hash(_body(state)) != rows[0]["digest"]
                or _configuration(state) != state.configuration_sha256
                or self._instance not in {None, state.instance}
                or len(meta) != 1
                or meta[0]["id"] != 1
                or meta[0]["mode"] != MODE
                or meta[0]["halted"] not in {0, 1}
                or OrderLimits.model_validate_json(meta[0]["limits_json"]) != state.limits
                or len(gate) != 1
                or gate[0]["id"] != 1
                or AccountPolicy.model_validate_json(gate[0]["policy_json"]) != state.policy
            ):
                raise ValueError
            post = self.posts.snapshot()
            if (
                post["instance"] != state.post_instance
                or post["read_instance"] != state.read_instance
                or post["scope"] != state.scope
                or str(self.posts.path) != state.post_path
                or self.posts.execution_binding()
                != {"instance": state.instance, "path": str(self.path.parent)}
            ):
                raise ValueError
            if state.approval is not None and (
                state.approval.account_id != state.policy.account_id
                or state.approval.configuration_sha256 != state.configuration_sha256
                or state.approval.implementation_sha256 != state.implementation_sha256
            ):
                raise ValueError
            if (state.phase == "ENABLED") != (
                state.approval is not None and state.phase != "STOPPED"
            ):
                raise ValueError
            return state
        except (ValueError, TypeError, KeyError, OSError):
            raise LiveOrderError("live_journal_integrity_or_binding_failed") from None

    @contextmanager
    def _transaction(self):
        with self._lock, super()._transaction() as conn:
            self._live_state(conn)
            yield conn

    @contextmanager
    def _mutation(self):
        with self._lock:
            if self.posts.owns_operation():
                yield
            else:
                with self.posts._ownership():
                    yield

    def _write_live(self, conn, state, **changes):
        updated = LiveState.model_validate(
            {
                **state.model_dump(),
                **changes,
                "revision": state.revision + 1,
            }
        )
        conn.execute(
            "UPDATE live_control SET body=?,digest=? WHERE id=1",
            (_body(updated), _hash(_body(updated))),
        )
        return updated

    @staticmethod
    def _current_implementation():
        try:
            return implementation_sha256()
        except (OSError, ImportError):
            raise LiveOrderError("live_implementation_unavailable") from None

    def _activation_state(self, state):
        candidate = state.model_copy(
            update={"implementation_sha256": self._current_implementation()}
        )
        return candidate.model_copy(update={"configuration_sha256": _configuration(candidate)})

    def activation_context(self):
        """Read current approval fingerprints. Never renew approval or rewrite saved state."""
        with self._transaction() as conn:
            state = self._activation_state(self._live_state(conn))
            return {
                "account_id": state.policy.account_id,
                "configuration_sha256": state.configuration_sha256,
                "implementation_sha256": state.implementation_sha256,
                "revision": state.revision,
            }

    def activate(self, approval, *, expected_revision, confirmations, now=None):
        approval = LiveApproval.model_validate(approval.model_dump())
        now = self._clock(now if now is not None else self.clock())
        if frozenset(confirmations) != CONFIRMATIONS:
            raise LiveOrderError("explicit_live_acceptance_confirmations_required")
        with self._mutation(), self._transaction() as conn:
            state = self._live_state(conn)
            candidate = self._activation_state(state)
            if (
                state.phase == "STOPPED"
                or state.revision != expected_revision
                or self.posts.snapshot()["blocked"]
                or self.posts.reads.status()["blocked"]
                or approval.account_id != state.policy.account_id
                or approval.configuration_sha256 != candidate.configuration_sha256
                or approval.implementation_sha256 != candidate.implementation_sha256
                or not approval.accepted_at <= now < approval.expires_at
            ):
                raise LiveOrderError("live_activation_refused")
            if conn.execute("SELECT halted FROM metadata").fetchone()[0] or any(
                row[0] not in {"PREPARED", "ABANDONED", "FILLED", "CANCELED", "EXPIRED"}
                for row in conn.execute("SELECT state FROM orders")
            ):
                raise LiveOrderError("live_activation_refused")
            self._write_live(
                conn,
                state,
                implementation_sha256=candidate.implementation_sha256,
                configuration_sha256=candidate.configuration_sha256,
                phase="ENABLED",
                approval=approval,
            )
            self._event(
                conn,
                None,
                "LIVE_ACTIVATED",
                {
                    "approval": approval.model_dump(mode="json"),
                    "previous_implementation_sha256": state.implementation_sha256,
                    "previous_configuration_sha256": state.configuration_sha256,
                },
            )

    def _authorize(self, conn, now):
        state = self._live_state(conn)
        if (
            state.phase != "ENABLED"
            or state.implementation_sha256 != self._current_implementation()
            or state.approval is None
            or not state.approval.accepted_at <= now < state.approval.expires_at
            or self.posts.reads.status()["blocked"]
            or conn.execute("SELECT halted FROM metadata").fetchone()[0]
        ):
            raise LiveOrderError("live_orders_not_enabled")
        return state

    def request(self, client_id):
        with self._transaction() as conn:
            self._authorize(conn, self._clock(self.clock()))
            row = self._row(conn, client_id)
            if row["state"] != "PREPARED":
                raise LiveOrderError("live_submission_already_claimed")
            plan = order_request(OrderIntent.model_validate_json(row["intent_json"]), self.limits)
            self._unclaimed(conn, client_id, plan)
            return plan

    def _unclaimed(self, conn, client_id, plan):
        if conn.execute(
            "SELECT 1 FROM events WHERE client_id=? AND kind='SUBMITTING' LIMIT 1", (client_id,)
        ).fetchone():
            raise LiveOrderError("live_submission_already_claimed")
        prepared = conn.execute(
            "SELECT payload_json FROM events WHERE client_id=? AND kind='PREPARED' LIMIT 2",
            (client_id,),
        ).fetchall()
        if len(prepared) != 1 or json.loads(prepared[0][0]) != {
            "path": plan.path,
            "body": json.loads(plan.body),
        }:
            raise LiveOrderError("live_prepared_plan_integrity_failed")

    def _account_proof(self, conn):
        gate = self._gate(conn)
        if not gate["proof_json"]:
            raise LiveOrderError("live_account_proof_required")
        proof = json.loads(gate["proof_json"])
        row = conn.execute(
            "SELECT payload_json FROM events WHERE kind='ACCOUNT_RECONCILED' "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row is None:
            raise LiveOrderError("live_account_proof_integrity_failed")
        saved = json.loads(row[0])
        if any(saved[key] != proof[key] for key in ("revision", "snapshot", "quote")) or (
            proof["snapshot"]["complete"] is not True
        ):
            raise LiveOrderError("live_account_proof_integrity_failed")

    def prepare(self, intent):
        with self._mutation():
            return super().prepare(intent)

    def update_account(self, snapshot, quote, *, now=None):
        with self._mutation():
            return super().update_account(
                snapshot, quote, now=now if now is not None else self.clock()
            )

    def begin_submission(self, client_id, *, quote=None, now=None):
        now = self._clock(now if now is not None else self.clock())
        with self._mutation(), self._transaction() as conn:
            self._authorize(conn, now)
            row = self._row(conn, client_id)
            plan = order_request(OrderIntent.model_validate_json(row["intent_json"]), self.limits)
            self._unclaimed(conn, client_id, plan)
            self._account_proof(conn)
            self.posts.require_operation(
                "order" if plan.path == "/v1/order" else "close_order",
                hashlib.sha256(plan.body).hexdigest(),
            )
            request, blocked = self._begin_submission(conn, client_id, quote, now)
        if blocked is not None:
            raise LiveOrderError("live_account_risk_refused")
        return request

    def validate_dispatch(self, client_id, plan):
        with self._transaction() as conn:
            self._authorize(conn, self._clock(self.clock()))
            row = self._row(conn, client_id)
            if row["state"] != "SUBMITTING" or plan != order_request(
                OrderIntent.model_validate_json(row["intent_json"]), self.limits
            ):
                raise LiveOrderError("live_dispatch_claim_mismatch")
            self.posts.require_operation(
                "order" if plan.path == "/v1/order" else "close_order",
                hashlib.sha256(plan.body).hexdigest(),
            )

    def reconcile(self, evidence):
        with self._mutation():
            return super().reconcile(evidence)

    def order_recovery_context(self, client_id):
        """Local checkpoint for GET investigation; never infer an absent order."""
        with self._transaction() as conn:
            return self._order_recovery_context(conn, client_id)

    def _order_recovery_context(self, conn, client_id):
        state = self._live_state(conn)
        row = dict(self._row(conn, client_id))
        intent = OrderIntent.model_validate_json(row["intent_json"])
        order_plan = order_request(intent, state.limits)
        plan = order_plan
        post = self.posts.snapshot()
        operation = "order" if intent.effect == "OPEN" else "close_order"
        if post["operation"] == "cancel":
            if not row["evidence_json"]:
                raise LiveOrderError("order_recovery_claim_mismatch")
            evidence = OrderEvidence.model_validate_json(row["evidence_json"])
            if evidence.intent != intent:
                raise LiveOrderError("order_recovery_claim_mismatch")
            plan = cancel_request(evidence.root_order_id)
            operation = "cancel"
            claims = conn.execute(
                "SELECT payload_json FROM events WHERE client_id=? AND kind='CANCEL_CLAIMED'",
                (client_id,),
            ).fetchall()
            if len(claims) != 1:
                raise LiveOrderError("order_recovery_claim_mismatch")
            saved = json.loads(claims[0][0])
            if (
                not isinstance(saved, dict)
                or set(saved)
                != {"post_claim", "request_sha256", "root_order_id", "order_id", "evidence_sha256"}
                or saved["post_claim"] != post["claim"]
                or saved["request_sha256"] != hashlib.sha256(plan.body).hexdigest()
                or saved["root_order_id"] != evidence.root_order_id
                or saved["order_id"] != evidence.order_id
            ):
                raise LiveOrderError("order_recovery_claim_mismatch")
            # Later GET evidence may contain additional fills. Bind the cancel
            # attempt to the exact earlier evidence, without requiring it to stay current.
            history = conn.execute(
                "SELECT payload_json FROM events WHERE client_id=? AND kind='RECONCILED'",
                (client_id,),
            ).fetchall()
            if not any(
                _hash(OrderEvidence.model_validate(json.loads(h[0])["evidence"]).model_dump_json())
                == saved["evidence_sha256"]
                for h in history
            ):
                raise LiveOrderError("order_recovery_claim_mismatch")
        submitted = conn.execute(
            "SELECT payload_json FROM events WHERE client_id=? AND kind='SUBMITTING'",
            (client_id,),
        ).fetchall()
        prepared = conn.execute(
            "SELECT payload_json FROM events WHERE client_id=? AND kind='PREPARED'",
            (client_id,),
        ).fetchall()
        if (
            post["phase"] not in {"IN_FLIGHT", "STOPPED"}
            or post["claim"] is None
            or post["operation"] != operation
            or post["request_sha256"] != hashlib.sha256(plan.body).hexdigest()
            or row["state"] in {"PREPARED", "ABANDONED"}
            or len(submitted) != 1
            or len(prepared) != 1
            or json.loads(prepared[0][0])
            != {"path": order_plan.path, "body": json.loads(order_plan.body)}
        ):
            raise LiveOrderError("order_recovery_claim_mismatch")
        checkpoint = {
            "live": state.model_dump(mode="json"),
            "order": row,
            "event_id": conn.execute("SELECT COALESCE(MAX(id),0) FROM events").fetchone()[0],
            "halted": conn.execute("SELECT halted FROM metadata").fetchone()[0],
            "post": {
                k: v for k, v in post.items() if k not in {"blocked", "live_enabled", "complete"}
            },
        }
        return {
            "client_id": client_id,
            "checkpoint_sha256": _hash(
                json.dumps(checkpoint, sort_keys=True, separators=(",", ":"))
            ),
            "post_revision": post["revision"],
            "post_claim": post["claim"],
            "post_reason": post["reason"],
            "post_operation": post["operation"],
            "live_revision": state.revision,
            "event_id": checkpoint["event_id"],
            "live_enabled": False,
            "complete": False,
        }

    def reconcile_unknown_order(self, client_id, *, expected_sha256, collect):
        """Hold the POST OS owner across a trusted GET collector and persistence.

        The collector returns an OrderReadReport, never a complete account proof.
        Preserve the ambiguous POST claim and all stops, including on interruption.
        """
        with self._lock, self.posts._ownership():
            before = self.order_recovery_context(client_id)
            if before["checkpoint_sha256"] != expected_sha256:
                raise LiveOrderError("order_recovery_checkpoint_changed")
            if self.posts.reads.status()["blocked"]:
                raise LiveOrderError("order_recovery_reads_blocked")
            with self._transaction() as conn:
                intent = OrderIntent.model_validate_json(self._row(conn, client_id)["intent_json"])
            report = collect(intent)
            if not isinstance(report, OrderReadReport):
                raise LiveOrderError("order_recovery_read_report_required")
            report = OrderReadReport.model_validate(report.model_dump())
            if report.evidence.intent != intent or report.evidence.executions_complete:
                raise LiveOrderError("order_recovery_evidence_mismatch")
            if self.order_recovery_context(client_id) != before:
                raise LiveOrderError("order_recovery_checkpoint_changed")
            if self.posts.reads.status()["blocked"]:
                raise LiveOrderError("order_recovery_reads_blocked")
            try:
                with self._transaction() as conn:
                    if self._order_recovery_context(conn, client_id) != before:
                        raise LiveOrderError("order_recovery_checkpoint_changed")
                    # Stop, normalized evidence and its GET provenance commit
                    # together. A dead sender may not have run its stop handler.
                    state = self._live_state(conn)
                    conn.execute("UPDATE metadata SET halted=1 WHERE id=1")
                    if state.phase != "STOPPED":
                        self._write_live(conn, state, phase="STOPPED")
                        self._event(conn, None, "LIVE_STOPPED", {})
                    result = self._reconcile_in_transaction(conn, report.evidence)
                    self._event(
                        conn,
                        client_id,
                        "ORDER_GET_OBSERVED",
                        {
                            "checkpoint_sha256": expected_sha256,
                            "report_sha256": _hash(_body(report)),
                            "observations": [
                                o.model_dump(mode="json") for o in report.observations
                            ],
                        },
                    )
            except LiveOrderError:
                raise
            except (ValueError, KeyError, TypeError, ArithmeticError):
                self.halt()
                raise
            return {
                "client_id": client_id,
                "order_id": report.evidence.order_id,
                "broker_status": report.evidence.status,
                "state": result,
                "executions_complete": False,
                "post_claim_retained": True,
                "recovery_required": True,
                "live_enabled": False,
                "complete": False,
            }

    def acknowledge_submission(self, receipt):
        with self._mutation():
            return super().acknowledge_submission(receipt)

    def unknown(self, client_id):
        with self._mutation():
            return super().unknown(client_id)

    def abandon(self, client_id):
        with self._mutation():
            return super().abandon(client_id)

    def _cancel_plan(self, conn, client_id, now=None, *, claimed=False):
        row = self._row(conn, client_id)
        allowed = {"CANCEL_PENDING"} if claimed else {"WORKING", "PARTIAL", "RECONCILING"}
        if row["state"] not in allowed or not row["evidence_json"]:
            raise LiveOrderError("live_cancel_confirmed_order_required")
        intent = OrderIntent.model_validate_json(row["intent_json"])
        evidence = OrderEvidence.model_validate_json(row["evidence_json"])
        validate_evidence(evidence)
        recorded = conn.execute(
            "SELECT payload_json FROM events WHERE client_id=? AND kind='RECONCILED' "
            "ORDER BY id DESC LIMIT 1",
            (client_id,),
        ).fetchone()
        if (
            recorded is None
            or OrderEvidence.model_validate(json.loads(recorded[0])["evidence"]) != evidence
        ):
            raise LiveOrderError("live_cancel_evidence_integrity_failed")
        if (
            evidence.intent != intent
            or evidence.status not in {"WAITING", "ORDERED", "MODIFYING"}
            or sum(e.units for e in evidence.executions) >= intent.units
        ):
            raise LiveOrderError("live_cancel_evidence_mismatch")
        if now is not None and not (
            0
            <= (now - evidence.observed_at).total_seconds()
            <= self._live_state(conn).policy.max_snapshot_age_seconds
        ):
            raise LiveOrderError("live_cancel_evidence_stale")
        plan = order_request(intent, self.limits)
        prepared = conn.execute(
            "SELECT payload_json FROM events WHERE client_id=? AND kind='PREPARED'", (client_id,)
        ).fetchall()
        submitted = conn.execute(
            "SELECT 1 FROM events WHERE client_id=? AND kind='SUBMITTING'", (client_id,)
        ).fetchall()
        if (
            len(prepared) != 1
            or len(submitted) != 1
            or json.loads(prepared[0][0]) != {"path": plan.path, "body": json.loads(plan.body)}
        ):
            raise LiveOrderError("live_cancel_submission_integrity_failed")
        receipt = self._receipt(conn, client_id)
        if receipt is not None:
            self._check_receipt_evidence(receipt, evidence)
        if (
            not claimed
            and conn.execute(
                "SELECT 1 FROM events WHERE client_id=? AND kind='CANCEL_CLAIMED'", (client_id,)
            ).fetchone()
        ):
            raise LiveOrderError("live_cancel_already_claimed")
        return cancel_request(evidence.root_order_id), evidence

    def cancel_request(self, client_id):
        with self._transaction() as conn:
            now = self._clock(self.clock())
            self._authorize(conn, now)
            return self._cancel_plan(conn, client_id, now)[0]

    def begin_cancel(self, client_id):
        with self._mutation(), self._transaction() as conn:
            now = self._clock(self.clock())
            self._authorize(conn, now)
            plan, evidence = self._cancel_plan(conn, client_id, now)
            digest = hashlib.sha256(plan.body).hexdigest()
            claim = self.posts.require_operation("cancel", digest)
            conn.execute("UPDATE orders SET state='CANCEL_PENDING' WHERE client_id=?", (client_id,))
            self._event(
                conn,
                client_id,
                "CANCEL_CLAIMED",
                {
                    "post_claim": claim,
                    "request_sha256": digest,
                    "root_order_id": evidence.root_order_id,
                    "order_id": evidence.order_id,
                    "evidence_sha256": _hash(evidence.model_dump_json()),
                },
            )
            return plan

    def _cancel_claim(self, conn, client_id, plan, evidence):
        rows = conn.execute(
            "SELECT payload_json FROM events WHERE client_id=? AND kind='CANCEL_CLAIMED'",
            (client_id,),
        ).fetchall()
        post = self.posts.snapshot()
        expected = {
            "post_claim": post["claim"],
            "request_sha256": hashlib.sha256(plan.body).hexdigest(),
            "root_order_id": evidence.root_order_id,
            "order_id": evidence.order_id,
            "evidence_sha256": _hash(evidence.model_dump_json()),
        }
        if (
            len(rows) != 1
            or json.loads(rows[0][0]) != expected
            or post["operation"] != "cancel"
            or post["claim"] is None
            or post["request_sha256"] != expected["request_sha256"]
        ):
            raise LiveOrderError("live_cancel_claim_integrity_failed")

    def validate_cancel_dispatch(self, client_id, plan):
        with self._transaction() as conn:
            now = self._clock(self.clock())
            self._authorize(conn, now)
            current, evidence = self._cancel_plan(conn, client_id, now, claimed=True)
            if current != plan:
                raise LiveOrderError("live_cancel_plan_changed")
            self.posts.require_operation("cancel", hashlib.sha256(plan.body).hexdigest())
            self._cancel_claim(conn, client_id, plan, evidence)

    @staticmethod
    def _cancel_receipt(conn, client_id):
        rows = conn.execute(
            "SELECT payload_json FROM events WHERE client_id=? AND kind='CANCEL_RECEIPT'",
            (client_id,),
        ).fetchall()
        if not rows:
            return None
        if len(rows) != 1:
            raise LiveOrderError("live_cancel_receipt_conflict")
        receipt = CancellationReceipt.model_validate_json(rows[0][0])
        if receipt.client_id != client_id:
            raise LiveOrderError("live_cancel_receipt_conflict")
        return receipt

    def acknowledge_cancel(self, receipt):
        receipt = CancellationReceipt.model_validate(receipt.model_dump())
        with self._mutation(), self._transaction() as conn:
            saved = self._cancel_receipt(conn, receipt.client_id)
            if saved is not None:
                if saved != receipt:
                    raise LiveOrderError("live_cancel_receipt_conflict")
                return
            plan, evidence = self._cancel_plan(conn, receipt.client_id, claimed=True)
            self._cancel_claim(conn, receipt.client_id, plan, evidence)
            if not self.posts.owns_operation() or receipt.root_order_id != evidence.root_order_id:
                raise LiveOrderError("live_cancel_receipt_mismatch")
            self._event(conn, receipt.client_id, "CANCEL_RECEIPT", receipt.model_dump(mode="json"))

    def cancellation_response(self, client_id, response):
        raise LiveOrderError("explicit_live_cancel_receipt_required")

    def halt(self):
        # Emergency stop may be persisted while another thread is sending.
        with self._transaction() as conn:
            conn.execute("UPDATE metadata SET halted=1 WHERE id=1")
            state = self._live_state(conn)
            self._write_live(conn, state, phase="STOPPED")
            self._event(conn, None, "LIVE_STOPPED", {})

    def snapshot(self):
        result = super().snapshot()
        with self._transaction() as conn:
            state = self._live_state(conn)
            now = self._clock(self.clock())
            for row in result["orders"]:
                receipt = self._cancel_receipt(conn, row["client_id"])
                row["cancellation_receipt"] = receipt.model_dump(mode="json") if receipt else None
        result["live_control"] = state.model_dump(mode="json")
        try:
            implementation_matches = state.implementation_sha256 == self._current_implementation()
        except LiveOrderError:
            implementation_matches = False
        result["implementation_matches"] = implementation_matches
        result["live_enabled"] = (
            implementation_matches
            and state.phase == "ENABLED"
            and state.approval is not None
            and state.approval.accepted_at <= now < state.approval.expires_at
            and not result["halted"]
            and self.posts.snapshot()["phase"] != "STOPPED"
            and not self.posts.reads.status()["stopped"]
        )
        return result
