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

from trading.account_guard import (
    AccountPolicy,
    AccountQuote,
    AccountSnapshot,
    fresh,
    reconcile_account,
    revision,
)
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
from trading.known_orders import KnownOrder
from trading.live_execution import credential_binding, execution_context, require_checkpoint
from trading.live_operations import LiveOperations, LiveOperationsError, OperationsBinding
from trading.order_journal import SCHEMA, OrderBlocked, OrderJournal
from trading.order_receipts import CancellationReceipt, SubmissionReceipt
from trading.post_control import PersistentPostLimiter
from trading.storage_init import new_storage_directory

MODE = "live-execution-v1"
MONITOR_STOP_REASONS = frozenset(
    {
        "private_sync_stopped",
        "private_sync_owner_missing",
        "private_sync_stale",
        "private_reads_blocked",
        "private_cash_halted",
        "private_journal_unresolved",
        "private_sync_unavailable",
        "private_posts_stopped",
        "private_posts_owner_missing",
        "private_live_unavailable",
    }
)
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
CANCEL_CONFIRMATIONS = frozenset(
    {
        "cancel-only",
        "account-identity",
        "broker-rules",
        "read-acceptance",
        "external-writers-paused",
        "preserve-stops",
    }
)
CANCEL_EVIDENCE_KINDS = frozenset({"identity", "rules", "read_acceptance"})
RESOLUTION_CONFIRMATIONS = frozenset(
    {
        "terminal-order",
        "complete-history",
        "complete-account",
        "account-identity",
        "external-writers-paused",
        "old-clients-closed",
        "preserve-stops",
    }
)
# A still-active accepted order is confirmed as such, never as a terminal one.
ACTIVE_RESOLUTION_CONFIRMATIONS = (RESOLUTION_CONFIRMATIONS - {"terminal-order"}) | {"active-order"}
RESTART_CONFIRMATIONS = CONFIRMATIONS | frozenset(
    {"restart-orders", "stop-cause-reviewed", "old-clients-closed", "preserve-loss-stop"}
)
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
    "live_operations.py",
    "private_operations.py",
    "private_sync.py",
    "stream_control.py",
    "live_monitor_target.py",
    "known_orders.py",
    "execution_cash_book.py",
    "event_journal.py",
    "segmented_journal.py",
    "live_execution.py",
    "credential_store.py",
    "order_credentials.py",
    "order_runtime.py",
    "live_account.py",
    "live_order_sync.py",
)


class LiveOrderError(OrderBlocked):
    """Fixed reason codes; no credentials or remote responses."""


def _body(value):
    data = value.model_dump(mode="json")
    if isinstance(value, LiveState) and value.operations is None:
        data.pop("operations")
    return json.dumps(data, sort_keys=True, separators=(",", ":"))


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
    operations: OperationsBinding | None = None


class CancelApproval(Contract):
    """Operator acceptance for one exact cancellation, never a live activation."""

    account_id: str = Field(min_length=1, max_length=100)
    checkpoint_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    accepted_at: AwareDatetime = Field(strict=True)
    expires_at: AwareDatetime = Field(strict=True)
    evidence: tuple[AcceptanceEvidence, ...]

    @model_validator(mode="after")
    def coherent(self):
        if (
            not self.accepted_at < self.expires_at <= self.accepted_at + timedelta(minutes=10)
            or len(self.evidence) != len(CANCEL_EVIDENCE_KINDS)
            or {e.kind for e in self.evidence} != CANCEL_EVIDENCE_KINDS
        ):
            raise ValueError("invalid_cancel_approval")
        return self


class OrderResolutionApproval(Contract):
    """Verified terminal history and account acceptance, not a GET completeness shortcut."""

    account_id: str = Field(min_length=1, max_length=100)
    checkpoint_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    accepted_at: AwareDatetime = Field(strict=True)
    expires_at: AwareDatetime = Field(strict=True)
    evidence: tuple[AcceptanceEvidence, ...]

    @model_validator(mode="after")
    def coherent(self):
        if (
            not self.accepted_at < self.expires_at <= self.accepted_at + timedelta(minutes=10)
            or len(self.evidence) != len(EVIDENCE_KINDS)
            or {e.kind for e in self.evidence} != EVIDENCE_KINDS
        ):
            raise ValueError("invalid_order_resolution_approval")
        return self


class LiveRestartApproval(Contract):
    checkpoint_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    approval: LiveApproval
    stop_review_reference: str = Field(min_length=1, max_length=256)
    stop_review_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def printable(self):
        if any(ord(c) < 32 or ord(c) == 127 for c in self.stop_review_reference):
            raise ValueError("invalid_stop_review_reference")
        return self


def _configuration(state):
    configuration = {
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
    }
    if state.operations is not None:
        configuration["operations"] = state.operations.model_dump(mode="json")
    return _hash(
        json.dumps(
            configuration,
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
            bindings = conn.execute(
                "SELECT payload_json FROM events WHERE kind='LIVE_OPERATIONS_BOUND' LIMIT 2"
            ).fetchall()
            if (state.operations is None and bindings) or (
                state.operations is not None
                and (
                    len(bindings) != 1
                    or json.loads(bindings[0][0]) != state.operations.model_dump(mode="json")
                )
            ):
                raise ValueError
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
            for resolved in self.posts.trade_resolutions():
                reference = resolved["reference"]
                prepared = conn.execute(
                    "SELECT client_id,kind,payload_json FROM events WHERE id=?",
                    (reference["prepared_id"],),
                ).fetchone()
                if (
                    reference["live_instance"] != state.instance
                    or reference["live_path"] != str(self.path.parent)
                    or prepared is None
                    or prepared["client_id"] != reference["client_id"]
                    or prepared["kind"] != "ORDER_RESOLUTION_PREPARED"
                    or self._checkpoint(json.loads(prepared["payload_json"]))
                    != reference["prepared_sha256"]
                    or json.loads(prepared["payload_json"])["post"] != resolved["post_before"]
                ):
                    raise ValueError
            restarts = self.posts.execution_restarts()
            for restarted in restarts:
                reference = restarted["reference"]
                prepared = conn.execute(
                    "SELECT client_id,kind,payload_json FROM events WHERE id=?",
                    (reference["prepared_id"],),
                ).fetchone()
                if (
                    reference["live_instance"] != state.instance
                    or reference["live_path"] != str(self.path.parent)
                    or prepared is None
                    or prepared["client_id"] is not None
                    or prepared["kind"] != "LIVE_RESTART_PREPARED"
                    or self._checkpoint(json.loads(prepared["payload_json"]))
                    != reference["prepared_sha256"]
                    or json.loads(prepared["payload_json"])["post"] != restarted["post_before"]
                ):
                    raise ValueError
            if state.phase == "ENABLED" and restarts:
                reference = restarts[-1]["reference"]
                completed = conn.execute(
                    "SELECT payload_json FROM events WHERE kind='LIVE_RESTARTED'"
                ).fetchall()
                if sum(json.loads(r[0]) == reference for r in completed) != 1:
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

    def _restart_context(self, conn, now, *, event_id=None):
        state = self._live_state(conn)
        post = {
            k: v
            for k, v in self.posts.snapshot().items()
            if k not in {"blocked", "live_enabled", "complete"}
        }
        if (
            state.phase != "STOPPED"
            or not conn.execute("SELECT halted FROM metadata").fetchone()[0]
            or post["phase"] not in {"READY", "STOPPED"}
            or post["claim"] is not None
            or post["reason"] == "token_failed"
            or self.posts.reads.status()["blocked"]
        ):
            raise LiveOrderError("live_restart_dependencies_refused")
        rows = [dict(r) for r in conn.execute("SELECT * FROM orders ORDER BY client_id")]
        if any(
            r["state"]
            not in {"PREPARED", "ABANDONED", "FILLED", "CANCELED", "EXPIRED", "WORKING", "PARTIAL"}
            for r in rows
        ):
            raise LiveOrderError("live_restart_unresolved_order")
        for row in rows:
            intent = OrderIntent.model_validate_json(row["intent_json"])
            plan = order_request(intent, state.limits)
            if row["state"] in {"PREPARED", "ABANDONED"}:
                self._unclaimed(conn, row["client_id"], plan)
                continue
            evidence = OrderEvidence.model_validate_json(row["evidence_json"])
            validate_evidence(evidence)
            filled = sum(e.units for e in evidence.executions)
            expected_state = (
                "FILLED"
                if evidence.status == "EXECUTED" and filled == intent.units
                else evidence.status
                if evidence.status in {"CANCELED", "EXPIRED"}
                else "PARTIAL"
                if evidence.status in {"WAITING", "ORDERED", "MODIFYING"} and filled
                else "WORKING"
                if evidence.status in {"WAITING", "ORDERED", "MODIFYING"}
                else None
            )
            saved = conn.execute(
                "SELECT payload_json FROM events WHERE client_id=? AND kind='RECONCILED' "
                "ORDER BY id DESC LIMIT 1",
                (row["client_id"],),
            ).fetchone()
            prepared = conn.execute(
                "SELECT payload_json FROM events WHERE client_id=? AND kind='PREPARED'",
                (row["client_id"],),
            ).fetchall()
            if (
                not evidence.executions_complete
                or evidence.intent != intent
                or row["state"] != expected_state
                or len(prepared) != 1
                or json.loads(prepared[0][0]) != {"path": plan.path, "body": json.loads(plan.body)}
                or conn.execute(
                    "SELECT COUNT(*) FROM events WHERE client_id=? AND kind='SUBMITTING'",
                    (row["client_id"],),
                ).fetchone()[0]
                != 1
                or saved is None
                or json.loads(saved[0])
                != {"state": row["state"], "evidence": evidence.model_dump(mode="json")}
            ):
                raise LiveOrderError("live_restart_complete_order_required")
            receipt = self._receipt(conn, row["client_id"])
            if receipt is not None:
                self._check_receipt_evidence(receipt, evidence)
        self._account_proof(conn)
        proof = json.loads(self._gate(conn)["proof_json"])
        snapshot = AccountSnapshot.model_validate(proof["snapshot"])
        quote = AccountQuote.model_validate(proof["quote"])
        if (
            proof["revision"] != revision(rows)
            or reconcile_account(state.policy, rows, snapshot, quote, now)
            or int(snapshot.observed_at.timestamp() * 1_000_000_000) < post["wall_ns"]
        ):
            raise LiveOrderError("live_restart_complete_account_required")
        candidate = self._activation_state(state)
        context = {
            "live": state.model_dump(mode="json"),
            "post": post,
            "orders_sha256": self._checkpoint(rows),
            "account_gate_sha256": self._checkpoint(dict(self._gate(conn))),
            "account_id": state.policy.account_id,
            "account_observed_at": snapshot.observed_at.isoformat(),
            "configuration_sha256": candidate.configuration_sha256,
            "implementation_sha256": candidate.implementation_sha256,
            "confirmations": sorted(
                RESTART_CONFIRMATIONS
                | ({"clock-repaired"} if post["reason"] == "clock_invalid" else set())
            ),
            "event_id": conn.execute("SELECT COALESCE(MAX(id),0) FROM events").fetchone()[0]
            if event_id is None
            else event_id,
        }
        return {**context, "checkpoint_sha256": self._checkpoint(context)}

    def restart_context(self):
        """Inspect the current stop and fresh full-account acceptance; never resume implicitly."""
        with self._transaction() as conn:
            return self._restart_context(conn, self._clock(self.clock()))

    def restart(self, acceptance, *, confirmations):
        acceptance = LiveRestartApproval.model_validate(acceptance.model_dump())
        approval = acceptance.approval
        with self._lock, self.posts._ownership() as owner:
            with self._transaction() as conn:
                now = self._clock(self.clock())
                context = self._restart_context(conn, now)
                previous = self._live_state(conn).approval
                if (
                    frozenset(confirmations) != frozenset(context["confirmations"])
                    or acceptance.checkpoint_sha256 != context["checkpoint_sha256"]
                    or approval.account_id != context["account_id"]
                    or approval.configuration_sha256 != context["configuration_sha256"]
                    or approval.implementation_sha256 != context["implementation_sha256"]
                    or not approval.accepted_at <= now < approval.expires_at
                    or approval.accepted_at < datetime.fromisoformat(context["account_observed_at"])
                    or (previous is not None and approval.accepted_at <= previous.accepted_at)
                ):
                    raise LiveOrderError("explicit_live_restart_acceptance_required")
                payload = {
                    "context": context,
                    "acceptance": acceptance.model_dump(mode="json"),
                    "post": context["post"],
                }
                self._event(conn, None, "LIVE_RESTART_PREPARED", payload)
                prepared_id = conn.execute("SELECT MAX(id) FROM events").fetchone()[0]
            with self._transaction() as conn:
                now = self._clock(self.clock())
                prepared = conn.execute(
                    "SELECT payload_json FROM events WHERE id=? AND kind='LIVE_RESTART_PREPARED'",
                    (prepared_id,),
                ).fetchone()
                if (
                    self._restart_context(conn, now, event_id=context["event_id"]) != context
                    or conn.execute("SELECT MAX(id) FROM events").fetchone()[0] != prepared_id
                    or prepared is None
                    or json.loads(prepared[0]) != payload
                ):
                    raise LiveOrderError("live_restart_checkpoint_changed")
                state = self._live_state(conn)
                proof = json.loads(self._gate(conn)["proof_json"])
                snapshot = AccountSnapshot.model_validate(proof["snapshot"])
                quote = AccountQuote.model_validate(proof["quote"])

                def validate_commit():
                    stamp = self._clock(self.clock())
                    if (
                        not approval.accepted_at <= stamp < approval.expires_at
                        or self._current_implementation() != context["implementation_sha256"]
                        or not fresh(
                            snapshot.observed_at, stamp, state.policy.max_snapshot_age_seconds
                        )
                        or not fresh(quote.observed_at, stamp, state.policy.max_quote_age_seconds)
                        or self.posts.reads.status()["blocked"]
                    ):
                        raise LiveOrderError("live_restart_checkpoint_changed")

                validate_commit()
                self._write_live(
                    conn,
                    state,
                    phase="ENABLED",
                    approval=approval,
                    configuration_sha256=approval.configuration_sha256,
                    implementation_sha256=approval.implementation_sha256,
                )
                conn.execute("UPDATE metadata SET halted=0 WHERE id=1")
                reference = {
                    "live_instance": state.instance,
                    "live_path": str(self.path.parent),
                    "client_id": None,
                    "prepared_id": prepared_id,
                    "prepared_sha256": self._checkpoint(payload),
                }
                self._event(conn, None, "LIVE_RESTARTED", reference)
                updated = self.posts._restart_execution(
                    expected=context["post"],
                    reference=reference,
                    owner=owner,
                    validate=validate_commit,
                )
                # A failed final live commit must leave trading stopped even if
                # POST's prior commit already succeeded. Recheck before committing it.
                validate_commit()
            return {
                "live_restarted": True,
                "post_revision": updated["revision"],
                "new_clients_required": True,
                "complete": False,
            }

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
        if state.operations is not None:
            try:
                LiveOperations(
                    state.operations,
                    self._operations_target(state, state.operations.sync_instance),
                    clock=self.clock,
                    monotonic=self.posts._mono,
                ).require_healthy(now)
            except LiveOperationsError as error:
                raise LiveOrderError(str(error)) from None
        return state

    def _operations_target(self, state, sync_instance):
        return {
            "scope": state.scope,
            "sync_instance": sync_instance,
            "read_instance": state.read_instance,
            "post_instance": state.post_instance,
            "live_instance": state.instance,
            "read_directory": str(self.posts.reads.path.parent),
            "post_directory": str(self.posts.path.parent),
            "live_directory": str(self.path.parent),
        }

    def bind_operations(
        self,
        directory,
        *,
        expected_revision,
        expected_plan_sha256,
        expected_monitor_instance,
        max_sync_age_seconds=120,
        max_watchdog_age_seconds=120,
        operations_confirmed=False,
    ):
        if operations_confirmed is not True or type(expected_revision) is not int:
            raise LiveOrderError("explicit_live_operations_confirmation_required")
        with self._transaction() as conn:
            initial = self._live_state(conn)
        binding = LiveOperations.capture(
            directory,
            self._operations_target(initial, "0" * 32),
            expected_plan_sha256=expected_plan_sha256,
            expected_monitor_instance=expected_monitor_instance,
            max_sync_age_seconds=max_sync_age_seconds,
            max_watchdog_age_seconds=max_watchdog_age_seconds,
            clock=self.clock,
            monotonic=self.posts._mono,
        )
        with self._mutation(), self._transaction() as conn:
            state = self._live_state(conn)
            if (
                state.revision != expected_revision
                or state.phase != "DISABLED"
                or state.approval is not None
                or state.operations is not None
                or conn.execute("SELECT halted FROM metadata WHERE id=1").fetchone()[0]
                or self.posts.snapshot()["blocked"]
                or conn.execute(
                    "SELECT 1 FROM events WHERE kind IN ('SUBMITTING','CANCEL_REQUESTED') LIMIT 1"
                ).fetchone()
            ):
                raise LiveOrderError("live_operations_registration_refused")
            LiveOperations(
                binding,
                self._operations_target(state, binding.sync_instance),
                clock=self.clock,
                monotonic=self.posts._mono,
            ).enrollment_check()
            candidate = self._activation_state(state.model_copy(update={"operations": binding}))
            self._write_live(
                conn,
                state,
                operations=binding,
                implementation_sha256=candidate.implementation_sha256,
                configuration_sha256=candidate.configuration_sha256,
            )
            self._event(conn, None, "LIVE_OPERATIONS_BOUND", binding.model_dump(mode="json"))
        return self.activation_context()

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

    def credential_binding(self):
        with self._transaction() as conn:
            return credential_binding(self, self._live_state(conn))

    def execution_context(
        self, client_id, *, operation="submit", quote=None, authorization_sha256=None
    ):
        with self._transaction() as conn:
            return execution_context(
                self,
                conn,
                client_id,
                operation=operation,
                quote=quote,
                authorization_sha256=authorization_sha256,
            )

    def begin_submission(self, client_id, *, quote=None, now=None, expected_execution_sha256=None):
        now = self._clock(now if now is not None else self.clock())
        with self._mutation(), self._transaction() as conn:
            if expected_execution_sha256 is not None:
                require_checkpoint(
                    execution_context(
                        self, conn, client_id, operation="submit", quote=quote, after_claim=True
                    ),
                    expected_execution_sha256,
                )
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

    def catalog_orders(self):
        """Export positively identified orders for reads; never infer execution completeness."""
        with self._transaction() as conn:
            state = self._live_state(conn)
            orders, unidentified, identities, roots = [], [], set(), set()
            history = {}
            for event in conn.execute(
                "SELECT id,client_id,kind,payload_json FROM events "
                "WHERE kind IN ('PREPARED','SUBMITTING','SUBMISSION_ACK','RECONCILED') ORDER BY id"
            ):
                history.setdefault(event["client_id"], {}).setdefault(event["kind"], []).append(
                    event
                )
            for row in conn.execute("SELECT * FROM orders ORDER BY client_id"):
                client_id = row["client_id"]
                intent = OrderIntent.model_validate_json(row["intent_json"])
                plan = order_request(intent, state.limits)
                events = history.get(client_id, {})
                prepared, submitted = events.get("PREPARED", []), events.get("SUBMITTING", [])
                acknowledgments = events.get("SUBMISSION_ACK", [])
                if len(acknowledgments) > 1:
                    raise LiveOrderError("live_catalog_order_integrity_failed")
                receipt = (
                    SubmissionReceipt.model_validate_json(acknowledgments[0]["payload_json"])
                    if acknowledgments
                    else None
                )
                evidence = (
                    OrderEvidence.model_validate_json(row["evidence_json"])
                    if row["evidence_json"] is not None
                    else None
                )
                if (
                    intent.client_id != client_id
                    or len(prepared) != 1
                    or json.loads(prepared[0]["payload_json"])
                    != {"path": plan.path, "body": json.loads(plan.body)}
                ):
                    raise LiveOrderError("live_catalog_order_integrity_failed")
                if row["state"] in {"PREPARED", "ABANDONED"}:
                    if submitted or receipt is not None or evidence is not None:
                        raise LiveOrderError("live_catalog_order_integrity_failed")
                    continue
                if row["state"] not in {
                    "SUBMITTING",
                    "RECONCILING",
                    "UNKNOWN",
                    "WORKING",
                    "PARTIAL",
                    "CANCEL_PENDING",
                    "FILLED",
                    "CANCELED",
                    "EXPIRED",
                } or (
                    row["state"] not in {"SUBMITTING", "RECONCILING", "UNKNOWN"}
                    and evidence is None
                ):
                    raise LiveOrderError("live_catalog_order_integrity_failed")
                if (
                    len(submitted) != 1
                    or json.loads(submitted[0]["payload_json"]) != {}
                    or submitted[0]["id"] <= prepared[0]["id"]
                ):
                    raise LiveOrderError("live_catalog_order_integrity_failed")
                if receipt is not None:
                    if receipt.intent != intent or acknowledgments[0]["id"] <= submitted[0]["id"]:
                        raise LiveOrderError("live_catalog_order_integrity_failed")
                if evidence is not None:
                    validate_evidence(evidence)
                    reconciled = events.get("RECONCILED", [])
                    saved = reconciled[-1] if reconciled else None
                    if (
                        evidence.intent != intent
                        or saved is None
                        or saved["id"] <= submitted[0]["id"]
                        or json.loads(saved["payload_json"])["evidence"]
                        != evidence.model_dump(mode="json")
                    ):
                        raise LiveOrderError("live_catalog_order_integrity_failed")
                    if receipt is not None:
                        self._check_receipt_evidence(receipt, evidence)
                identified = receipt if receipt is not None else evidence
                if identified is None:
                    if row["state"] not in {"SUBMITTING", "UNKNOWN"}:
                        raise LiveOrderError("live_catalog_order_integrity_failed")
                    unidentified.append(client_id)
                    continue
                if identified.order_id in identities or identified.root_order_id in roots:
                    raise LiveOrderError("live_catalog_broker_identity_reused")
                identities.add(identified.order_id)
                roots.add(identified.root_order_id)
                orders.append(
                    {
                        "order": KnownOrder(order_id=identified.order_id, intent=intent).model_dump(
                            mode="json"
                        ),
                        "source_ref": f"live/{state.instance}/"
                        + self._checkpoint(identified.model_dump(mode="json")),
                    }
                )
            return {
                "instance": state.instance,
                "read_instance": state.read_instance,
                "post_instance": state.post_instance,
                "scope": state.scope,
                "orders": orders,
                "unidentified_client_ids": unidentified,
                "complete": False,
                "live_enabled": False,
            }

    def verify_catalog_evidence(self, reports):
        """Check read reports against saved live receipts/history without changing lifecycle."""
        with self._transaction() as conn:
            for raw in reports:
                report = OrderReadReport.model_validate(raw.model_dump())
                evidence = report.evidence
                validate_evidence(evidence)
                row = conn.execute(
                    "SELECT * FROM orders WHERE client_id=?", (evidence.intent.client_id,)
                ).fetchone()
                if row is None:
                    continue  # An independently declared external order remains a catalog input.
                if OrderIntent.model_validate_json(row["intent_json"]) != evidence.intent:
                    raise LiveOrderError("live_catalog_read_intent_mismatch")
                receipt = self._receipt(conn, evidence.intent.client_id)
                if receipt is not None:
                    self._check_receipt_evidence(receipt, evidence)
                previous = (
                    OrderEvidence.model_validate_json(row["evidence_json"])
                    if row["evidence_json"] is not None
                    else None
                )
                if previous is None and receipt is None:
                    raise LiveOrderError("live_catalog_read_identity_required")
                if previous is not None:
                    old = {e.execution_id: e for e in previous.executions}
                    new = {e.execution_id: e for e in evidence.executions}
                    if (
                        (previous.root_order_id, previous.order_id)
                        != (evidence.root_order_id, evidence.order_id)
                        or evidence.observed_at < previous.observed_at
                        or any(new.get(key) != value for key, value in old.items())
                        or previous.status in {"EXECUTED", "CANCELED", "EXPIRED"}
                        and evidence.status != previous.status
                        or previous.executions_complete
                        and previous.status in {"EXECUTED", "CANCELED", "EXPIRED"}
                        and evidence.executions != previous.executions
                        or evidence.observed_at == previous.observed_at
                        and evidence.model_copy(
                            update={"executions_complete": previous.executions_complete}
                        )
                        != previous
                    ):
                        raise LiveOrderError("live_catalog_read_history_mismatch")

    def order_recovery_context(self, client_id):
        """Local checkpoint for GET investigation; never infer an absent order."""
        with self._transaction() as conn:
            return self._order_recovery_context(conn, client_id)

    def _order_recovery_context(self, conn, client_id, *, event_id=None):
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
            "event_id": (
                conn.execute("SELECT COALESCE(MAX(id),0) FROM events").fetchone()[0]
                if event_id is None
                else event_id
            ),
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

    def _resolution_context(self, conn, client_id, now, *, event_id=None):
        recovery = self._order_recovery_context(conn, client_id, event_id=event_id)
        state = self._live_state(conn)
        row = self._row(conn, client_id)
        if (
            state.phase != "STOPPED"
            or not conn.execute("SELECT halted FROM metadata").fetchone()[0]
        ):
            raise LiveOrderError("order_resolution_stop_required")
        terminal = row["state"] in {"FILLED", "CANCELED", "EXPIRED"}
        # An accepted, still-active order resolves only a submission claim. A cancel
        # attempt whose order is still active is not known to have failed or succeeded.
        active = row["state"] in {"WORKING", "PARTIAL"} and recovery["post_operation"] in {
            "order",
            "close_order",
        }
        if not (terminal or active) or not row["evidence_json"]:
            raise LiveOrderError("order_resolution_terminal_evidence_required")
        evidence = OrderEvidence.model_validate_json(row["evidence_json"])
        validate_evidence(evidence)
        saved = conn.execute(
            "SELECT payload_json FROM events WHERE client_id=? AND kind='RECONCILED' "
            "ORDER BY id DESC LIMIT 1",
            (client_id,),
        ).fetchone()
        filled = sum(e.units for e in evidence.executions)
        expected = (
            {"EXECUTED": "FILLED", "CANCELED": "CANCELED", "EXPIRED": "EXPIRED"}.get(
                evidence.status
            )
            if terminal
            else ("PARTIAL" if filled else "WORKING")
            if evidence.status in {"WAITING", "ORDERED", "MODIFYING"}
            else None
        )
        if (
            not evidence.executions_complete
            or row["state"] != expected
            or (not terminal and filled >= evidence.intent.units)
            or evidence.intent != OrderIntent.model_validate_json(row["intent_json"])
            or (
                evidence.status == "EXECUTED"
                and sum(e.units for e in evidence.executions) != evidence.intent.units
            )
            or saved is None
            or json.loads(saved[0])
            != {"state": row["state"], "evidence": evidence.model_dump(mode="json")}
        ):
            raise LiveOrderError("order_resolution_terminal_evidence_required")
        self._account_proof(conn)
        proof = json.loads(self._gate(conn)["proof_json"])
        snapshot = AccountSnapshot.model_validate(proof["snapshot"])
        quote = AccountQuote.model_validate(proof["quote"])
        rows = [dict(r) for r in conn.execute("SELECT * FROM orders")]
        post = self.posts.snapshot()
        if (
            self.posts.reads.status()["blocked"]
            or proof["revision"] != revision(rows)
            or reconcile_account(state.policy, rows, snapshot, quote, now)
            or int(snapshot.observed_at.timestamp() * 1_000_000_000) < post["wall_ns"]
        ):
            raise LiveOrderError("order_resolution_complete_account_required")
        context = {
            "recovery": recovery,
            "account_id": state.policy.account_id,
            "implementation_sha256": self._current_implementation(),
            "account_gate_sha256": self._checkpoint(dict(self._gate(conn))),
            "terminal_state": row["state"] if terminal else None,
            "active_state": None if terminal else row["state"],
            "evidence_sha256": _hash(evidence.model_dump_json()),
        }
        return {**context, "checkpoint_sha256": self._checkpoint(context)}

    def order_resolution_context(self, client_id):
        """Local diagnostic; requires separately established terminal and account completeness."""
        with self._transaction() as conn:
            return self._resolution_context(conn, client_id, self._clock(self.clock()))

    def resolve_order_claim(self, client_id, approval, *, confirmations):
        approval = OrderResolutionApproval.model_validate(approval.model_dump())
        confirmations = frozenset(confirmations)
        if confirmations not in {RESOLUTION_CONFIRMATIONS, ACTIVE_RESOLUTION_CONFIRMATIONS}:
            raise LiveOrderError("explicit_order_resolution_confirmations_required")
        with self._lock, self.posts._ownership() as owner:
            with self._transaction() as conn:
                now = self._clock(self.clock())
                context = self._resolution_context(conn, client_id, now)
                if confirmations != (
                    ACTIVE_RESOLUTION_CONFIRMATIONS
                    if context["active_state"]
                    else RESOLUTION_CONFIRMATIONS
                ):
                    raise LiveOrderError("explicit_order_resolution_confirmations_required")
                if (
                    context["checkpoint_sha256"] != approval.checkpoint_sha256
                    or context["account_id"] != approval.account_id
                    or not approval.accepted_at <= now < approval.expires_at
                ):
                    raise LiveOrderError("order_resolution_acceptance_refused")
                state = self._live_state(conn)
                post = {
                    k: v
                    for k, v in self.posts.snapshot().items()
                    if k not in {"blocked", "live_enabled", "complete"}
                }
                payload = {
                    "context": context,
                    "approval": approval.model_dump(mode="json"),
                    "post": post,
                }
                self._event(conn, client_id, "ORDER_RESOLUTION_PREPARED", payload)
                prepared_id = conn.execute("SELECT MAX(id) FROM events").fetchone()[0]
            # This commit preserves authorization before any claim can be cleared.
            # Hold both the OS owner and live DB transaction across the POST commit.
            with self._transaction() as conn:
                now = self._clock(self.clock())
                # Ignore only our own prepared event while comparing the full checkpoint.
                current = self._resolution_context(
                    conn, client_id, now, event_id=context["recovery"]["event_id"]
                )
                prepared = conn.execute(
                    "SELECT client_id,kind,payload_json FROM events WHERE id=?", (prepared_id,)
                ).fetchone()
                if (
                    current != context
                    or conn.execute("SELECT MAX(id) FROM events").fetchone()[0] != prepared_id
                    or not approval.accepted_at <= now < approval.expires_at
                    or prepared is None
                    or prepared["client_id"] != client_id
                    or prepared["kind"] != "ORDER_RESOLUTION_PREPARED"
                    or json.loads(prepared["payload_json"]) != payload
                ):
                    raise LiveOrderError("order_resolution_checkpoint_changed")
                proof = json.loads(self._gate(conn)["proof_json"])
                snapshot = AccountSnapshot.model_validate(proof["snapshot"])
                quote = AccountQuote.model_validate(proof["quote"])

                def validate_commit():
                    stamp = self._clock(self.clock())
                    if (
                        not approval.accepted_at <= stamp < approval.expires_at
                        or self._current_implementation() != context["implementation_sha256"]
                        or not fresh(
                            snapshot.observed_at, stamp, state.policy.max_snapshot_age_seconds
                        )
                        or not fresh(quote.observed_at, stamp, state.policy.max_quote_age_seconds)
                        or self.posts.reads.status()["blocked"]
                    ):
                        raise LiveOrderError("order_resolution_checkpoint_changed")

                updated = self.posts._resolve_trade(
                    expected=post,
                    reference={
                        "live_instance": state.instance,
                        "live_path": str(self.path.parent),
                        "client_id": client_id,
                        "prepared_id": prepared_id,
                        "prepared_sha256": self._checkpoint(payload),
                    },
                    owner=owner,
                    validate=validate_commit,
                )
            return {
                "client_id": client_id,
                "post_claim_resolved": True,
                "post_revision": updated["revision"],
                "post_phase": "STOPPED",
                "live_enabled": False,
                "restart_required": True,
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

    def _cancel_context(self, conn, client_id, now, *, claimed=False):
        state = self._live_state(conn)
        plan, evidence = self._cancel_plan(conn, client_id, now, claimed=claimed)
        row = dict(self._row(conn, client_id))
        row.pop("state")  # The durable cancel claim changes only this column.
        post = self.posts.snapshot()
        return {
            "client_id": client_id,
            "account_id": state.policy.account_id,
            "implementation_sha256": self._current_implementation(),
            "live_sha256": _hash(_body(state)),
            "order_sha256": _hash(json.dumps(row, sort_keys=True, separators=(",", ":"))),
            "gate_sha256": _hash(json.dumps(dict(self._gate(conn)), sort_keys=True)),
            "halted": conn.execute("SELECT halted FROM metadata").fetchone()[0],
            "event_id": conn.execute("SELECT COALESCE(MAX(id),0) FROM events").fetchone()[0],
            "post_instance": post["instance"],
            "post_revision": post["revision"],
            "root_order_id": evidence.root_order_id,
            "order_id": evidence.order_id,
            "request_sha256": hashlib.sha256(plan.body).hexdigest(),
        }

    @staticmethod
    def _checkpoint(context):
        return _hash(json.dumps(context, sort_keys=True, separators=(",", ":")))

    def cancel_context(self, client_id):
        """Inspect a restricted cancellation checkpoint without changing any permission."""
        with self._transaction() as conn:
            context = self._cancel_context(conn, client_id, self._clock(self.clock()))
            return {**context, "checkpoint_sha256": self._checkpoint(context)}

    def authorize_cancel(self, client_id, approval, *, confirmations):
        approval = CancelApproval.model_validate(approval.model_dump())
        if frozenset(confirmations) != CANCEL_CONFIRMATIONS:
            raise LiveOrderError("explicit_cancel_acceptance_confirmations_required")
        with self._mutation(), self._transaction() as conn:
            now = self._clock(self.clock())
            context = self._cancel_context(conn, client_id, now)
            self.posts.check()
            if (
                self.posts.snapshot()["blocked"]
                or self.posts.reads.status()["blocked"]
                or approval.account_id != context["account_id"]
                or approval.checkpoint_sha256 != self._checkpoint(context)
                or not approval.accepted_at <= now < approval.expires_at
            ):
                raise LiveOrderError("live_cancel_authorization_refused")
            payload = {"context": context, "approval": approval.model_dump(mode="json")}
            self._event(conn, client_id, "CANCEL_AUTHORIZED", payload)
            # The event is durable before this capability is returned. No state or stop changes.
            return self._checkpoint(payload)

    def _authorize_cancel(self, conn, client_id, now, authorization_sha256, *, claimed=False):
        if authorization_sha256 is None:
            return self._authorize(conn, now)
        saved = conn.execute(
            "SELECT id,payload_json FROM events WHERE client_id=? AND kind='CANCEL_AUTHORIZED' "
            "ORDER BY id DESC LIMIT 1",
            (client_id,),
        ).fetchone()
        if saved is None:
            raise LiveOrderError("live_cancel_authorization_refused")
        payload = json.loads(saved["payload_json"])
        approval = CancelApproval.model_validate_json(json.dumps(payload["approval"]))
        context = payload["context"]
        current = self._cancel_context(conn, client_id, now, claimed=claimed)
        post = self.posts.snapshot()
        # Issuance appends one event; claiming appends one more and advances the
        # shared POST revision once. Any intervening operation or journal mutation fences it.
        in_flight = post["phase"] == "IN_FLIGHT"
        expected_head = saved["id"] + int(claimed)
        expected_revision = context["post_revision"] + int(in_flight)
        if (
            self._checkpoint(payload) != authorization_sha256
            or approval.checkpoint_sha256 != self._checkpoint(context)
            or approval.account_id != context["account_id"]
            or not approval.accepted_at <= now < approval.expires_at
            or current["event_id"] != expected_head
            or current["post_revision"] != expected_revision
            or post["phase"] not in {"READY", "IN_FLIGHT"}
            or (in_flight and not self.posts.owns_operation())
            or (claimed and not in_flight)
            or self.posts.reads.status()["blocked"]
        ):
            raise LiveOrderError("live_cancel_authorization_refused")
        current.update(event_id=context["event_id"], post_revision=context["post_revision"])
        if current != context:
            raise LiveOrderError("live_cancel_checkpoint_changed")

    def cancel_request(self, client_id, *, authorization_sha256=None):
        with self._transaction() as conn:
            now = self._clock(self.clock())
            self._authorize_cancel(conn, client_id, now, authorization_sha256)
            return self._cancel_plan(conn, client_id, now)[0]

    def begin_cancel(self, client_id, *, authorization_sha256=None, expected_execution_sha256=None):
        with self._mutation(), self._transaction() as conn:
            if expected_execution_sha256 is not None:
                require_checkpoint(
                    execution_context(
                        self,
                        conn,
                        client_id,
                        operation="cancel",
                        authorization_sha256=authorization_sha256,
                        after_claim=True,
                    ),
                    expected_execution_sha256,
                )
            now = self._clock(self.clock())
            self._authorize_cancel(conn, client_id, now, authorization_sha256)
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

    def validate_cancel_dispatch(self, client_id, plan, *, authorization_sha256=None):
        with self._transaction() as conn:
            now = self._clock(self.clock())
            self._authorize_cancel(conn, client_id, now, authorization_sha256, claimed=True)
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

    def monitoring_status(self):
        """Small local state for monitoring; no amounts, evidence references or order payloads."""
        with self._transaction() as conn:
            state = self._live_state(conn)
            halted = bool(conn.execute("SELECT halted FROM metadata WHERE id=1").fetchone()[0])
            entry_halted = bool(self._gate(conn)["entry_halted"])
            try:
                current = self._current_implementation() == state.implementation_sha256
                now = self._clock(self.clock())
                valid = (
                    current
                    and state.approval is not None
                    and (state.approval.accepted_at <= now < state.approval.expires_at)
                )
            except (ValueError, OSError):
                valid = False
            return {
                "instance": state.instance,
                "revision": state.revision,
                "phase": state.phase,
                "halted": halted,
                "entry_halted": entry_halted,
                "approval_valid": valid,
                "complete": False,
                "live_enabled": False,
            }

    def halt_for_monitor(self, monitor_instance, reasons):
        if (
            not isinstance(monitor_instance, str)
            or len(monitor_instance) != 32
            or any(c not in "0123456789abcdef" for c in monitor_instance)
            or not isinstance(reasons, (list, tuple, set, frozenset))
            or not reasons
            or any(
                not isinstance(reason, str) or reason not in MONITOR_STOP_REASONS
                for reason in reasons
            )
        ):
            raise LiveOrderError("invalid_live_monitor_stop")
        with self._transaction() as conn:
            state = self._live_state(conn)
            if (
                state.phase == "STOPPED"
                and conn.execute("SELECT halted FROM metadata").fetchone()[0]
            ):
                return {"status": "already_stopped", "changed": False}
            conn.execute("UPDATE metadata SET halted=1 WHERE id=1")
            self._write_live(conn, state, phase="STOPPED")
            self._event(
                conn,
                None,
                "LIVE_MONITOR_STOPPED",
                {
                    "monitor_instance": monitor_instance,
                    "reasons": sorted(set(reasons)),
                },
            )
            return {"status": "stopped", "changed": True}

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
