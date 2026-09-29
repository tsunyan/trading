"""Durable OFFLINE order lifecycle; deliberately has no transport or live mode."""

import json
import sqlite3
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from trading.account_guard import (
    RETRYABLE_ACCOUNT_ERRORS,
    AccountPolicy,
    AccountQuote,
    AccountSnapshot,
    evaluate_risk,
    reconcile_account,
    revision,
)
from trading.broker_contracts import (
    OrderEvidence,
    OrderIntent,
    OrderLimits,
    cancel_request,
    cancellation_accepted,
    order_request,
    parse_evidence,
    validate_evidence,
)
from trading.order_states import TERMINAL

SCHEMA = """
CREATE TABLE metadata (id INTEGER PRIMARY KEY CHECK(id=1), mode TEXT, limits_json TEXT,
                       halted INTEGER NOT NULL DEFAULT 0);
CREATE TABLE orders (client_id TEXT PRIMARY KEY, intent_json TEXT NOT NULL,
                     state TEXT NOT NULL, evidence_json TEXT);
CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, recorded_at TEXT NOT NULL,
                     client_id TEXT, kind TEXT NOT NULL, payload_json TEXT NOT NULL);
CREATE TABLE account_gate (id INTEGER PRIMARY KEY CHECK(id=1), policy_json TEXT NOT NULL,
                           proof_json TEXT, peak TEXT NOT NULL,
                           entry_halted INTEGER NOT NULL DEFAULT 0);
"""


class OrderBlocked(ValueError):
    pass


class OrderJournal:
    def __init__(self, directory: Path):
        self.path = directory.resolve() / "order-lab.sqlite"
        with self._transaction() as conn:
            row = conn.execute("SELECT * FROM metadata WHERE id=1").fetchone()
            if not row or row["mode"] not in {
                "offline-execution-lab-v1",
                "offline-execution-lab-v2-guarded",
            }:
                raise OrderBlocked("not an offline execution journal")
            self.limits = OrderLimits.model_validate_json(row["limits_json"])
            self._gate(conn)  # Guarded journals must never silently degrade to v1.

    @classmethod
    def create(
        cls, directory: Path, limits: OrderLimits, *, account_policy: AccountPolicy | None = None
    ):
        limits = OrderLimits.model_validate(limits.model_dump())
        if account_policy is not None:
            account_policy = AccountPolicy.model_validate(account_policy.model_dump())
        directory.mkdir(parents=True, exist_ok=False)
        with closing(sqlite3.connect(directory / "order-lab.sqlite")) as conn:
            conn.executescript(SCHEMA)
            conn.execute(
                "INSERT INTO metadata(id,mode,limits_json) VALUES (1,?,?)",
                (
                    "offline-execution-lab-v1"
                    if account_policy is None
                    else "offline-execution-lab-v2-guarded",
                    limits.model_dump_json(),
                ),
            )
            if account_policy is not None:
                conn.execute(
                    "INSERT INTO account_gate(id,policy_json,peak) VALUES(1,?,?)",
                    (account_policy.model_dump_json(), str(account_policy.starting_balance)),
                )
            conn.commit()
        return cls(directory)

    @contextmanager
    def _transaction(self):
        # mode=rw must not silently create a new empty account on a path typo.
        with closing(sqlite3.connect(self.path.as_uri() + "?mode=rw", uri=True, timeout=5)) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                conn.commit()
            except BaseException:
                conn.rollback()
                raise

    @staticmethod
    def _event(conn, client_id, kind, payload):
        conn.execute(
            "INSERT INTO events(recorded_at,client_id,kind,payload_json) VALUES (?,?,?,?)",
            (datetime.now(UTC).isoformat(), client_id, kind, json.dumps(payload)),
        )

    @staticmethod
    def _row(conn, client_id):
        row = conn.execute("SELECT * FROM orders WHERE client_id=?", (client_id,)).fetchone()
        if row is None:
            raise OrderBlocked("unknown client ID")
        return row

    @staticmethod
    def _available(conn, except_id=None):
        if conn.execute("SELECT halted FROM metadata WHERE id=1").fetchone()[0]:
            raise OrderBlocked("journal is halted")
        rows = conn.execute("SELECT client_id,state FROM orders").fetchall()
        if any(row[0] != except_id and row[1] not in TERMINAL for row in rows):
            raise OrderBlocked("another order is unresolved; reconcile first")

    @staticmethod
    def _gate(conn):
        mode = conn.execute("SELECT mode FROM metadata WHERE id=1").fetchone()[0]
        if mode == "offline-execution-lab-v1":
            return None
        row = conn.execute("SELECT * FROM account_gate WHERE id=1").fetchone()
        if row is None:
            raise OrderBlocked("guarded journal has missing risk policy")
        return row

    def update_account(self, snapshot: AccountSnapshot, quote: AccountQuote, *, now=None):
        """Reconcile a normalized complete account snapshot; never accepts raw API data."""
        now = now or datetime.now(UTC)
        try:
            snapshot = AccountSnapshot.model_validate(snapshot.model_dump())
            quote = AccountQuote.model_validate(quote.model_dump())
        except (ValueError, TypeError):
            self.halt()
            raise
        with self._transaction() as conn:
            gate = self._gate(conn)
            if gate is None:
                raise OrderBlocked("account guard not enabled on this offline journal")
            policy = AccountPolicy.model_validate_json(gate["policy_json"])
            rows = [dict(row) for row in conn.execute("SELECT * FROM orders")]
            previous = json.loads(gate["proof_json"]) if gate["proof_json"] else None
            previous_time = (
                AccountSnapshot.model_validate(previous["snapshot"]).observed_at
                if previous
                else None
            )
            if previous_time is not None and snapshot.observed_at < previous_time:
                errors = ["account_time_moved_backwards"]
            else:
                try:
                    errors = reconcile_account(policy, rows, snapshot, quote, now)
                except ValueError as exc:
                    errors = [str(exc)]
            proof = {
                "snapshot": snapshot.model_dump(mode="json"),
                "quote": quote.model_dump(mode="json"),
                "revision": revision(rows),
            }
            if previous:
                if (
                    snapshot.observed_at == previous_time
                    and proof["snapshot"] != previous["snapshot"]
                ):
                    errors.append("conflicting_same_time_account")
            if errors:
                if set(errors) - RETRYABLE_ACCOUNT_ERRORS:
                    conn.execute("UPDATE metadata SET halted=1 WHERE id=1")
                self._event(conn, None, "ACCOUNT_REJECTED", {"reasons": sorted(set(errors))})
            else:
                peak = max(Decimal(gate["peak"]), snapshot.equity)
                entry_halted = bool(gate["entry_halted"]) or (
                    policy.starting_balance - snapshot.equity >= policy.max_loss_jpy
                    or (peak - snapshot.equity) / peak >= policy.max_drawdown
                )
                conn.execute(
                    "UPDATE account_gate SET proof_json=?,peak=?,entry_halted=? WHERE id=1",
                    (json.dumps(proof), str(peak), int(entry_halted)),
                )
                self._event(
                    conn,
                    None,
                    "ACCOUNT_RECONCILED",
                    {
                        "revision": proof["revision"],
                        "entry_halted": entry_halted,
                        "snapshot": proof["snapshot"],
                        "quote": proof["quote"],
                    },
                )
        if errors:
            raise OrderBlocked("account reconciliation failed: " + ", ".join(sorted(set(errors))))
        return {"reconciled": True, "entry_halted": entry_halted}

    def prepare(self, intent: OrderIntent) -> str:
        request = order_request(intent, self.limits)
        with self._transaction() as conn:
            existing = conn.execute(
                "SELECT intent_json,state FROM orders WHERE client_id=?", (intent.client_id,)
            ).fetchone()
            if existing:
                if OrderIntent.model_validate_json(existing[0]) != intent:
                    raise OrderBlocked("client ID reused for a different intent")
                return existing[1]
            self._available(conn)
            conn.execute(
                "INSERT INTO orders VALUES (?,?,'PREPARED',NULL)",
                (intent.client_id, intent.model_dump_json()),
            )
            self._event(
                conn,
                intent.client_id,
                "PREPARED",
                {
                    "path": request.path,
                    "body": json.loads(request.body),
                },
            )
        return "PREPARED"

    def begin_submission(self, client_id: str, *, quote: AccountQuote | None = None, now=None):
        """Commit the consumed attempt BEFORE returning a plan; never send here.

        Crash after this commit, even before a caller sends anything, requires
        reconciliation. At-most-once local claiming is not broker exactly-once.
        """
        now = now or datetime.now(UTC)
        blocked = None
        with self._transaction() as conn:
            row = self._row(conn, client_id)
            self._available(conn, client_id)
            if row["state"] != "PREPARED":
                raise OrderBlocked("submission already claimed; do not resend")
            request = order_request(
                OrderIntent.model_validate_json(row["intent_json"]), self.limits
            )
            gate = self._gate(conn)
            if gate is not None:
                if not gate["proof_json"] or quote is None:
                    raise OrderBlocked("account proof and fresh quote required")
                quote = AccountQuote.model_validate(quote.model_dump())
                policy = AccountPolicy.model_validate_json(gate["policy_json"])
                proof = json.loads(gate["proof_json"])
                if quote.observed_at < AccountQuote.model_validate(proof["quote"]).observed_at:
                    raise OrderBlocked("stale_or_future_quote: quote predates account proof quote")
                rows = [dict(item) for item in conn.execute("SELECT * FROM orders")]
                if proof["revision"] != revision(rows):
                    raise OrderBlocked("account proof invalidated by order changes")
                result = evaluate_risk(
                    policy,
                    AccountSnapshot.model_validate(proof["snapshot"]),
                    quote,
                    rows,
                    OrderIntent.model_validate_json(row["intent_json"]),
                    now,
                    Decimal(gate["peak"]),
                    bool(gate["entry_halted"]),
                )
                conn.execute(
                    "UPDATE account_gate SET peak=?,entry_halted=? WHERE id=1",
                    (result["peak"], int(result["entry_halted"])),
                )
                self._event(conn, client_id, "RISK_CHECK", result)
                if not result["allowed"]:
                    blocked = ", ".join(result["reasons"])
            if blocked is None:
                conn.execute("UPDATE orders SET state='SUBMITTING' WHERE client_id=?", (client_id,))
                self._event(conn, client_id, "SUBMITTING", {})
        if blocked is not None:
            # Persist the rejection/entry-stop, but never consume the send claim.
            raise OrderBlocked("account risk blocked: " + blocked)
        return request

    def unknown(self, client_id: str):
        """Timeout/error/malformed response: keep the intent, never free its ID."""
        with self._transaction() as conn:
            row = self._row(conn, client_id)
            if row["state"] in TERMINAL or row["state"] == "PREPARED":
                raise OrderBlocked("order has no ambiguous in-flight operation")
            conn.execute("UPDATE orders SET state='UNKNOWN' WHERE client_id=?", (client_id,))
            self._event(conn, client_id, "UNKNOWN", {})

    def abandon(self, client_id: str):
        """Only a provably never-claimed intent can be locally abandoned."""
        with self._transaction() as conn:
            if self._row(conn, client_id)["state"] != "PREPARED":
                raise OrderBlocked("cannot abandon a potentially sent order")
            conn.execute("UPDATE orders SET state='ABANDONED' WHERE client_id=?", (client_id,))
            self._event(conn, client_id, "ABANDONED", {})

    def reconcile(self, evidence: OrderEvidence) -> str:
        try:
            return self._reconcile(evidence)
        except (ValueError, KeyError, TypeError, ArithmeticError):
            # Persist the stop in a separate transaction after rolling back the
            # inconsistent snapshot. A terminal order must not hide this alarm.
            self.halt()
            raise

    def reconcile_responses(
        self, client_id, orders, executions, observed_at, *, executions_complete=False
    ):
        """Fail closed on raw response/schema errors as well as state mismatches."""
        try:
            with self._transaction() as conn:
                intent = OrderIntent.model_validate_json(self._row(conn, client_id)["intent_json"])
            evidence = parse_evidence(
                intent,
                orders,
                executions,
                observed_at,
                executions_complete=executions_complete,
            )
            return self.reconcile(evidence)
        except (ValueError, KeyError, TypeError, ArithmeticError):
            self.halt()
            raise

    def _reconcile(self, evidence: OrderEvidence) -> str:
        evidence = OrderEvidence.model_validate(evidence.model_dump())
        validate_evidence(evidence)
        client_id = evidence.intent.client_id
        with self._transaction() as conn:
            row = self._row(conn, client_id)
            if OrderIntent.model_validate_json(row["intent_json"]) != evidence.intent:
                raise OrderBlocked("evidence belongs to a different intent")
            if row["state"] in {"PREPARED", "ABANDONED"}:
                raise OrderBlocked("unexpected broker order for unsubmitted intent")
            previous = (
                OrderEvidence.model_validate_json(row["evidence_json"])
                if row["evidence_json"]
                else None
            )
            if previous:
                if (previous.root_order_id, previous.order_id) != (
                    evidence.root_order_id,
                    evidence.order_id,
                ):
                    raise OrderBlocked("broker order identity changed")
                if evidence.observed_at < previous.observed_at:
                    raise OrderBlocked("stale evidence")
                old = {e.execution_id: e for e in previous.executions}
                new = {e.execution_id: e for e in evidence.executions}
                if any(new.get(key) != value for key, value in old.items()):
                    raise OrderBlocked("executions disappeared or changed")
                if evidence.observed_at == previous.observed_at and evidence != previous:
                    raise OrderBlocked("conflicting same-time evidence")
                if previous.status in {"EXECUTED", "CANCELED", "EXPIRED"} and (
                    evidence.status != previous.status
                ):
                    raise OrderBlocked("terminal broker status changed")
            # Broker identifiers must never bind to a different local intent.
            for other in conn.execute(
                "SELECT evidence_json FROM orders WHERE client_id!=? AND evidence_json IS NOT NULL",
                (client_id,),
            ):
                item = OrderEvidence.model_validate_json(other[0])
                if (
                    item.order_id == evidence.order_id
                    or item.root_order_id == evidence.root_order_id
                ):
                    raise OrderBlocked("broker ID already bound to another intent")
                if {e.execution_id for e in item.executions} & {
                    e.execution_id for e in evidence.executions
                }:
                    raise OrderBlocked("execution ID already bound to another intent")
            filled = sum(e.units for e in evidence.executions)
            if not evidence.executions_complete:
                state = "RECONCILING"
            elif evidence.status == "EXECUTED":
                state = "FILLED" if filled == evidence.intent.units else "RECONCILING"
            elif evidence.status in {"CANCELED", "EXPIRED"}:
                state = evidence.status
            else:
                state = "PARTIAL" if filled else "WORKING"
                # An acknowledgement is not final cancellation. Do not release
                # this gate on an unchanged working snapshot after a cancel request.
                if row["state"] == "CANCEL_PENDING":
                    state = "CANCEL_PENDING"
            if row["state"] in TERMINAL:
                # Even late extra fills must be escalated, not silently accepted
                # after releasing a reservation for a supposedly final snapshot.
                if (
                    state != row["state"]
                    or evidence.model_copy(update={"observed_at": previous.observed_at}) != previous
                ):
                    raise OrderBlocked("final evidence changed; halt and investigate")
                return state
            if evidence == previous and state == row["state"]:
                return state
            conn.execute(
                "UPDATE orders SET state=?,evidence_json=? WHERE client_id=?",
                (state, evidence.model_dump_json(), client_id),
            )
            self._event(
                conn,
                client_id,
                "RECONCILED",
                {
                    "state": state,
                    "evidence": evidence.model_dump(mode="json"),
                },
            )
        return state

    def begin_cancel(self, client_id: str):
        with self._transaction() as conn:
            row = self._row(conn, client_id)
            if row["state"] not in {"WORKING", "PARTIAL"}:
                raise OrderBlocked("order not in cancelable confirmed state")
            evidence = OrderEvidence.model_validate_json(row["evidence_json"])
            request = cancel_request(evidence.root_order_id)
            conn.execute("UPDATE orders SET state='CANCEL_PENDING' WHERE client_id=?", (client_id,))
            self._event(conn, client_id, "CANCEL_PENDING", {})
        return request

    def cancellation_response(self, client_id: str, response: dict):
        with self._transaction() as conn:
            row = self._row(conn, client_id)
            if row["state"] != "CANCEL_PENDING":
                raise OrderBlocked("no pending cancel")
            evidence = OrderEvidence.model_validate_json(row["evidence_json"])
            accepted = cancellation_accepted(response, evidence.root_order_id, client_id)
            if not accepted:
                state = "PARTIAL" if evidence.executions else "WORKING"
                conn.execute("UPDATE orders SET state=? WHERE client_id=?", (state, client_id))
            # Even a successful response only says the request was accepted.
            self._event(conn, client_id, "CANCEL_ACK", {"accepted": accepted})
        return accepted

    def halt(self):
        with self._transaction() as conn:
            conn.execute("UPDATE metadata SET halted=1 WHERE id=1")
            self._event(conn, None, "HALTED", {})

    def snapshot(self) -> dict:
        with self._transaction() as conn:
            orders = [dict(row) for row in conn.execute("SELECT * FROM orders ORDER BY rowid")]
            events = [dict(row) for row in conn.execute("SELECT * FROM events ORDER BY id")]
            halted = bool(conn.execute("SELECT halted FROM metadata WHERE id=1").fetchone()[0])
            mode = conn.execute("SELECT mode FROM metadata WHERE id=1").fetchone()[0]
            gate = self._gate(conn)
            account_guard = (
                None
                if gate is None
                else {
                    "policy": json.loads(gate["policy_json"]),
                    "peak": gate["peak"],
                    "entry_halted": bool(gate["entry_halted"]),
                    "last_proof": json.loads(gate["proof_json"] or "null"),
                }
            )
        for row in orders:
            row["intent"] = json.loads(row.pop("intent_json"))
            row["evidence"] = json.loads(row.pop("evidence_json") or "null")
        for event in events:
            event["payload"] = json.loads(event.pop("payload_json"))
        return {
            "mode": mode,
            "live_enabled": False,
            "account_guard": account_guard,
            "halted": halted,
            "orders": orders,
            "events": events,
        }
