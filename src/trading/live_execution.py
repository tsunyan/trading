"""Read-only review of an exact operation before credentials or durable send claims."""

import hashlib
import json
import re
from decimal import Decimal

from trading.account_guard import AccountQuote, AccountSnapshot, evaluate_risk, revision
from trading.broker_contracts import OrderIntent, order_request
from trading.order_journal import OrderBlocked


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def credential_binding(journal, state):
    if state.operations is None:
        raise OrderBlocked("registered_live_operations_required")
    return {
        "live_instance": state.instance,
        "post_instance": state.post_instance,
        "read_instance": state.read_instance,
        "scope": state.scope,
        "account_sha256": digest(state.policy.account_id),
        "operations_sha256": digest(state.operations.model_dump(mode="json")),
        "directories_sha256": digest(
            [str(journal.path), str(journal.posts.path), str(journal.posts.reads.path)]
        ),
    }


def execution_context(
    journal, conn, client_id, *, operation, quote=None, authorization_sha256=None, after_claim=False
):
    if operation not in {"submit", "cancel"} or type(after_claim) is not bool:
        raise OrderBlocked("invalid_execution_operation")
    if operation == "submit" and authorization_sha256 is not None:
        raise OrderBlocked("invalid_submission_authorization")
    if operation == "cancel" and quote is not None:
        raise OrderBlocked("invalid_cancel_quote")
    now = journal._clock(journal.clock())
    state = journal._live_state(conn)
    binding = credential_binding(journal, state)
    risk = None
    if operation == "submit":
        journal._authorize(conn, now)
        row = journal._row(conn, client_id)
        if row["state"] != "PREPARED":
            raise OrderBlocked("live_submission_already_claimed")
        journal._available(conn, client_id)
        intent = OrderIntent.model_validate_json(row["intent_json"])
        plan = order_request(intent, state.limits)
        journal._unclaimed(conn, client_id, plan)
        journal._account_proof(conn)
        if not isinstance(quote, AccountQuote):
            raise OrderBlocked("execution_quote_required")
        quote = AccountQuote.model_validate(quote.model_dump())
        gate = journal._gate(conn)
        proof = json.loads(gate["proof_json"])
        rows = [dict(r) for r in conn.execute("SELECT * FROM orders ORDER BY client_id")]
        if (
            proof["revision"] != revision(rows)
            or quote.observed_at < AccountQuote.model_validate(proof["quote"]).observed_at
        ):
            raise OrderBlocked("execution_account_checkpoint_changed")
        risk = evaluate_risk(
            state.policy,
            AccountSnapshot.model_validate(proof["snapshot"]),
            quote,
            rows,
            intent,
            now,
            Decimal(gate["peak"]),
            bool(gate["entry_halted"]),
        )
        if not risk["allowed"]:
            raise OrderBlocked("live_account_risk_refused")
        kind = "order" if plan.path == "/v1/order" else "close_order"
    else:
        journal._authorize_cancel(conn, client_id, now, authorization_sha256)
        plan, _ = journal._cancel_plan(conn, client_id, now)
        kind = "cancel"
    request_sha256 = hashlib.sha256(plan.body).hexdigest()
    journal.posts.check()
    post = journal.posts.snapshot()
    if after_claim:
        journal.posts.require_operation(kind, request_sha256)
    elif post["blocked"]:
        raise OrderBlocked("execution_post_control_blocked")
    if journal.posts.reads.status()["blocked"]:
        raise OrderBlocked("execution_read_control_blocked")
    context = {
        "version": 1,
        "operation": operation,
        "client_id": client_id,
        "account_id": state.policy.account_id,
        "binding": binding,
        "live_sha256": digest(state.model_dump(mode="json")),
        "implementation_sha256": journal._current_implementation(),
        "orders_sha256": digest(
            [dict(r) for r in conn.execute("SELECT * FROM orders ORDER BY client_id")]
        ),
        "gate_sha256": digest(dict(journal._gate(conn))),
        "event_id": conn.execute("SELECT COALESCE(MAX(id),0) FROM events").fetchone()[0],
        "post_revision": post["revision"] - int(after_claim),
        "authorization_sha256": authorization_sha256,
        "halted": bool(conn.execute("SELECT halted FROM metadata WHERE id=1").fetchone()[0]),
        "path": plan.path,
        "body": json.loads(plan.body),
        "request_sha256": request_sha256,
        "quote": quote.model_dump(mode="json") if quote is not None else None,
        "risk": risk,
    }
    return {**context, "checkpoint_sha256": digest(context)}


def require_checkpoint(context, expected_sha256):
    if (
        not isinstance(expected_sha256, str)
        or not re.fullmatch(r"[a-f0-9]{64}", expected_sha256)
        or context["checkpoint_sha256"] != expected_sha256
    ):
        raise OrderBlocked("execution_checkpoint_changed")
