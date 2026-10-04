"""Read-only readiness report: which local gate would refuse the next live send, and why.

Each gate is evaluated on its own, so one report lists every blocking condition instead of
the first refusal a send would hit. No credentials, HTTP, claims or state changes.
"""

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from trading.account_guard import AccountPolicy, AccountSnapshot, fresh
from trading.config import load_settings
from trading.live_operations import LiveOperations
from trading.private_order_recovery import PrivateOrderRecovery
from trading.promotion import require_live

SETTLED = {"FILLED", "CANCELED", "EXPIRED", "ABANDONED"}


def _gate(check):
    try:
        reason = check()
    except Exception as error:
        text = str(error)
        reason = text if text and len(text) <= 64 and " " not in text else type(error).__name__
    return {"ok": reason is None, "reason": reason}


CYCLE_STALE_SECONDS = 2 * 3600


def diagnose(journal, now, *, candidate=None, cycle=None):
    def reads():
        status = journal.posts.reads.status()
        return "read_control_blocked" if status["blocked"] else None

    def posts():
        post = journal.posts.snapshot()
        if post["phase"] == "STOPPED":
            return f"post_stopped:{post['reason']}"
        journal.posts.check()
        if post["claim"] is not None:
            return "post_claim_in_flight"
        return "post_control_blocked" if post["blocked"] else None

    with journal._transaction() as conn:
        state = journal._live_state(conn)
        gate = journal._gate(conn)
        halted = bool(conn.execute("SELECT halted FROM metadata").fetchone()[0])
        orders = [
            {"client_id": row["client_id"], "state": row["state"]}
            for row in conn.execute("SELECT client_id,state FROM orders ORDER BY client_id")
        ]

    def approval():
        if state.phase != "ENABLED":
            return f"phase_{state.phase.lower()}"
        if halted:
            return "journal_halted"
        if state.implementation_sha256 != journal._current_implementation():
            return "implementation_changed"
        if state.approval is None or not state.approval.accepted_at <= now:
            return "approval_missing"
        return "approval_expired" if now >= state.approval.expires_at else None

    def operations():
        if state.operations is None:
            return "operations_not_registered"
        LiveOperations(
            state.operations,
            journal._operations_target(state, state.operations.sync_instance),
            clock=journal.clock,
            monotonic=journal.posts._mono,
        ).require_healthy(now)
        return None

    def account():
        if not gate["proof_json"]:
            return "account_proof_missing"
        policy = AccountPolicy.model_validate_json(gate["policy_json"])
        snapshot = AccountSnapshot.model_validate(json.loads(gate["proof_json"])["snapshot"])
        if not fresh(snapshot.observed_at, now, policy.max_snapshot_age_seconds):
            return "account_proof_stale"
        return None

    def queue():
        # A PREPARED order is what a send uses; anything else unsettled blocks the next one.
        waiting = [o for o in orders if o["state"] not in SETTLED | {"PREPARED"}]
        return f"orders_awaiting_reconciliation:{len(waiting)}" if waiting else None

    def promotion():
        ledger, hypothesis, cfg = candidate
        require_live(ledger, hypothesis, cfg)
        return None

    gates = {
        "read_control": _gate(reads),
        "post_control": _gate(posts),
        "approval": _gate(approval),
        "sync_and_watchdog": _gate(operations),
        "account_proof": _gate(account),
        "order_queue": _gate(queue),
    }
    if candidate is not None:
        # Only new entries depend on promotion; closes and flattening never do.
        gates["strategy_promotion"] = _gate(promotion)

    def cycle_freshness():
        finished = (cycle or {}).get("finished_at")
        if not finished:
            return "cycle_result_missing"
        age = (now - datetime.fromisoformat(finished)).total_seconds()
        return "cycle_stale" if age > CYCLE_STALE_SECONDS or age < -60 else None

    if cycle is not None:
        # The hourly task itself: a page or check that never notices it stopped is blind.
        gates["scheduled_cycle"] = _gate(cycle_freshness)
    expires = state.approval.expires_at if state.approval is not None else None
    return {
        "checked_at": now.isoformat(),
        "send_ready": all(g["ok"] for g in gates.values()),
        "gates": gates,
        "entry_halted": bool(gate["entry_halted"]),
        "approval_expires_at": expires.isoformat() if expires else None,
        "approval_seconds_left": (
            max(0, int((expires - now).total_seconds())) if expires is not None else None
        ),
        "orders": orders,
        "network_used": False,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--read-control-directory", type=Path, required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--ledger", type=Path)
    parser.add_argument("--hypothesis")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--cycle-result", type=Path)
    args = parser.parse_args(argv)
    try:
        journal = PrivateOrderRecovery(
            args.directory, args.read_control_directory, args.scope
        ).journal
        given = [v is not None for v in (args.ledger, args.hypothesis, args.config)]
        if any(given) and not all(given):
            raise ValueError("ledger_hypothesis_and_config_required_together")
        candidate = (
            (args.ledger, args.hypothesis, load_settings(args.config)) if all(given) else None
        )
        cycle = None
        if args.cycle_result is not None:
            try:
                cycle = json.loads(args.cycle_result.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                cycle = {}
        result = diagnose(journal, datetime.now(UTC), candidate=candidate, cycle=cycle)
    except Exception as error:
        parser.exit(2, f"live_doctor_failed: {type(error).__name__}\n")
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["send_ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
