"""Promotion state of a frozen research candidate: paper, then live. Append-only.

A hypothesis can be promoted to `paper` only after it is frozen, and to `live` only from
`paper` and only when a forward out-of-sample ledger entry for it carries an `advance`
decision. `require_live` lets live tools refuse any strategy settings other than the
frozen candidate's exact configuration. Promotion is an operator record, not a proof
that the strategy is profitable.
"""

import argparse
import json
from datetime import datetime
from pathlib import Path

from trading.config import Settings, load_settings
from trading.ledger import _connect, _hypothesis, _now

STAGES = ("paper", "live")
ACTIONS = (*STAGES, "revoked")
TABLE = """
CREATE TABLE IF NOT EXISTS promotions (
    promotion_id INTEGER PRIMARY KEY AUTOINCREMENT,
    hypothesis_id TEXT NOT NULL REFERENCES hypotheses(hypothesis_id),
    action TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    reason TEXT NOT NULL,
    frozen_spec TEXT NOT NULL
)
"""


class PromotionError(ValueError):
    """Fixed local reasons only."""


def _history(connection, hypothesis_id):
    # Additive table: the ledger schema version and its existing tables stay unchanged.
    connection.execute(TABLE)
    return [
        dict(row)
        for row in connection.execute(
            "SELECT * FROM promotions WHERE hypothesis_id = ? ORDER BY promotion_id",
            (hypothesis_id,),
        )
    ]


def _stage(history):
    return history[-1]["action"] if history else None


def _forward_advanced(connection, hypothesis_id):
    return connection.execute(
        "SELECT 1 FROM entries e JOIN decisions d ON d.decision_id = ("
        " SELECT max(decision_id) FROM decisions WHERE entry_id = e.entry_id)"
        " WHERE e.hypothesis_id = ? AND e.period = 'forward_oos' AND d.decision = 'advance'",
        (hypothesis_id,),
    ).fetchone()


def promote(
    database: Path, hypothesis_id: str, stage: str, reason: str, now: datetime | None = None
):
    if stage not in STAGES:
        raise PromotionError("invalid_promotion_stage")
    if not isinstance(reason, str) or not reason.strip():
        raise PromotionError("promotion_reason_required")
    with _connect(database) as connection:
        hypothesis = _hypothesis(connection, hypothesis_id)
        if not hypothesis["frozen_at"]:
            raise PromotionError("frozen_candidate_required")
        history = _history(connection, hypothesis_id)
        current = _stage(history)
        if stage == "paper" and current not in {None, "revoked"}:
            raise PromotionError("already_promoted")
        if stage == "live":
            if current != "paper":
                raise PromotionError("paper_stage_required")
            if not _forward_advanced(connection, hypothesis_id):
                raise PromotionError("advanced_forward_oos_entry_required")
        connection.execute(
            "INSERT INTO promotions (hypothesis_id, action, recorded_at, reason, frozen_spec)"
            " VALUES (?, ?, ?, ?, ?)",
            (hypothesis_id, stage, _now(now), reason.strip(), hypothesis["frozen_spec"]),
        )
        return status_in(connection, hypothesis_id)


def revoke(database: Path, hypothesis_id: str, reason: str, now: datetime | None = None):
    if not isinstance(reason, str) or not reason.strip():
        raise PromotionError("promotion_reason_required")
    with _connect(database) as connection:
        hypothesis = _hypothesis(connection, hypothesis_id)
        if _stage(_history(connection, hypothesis_id)) in {None, "revoked"}:
            raise PromotionError("nothing_to_revoke")
        connection.execute(
            "INSERT INTO promotions (hypothesis_id, action, recorded_at, reason, frozen_spec)"
            " VALUES (?, 'revoked', ?, ?, ?)",
            (hypothesis_id, _now(now), reason.strip(), hypothesis["frozen_spec"] or "null"),
        )
        return status_in(connection, hypothesis_id)


def status_in(connection, hypothesis_id):
    hypothesis = _hypothesis(connection, hypothesis_id)
    history = _history(connection, hypothesis_id)
    return {
        "hypothesis_id": hypothesis_id,
        "frozen_at": hypothesis["frozen_at"],
        "frozen_spec": json.loads(hypothesis["frozen_spec"]) if hypothesis["frozen_spec"] else None,
        "stage": _stage(history),
        "history": [{k: row[k] for k in ("action", "recorded_at", "reason")} for row in history],
    }


def status(database: Path, hypothesis_id: str):
    with _connect(database) as connection:
        return status_in(connection, hypothesis_id)


def require_live(database: Path, hypothesis_id: str, cfg: Settings):
    """The live candidate's frozen spec, only for exactly its configuration."""
    if not isinstance(cfg, Settings):
        raise PromotionError("strategy_settings_required")
    current = status(database, hypothesis_id)
    if current["stage"] != "live":
        raise PromotionError("strategy_not_promoted_for_live")
    spec = current["frozen_spec"]
    if (
        spec["config_sha256"] != cfg.fingerprint
        or spec["strategy"] != cfg.strategy
        or spec["strategy_parameters"] != cfg.strategy_parameters
    ):
        raise PromotionError("strategy_config_differs_from_candidate")
    return spec


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("promote", "revoke", "status", "check"))
    parser.add_argument("--ledger", type=Path, default=Path("runs/ledger.sqlite"))
    parser.add_argument("--hypothesis", required=True)
    parser.add_argument("--stage", choices=STAGES)
    parser.add_argument("--reason")
    parser.add_argument("--config", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "promote":
            result = promote(args.ledger, args.hypothesis, args.stage, args.reason)
        elif args.command == "revoke":
            result = revoke(args.ledger, args.hypothesis, args.reason)
        elif args.command == "check":
            spec = require_live(args.ledger, args.hypothesis, load_settings(args.config))
            result = {"hypothesis_id": args.hypothesis, "stage": "live", "frozen_spec": spec}
        else:
            result = status(args.ledger, args.hypothesis)
    except PromotionError as error:
        parser.exit(2, f"promotion_failed: {error}\n")
    except Exception as error:
        parser.exit(2, f"promotion_failed: {type(error).__name__}\n")
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
