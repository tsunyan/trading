"""Promotion state of a frozen research candidate: paper, then live. Append-only.

A hypothesis can be promoted to `paper` only after it is frozen, and to `live` only from
`paper` and only when a forward out-of-sample ledger entry for it carries an `advance`
decision. `require_live` lets live tools refuse any strategy settings other than the
frozen candidate's exact configuration. Promotion is an operator record, not a proof
that the strategy is profitable.
"""

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path

from trading.config import Settings, load_settings
from trading.ledger import _connect, _hypothesis, _now
from trading.ledger import decide as ledger_decide

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


CRITERIA_TABLE = """
CREATE TABLE IF NOT EXISTS forward_criteria (
    hypothesis_id TEXT PRIMARY KEY REFERENCES hypotheses(hypothesis_id),
    recorded_at TEXT NOT NULL,
    criteria_json TEXT NOT NULL,
    criteria_sha256 TEXT NOT NULL
)
"""
OPERATORS = {
    ">=": lambda a, b: a >= b,
    "<=": lambda a, b: a <= b,
    ">": lambda a, b: a > b,
    "<": lambda a, b: a < b,
}


class PromotionError(ValueError):
    """Fixed local reasons only."""


def _exists(connection, table):
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone()


def _criteria_rows(connection, hypothesis_id):
    # Reads never add tables to a research ledger; only writes create them.
    if not _exists(connection, "forward_criteria"):
        return None
    return connection.execute(
        "SELECT * FROM forward_criteria WHERE hypothesis_id = ?", (hypothesis_id,)
    ).fetchone()


def _validate_criteria(criteria):
    if not isinstance(criteria, list) or not 1 <= len(criteria) <= 20:
        raise PromotionError("invalid_forward_criteria")
    for item in criteria:
        if (
            not isinstance(item, dict)
            or set(item) != {"path", "op", "value"}
            or not isinstance(item["path"], str)
            or not item["path"]
            or item["op"] not in OPERATORS
            or isinstance(item["value"], bool)
            or not isinstance(item["value"], (int, float))
        ):
            raise PromotionError("invalid_forward_criteria")
    return criteria


def set_criteria(database: Path, hypothesis_id: str, criteria, now: datetime | None = None):
    """Fix the forward-OOS pass criteria after freezing and before any forward result exists."""
    criteria = _validate_criteria(criteria)
    body = json.dumps(criteria, sort_keys=True, separators=(",", ":"))
    with _connect(database) as connection:
        hypothesis = _hypothesis(connection, hypothesis_id)
        if not hypothesis["frozen_at"]:
            raise PromotionError("frozen_candidate_required")
        connection.execute(CRITERIA_TABLE)
        if _criteria_rows(connection, hypothesis_id) is not None:
            raise PromotionError("forward_criteria_already_fixed")
        if connection.execute(
            "SELECT 1 FROM entries WHERE hypothesis_id = ? AND period = 'forward_oos' LIMIT 1",
            (hypothesis_id,),
        ).fetchone():
            raise PromotionError("forward_results_already_recorded")
        connection.execute(
            "INSERT INTO forward_criteria VALUES (?, ?, ?, ?)",
            (hypothesis_id, _now(now), body, hashlib.sha256(body.encode()).hexdigest()),
        )
        return {"hypothesis_id": hypothesis_id, "criteria": criteria}


def _lookup(report, path):
    value = report
    for part in path.split("."):
        if isinstance(value, list) and part.isdigit() and int(part) < len(value):
            value = value[int(part)]
        elif isinstance(value, dict) and part in value:
            value = value[part]
        else:
            return None
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def judge(database: Path, hypothesis_id: str, entry_id: int, now: datetime | None = None):
    """Apply the fixed criteria to one forward-OOS entry and append the ledger decision."""
    with _connect(database) as connection:
        row = _criteria_rows(connection, hypothesis_id)
        if row is None:
            raise PromotionError("forward_criteria_required")
        entry = connection.execute(
            "SELECT * FROM entries WHERE entry_id = ? AND hypothesis_id = ?",
            (entry_id, hypothesis_id),
        ).fetchone()
        if entry is None or entry["period"] != "forward_oos":
            raise PromotionError("forward_oos_entry_required")
        criteria = json.loads(row["criteria_json"])
        directory = Path(entry["run_dir"]) / entry["candidate"]
        raw = (directory / "report.json").read_bytes()
        if hashlib.sha256(raw).hexdigest() != entry["artifact_sha256"]:
            raise PromotionError("report_changed_since_recording")
    report = json.loads(raw)
    checks = []
    for item in criteria:
        actual = _lookup(report, item["path"])
        passed = actual is not None and OPERATORS[item["op"]](actual, item["value"])
        checks.append({**item, "actual": actual, "passed": passed})
    decision = "advance" if all(c["passed"] for c in checks) else "reject"
    reason = (
        "fixed criteria "
        + row["criteria_sha256"][:12]
        + ": "
        + "; ".join(
            f"{c['path']} {c['op']} {c['value']} (actual {c['actual']}) "
            + ("pass" if c["passed"] else "fail")
            for c in checks
        )
    )
    ledger_decide(database, entry_id, decision, reason, now=now)
    return {"entry_id": entry_id, "decision": decision, "checks": checks}


def _history(connection, hypothesis_id):
    # Additive table: the ledger schema version and its existing tables stay unchanged.
    if not _exists(connection, "promotions"):
        return []
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
    criteria = _criteria_rows(connection, hypothesis_id)
    # With fixed criteria, only an advance that judge() derived from them counts.
    prefix = "%" if criteria is None else f"fixed criteria {criteria['criteria_sha256'][:12]}:%"
    return connection.execute(
        "SELECT 1 FROM entries e JOIN decisions d ON d.decision_id = ("
        " SELECT max(decision_id) FROM decisions WHERE entry_id = e.entry_id)"
        " WHERE e.hypothesis_id = ? AND e.period = 'forward_oos' AND d.decision = 'advance'"
        " AND d.reason LIKE ?",
        (hypothesis_id, prefix),
    ).fetchone()


def promote(
    database: Path, hypothesis_id: str, stage: str, reason: str, now: datetime | None = None
):
    if stage not in STAGES:
        raise PromotionError("invalid_promotion_stage")
    if not isinstance(reason, str) or not reason.strip():
        raise PromotionError("promotion_reason_required")
    with _connect(database) as connection:
        connection.execute(TABLE)
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
        connection.execute(TABLE)
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
    if not Path(database).is_file():
        # Opening a missing path would create an empty ledger; a read must not.
        raise PromotionError("ledger_not_found")
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
    parser.add_argument(
        "command", choices=("promote", "revoke", "status", "check", "set-criteria", "judge")
    )
    parser.add_argument("--ledger", type=Path, default=Path("runs/ledger.sqlite"))
    parser.add_argument("--hypothesis", required=True)
    parser.add_argument("--stage", choices=STAGES)
    parser.add_argument("--reason")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--criteria", type=Path)
    parser.add_argument("--entry", type=int)
    args = parser.parse_args(argv)
    try:
        if args.command == "promote":
            result = promote(args.ledger, args.hypothesis, args.stage, args.reason)
        elif args.command == "set-criteria":
            result = set_criteria(
                args.ledger,
                args.hypothesis,
                json.loads(args.criteria.read_text(encoding="utf-8")),
            )
        elif args.command == "judge":
            result = judge(args.ledger, args.hypothesis, args.entry)
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
