"""Local experiment ledger: every evaluated run is a trial counted against a hypothesis.

The ledger lives outside Git (runs/ledger.sqlite by default). Rows are append-only:
decisions are separate rows, so a changed mind stays visible next to the first call.
"""

import json
import re
import sqlite3
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

SCHEMA = """
CREATE TABLE IF NOT EXISTS hypotheses (
    hypothesis_id TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    created_at TEXT NOT NULL,
    frozen_at TEXT
);
CREATE TABLE IF NOT EXISTS entries (
    entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
    recorded_at TEXT NOT NULL,
    imported INTEGER NOT NULL,
    hypothesis_id TEXT NOT NULL REFERENCES hypotheses(hypothesis_id),
    purpose TEXT NOT NULL,
    run_dir TEXT NOT NULL,
    candidate TEXT NOT NULL,
    mode TEXT,
    comparison_id TEXT,
    experiment_id TEXT,
    strategy TEXT,
    strategy_parameters TEXT,
    config_sha256 TEXT,
    data_sha256 TEXT,
    swap_sha256 TEXT,
    data_start TEXT NOT NULL,
    data_end TEXT NOT NULL,
    evaluated_start TEXT NOT NULL,
    period TEXT NOT NULL,
    code_sha256 TEXT,
    git_commit TEXT,
    git_dirty INTEGER,
    verdict TEXT,
    result_viewed INTEGER NOT NULL,
    UNIQUE (run_dir, candidate)
);
CREATE TABLE IF NOT EXISTS decisions (
    decision_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_id INTEGER NOT NULL REFERENCES entries(entry_id),
    decided_at TEXT NOT NULL,
    decision TEXT NOT NULL,
    reason TEXT NOT NULL
);
"""

DECISIONS = ("advance", "reject", "revise")
HYPOTHESIS_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")


@contextmanager
def _connect(database: Path):
    database.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(database, timeout=10, isolation_level=None)) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.executescript(SCHEMA)
        connection.execute("BEGIN IMMEDIATE")
        try:
            yield connection
            connection.execute("COMMIT")
        except BaseException:
            connection.execute("ROLLBACK")
            raise


def _now(now: datetime | None) -> str:
    now = now or datetime.now(UTC)
    if now.tzinfo is None:
        raise ValueError("now must have a timezone")
    return now.astimezone(UTC).isoformat()


def _hypothesis(connection: sqlite3.Connection, hypothesis_id: str) -> sqlite3.Row:
    row = connection.execute(
        "SELECT * FROM hypotheses WHERE hypothesis_id = ?", (hypothesis_id,)
    ).fetchone()
    if row is None:
        raise ValueError(
            f"hypothesis {hypothesis_id!r} is not registered; run `trading ledger add-hypothesis`"
        )
    return row


def add_hypothesis(
    database: Path, hypothesis_id: str, description: str, now: datetime | None = None
) -> dict:
    if not HYPOTHESIS_ID.fullmatch(hypothesis_id):
        raise ValueError("hypothesis id must be 1-64 letters, digits, '_' or '-'")
    if not description.strip():
        raise ValueError("hypothesis description must not be empty")
    with _connect(database) as connection:
        if connection.execute(
            "SELECT 1 FROM hypotheses WHERE hypothesis_id = ?", (hypothesis_id,)
        ).fetchone():
            raise ValueError(f"hypothesis {hypothesis_id!r} already exists")
        connection.execute(
            "INSERT INTO hypotheses VALUES (?, ?, ?, NULL)",
            (hypothesis_id, description.strip(), _now(now)),
        )
        return dict(_hypothesis(connection, hypothesis_id))


def freeze_hypothesis(database: Path, hypothesis_id: str, now: datetime | None = None) -> dict:
    """Mark the point after which newly arriving data is forward out-of-sample."""
    with _connect(database) as connection:
        if _hypothesis(connection, hypothesis_id)["frozen_at"]:
            raise ValueError(f"hypothesis {hypothesis_id!r} is already frozen")
        connection.execute(
            "UPDATE hypotheses SET frozen_at = ? WHERE hypothesis_id = ?",
            (_now(now), hypothesis_id),
        )
        return dict(_hypothesis(connection, hypothesis_id))


def require_hypothesis(database: Path, hypothesis_id: str) -> None:
    """Fail before a run starts, so an unrecordable trial is never executed."""
    with _connect(database) as connection:
        _hypothesis(connection, hypothesis_id)


def _period(evaluated_start: pd.Timestamp, data_end: pd.Timestamp, frozen_at: str | None) -> str:
    """Warm-up bars only feed indicators, so the split is judged from the first decision bar."""
    if frozen_at is None:
        return "research"
    frozen = pd.Timestamp(frozen_at)
    if evaluated_start >= frozen:
        return "forward_oos"
    return "research" if data_end < frozen else "mixed"


def _strategy(config: dict) -> tuple[str, dict]:
    strategy = config.get("strategy", "sma_cross")
    names = ("fast", "slow") if strategy == "sma_cross" else ("lookback", "signal_threshold")
    return strategy, {name: config.get(name) for name in names}


def _evaluated_start(times: pd.Series, report: dict, config: dict) -> pd.Timestamp:
    """First bar whose close can produce a decision.

    Evaluations report their warm-up and start deciding at that index; a plain backtest
    decides as soon as it holds a full warm-up window, one bar earlier.
    """
    if report.get("warmup_bars") is not None:
        index = int(report["warmup_bars"])
    else:
        strategy, parameters = _strategy(config)
        warmup = parameters["slow"] if strategy == "sma_cross" else parameters["lookback"] + 1
        index = warmup - 1
    return pd.Timestamp(times.iloc[min(index, len(times) - 1)])


def _run_rows(run_dir: Path) -> list[tuple[str, Path, dict]]:
    """(candidate name, directory, report) for a single run or each comparison candidate."""
    report = json.loads((run_dir / "report.json").read_text(encoding="utf-8"))
    if "candidates" not in report:
        return [("", run_dir, report)]
    rows = []
    for item in report["candidates"]:
        directory = run_dir / item["candidate"]
        candidate_report = json.loads((directory / "report.json").read_text(encoding="utf-8"))
        rows.append((item["candidate"], directory, candidate_report))
    return rows


def record_run(
    database: Path,
    run_dir: Path,
    hypothesis_id: str,
    purpose: str,
    *,
    imported: bool = False,
    now: datetime | None = None,
) -> list[int]:
    """Record a saved backtest, evaluation or comparison; one entry per evaluated config.

    Results are treated as viewed: the CLI prints them, and for imported runs it is
    unknown whether anyone looked, so the conservative answer is yes.
    """
    if not purpose.strip():
        raise ValueError("purpose must not be empty")
    run_dir = run_dir.resolve()
    parent = json.loads((run_dir / "report.json").read_text(encoding="utf-8"))
    comparison_id = parent.get("comparison_id") if "candidates" in parent else None
    recorded_at = _now(now)
    with _connect(database) as connection:
        frozen_at = _hypothesis(connection, hypothesis_id)["frozen_at"]
        entry_ids = []
        for candidate, directory, report in _run_rows(run_dir):
            config = json.loads((directory / "config.json").read_text(encoding="utf-8"))
            strategy, parameters = _strategy(config)
            times = pd.read_parquet(directory / "bars.parquet", columns=["timestamp"]).timestamp
            data_start, data_end = pd.Timestamp(times.iloc[0]), pd.Timestamp(times.iloc[-1])
            evaluated_start = _evaluated_start(times, report, config)
            verdict = (report.get("verdict") or {}).get("status")
            try:
                cursor = connection.execute(
                    "INSERT INTO entries (recorded_at, imported, hypothesis_id, purpose, run_dir,"
                    " candidate, mode, comparison_id, experiment_id, strategy,"
                    " strategy_parameters, config_sha256, data_sha256, swap_sha256, data_start,"
                    " data_end, evaluated_start, period, code_sha256, git_commit, git_dirty,"
                    " verdict, result_viewed)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
                    (
                        recorded_at,
                        int(imported),
                        hypothesis_id,
                        purpose.strip(),
                        run_dir.as_posix(),
                        candidate,
                        report.get("mode"),
                        comparison_id,
                        report.get("experiment_id"),
                        strategy,
                        json.dumps(parameters, sort_keys=True),
                        report.get("config_sha256"),
                        report.get("data_sha256"),
                        report.get("swap_sha256"),
                        data_start.isoformat(),
                        data_end.isoformat(),
                        evaluated_start.isoformat(),
                        _period(evaluated_start, data_end, frozen_at),
                        report.get("code_sha256"),
                        report.get("git_commit"),
                        None if report.get("git_dirty") is None else int(report["git_dirty"]),
                        verdict,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                if "UNIQUE" not in str(exc):
                    raise
                raise ValueError(f"{run_dir} is already in the ledger") from exc
            entry_ids.append(cursor.lastrowid)
        return entry_ids


def decide(
    database: Path, entry_id: int, decision: str, reason: str, now: datetime | None = None
) -> dict:
    """Append a decision; earlier decisions on the same entry are kept."""
    if decision not in DECISIONS:
        raise ValueError(f"decision must be one of {', '.join(DECISIONS)}")
    if not reason.strip():
        raise ValueError("a decision needs a reason")
    with _connect(database) as connection:
        if not connection.execute(
            "SELECT 1 FROM entries WHERE entry_id = ?", (entry_id,)
        ).fetchone():
            raise ValueError(f"ledger entry {entry_id} does not exist")
        cursor = connection.execute(
            "INSERT INTO decisions (entry_id, decided_at, decision, reason) VALUES (?, ?, ?, ?)",
            (entry_id, _now(now), decision, reason.strip()),
        )
        return dict(
            connection.execute(
                "SELECT * FROM decisions WHERE decision_id = ?", (cursor.lastrowid,)
            ).fetchone()
        )


def summary(database: Path, hypothesis_id: str | None = None) -> dict:
    """Hypotheses with trial and distinct-config counts, and entries with latest decisions."""
    with _connect(database) as connection:
        hypotheses = [
            dict(row)
            for row in connection.execute(
                "SELECT * FROM hypotheses"
                + (" WHERE hypothesis_id = ?" if hypothesis_id else "")
                + " ORDER BY created_at",
                (hypothesis_id,) if hypothesis_id else (),
            )
        ]
        if hypothesis_id and not hypotheses:
            _hypothesis(connection, hypothesis_id)
        for hypothesis in hypotheses:
            entries = [
                dict(row)
                for row in connection.execute(
                    "SELECT e.*, d.decision, d.reason AS decision_reason, d.decided_at"
                    " FROM entries e LEFT JOIN decisions d ON d.decision_id = ("
                    "  SELECT max(decision_id) FROM decisions WHERE entry_id = e.entry_id)"
                    " WHERE e.hypothesis_id = ? ORDER BY e.entry_id",
                    (hypothesis["hypothesis_id"],),
                )
            ]
            for entry in entries:
                entry["strategy_parameters"] = json.loads(entry["strategy_parameters"])
            hypothesis["trials"] = len(entries)
            # Reruns of one config (e.g. after a bug fix) are not new chances to win by luck.
            hypothesis["distinct_configs"] = len(
                {
                    (
                        entry["strategy"],
                        json.dumps(entry["strategy_parameters"], sort_keys=True),
                        entry["config_sha256"],
                    )
                    for entry in entries
                }
            )
            hypothesis["entries"] = entries
        return {"database": str(database), "hypotheses": hypotheses}
