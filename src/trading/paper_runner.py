"""Bounded, restartable operation of an existing frozen FX observation account."""

import argparse
import errno
import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, model_validator

from trading.observer import load_manifest


class RunnerPolicy(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid")

    interval_seconds: int = Field(default=300, ge=60, le=86400)
    cycle_timeout_seconds: int = Field(default=240, ge=1, le=86400)
    attempt_timeout_seconds: int = Field(default=180, ge=1, le=86400)
    max_attempts: int = Field(default=3, ge=1, le=10)
    backoff_seconds: int = Field(default=5, ge=1, le=300)
    max_backoff_seconds: int = Field(default=30, ge=1, le=300)
    failure_alert_threshold: int = Field(default=3, ge=1, le=100)
    stale_after_seconds: int = Field(default=900, ge=60, le=86400)

    @model_validator(mode="after")
    def coherent(self):
        if not self.attempt_timeout_seconds <= self.cycle_timeout_seconds <= self.interval_seconds:
            raise ValueError("attempt timeout <= cycle timeout <= interval is required")
        if self.max_backoff_seconds < self.backoff_seconds:
            raise ValueError("maximum backoff must cover initial backoff")
        if self.stale_after_seconds <= self.interval_seconds:
            raise ValueError("stale threshold must exceed the observation interval")
        return self


SCHEMA = """
CREATE TABLE IF NOT EXISTS identity (
 id INTEGER PRIMARY KEY CHECK(id=1), observer_id TEXT NOT NULL,
 policy_json TEXT NOT NULL, initialized_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cycles (
 id INTEGER PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT,
 status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
 duration_seconds REAL, detail_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS attempts (
 id INTEGER PRIMARY KEY, cycle_id INTEGER NOT NULL REFERENCES cycles(id),
 started_at TEXT NOT NULL, finished_at TEXT NOT NULL, duration_seconds REAL NOT NULL,
 detail_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS alerts (
 id INTEGER PRIMARY KEY, created_at TEXT NOT NULL, kind TEXT NOT NULL,
 detail_json TEXT NOT NULL, acknowledged_at TEXT
);
CREATE TABLE IF NOT EXISTS conditions (
 kind TEXT PRIMARY KEY, raised_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS event_cursor (
 id INTEGER PRIMARY KEY CHECK(id=1), last_event_id INTEGER NOT NULL
);
"""


def _stamp(clock):
    now = clock()
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("runner clock must be timezone-aware")
    return now.astimezone(UTC).isoformat()


@contextmanager
def _process_lock(path):
    """Stable OS lock; process exit releases ownership, file existence proves nothing."""
    with path.open("a+b") as handle:
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        acquired = False
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except OSError as exc:
            # Yield a refusal rather than treating a live owner as a crashed process.
            if exc.errno not in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}:
                raise
        try:
            yield acquired
        finally:
            if acquired:
                handle.seek(0)
                if os.name == "nt":
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def python_process_args(code, *args):
    # Windows venv python.exe can be a redirector: killing it leaves its interpreter
    # child alive. Launch the real interpreter with the current dependency search path.
    bootstrap = "import json,sys; sys.path[:]=json.loads(sys.argv.pop(1)); exec(sys.argv.pop(1))"
    return [sys._base_executable, "-c", bootstrap, json.dumps(sys.path), code, *map(str, args)]


def run_observation(directory, timeout):
    """A timed-out worker may have committed a fill; never claim that it did not."""
    try:
        process = subprocess.run(
            python_process_args(
                "import runpy,sys; sys.argv=['trading.observer','step','--directory',sys.argv[1]]; "
                "runpy.run_module('trading.observer',run_name='__main__')",
                directory,
            ),
            capture_output=True,
            text=True,
            encoding="utf-8",
            env={**os.environ, "PYTHONIOENCODING": "utf-8"},
            timeout=timeout,
            **({"creationflags": subprocess.CREATE_NO_WINDOW} if os.name == "nt" else {}),
        )
    except subprocess.TimeoutExpired:
        return {"status": "error", "error_type": "WorkerTimeout", "outcome_unknown": True}
    if len(process.stdout) > 256_000:
        return {"status": "error", "error_type": "InvalidWorkerOutput", "outcome_unknown": True}
    try:
        result = json.loads(process.stdout)
        if not isinstance(result, dict) or result.get("status") not in {
            "ok",
            "market_closed",
            "error",
            "busy",
        }:
            raise ValueError("unexpected worker status")
        if process.returncode != int(result["status"] == "error"):
            raise ValueError("worker status and exit code disagree")
    except (ValueError, TypeError):
        return {"status": "error", "error_type": "InvalidWorkerOutput", "outcome_unknown": True}
    return result


def retryable(result):
    # Never retry rule/spec/account/freshness validation failures indiscriminately.
    return result.get("status") == "busy" or result.get("error_type") in {
        "WorkerTimeout",
        "ConnectError",
        "ConnectTimeout",
        "ReadTimeout",
        "WriteTimeout",
        "PoolTimeout",
        "ReadError",
        "WriteError",
        "RemoteProtocolError",
    }


class PaperRunner:
    def __init__(
        self,
        directory,
        policy=None,
        *,
        worker=run_observation,
        clock=lambda: datetime.now(UTC),
        monotonic=time.monotonic,
        sleep=time.sleep,
    ):
        self.directory = Path(directory).resolve()
        self.manifest = load_manifest(self.directory)
        if self.manifest.get("mode") != "exploratory_forward_paper":
            raise ValueError("runner requires an existing exploratory paper account")
        self.policy = policy or RunnerPolicy()
        self.worker, self.clock, self.monotonic, self.sleep = worker, clock, monotonic, sleep
        self.database = self.directory / "operations.sqlite"
        self.lock = self.directory / "operations.lock"

    @contextmanager
    def _store(self):
        with closing(sqlite3.connect(self.database, timeout=1)) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys=ON")
            conn.executescript(SCHEMA)
            row = conn.execute("SELECT * FROM identity WHERE id=1").fetchone()
            policy_json = self.policy.model_dump_json()
            if row is None:
                conn.execute(
                    "INSERT INTO identity VALUES (1,?,?,?)",
                    (self.manifest["observer_id"], policy_json, _stamp(self.clock)),
                )
                # Existing observations are history, not new notifications.
                paper = self.directory / "paper.sqlite"
                last_event = 0
                if paper.exists():
                    with closing(sqlite3.connect(paper.as_uri() + "?mode=ro", uri=True)) as old:
                        last_event = old.execute(
                            "SELECT COALESCE(MAX(id),0) FROM events"
                        ).fetchone()[0]
                conn.execute("INSERT INTO event_cursor VALUES (1,?)", (last_event,))
                conn.commit()
            elif row["observer_id"] != self.manifest["observer_id"]:
                raise ValueError("operations database belongs to another observer")
            elif json.loads(row["policy_json"]) != self.policy.model_dump():
                raise ValueError("runner policy changed; use the saved policy for this account")
            yield conn

    def _alert(self, conn, kind, detail):
        conn.execute(
            "INSERT INTO alerts(created_at,kind,detail_json) VALUES (?,?,?)",
            (_stamp(self.clock), kind, json.dumps(detail)),
        )

    def _raise_condition(self, conn, kind, detail):
        if conn.execute("SELECT 1 FROM conditions WHERE kind=?", (kind,)).fetchone() is None:
            conn.execute("INSERT INTO conditions VALUES (?,?)", (kind, _stamp(self.clock)))
            self._alert(conn, kind, detail)

    def _clear_condition(self, conn, kind):
        if conn.execute("DELETE FROM conditions WHERE kind=?", (kind,)).rowcount:
            self._alert(conn, "recovered", {"condition": kind})

    def _collect_events(self, conn):
        paper = self.directory / "paper.sqlite"
        if not paper.exists():
            return
        cursor = conn.execute("SELECT last_event_id FROM event_cursor WHERE id=1").fetchone()
        if cursor is None:
            raise ValueError("operations event cursor missing")
        with closing(sqlite3.connect(paper.as_uri() + "?mode=ro", uri=True)) as source:
            for event_id, payload in source.execute(
                "SELECT id,payload_json FROM events WHERE id>? ORDER BY id", (cursor[0],)
            ):
                event = json.loads(payload)
                if event["filled_units"]:
                    self._alert(
                        conn,
                        "paper_fill",
                        {
                            "event_id": event_id,
                            "observed_at": event["observed_at"],
                            "action": event["action"],
                            "units": event["filled_units"],
                            "price": event["fill_price"],
                        },
                    )
                if event["state"]["halted"]:
                    self._raise_condition(conn, "risk_halted", {"event_id": event_id})
                conn.execute("UPDATE event_cursor SET last_event_id=? WHERE id=1", (event_id,))

    def step(self):
        with _process_lock(self.lock) as owned:
            if not owned:
                return {"status": "busy", "reason": "runner_already_active"}
            with self._store() as conn:
                interrupted = conn.execute(
                    "SELECT id FROM cycles WHERE status='running'"
                ).fetchall()
                for row in interrupted:
                    detail = {"cycle_id": row["id"], "outcome_unknown": True}
                    conn.execute(
                        "UPDATE cycles SET status='interrupted', finished_at=?, "
                        "detail_json=? WHERE id=?",
                        (_stamp(self.clock), json.dumps(detail), row["id"]),
                    )
                    self._alert(conn, "interrupted", detail)
                started = _stamp(self.clock)
                cycle = conn.execute(
                    "INSERT INTO cycles(started_at,status) VALUES (?, 'running')", (started,)
                ).lastrowid
                conn.commit()  # Survives termination before, during, or after paper commit.
                began = self.monotonic()
                deadline = began + self.policy.cycle_timeout_seconds
                result, attempts = {"status": "error", "error_type": "CycleDeadline"}, 0
                for number in range(self.policy.max_attempts):
                    remaining = deadline - self.monotonic()
                    if remaining <= 0:
                        break
                    attempt_started = _stamp(self.clock)
                    attempt_began = self.monotonic()
                    result = self.worker(
                        self.directory, min(self.policy.attempt_timeout_seconds, remaining)
                    )
                    attempts += 1
                    conn.execute(
                        "INSERT INTO attempts(cycle_id,started_at,finished_at,"
                        "duration_seconds,detail_json) VALUES (?,?,?,?,?)",
                        (
                            cycle,
                            attempt_started,
                            _stamp(self.clock),
                            self.monotonic() - attempt_began,
                            json.dumps(result),
                        ),
                    )
                    conn.commit()
                    if not retryable(result) or number + 1 == self.policy.max_attempts:
                        break
                    delay = min(
                        self.policy.backoff_seconds * 2**number, self.policy.max_backoff_seconds
                    )
                    if deadline - self.monotonic() <= delay:
                        break
                    self.sleep(delay)
                duration = self.monotonic() - began
                conn.execute(
                    "UPDATE cycles SET finished_at=?,status=?,attempts=?,duration_seconds=?,"
                    "detail_json=? WHERE id=?",
                    (
                        _stamp(self.clock),
                        result["status"],
                        attempts,
                        duration,
                        json.dumps(result),
                        cycle,
                    ),
                )
                if result["status"] in {"ok", "market_closed"}:
                    self._clear_condition(conn, "consecutive_failures")
                    self._clear_condition(conn, "stale")
                else:
                    streak = 0
                    for row in conn.execute("SELECT status FROM cycles ORDER BY id DESC"):
                        if row["status"] in {"ok", "market_closed"}:
                            break
                        streak += 1
                    if streak >= self.policy.failure_alert_threshold:
                        self._raise_condition(
                            conn, "consecutive_failures", {"cycles": streak, "latest": result}
                        )
                if result.get("event", {}).get("state", {}).get("halted"):
                    self._raise_condition(conn, "risk_halted", {"cycle_id": cycle})
                self._collect_events(conn)
                conn.commit()
                return {
                    "status": result["status"],
                    "cycle_id": cycle,
                    "attempts": attempts,
                    "duration_seconds": duration,
                    "observation": result,
                }

    def check(self):
        with _process_lock(self.lock) as owned:
            if not owned:
                return {"status": "busy", "reason": "runner_already_active"}
            with self._store() as conn:
                report = status(self.directory, now=self.clock())
                if report["stale"]:
                    self._raise_condition(
                        conn, "stale", {"last_started_at": report["last_started_at"]}
                    )
                conn.commit()
                return report


def status(directory, *, now=None):
    directory = Path(directory).resolve()
    now = now or datetime.now(UTC)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("status clock must be timezone-aware")
    manifest = load_manifest(directory)
    path = directory / "operations.sqlite"
    if not path.exists():
        return {"status": "not_started", "stale": True, "last_started_at": None}
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        identity = conn.execute("SELECT * FROM identity WHERE id=1").fetchone()
        if identity is None or identity["observer_id"] != manifest["observer_id"]:
            raise ValueError("operations identity mismatch")
        policy = RunnerPolicy.model_validate(json.loads(identity["policy_json"]))
        last = conn.execute("SELECT * FROM cycles ORDER BY id DESC LIMIT 1").fetchone()
        last_ok = conn.execute(
            "SELECT finished_at FROM cycles WHERE status IN ('ok','market_closed') "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
        last_started = last["started_at"] if last else None
        initialized = datetime.fromisoformat(identity["initialized_at"])
        timestamp = datetime.fromisoformat(last_started) if last_started else initialized
        age = (now - timestamp).total_seconds()
        # A future timestamp does not prove that the job is alive.
        stale = age < 0 or age > policy.stale_after_seconds or last is None
        conditions = [row[0] for row in conn.execute("SELECT kind FROM conditions ORDER BY kind")]
        deliveries = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='deliveries'"
        ).fetchone()
        waiting = conn.execute(
            "SELECT COUNT(*) FROM alerts a LEFT JOIN deliveries d ON a.id=d.alert_id "
            "WHERE a.acknowledged_at IS NULL AND d.submitted_at IS NULL"
            if deliveries
            else "SELECT COUNT(*) FROM alerts WHERE acknowledged_at IS NULL"
        ).fetchone()[0]
        return {
            "status": "stale" if stale else last["status"],
            "stale": stale,
            "observer_id": identity["observer_id"],
            "policy": policy.model_dump(),
            "last_started_at": last_started,
            "last_finished_at": last["finished_at"] if last else None,
            "last_success_at": last_ok[0] if last_ok else None,
            "last_duration_seconds": last["duration_seconds"] if last else None,
            "last_attempts": last["attempts"] if last else 0,
            "last_observation": json.loads(last["detail_json"]) if last else None,
            "conditions": conditions,
            "notifications_waiting_submission": waiting,
            "pending_alerts": conn.execute(
                "SELECT COUNT(*) FROM alerts WHERE acknowledged_at IS NULL"
            ).fetchone()[0],
        }


def read_alerts(directory):
    path = Path(directory).resolve() / "operations.sqlite"
    if not path.exists():
        return []
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as conn:
        return [
            {"id": row[0], "created_at": row[1], "kind": row[2], "detail": json.loads(row[3])}
            for row in conn.execute(
                "SELECT id,created_at,kind,detail_json FROM alerts "
                "WHERE acknowledged_at IS NULL ORDER BY id LIMIT 100"
            )
        ]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("step", "run", "status", "check", "alerts", "ack", "notify")
    )
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--policy", type=Path)
    parser.add_argument("--alert-id", type=int)
    args = parser.parse_args(argv)
    try:
        if args.command == "status":
            result = status(args.directory)
        elif args.command == "alerts":
            result = {"alerts": read_alerts(args.directory)}
        elif args.command == "notify":
            from trading.windows_notify import deliver_alerts

            result = deliver_alerts(args.directory)
        else:
            policy = (
                RunnerPolicy.model_validate_json(args.policy.read_text(encoding="utf-8"))
                if args.policy
                else RunnerPolicy()
            )
            runner = PaperRunner(args.directory, policy)
            if args.command == "step":
                result = runner.step()
            elif args.command == "check":
                result = runner.check()
            elif args.command == "ack":
                if args.alert_id is None or args.alert_id <= 0:
                    raise ValueError("ack requires a positive --alert-id")
                with _process_lock(runner.lock) as owned:
                    if not owned:
                        raise ValueError("runner already active")
                    with runner._store() as conn:
                        changed = conn.execute(
                            "UPDATE alerts SET acknowledged_at=? WHERE id=? "
                            "AND acknowledged_at IS NULL",
                            (_stamp(runner.clock), args.alert_id),
                        ).rowcount
                        if not changed:
                            raise ValueError("alert missing or already acknowledged")
                        conn.commit()
                result = {"status": "acknowledged", "alert_id": args.alert_id}
            else:
                stop = threading.Event()
                signal.signal(signal.SIGINT, lambda *_: stop.set())
                signal.signal(signal.SIGTERM, lambda *_: stop.set())
                while not stop.is_set():
                    began = time.monotonic()
                    print(json.dumps(runner.step(), ensure_ascii=True), flush=True)
                    stop.wait(max(0, runner.policy.interval_seconds - (time.monotonic() - began)))
                result = {"status": "stopped"}
        print(json.dumps(result, ensure_ascii=True, indent=2))
        return int(result.get("status") in {"error", "stale", "not_started", "busy"})
    except (ValueError, OSError, sqlite3.Error) as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "error": str(exc)}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
