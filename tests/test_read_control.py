import json
import socket
import sqlite3
import subprocess
import sys
import time
from datetime import UTC, datetime

import httpx
import pytest
from pydantic import SecretStr

from trading.broker_contracts import RequestPlan
from trading.private_read import PrivateReadClient, PrivateReadError
from trading.read_control import PersistentReadLimiter, main

SCOPE = "synthetic-account"


class Clock:
    def __init__(self):
        self.wall = 1_790_726_400_000_000_000
        self.mono = 0

    def wall_ns(self):
        return self.wall

    def monotonic_ns(self):
        return self.mono

    def sleep(self, seconds):
        elapsed = round(seconds * 1_000_000_000)
        self.wall += elapsed
        self.mono += elapsed

    def args(self):
        return dict(wall_ns=self.wall_ns, monotonic_ns=self.monotonic_ns, sleep=self.sleep)


def create(tmp_path, clock=None):
    return PersistentReadLimiter.create(tmp_path / "control", SCOPE, **(clock or Clock()).args())


def events(control):
    with sqlite3.connect(control.path) as conn:
        return conn.execute("SELECT kind, token FROM events ORDER BY id").fetchall()


def test_create_reopen_spacing_and_audit(tmp_path):
    clock = Clock()
    first = create(tmp_path, clock)
    assert first.status()["blocked"] is False
    with first.slot():
        assert first.status()["in_flight"]
        assert clock.mono == 250_000_000
    second = PersistentReadLimiter(first.path.parent, SCOPE, **clock.args())
    with second.slot():
        assert clock.mono == 500_000_000
    assert not second.status()["blocked"]
    log = events(first)
    assert [kind for kind, _ in log] == ["CREATED", "CLAIMED", "COMPLETED", "CLAIMED", "COMPLETED"]
    assert log[1][1] == log[2][1] and log[3][1] == log[4][1]
    assert log[1][1] != log[3][1]


def test_nested_or_second_instance_cannot_claim(tmp_path):
    clock = Clock()
    first = create(tmp_path, clock)
    second = PersistentReadLimiter(first.path.parent, SCOPE, **clock.args())
    with first.slot(), pytest.raises(PrivateReadError, match="claim_unresolved"), second.slot():
        pytest.fail("second claim entered")
    assert not second.status()["blocked"]


def test_operator_stop_during_request_is_not_overwritten_by_finish(tmp_path):
    clock = Clock()
    first = create(tmp_path, clock)
    second = PersistentReadLimiter(first.path.parent, SCOPE, **clock.args())
    with first.slot():
        second.stop("operator_stop")
        assert second.status()["in_flight"]
    restored = PersistentReadLimiter(first.path.parent, SCOPE, **clock.args())
    assert restored.status()["stopped"] and not restored.status()["in_flight"]
    with pytest.raises(PrivateReadError, match="account_reads_stopped"), restored.slot():
        pytest.fail("stop lost")
    restored.stop()
    assert restored.status()["reason"] == "operator_stop"


def test_stop_during_wait_prevents_yield(tmp_path):
    clock = Clock()
    control = create(tmp_path, clock)
    peer = PersistentReadLimiter(control.path.parent, SCOPE, **clock.args())

    def wait(seconds):
        clock.sleep(seconds)
        peer.stop("operator_stop")

    control._wait = wait
    with pytest.raises(PrivateReadError, match="account_reads_stopped"), control.slot():
        pytest.fail("stopped request entered")
    assert control.status()["blocked"] and not control.status()["in_flight"]


@pytest.mark.parametrize("failure", [KeyboardInterrupt, SystemExit])
def test_interruption_latches_stop(tmp_path, failure):
    control = create(tmp_path)
    with pytest.raises(failure), control.slot():
        raise failure()
    assert control.status()["reason"] == "interrupted"
    assert not control.status()["in_flight"]


def test_ordinary_failure_releases_claim_but_audits_failure(tmp_path):
    control = create(tmp_path)
    with pytest.raises(RuntimeError), control.slot():
        raise RuntimeError("must-not-be-in-db")
    assert not control.status()["blocked"]
    assert events(control)[-1][0] == "FAILED"
    assert b"must-not-be-in-db" not in control.path.read_bytes()


@pytest.mark.parametrize("timing", ["before", "after"])
def test_backwards_wall_clock_persists_stop(tmp_path, timing):
    clock = Clock()
    control = create(tmp_path, clock)
    with control.slot():
        pass
    if timing == "before":
        clock.wall -= 1
        with pytest.raises(PrivateReadError, match="clock_invalid"), control.slot():
            pytest.fail("bad clock entered")
    else:
        with pytest.raises(PrivateReadError, match="clock_invalid"), control.slot():
            clock.wall -= 1
    assert control.status()["reason"] == "clock_invalid"
    restored = PersistentReadLimiter(control.path.parent, SCOPE, **clock.args())
    with pytest.raises(PrivateReadError, match="account_reads_stopped"), restored.slot():
        pytest.fail("clock halt lost")


@pytest.mark.parametrize("elapsed", [0, -1, 6])
def test_early_wake_reverse_monotonic_or_suspend_is_stopped(tmp_path, elapsed):
    clock = Clock()
    control = create(tmp_path, clock)
    control._wait = lambda seconds: clock.sleep(elapsed)
    with pytest.raises(PrivateReadError), control.slot():
        pytest.fail("invalid timing entered")
    assert control.status()["reason"] == "clock_invalid"


def test_forward_wall_jump_cannot_skip_spacing(tmp_path):
    clock = Clock()
    control = create(tmp_path, clock)
    with control.slot():
        pass
    clock.wall += 100_000_000_000
    other = PersistentReadLimiter(control.path.parent, SCOPE, **clock.args())
    before = clock.mono
    with other.slot():
        assert clock.mono - before == 250_000_000


def test_clock_callback_error_is_redacted_and_stopped(tmp_path):
    control = create(tmp_path)

    def broken():
        raise RuntimeError("private-clock-detail")

    control._mono_ns = broken
    with pytest.raises(PrivateReadError) as caught, control.slot():
        pytest.fail("invalid clock entered")
    assert str(caught.value) == "control_wait_invalid"
    assert control.status()["reason"] == "clock_invalid"


def test_storage_failure_after_claim_cannot_release_claim(tmp_path):
    control = create(tmp_path)
    saved = control.path.with_suffix(".saved")
    with pytest.raises(PrivateReadError), control.slot():
        control.path.rename(saved)
    saved.rename(control.path)
    restored = PersistentReadLimiter(control.path.parent, SCOPE)
    assert restored.status()["in_flight"]
    with pytest.raises(PrivateReadError, match="storage_failed"), control.slot():
        pytest.fail("failed instance reused")


def test_missing_database_never_recreated(tmp_path):
    with pytest.raises(PrivateReadError, match="storage_failed"):
        PersistentReadLimiter(tmp_path, SCOPE)
    assert not (tmp_path / "read-control.sqlite").exists()
    control = create(tmp_path)
    control.path.rename(control.path.with_suffix(".saved"))
    with pytest.raises(PrivateReadError), control.slot():
        pytest.fail("missing db entered")
    assert not control.path.exists()


def test_existing_directory_and_wrong_scope_rejected(tmp_path):
    with pytest.raises(FileExistsError):
        PersistentReadLimiter.create(tmp_path, SCOPE)
    control = create(tmp_path)
    with pytest.raises(PrivateReadError, match="identity_or_state"):
        PersistentReadLimiter(control.path.parent, "other-account")
    assert not control.status()["blocked"]


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM control",
        "UPDATE control SET version=99",
        "UPDATE control SET instance_id='ffffffffffffffffffffffffffffffff'",
        "UPDATE control SET in_flight='not-a-token'",
        "UPDATE control SET reason='invalid'",
        "DROP TABLE events",
    ],
)
def test_corruption_or_replacement_is_fail_closed(tmp_path, sql):
    control = create(tmp_path)
    with sqlite3.connect(control.path) as conn:
        conn.execute(sql)
    with pytest.raises(PrivateReadError), control.slot():
        pytest.fail("bad store entered")


def test_claim_mismatch_never_clears_another_token(tmp_path):
    control = create(tmp_path)
    with pytest.raises(PrivateReadError, match="claim_mismatch"), control.slot():
        with sqlite3.connect(control.path) as conn:
            conn.execute("UPDATE control SET in_flight=?", ("f" * 32,))
    assert control.status()["reason"] == "claim_mismatch"
    assert control.status()["in_flight"]


@pytest.mark.parametrize("status", [401, 403, 429])
def test_http_stop_survives_restart_and_never_exposes_credentials(tmp_path, monkeypatch, status):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("network attempted"))
    clock = Clock()
    control = create(tmp_path, clock)
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(status, text="remote-secret")

    def client(limiter):
        return PrivateReadClient(
            SecretStr("dummy-key"),
            SecretStr("dummy-secret"),
            limiter=limiter,
            transport=httpx.MockTransport(handler),
            clock=lambda: datetime.fromtimestamp(clock.wall / 1e9, UTC),
            monotonic=lambda: clock.mono / 1e9,
        )

    with client(control) as transport, pytest.raises(PrivateReadError):
        transport.get(RequestPlan("GET", "/v1/account/assets"))
    restored = PersistentReadLimiter(control.path.parent, SCOPE, **clock.args())
    with client(restored) as transport, pytest.raises(PrivateReadError, match="reads_stopped"):
        transport.get(RequestPlan("GET", "/v1/account/assets"))
    assert len(calls) == 1
    for secret in [b"dummy-key", b"dummy-secret", b"remote-secret"]:
        assert secret not in control.path.read_bytes()


def test_crashed_process_claim_never_expires(tmp_path):
    control = PersistentReadLimiter.create(tmp_path / "control", SCOPE)
    script = """
import os, sys
from pathlib import Path
from trading.read_control import PersistentReadLimiter
control = PersistentReadLimiter(Path(sys.argv[1]), sys.argv[2])
with control.slot():
    os._exit(23)
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(control.path.parent), SCOPE],
        capture_output=True,
        timeout=10,
    )
    assert result.returncode == 23
    restored = PersistentReadLimiter(control.path.parent, SCOPE)
    assert restored.status()["in_flight"] and restored.status()["blocked"]
    with pytest.raises(PrivateReadError, match="claim_unresolved"), restored.slot():
        pytest.fail("crashed claim reclaimed")
    restored.stop("operator_stop")
    assert restored.status()["in_flight"]


def test_two_processes_cannot_overlap(tmp_path):
    control = PersistentReadLimiter.create(tmp_path / "control", SCOPE)
    marker = tmp_path / "entered.txt"
    script = """
import sys, time
from pathlib import Path
from trading.read_control import PersistentReadLimiter
control = PersistentReadLimiter(Path(sys.argv[1]), sys.argv[2])
with control.slot():
    Path(sys.argv[3]).write_text('entered')
    time.sleep(1.5)
"""
    child = subprocess.Popen(
        [sys.executable, "-c", script, str(control.path.parent), SCOPE, str(marker)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 5
        while not marker.exists() and time.monotonic() < deadline and child.poll() is None:
            time.sleep(0.01)
        assert marker.exists()
        with pytest.raises(PrivateReadError, match="claim_unresolved"), control.slot():
            pytest.fail("overlapping request")
        control.stop("operator_stop")
        assert control.status()["stopped"]
        _, error = child.communicate(timeout=5)
        assert child.returncode == 0, error.decode()
        assert control.status()["stopped"] and not control.status()["in_flight"]
    finally:
        if child.poll() is None:
            child.terminate()
            child.communicate(timeout=5)


def test_local_cli_status_and_stop_no_reset(tmp_path, capsys):
    args = ["--directory", str(tmp_path / "cli"), "--scope", SCOPE]
    main(["init", *args])
    assert not json.loads(capsys.readouterr().out)["blocked"]
    main(["stop", *args])
    assert json.loads(capsys.readouterr().out)["reason"] == "operator_stop"
    main(["status", *args])
    assert json.loads(capsys.readouterr().out)["blocked"]
    with pytest.raises(SystemExit):
        main(["reset", *args])


def test_busy_finish_retries_without_repeating_get_or_poisoning_control(tmp_path):
    clock = Clock()
    control = create(tmp_path, clock)
    lock = sqlite3.connect(control.path)
    retries, requests = [], []

    def wait(seconds):
        clock.sleep(seconds)
        if seconds == 0.05:
            retries.append(seconds)
            lock.rollback()

    control._wait = wait
    try:
        with control.slot():
            requests.append(True)
            lock.execute("BEGIN IMMEDIATE")
        assert requests == [True] and retries == [0.05]
        assert not control.status()["blocked"]
        assert [kind for kind, _ in events(control)] == ["CREATED", "CLAIMED", "COMPLETED"]
    finally:
        lock.close()


def test_busy_finish_exhaustion_keeps_durable_claim_and_readable_status(tmp_path):
    control = create(tmp_path)
    with sqlite3.connect(control.path) as lock:
        with pytest.raises(PrivateReadError, match="storage_busy"), control.slot():
            lock.execute("BEGIN IMMEDIATE")
        lock.rollback()
    assert control.status()["in_flight"]
    with pytest.raises(PrivateReadError, match="claim_unresolved"), control.slot():
        pytest.fail("unresolved claim reused")


def test_failed_stop_under_busy_cannot_release_claim_or_resume(tmp_path):
    control = create(tmp_path)
    with sqlite3.connect(control.path) as lock:
        with pytest.raises(PrivateReadError), control.slot():
            lock.execute("BEGIN IMMEDIATE")
            control.stop()
        lock.rollback()
    with pytest.raises(PrivateReadError, match="storage_failed"), control.slot():
        pytest.fail("failed stop resumed")
    assert PersistentReadLimiter(control.path.parent, SCOPE).status()["in_flight"]


def test_generation_query_uses_index_in_existing_database(tmp_path):
    control = create(tmp_path)
    with sqlite3.connect(control.path) as conn:
        conn.execute("DROP INDEX events_kind_id")
    PersistentReadLimiter(control.path.parent, SCOPE)
    with sqlite3.connect(control.path) as conn:
        plan = conn.execute(
            "EXPLAIN QUERY PLAN SELECT COALESCE(MAX(id),0) FROM events "
            "WHERE kind='RECOVERY_APPROVED'"
        ).fetchall()
    assert any("events_kind_id" in row[-1] for row in plan)


def test_api_body_stop_survives_control_reopen(tmp_path):
    clock = Clock()
    control = create(tmp_path, clock)
    with PrivateReadClient(
        SecretStr("fixture-key"),
        SecretStr("fixture-secret"),
        limiter=control,
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"status": 1})),
        clock=lambda: datetime(2026, 9, 30, tzinfo=UTC),
    ) as client:
        with pytest.raises(PrivateReadError, match="api_error_stop"):
            client.get(RequestPlan("GET", "/v1/account/assets"))
    status = PersistentReadLimiter(control.path.parent, SCOPE).status()
    assert status["stopped"] and not status["in_flight"]
