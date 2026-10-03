"""Persistent account POST pacing, ownership and unresolved outcome fences."""

import ctypes
import json
import socket
import sqlite3
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import httpx
import pytest
from test_private_stream import client, response
from test_private_supervisor import Socket
from test_private_sync import make_setup, options

from trading.paper_runner import python_process_args
from trading.post_control import PersistentPostLimiter, PostControlError, main
from trading.private_supervisor import PrivateStreamSupervisor
from trading.read_control import PersistentReadLimiter


@pytest.fixture(autouse=True)
def no_native_or_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("real network attempted"))
    monkeypatch.setattr(
        ctypes, "WinDLL", lambda *a, **k: pytest.fail("real credentials attempted"), raising=False
    )


class Clock:
    def __init__(self):
        self.wall, self.mono = 1_700_000_000_000_000_000, 0.0

    def advance(self, seconds):
        self.wall += int(seconds * 1e9)
        self.mono += seconds

    def args(self):
        return {"wall_ns": lambda: self.wall, "monotonic": lambda: self.mono, "sleep": self.advance}


@pytest.fixture
def setup(tmp_path):
    clock = Clock()
    reads = PersistentReadLimiter.create(tmp_path / "reads", "synthetic")
    control = PersistentPostLimiter.create(tmp_path / "posts", reads, **clock.args())
    return clock, reads, control


def reopen(control, reads, clock):
    return PersistentPostLimiter(control.path.parent, reads, **clock.args())


def test_create_status_binding_and_minimum_wait_survive_reopen(setup):
    clock, reads, control = setup
    assert reads.post_binding() == {
        "instance": control.snapshot()["instance"],
        "path": str(control.path.parent),
    }
    assert not control.snapshot()["blocked"] and clock.mono == 0
    for index in range(2):
        other = reopen(control, reads, clock)
        with other.slot():
            assert clock.mono == pytest.approx(1.1 * (index + 1))
            assert other.snapshot()["phase"] == "IN_FLIGHT"
        assert other.snapshot()["phase"] == "READY"
    assert control.snapshot()["revision"] == 4
    assert not control.snapshot()["live_enabled"] and not control.snapshot()["complete"]


def test_forward_wall_jump_and_new_process_object_cannot_skip_wait(setup):
    clock, reads, control = setup
    with control.slot():
        pass
    clock.wall += 10_000_000_000_000
    with reopen(control, reads, clock).slot():
        assert clock.mono == pytest.approx(2.2)


def test_small_early_timer_return_waits_remaining_time_before_yield(setup):
    clock, _, control = setup
    waits = []

    def wait(seconds):
        waits.append(seconds)
        clock.advance(seconds - 0.00001 if len(waits) == 1 else seconds)

    control._sleep = wait
    with control.slot():
        assert clock.mono >= 1.1
    assert waits == [1.1, 0.01]


@pytest.mark.parametrize("kind", ["exception", "interrupt"])
def test_ambiguous_outcome_never_releases_claim_or_automatically_recovers(setup, kind):
    clock, reads, control = setup
    error = RuntimeError if kind == "exception" else KeyboardInterrupt
    with pytest.raises(error), control.operation("order", request_sha256="a" * 64):
        raise error("synthetic")
    saved = control.snapshot()
    assert saved["phase"] == "STOPPED" and saved["operation"] == "order"
    assert saved["request_sha256"] == "a" * 64 and saved["claim"] is not None
    clock.advance(24 * 3600)
    with pytest.raises(PostControlError), reopen(control, reads, clock).slot():
        pytest.fail("unresolved POST was released")
    assert reads.post_binding()["instance"] == saved["instance"]


def test_nested_and_second_instance_cannot_claim_while_actual_owner_is_live(setup):
    clock, reads, control = setup
    with control.slot():
        for other in (control, reopen(control, reads, clock)):
            with pytest.raises(PostControlError, match="post_owner_busy"), other.slot():
                pytest.fail("parallel POST")
        assert control.check() == clock.mono
        assert reopen(control, reads, clock).check() == clock.mono
    assert control.snapshot()["phase"] == "READY"


def test_foreign_claim_completed_during_owner_probe_is_checked_again(setup, monkeypatch):
    clock, reads, control = setup
    peer = reopen(control, reads, clock)
    operation = control.slot()
    operation.__enter__()
    original = peer._ownership
    completed = False

    @contextmanager
    def after_completion():
        nonlocal completed
        operation.__exit__(None, None, None)
        completed = True
        with original() as handle:
            yield handle

    monkeypatch.setattr(peer, "_ownership", after_completion)
    try:
        assert peer.check() == clock.mono
        assert peer.snapshot()["phase"] == "READY"
    finally:
        if not completed:
            operation.__exit__(None, None, None)


def test_operator_stop_during_operation_persists_after_known_completion(setup):
    clock, reads, control = setup
    peer = reopen(control, reads, clock)
    with control.slot():
        peer.stop()
    state = control.snapshot()
    assert state["phase"] == "STOPPED" and state["claim"] is None
    assert state["reason"] == "operator_stop"
    with pytest.raises(PostControlError), control.check():
        pass


@pytest.mark.parametrize(
    "kind", ["wall", "mono", "short_wait", "long_wait", "wait_exception", "stop_during_wait"]
)
def test_invalid_clock_or_wait_never_yields_or_clears_consumed_claim(setup, kind):
    clock, reads, control = setup

    def wait(seconds):
        if kind == "wait_exception":
            raise RuntimeError("synthetic")
        clock.advance(
            seconds / 2 if kind == "short_wait" else 31 if kind == "long_wait" else seconds
        )
        if kind == "wall":
            clock.wall -= 10_000_000_000
        elif kind == "mono":
            clock.mono = -1
        elif kind == "stop_during_wait":
            reopen(control, reads, clock).stop()

    control._sleep = wait
    with pytest.raises((PostControlError, RuntimeError)), control.slot():
        pytest.fail("invalid wait yielded")
    saved = control.snapshot()
    assert saved["phase"] == "STOPPED" and saved["claim"] is not None


@pytest.mark.parametrize("kind", ["db", "lock", "lock_content", "digest", "transition"])
def test_missing_changed_or_corrupt_store_is_not_recreated(setup, kind):
    clock, reads, control = setup
    if kind == "db":
        control.path.rename(control.path.with_suffix(".saved"))
    elif kind == "lock":
        control.lock_path.rename(control.lock_path.with_suffix(".saved"))
        control.lock_path.write_text("0" * 32)
    elif kind == "lock_content":
        control.lock_path.write_text("0" * 32)
    else:
        with sqlite3.connect(control.path) as conn:
            conn.execute(
                "UPDATE control SET digest=?" if kind == "digest" else "UPDATE events SET digest=?",
                ("0" * 64,),
            )
    with pytest.raises(PostControlError):
        with reopen(control, reads, clock).slot():
            pytest.fail("changed storage used")
    if kind == "db":
        assert not control.path.exists()


def test_completion_storage_failure_retains_claim(setup, monkeypatch):
    clock, reads, control = setup
    original = control._write

    def failed(conn, state, kind, **changes):
        if kind == "COMPLETED":
            raise sqlite3.OperationalError("synthetic")
        return original(conn, state, kind, **changes)

    monkeypatch.setattr(control, "_write", failed)
    with pytest.raises(PostControlError), control.slot():
        pass
    peer = reopen(control, reads, clock)
    assert peer.snapshot()["phase"] == "IN_FLIGHT"
    with pytest.raises(PostControlError, match="post_claim_unresolved"):
        peer.check()


@pytest.mark.parametrize("damage", ["empty", "drop", "change", "receipt"])
def test_permanent_binding_rejects_alternate_domain_and_corrupt_binding(setup, tmp_path, damage):
    _, reads, control = setup
    with pytest.raises(PostControlError, match="post_control_already_bound"):
        PersistentPostLimiter.create(tmp_path / "second", reads)
    assert not (tmp_path / "second").exists()
    with sqlite3.connect(reads.path) as conn:
        conn.execute(
            {
                "empty": "DELETE FROM post_binding",
                "drop": "DROP TABLE post_binding",
                "change": "UPDATE post_binding SET instance='" + "0" * 32 + "'",
                "receipt": "DELETE FROM events WHERE kind='POST_BOUND'",
            }[damage]
        )
    with pytest.raises(ValueError, match="post_binding_integrity_failed"):
        PersistentReadLimiter(reads.path.parent, reads.scope).post_binding()
    assert control.path.exists()


def test_binding_race_has_one_winner(tmp_path):
    reads = PersistentReadLimiter.create(tmp_path / "reads", "synthetic")

    def create(index):
        peer = PersistentReadLimiter(reads.path.parent, reads.scope)
        try:
            control = PersistentPostLimiter.create(tmp_path / f"posts{index}", peer)
            return control.snapshot()["instance"]
        except ValueError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        winners = [result for result in pool.map(create, range(2)) if result is not None]
    assert winners == [reads.post_binding()["instance"]]


def test_actual_process_death_releases_os_owner_but_not_post_claim(setup, tmp_path):
    _, reads, control = setup
    ready = tmp_path / "ready"
    code = (
        "import time; from pathlib import Path; "
        "from trading.read_control import PersistentReadLimiter; "
        "from trading.post_control import PersistentPostLimiter; "
        "r=PersistentReadLimiter(Path(sys.argv[1]),'synthetic'); "
        "p=PersistentPostLimiter(Path(sys.argv[2]),r)\n"
        "with p.slot():\n Path(sys.argv[3]).write_text('ready'); time.sleep(30)"
    )
    child = subprocess.Popen(
        python_process_args(code, reads.path.parent, control.path.parent, ready)
    )
    try:
        deadline = time.monotonic() + 10
        while not ready.exists() and child.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert ready.read_text() == "ready"
        with pytest.raises(PostControlError, match="post_owner_busy"), control.slot():
            pass
        peer = PersistentPostLimiter(control.path.parent, reads)
        assert peer.check() >= 0  # Non-sending token checks accept a live owner.
        child.kill()
        child.wait(timeout=10)
        peer = PersistentPostLimiter(control.path.parent, reads)
        assert peer.snapshot()["phase"] == "IN_FLIGHT"
        with pytest.raises(PostControlError, match="post_claim_unresolved"):
            peer.check()
        with pytest.raises(PostControlError, match="post_claim_unresolved"), peer.slot():
            pytest.fail("dead claim expired")
    finally:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=10)


def test_real_token_client_uses_shared_durable_post_domain(tmp_path):
    from test_private_stream import Clock as StreamClock

    clock = StreamClock()
    reads = PersistentReadLimiter.create(tmp_path / "reads", "synthetic")
    posts = PersistentPostLimiter.create(
        tmp_path / "posts",
        reads,
        wall_ns=lambda: int(clock.wall.timestamp() * 1e9),
        monotonic=lambda: clock.mono,
        sleep=clock.advance,
    )
    calls = []

    def handler(request):
        calls.append((request.method, clock.mono, posts.snapshot()["phase"]))
        return response(clock, request.method)

    tokens = client(clock, handler, limiter=posts)
    tokens.acquire()
    tokens.close()
    assert calls == [
        ("POST", pytest.approx(1.1), "IN_FLIGHT"),
        ("DELETE", pytest.approx(2.2), "IN_FLIGHT"),
    ]
    assert posts.snapshot()["phase"] == "READY" and posts.snapshot()["revision"] == 4


def test_token_http_failure_leaves_durable_claim_and_sanitizes_remote_secrets(tmp_path):
    from test_private_stream import Clock as StreamClock

    clock = StreamClock()
    reads = PersistentReadLimiter.create(tmp_path / "reads", "synthetic")
    posts = PersistentPostLimiter.create(
        tmp_path / "posts",
        reads,
        wall_ns=lambda: int(clock.wall.timestamp() * 1e9),
        monotonic=lambda: clock.mono,
        sleep=clock.advance,
    )
    calls = []

    def handler(request):
        calls.append(request.method)
        return httpx.Response(503, content="secret-not-for-storage")

    tokens = client(clock, handler, limiter=posts)
    with pytest.raises(ValueError) as error:
        tokens.acquire()
    assert "secret-not-for-storage" not in str(error.value)
    assert posts.snapshot()["phase"] == "STOPPED" and posts.snapshot()["claim"] is not None
    assert calls == ["POST"]
    assert b"secret-not-for-storage" not in posts.path.read_bytes()
    assert b"fixture-key" not in posts.path.read_bytes()
    tokens.close()


@pytest.mark.parametrize("damage", ["stop", "drop_binding"])
def test_actual_private_sync_uses_bound_posts_without_modifying_frozen_plan(
    tmp_path, monkeypatch, damage
):
    clock, read_clocks, reads, backend, vault, workspace = make_setup(tmp_path)
    posts = PersistentPostLimiter.create(
        tmp_path / "posts",
        reads,
        wall_ns=lambda: int(clock.wall.timestamp() * 1e9),
        monotonic=lambda: clock.mono,
        sleep=clock.advance,
    )
    manifest = (workspace.directory / "sync-plan.json").read_bytes()
    import threading

    from test_private_sync import account_response

    stop = threading.Event()
    original = PrivateStreamSupervisor.step
    calls = []

    def step(runner):
        original(runner)
        if runner.control.snapshot()["sync_successes"]:
            stop.set()

    def token(request):
        calls.append((request.method, posts.snapshot()["phase"]))
        return response(clock, request.method)

    monkeypatch.setattr(PrivateStreamSupervisor, "step", step)
    result = workspace.run(
        stop,
        **options(workspace),
        vault=vault,
        read_transport=httpx.MockTransport(lambda request: account_response(clock, request)),
        token_transport=httpx.MockTransport(token),
        connector=lambda _: Socket(clock),
        read_clocks=read_clocks,
        stream_sleep=clock.advance,
    )
    assert result["control"]["sync_successes"] == 1
    assert result["posts"]["instance"] == posts.snapshot()["instance"]
    assert calls == [("POST", "IN_FLIGHT"), ("DELETE", "IN_FLIGHT")]
    assert (workspace.directory / "sync-plan.json").read_bytes() == manifest
    before = list(backend.reads)
    if damage == "stop":
        posts.stop()
    else:
        with sqlite3.connect(reads.path) as conn:
            conn.execute("DROP TABLE post_binding")
    with pytest.raises(ValueError, match="sync_dependencies_blocked|post_binding_integrity_failed"):
        workspace.run(threading.Event(), **options(workspace), vault=vault)
    assert backend.reads == before


def test_cli_local_status_and_stop_without_credentials_or_network(setup, capsys):
    _, reads, control = setup
    args = [
        "--directory",
        str(control.path.parent),
        "--read-control-directory",
        str(reads.path.parent),
        "--scope",
        reads.scope,
    ]
    main(["status", *args])
    assert json.loads(capsys.readouterr().out)["phase"] == "READY"
    main(["stop", *args])
    assert json.loads(capsys.readouterr().out)["phase"] == "STOPPED"


@pytest.mark.parametrize("phase", ["wait", "completion"])
def test_actual_exit_after_claim_or_before_completion_commit_keeps_blocked_claim(setup, phase):
    _, reads, control = setup
    code = """
import os
from pathlib import Path
from trading.read_control import PersistentReadLimiter
from trading.post_control import PersistentPostLimiter
r = PersistentReadLimiter(Path(sys.argv[1]), 'synthetic')
p = PersistentPostLimiter(Path(sys.argv[2]), r)
if sys.argv[3] == 'wait':
    p._sleep = lambda seconds: os._exit(37)
else:
    original = p._write
    def write(conn, state, kind, **changes):
        result = original(conn, state, kind, **changes)
        if kind == 'COMPLETED':
            os._exit(37)
        return result
    p._write = write
with p.slot():
    pass
"""
    result = subprocess.run(
        python_process_args(code, reads.path.parent, control.path.parent, phase),
        timeout=10,
        capture_output=True,
    )
    assert result.returncode == 37, result.stderr
    peer = PersistentPostLimiter(control.path.parent, reads)
    assert peer.snapshot()["phase"] == "IN_FLIGHT" and peer.snapshot()["claim"] is not None
    with pytest.raises(PostControlError, match="post_claim_unresolved"), peer.slot():
        pytest.fail("process-exit claim was cleared")
