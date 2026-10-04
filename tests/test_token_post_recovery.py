"""Token-only expiry recovery and contention must never resolve a trade claim."""

import json
import subprocess
from contextlib import contextmanager
from datetime import timedelta

import httpx
import pytest
from test_post_control import Clock, reopen
from test_private_stream import Clock as TokenClock
from test_private_stream import client, response

from trading.paper_runner import python_process_args
from trading.post_control import (
    TOKEN_CONFIRMATIONS,
    TOKEN_QUIET_SECONDS,
    PersistentPostLimiter,
    PostControlError,
    main,
)
from trading.private_stream_token import StreamBusyError, StreamError
from trading.read_control import PersistentReadLimiter


@pytest.fixture
def setup(tmp_path):
    clock = Clock()
    reads = PersistentReadLimiter.create(tmp_path / "reads", "synthetic")
    posts = PersistentPostLimiter.create(tmp_path / "posts", reads, **clock.args())
    return clock, reads, posts


def recovery(posts, saved=None, **overrides):
    saved = saved or posts.snapshot()
    return posts.recover_token(
        **{
            "expected_revision": saved["revision"],
            "expected_reason": saved["reason"],
            "expected_claim": saved["claim"],
            "confirmations": TOKEN_CONFIRMATIONS,
            **overrides,
        }
    )


def unknown(posts, kind):
    digest = "a" * 64 if kind in {"order", "close_order", "cancel"} else None
    with pytest.raises(RuntimeError), posts.operation(kind, request_sha256=digest):
        raise RuntimeError("synthetic")
    return posts.snapshot()


@pytest.mark.parametrize(
    "method,kind", [("POST", "token_acquire"), ("PUT", "token_renew"), ("DELETE", "token_delete")]
)
def test_token_kind_is_persisted_and_expiry_recovery_requires_fresh_handles(setup, method, kind):
    clock, reads, posts = setup
    peer = reopen(posts, reads, clock)
    with pytest.raises(RuntimeError), posts.token_slot(method):
        assert posts.snapshot()["operation"] == kind
        raise RuntimeError("synthetic")
    saved = posts.snapshot()
    assert saved["reason"] == "operation_unknown"
    clock.wall = saved["wall_ns"] + TOKEN_QUIET_SECONDS * 1_000_000_000 - 1
    with pytest.raises(PostControlError, match="expiry_wait_required"):
        recovery(posts, saved)
    assert posts.snapshot() == saved
    clock.wall += 1
    recovery(posts, saved)
    for old in (posts, peer):
        with pytest.raises(PostControlError, match="new_post_control_required"):
            old.snapshot()
        with pytest.raises(PostControlError), old.token_slot("POST"):
            pytest.fail("old client resumed")
    fresh = reopen(posts, reads, clock)
    assert fresh.snapshot()["reason"] == "token_recovered"
    assert fresh.snapshot()["claim"] is None
    before = clock.mono
    with fresh.token_slot("POST"):
        assert clock.mono >= before + 1.1
    assert reads.post_binding()["instance"] == fresh.snapshot()["instance"]


@pytest.mark.parametrize("kind", ["order", "close_order", "cancel", "private_stream"])
def test_recovery_refuses_trade_and_legacy_stream_claims_even_after_expiry(setup, kind):
    clock, _, posts = setup
    saved = unknown(posts, kind)
    clock.advance(7 * 86400)
    with pytest.raises(PostControlError, match="token_recovery_refused"):
        recovery(posts)
    assert posts.snapshot() == saved


@pytest.mark.parametrize("reason", ["operator_stop", "clock_invalid"])
def test_explicit_or_clock_stop_cannot_be_erased_by_token_failure_or_recovery(setup, reason):
    clock, _, posts = setup
    unknown(posts, "token_acquire")
    posts.stop(reason)
    posts.fail_token(StreamError("synthetic"))
    saved = posts.snapshot()
    assert saved["reason"] == reason
    clock.advance(86400)
    with pytest.raises(PostControlError, match="token_recovery_refused"):
        recovery(posts)
    assert posts.snapshot() == saved


@pytest.mark.parametrize("change", ["revision", "reason", "claim", "confirmations"])
def test_recovery_refuses_changed_checkpoint_or_missing_confirmation(setup, change):
    clock, _, posts = setup
    unknown(posts, "token_renew")
    clock.advance(86400)
    args = {
        "revision": {"expected_revision": -1},
        "reason": {"expected_reason": "operator_stop"},
        "claim": {"expected_claim": "b" * 32},
        "confirmations": {"confirmations": {"token-only"}},
    }[change]
    saved = posts.snapshot()
    with pytest.raises(PostControlError):
        recovery(posts, **args)
    assert posts.snapshot() == saved


def test_owner_presence_prevents_recovery_even_with_expiry_and_matching_checkpoint(setup):
    clock, reads, posts = setup
    peer = reopen(posts, reads, clock)
    with posts.token_slot("POST"):
        saved = peer.snapshot()
        clock.advance(86400)
        with pytest.raises(StreamBusyError, match="post_owner_busy"):
            recovery(peer, saved)
        assert peer.snapshot() == saved
    assert peer.snapshot()["phase"] == "READY"


def test_token_failure_after_completed_request_is_expiry_recoverable(setup):
    clock, reads, posts = setup
    with posts.token_slot("PUT"):
        pass
    posts.fail_token(StreamError("stream_token_expired_during_renewal"))
    saved = posts.snapshot()
    assert saved["reason"] == "token_failed" and saved["claim"] is None
    clock.advance(TOKEN_QUIET_SECONDS)
    recovery(posts)
    assert reopen(posts, reads, clock).snapshot()["phase"] == "READY"


def test_local_stop_without_token_failure_cannot_use_token_recovery(setup):
    clock, _, posts = setup
    posts.stop()
    clock.advance(86400)
    with pytest.raises(PostControlError, match="token_recovery_refused"):
        recovery(posts)


def token_setup(tmp_path):
    clock = TokenClock()
    reads = PersistentReadLimiter.create(tmp_path / "reads", "synthetic")
    args = {
        "wall_ns": lambda: int(clock.wall.timestamp() * 1e9),
        "monotonic": lambda: clock.mono,
        "sleep": clock.advance,
    }
    posts = PersistentPostLimiter.create(tmp_path / "posts", reads, **args)
    return clock, reads, posts, args


@pytest.mark.parametrize("failure", ["503", "timeout"])
def test_real_token_failure_recovery_then_new_client_uses_same_bound_domain(tmp_path, failure):
    clock, reads, posts, args = token_setup(tmp_path)
    calls = []

    def handler(request):
        calls.append(request.method)
        if failure == "timeout":
            raise httpx.ReadTimeout("synthetic-secret")
        return httpx.Response(503)

    tokens = client(clock, handler, limiter=posts)
    with pytest.raises(StreamError):
        tokens.acquire()
    tokens.close()
    saved = posts.snapshot()
    assert saved["reason"] == "operation_unknown" and saved["operation"] == "token_acquire"
    assert calls == ["POST"]
    clock.advance(TOKEN_QUIET_SECONDS)
    recovery(posts)
    fresh = PersistentPostLimiter(posts.path.parent, reads, **args)
    with client(clock, limiter=fresh) as new:
        new.acquire()
        assert new.connection_url()
    assert fresh.snapshot()["phase"] == "READY"
    assert b"synthetic-secret" not in posts.path.read_bytes()


def test_renewal_defers_on_live_trade_contention_without_stopping_either_domain(tmp_path):
    clock, reads, posts, args = token_setup(tmp_path)
    calls = []

    def handler(request):
        calls.append(request.method)
        return response(clock, request.method)

    tokens = client(clock, handler, limiter=posts)
    tokens.acquire()
    clock.advance(3000)
    writer = PersistentPostLimiter(posts.path.parent, reads, **args)
    with writer.operation("order", request_sha256="a" * 64):
        saved = writer.snapshot()
        before = clock.mono
        tokens.maintain()
        assert clock.mono == pytest.approx(before + 3)
        assert writer.snapshot() == saved
        assert not tokens.status()["token_client_failed"]
        assert tokens.connection_url()
        assert calls == ["POST"]
    tokens.maintain()
    assert calls == ["POST", "PUT"]
    tokens.close()
    assert posts.snapshot()["phase"] == "READY"


def test_renewal_contention_cannot_keep_expired_token_usable(tmp_path):
    clock, reads, posts, args = token_setup(tmp_path)
    tokens = client(clock, limiter=posts)
    tokens.acquire()
    clock.advance(tokens._expires - clock.mono - 2)
    writer = PersistentPostLimiter(posts.path.parent, reads, **args)
    with writer.operation("order", request_sha256="a" * 64):
        saved = writer.snapshot()
        with pytest.raises(StreamError, match="stream_token_expired"):
            tokens.maintain()
        assert writer.snapshot() == saved
        with pytest.raises(StreamError, match="not_usable"):
            tokens.connection_url()
    tokens.close()
    assert posts.snapshot()["phase"] == "READY"


def test_token_clock_reversal_is_not_eligible_for_expiry_recovery(tmp_path):
    clock, reads, posts, args = token_setup(tmp_path)
    tokens = client(clock, limiter=posts)
    tokens.acquire()
    invalid_wall = clock.wall - timedelta(seconds=1)
    tokens._clock = lambda: invalid_wall
    with pytest.raises(StreamError, match="stream_token_clock_invalid"):
        tokens.maintain()
    assert posts.snapshot()["reason"] == "clock_invalid"
    with pytest.raises(StreamError):
        tokens.close()
    clock.advance(86400)
    fresh = PersistentPostLimiter(posts.path.parent, reads, **args)
    with pytest.raises(PostControlError, match="token_recovery_refused"):
        recovery(fresh)


@pytest.mark.parametrize("action", ["acquire", "close"])
def test_busy_acquire_or_cleanup_does_not_stop_another_trade_claim(tmp_path, action):
    clock, reads, posts, args = token_setup(tmp_path)
    tokens = client(clock, limiter=posts)
    if action == "close":
        tokens.acquire()
    writer = PersistentPostLimiter(posts.path.parent, reads, **args)
    with writer.operation("close_order", request_sha256="a" * 64):
        saved = writer.snapshot()
        with pytest.raises(StreamError):
            getattr(tokens, action)()
        assert writer.snapshot() == saved
    tokens.close()
    assert writer.snapshot()["phase"] == "READY"


def test_token_wait_acquires_released_owner_before_deadline_and_still_paces(setup):
    clock, reads, posts = setup
    writer = reopen(posts, reads, clock)
    operation = writer.operation("order", request_sha256="a" * 64)
    operation.__enter__()
    waited = []

    def wait(seconds):
        waited.append(seconds)
        clock.advance(seconds)
        if len(waited) == 2:
            operation.__exit__(None, None, None)

    posts._sleep = wait
    with posts.token_slot("POST"):
        assert waited[:2] == [0.05, 0.05]
        assert waited[-1] == 1.1
    assert posts.snapshot()["phase"] == "READY"


def test_nonadvancing_contention_timer_fails_without_sending(setup):
    _, reads, posts = setup
    peer = PersistentPostLimiter(posts.path.parent, reads, **posts_clock(posts))
    with peer.operation("order", request_sha256="a" * 64):
        posts._sleep = lambda _: None
        with pytest.raises(PostControlError, match="clock_invalid"), posts.token_slot("POST"):
            pytest.fail("bad timer dispatched")
    assert peer.snapshot()["reason"] == "clock_invalid"


def posts_clock(posts):
    return {"wall_ns": posts._wall, "monotonic": posts._mono, "sleep": posts._sleep}


def test_recovery_commit_failure_rolls_back_claim_and_history(setup, monkeypatch):
    clock, reads, posts = setup
    saved = unknown(posts, "token_delete")
    clock.advance(86400)
    original = posts._transaction

    @contextmanager
    def broken():
        with original() as conn:
            yield conn
            if conn.execute("SELECT 1 FROM events WHERE kind='TOKEN_RECOVERED'").fetchone():
                raise RuntimeError("synthetic commit failure")

    monkeypatch.setattr(posts, "_transaction", broken)
    with pytest.raises(RuntimeError):
        recovery(posts, saved)
    assert reopen(posts, reads, clock).snapshot() == saved


@pytest.mark.parametrize("phase", ["claim", "recovery_commit"])
def test_child_process_death_preserves_token_claim_until_explicit_recovery(setup, phase):
    clock, reads, posts = setup
    if phase == "recovery_commit":
        unknown(posts, "token_acquire")
        clock.advance(86400)
    code = """
import os
from pathlib import Path
from trading.read_control import PersistentReadLimiter
from trading.post_control import PersistentPostLimiter, TOKEN_CONFIRMATIONS
reads = PersistentReadLimiter(Path(sys.argv[1]), 'synthetic')
posts = PersistentPostLimiter(Path(sys.argv[2]), reads, wall_ns=lambda: int(sys.argv[4]))
if sys.argv[3] == 'claim':
    posts._sleep = lambda _: os._exit(39)
    with posts.token_slot('POST'):
        pass
else:
    original = posts._write
    def write(conn, state, kind, **changes):
        result = original(conn, state, kind, **changes)
        if kind == 'TOKEN_RECOVERED':
            os._exit(39)
        return result
    posts._write = write
    state = posts.snapshot()
    posts.recover_token(expected_revision=state['revision'], expected_reason=state['reason'],
                        expected_claim=state['claim'], confirmations=TOKEN_CONFIRMATIONS)
"""
    result = subprocess.run(
        python_process_args(code, reads.path.parent, posts.path.parent, phase, clock.wall),
        capture_output=True,
        timeout=10,
    )
    assert result.returncode == 39, result.stderr
    fresh = reopen(posts, reads, clock)
    assert fresh.snapshot()["claim"] is not None
    assert fresh.snapshot()["operation"] == "token_acquire"
    clock.advance(TOKEN_QUIET_SECONDS)
    recovery(fresh)
    assert reopen(posts, reads, clock).snapshot()["phase"] == "READY"


def test_cli_token_recovery_keeps_other_control_bindings(setup, capsys):
    clock, reads, posts = setup
    saved = unknown(posts, "token_acquire")
    # Real wall clock is later than the synthetic failure by more than 61 min.
    main(
        [
            "recover-token",
            "--directory",
            str(posts.path.parent),
            "--read-control-directory",
            str(reads.path.parent),
            "--scope",
            reads.scope,
            "--expected-revision",
            str(saved["revision"]),
            "--expected-reason",
            saved["reason"],
            "--expected-claim",
            saved["claim"],
            *[part for item in sorted(TOKEN_CONFIRMATIONS) for part in ("--confirm", item)],
        ]
    )
    state = json.loads(capsys.readouterr().out)
    assert state["phase"] == "READY" and state["reason"] == "token_recovered"
    assert reads.post_binding()["instance"] == state["instance"]
