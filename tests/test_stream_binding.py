"""Permanent association prevents a second runtime from bypassing its stop."""

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing

import pytest
from test_read_control import Clock

from trading.private_read import PrivateReadError
from trading.read_control import PersistentReadLimiter


@pytest.fixture
def control(tmp_path):
    clock = Clock()
    return PersistentReadLimiter.create(tmp_path / "reads", "synthetic", **clock.args())


def test_read_is_noninitializing_and_binding_is_permanent_across_reopen(control):
    assert control.stream_binding() is None
    with closing(sqlite3.connect(control.path)) as conn:
        assert (
            conn.execute("SELECT 1 FROM sqlite_master WHERE name='stream_binding'").fetchone()
            is None
        )
    control.bind_stream("a" * 32)
    control.bind_stream("a" * 32)
    peer = PersistentReadLimiter(control.path.parent, "synthetic")
    assert peer.stream_binding() == "a" * 32
    with pytest.raises(PrivateReadError, match="already_bound"):
        peer.bind_stream("b" * 32)
    control.stop()
    assert control.stream_binding() == "a" * 32 and control.status()["blocked"]


def test_active_get_or_stop_prevents_registration(control):
    with control.slot(), pytest.raises(PrivateReadError, match="control_blocked"):
        control.bind_stream("a" * 32)
    assert control.stream_binding() is None
    control.stop()
    with pytest.raises(PrivateReadError, match="control_blocked"):
        control.bind_stream("a" * 32)
    assert control.stream_binding() is None


def test_concurrent_registration_has_one_winner(control):
    def bind(identity):
        peer = PersistentReadLimiter(control.path.parent, "synthetic")
        try:
            peer.bind_stream(identity)
            return identity
        except PrivateReadError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(bind, ("a" * 32, "b" * 32)))
    assert outcomes.count(None) == 1
    assert control.stream_binding() in outcomes


def test_empty_existing_binding_is_not_reinitialized(control):
    control.bind_stream("a" * 32)
    with closing(sqlite3.connect(control.path)) as conn:
        conn.execute("DELETE FROM stream_binding")
        conn.commit()
    peer = PersistentReadLimiter(control.path.parent, "synthetic")
    with pytest.raises(PrivateReadError, match="integrity_failed"):
        peer.bind_stream("b" * 32)
