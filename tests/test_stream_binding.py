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


@pytest.mark.parametrize(
    "sql",
    [
        "DROP TABLE stream_binding",
        "DELETE FROM stream_binding",
        "UPDATE stream_binding SET supervisor_id='bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'",
        "UPDATE events SET token='bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' WHERE kind='STREAM_BOUND'",
        "INSERT INTO events(wall_ns,kind,token) SELECT wall_ns,kind,token FROM events "
        "WHERE kind='STREAM_BOUND'",
    ],
)
def test_binding_history_refuses_deletion_replacement_or_duplicate_receipt(control, sql):
    control.bind_stream("a" * 32)
    with sqlite3.connect(control.path) as conn:
        conn.execute(sql)
    before = control.path.read_bytes()
    for method in ("read", "bind", "confirm"):
        peer = PersistentReadLimiter(control.path.parent, "synthetic")
        with pytest.raises(PrivateReadError, match="stream_binding_integrity_failed"):
            if method == "read":
                peer.stream_binding()
            else:
                peer.bind_stream("a" * 32, legacy_binding_confirmed=method == "confirm")
    assert control.path.read_bytes() == before


@pytest.mark.parametrize("stopped", [False, True])
def test_legacy_binding_requires_explicit_same_owner_confirmation_without_releasing_stop(
    control, stopped
):
    control.bind_stream("a" * 32)
    with sqlite3.connect(control.path) as conn:
        conn.execute("DELETE FROM events WHERE kind='STREAM_BOUND'")
    if stopped:
        control.stop()
    saved = control.status()
    before = control.path.read_bytes()
    with pytest.raises(PrivateReadError, match="history_confirmation_required"):
        control.stream_binding()
    assert control.path.read_bytes() == before
    with pytest.raises(PrivateReadError, match="already_bound"):
        control.bind_stream("b" * 32, legacy_binding_confirmed=True)
    control.bind_stream("a" * 32, legacy_binding_confirmed=True)
    assert control.stream_binding() == "a" * 32
    current = control.status()
    assert {k: v for k, v in current.items() if k != "events"} == {
        k: v for k, v in saved.items() if k != "events"
    }
    assert current["events"] == saved["events"] + 1
    control.bind_stream("a" * 32, legacy_binding_confirmed=True)
    assert control.status() == current


def test_confirmation_cannot_initialize_missing_binding_or_pass_live_get_owner(control):
    with pytest.raises(PrivateReadError, match="legacy_stream_binding_required"):
        control.bind_stream("a" * 32, legacy_binding_confirmed=True)
    control.bind_stream("a" * 32)
    peer = PersistentReadLimiter(control.path.parent, "synthetic")
    with control.slot(), pytest.raises(PrivateReadError, match="read_claim_unresolved"):
        peer.bind_stream("a" * 32, legacy_binding_confirmed=True)


def test_binding_and_receipt_commit_atomically_when_history_insert_fails(control):
    with sqlite3.connect(control.path) as conn:
        conn.execute(
            "CREATE TRIGGER fail_marker BEFORE INSERT ON events WHEN NEW.kind='STREAM_BOUND' "
            "BEGIN SELECT RAISE(ABORT, 'synthetic'); END"
        )
    with pytest.raises(PrivateReadError, match="storage_failed"):
        control.bind_stream("a" * 32)
    reopened = PersistentReadLimiter(control.path.parent, "synthetic")
    assert reopened.stream_binding() is None
    with sqlite3.connect(control.path) as conn:
        assert (
            conn.execute("SELECT COUNT(*) FROM events WHERE kind='STREAM_BOUND'").fetchone()[0] == 0
        )
