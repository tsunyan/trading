"""Explicit registration of sync/watchdog prerequisites on an existing stopped journal."""

import ctypes
import socket

import pytest
from test_account_guard import account, quote
from test_live_acceptance import synthetic_reads
from test_live_operations import bind
from test_live_operations import unbound as operations_unbound
from test_private_order import ready

from trading.live_acceptance import READ_KINDS, fingerprint, restart_approval
from trading.live_journal import EVIDENCE_KINDS, LiveOrderError, LiveOrderJournal
from trading.post_control import PersistentPostLimiter


@pytest.fixture(autouse=True)
def no_native_or_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("real credentials/network attempted")

    monkeypatch.setattr(ctypes, "WinDLL", forbidden, raising=False)
    monkeypatch.setattr(socket, "socket", forbidden)


@pytest.fixture
def enabled(tmp_path):
    """A journal activated before registration existed: the legacy shape to migrate."""
    unbound = operations_unbound.__wrapped__(tmp_path)
    ready(unbound[1])
    return unbound


def migrate(unbound, **updates):
    return bind(unbound, migration_confirmed=True, **updates)


def evidence(tmp_path, journal, now):
    items = synthetic_reads(journal, tmp_path, now)
    for kind in sorted(EVIDENCE_KINDS):
        if kind in READ_KINDS:
            continue
        path = tmp_path / f"{kind}.txt"
        path.write_text(f"reviewed {kind}")
        items.append(fingerprint(kind, path))
    return items


def test_migration_requires_a_stopped_journal(enabled):
    journal = enabled[1][3]
    before = journal.snapshot()["live_control"]
    for options in ({}, {"migration_confirmed": True}):
        with pytest.raises(LiveOrderError, match="registration_refused"):
            bind(enabled, **options)
    assert journal.snapshot()["live_control"] == before
    with pytest.raises(LiveOrderError, match="confirmation_required"):
        bind(enabled, migration_confirmed=1)


def test_stopped_journal_migrates_and_needs_a_fresh_restart(enabled, tmp_path):
    values, live, monitor = enabled
    clock, reads, posts, journal = live
    journal.halt()
    before = journal.snapshot()["live_control"]
    context = migrate(enabled)
    after = journal.snapshot()["live_control"]
    assert after["phase"] == "STOPPED" and after["operations"] is not None
    assert context["configuration_sha256"] != before["configuration_sha256"]
    assert before["approval"] is not None and after["approval"] is None
    voided = [e for e in journal.snapshot()["events"] if e["kind"] == "LIVE_APPROVAL_VOIDED"]
    assert [e["payload"]["approval"] for e in voided] == [before["approval"]]
    assert not journal.snapshot()["live_enabled"]
    with pytest.raises(LiveOrderError, match="registration_refused"):
        migrate(enabled)  # Registration stays permanent and single.

    clock.advance(1)
    journal.update_account(account(clock.now), quote(clock.now), now=clock.now)
    built, confirmations = restart_approval(
        journal, evidence(tmp_path, journal, clock.now), _review(tmp_path), hours=24, now=clock.now
    )
    assert built.approval.configuration_sha256 == context["configuration_sha256"]
    journal.restart(built, confirmations=confirmations)
    wall = values[0]
    fresh_posts = PersistentPostLimiter(
        posts.path.parent,
        reads,
        wall_ns=lambda: int(wall.wall.timestamp() * 1e9),
        monotonic=lambda: wall.mono,
        sleep=wall.advance,
    )
    reopened = LiveOrderJournal(journal.path.parent, fresh_posts, clock=lambda: clock.now)
    restarted = reopened.snapshot()
    assert restarted["live_enabled"] and restarted["live_control"]["operations"] is not None
    # Every send now requires the registered sync/watchdog health, which is not running here.
    with pytest.raises(LiveOrderError, match="live_(sync|watchdog)_"):
        reopened.request(restarted["orders"][0]["client_id"])
    assert values[3].reads == []


def _review(tmp_path):
    path = tmp_path / "migration review.md"
    path.write_text("legacy journal reviewed before registration")
    return path


def test_migration_also_works_while_post_control_is_stopped(enabled):
    _, live, _ = enabled
    journal = live[3]
    journal.halt()
    live[2].stop("operator_stop")
    context = migrate(enabled)
    assert journal.snapshot()["live_control"]["operations"] is not None
    assert context["configuration_sha256"]
