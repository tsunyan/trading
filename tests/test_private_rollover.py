"""Planned reconnect boundaries close owned resources and require a fresh baseline."""

import socket

import httpx
import pytest
from test_account_sync import report
from test_private_stream import KEY, SECRET, TOKEN, Clock, FakeSocket, client, frame, response

from trading.event_capture import JournaledEventCapture
from trading.private_stream import PrivateStreamReceiver
from trading.private_stream_token import StreamError
from trading.segmented_journal import SegmentedEventJournal


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("real network attempted"))


def receiver(journal, clock, limiter, handler=None):
    capture = JournaledEventCapture(
        journal, clock=lambda: clock.wall, monotonic_ns=lambda: int(clock.mono * 1e9)
    )
    tokens = client(clock, handler, limiter=limiter)
    sock = FakeSocket(clock)
    stream = PrivateStreamReceiver(
        tokens, capture, connector=lambda _: sock, monotonic=lambda: clock.mono
    )
    stream.start(expected_head=journal.head())
    return stream, sock, tokens


def test_three_planned_connections_reuse_pacing_and_always_require_new_rest(tmp_path):
    clock = Clock()
    limiter = clock.limiter()
    journal = SegmentedEventJournal.create(tmp_path / "series", "synthetic", max_records=8)
    calls = []

    def handler(request):
        calls.append((request.method, clock.mono))
        return response(clock, request.method)

    for index in range(3):
        stream, sock, tokens = receiver(journal, clock, limiter, handler)
        assert stream.status()["account"]["phase"] == "NEEDS_RESYNC"
        assert not stream.status()["complete"] and not stream.status()["live_enabled"]
        result = stream.resync(lambda: report(clock))
        assert result.structural_match
        sock.messages = [frame(timestamp=clock.wall.isoformat())]
        stream.step()
        stream.resync(lambda: report(clock))
        old = journal
        head = stream.close_for_rollover()
        assert sock.closed and not tokens.status()["token_owned"]
        assert tokens.status()["token_client_closed"]
        assert stream.status()["account"]["phase"] == "DISCONNECTED"
        assert not stream.status()["stream_cleanup_failed"]
        assert not stream.status()["token_cleanup_unknown"]
        assert all(secret not in repr(stream.status()) for secret in (KEY, SECRET, TOKEN))
        journal = journal.rotate(expected_head=head)
        assert journal.audit_history()["archived_segments"] == index + 1
        with pytest.raises(StreamError, match="not_running"):
            stream.resync(lambda: pytest.fail("retired REST collector used"))
        assert old.path == journal.path
    assert [method for method, _ in calls] == ["POST", "DELETE"] * 3
    for prior, next_call in zip(calls, calls[1:], strict=False):
        assert next_call[1] - prior[1] >= 1.1 - 1e-10
    assert journal.audit_history()["archived_records"] == 12


def test_unknown_token_cleanup_prevents_a_successful_rollover_boundary(tmp_path):
    clock = Clock()
    journal = SegmentedEventJournal.create(tmp_path / "series", "synthetic")

    def handler(request):
        if request.method == "DELETE":
            return httpx.Response(503, content=TOKEN)
        return response(clock, request.method)

    stream, sock, _ = receiver(journal, clock, clock.limiter(), handler)
    with pytest.raises(StreamError, match="private_stream_rollover_failed"):
        stream.close_for_rollover()
    state = stream.status()
    assert state["stream_closed"] and not state["stream_running"] and sock.closed
    assert state["token_cleanup_unknown"] and state["stream_cleanup_failed"]
    assert journal.audit_history()["archived_segments"] == 0
    with pytest.raises(StreamError):
        stream.start(expected_head=journal.head())


def test_rollover_during_rest_stops_transport_and_withholds_old_result(tmp_path):
    clock = Clock()
    journal = SegmentedEventJournal.create(tmp_path / "series", "synthetic")
    stream, sock, _ = receiver(journal, clock, clock.limiter())

    def collect():
        with pytest.raises(StreamError, match="private_stream_rollover_failed"):
            stream.close_for_rollover()
        return report(clock)

    with pytest.raises(ValueError):
        stream.resync(collect)
    assert sock.closed and stream.status()["stream_closed"]
    assert journal.inspect()["archived_segments"] == 0
