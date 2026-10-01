import hashlib
import hmac
import json
import logging
import socket
import ssl
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from pydantic import SecretStr

from trading.account_read_lab import demo_transcript, replay
from trading.account_sync import SyncError
from trading.event_capture import JournaledEventCapture
from trading.event_journal import EventJournal, JournalError
from trading.private_stream import CHANNELS, PrivateStreamReceiver, open_private_socket
from trading.private_stream_lab import demo
from trading.private_stream_token import (
    STREAM_ENDPOINT,
    TOKEN_ENDPOINT,
    PrivateStreamLimiter,
    PrivateTokenClient,
    StreamError,
)

NOW = datetime(2026, 10, 2, tzinfo=UTC)
KEY, SECRET, TOKEN = "fixture-key", "fixture-secret", "fixture-token-NOT-REAL"


class Clock:
    def __init__(self):
        self.mono, self.wall = 0.0, NOW

    def advance(self, seconds):
        self.mono += seconds
        self.wall += timedelta(seconds=seconds)

    def limiter(self):
        return PrivateStreamLimiter(monotonic=lambda: self.mono, sleep=self.advance)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    monkeypatch.setattr(socket, "socket", lambda *a, **k: pytest.fail("real network attempted"))


def response(clock, method="POST", **changes):
    return httpx.Response(
        200,
        json={
            "status": 0,
            "responsetime": clock.wall.isoformat(),
            **({"data": TOKEN} if method == "POST" else {}),
            **changes,
        },
    )


def client(clock, handler=None, *, limiter=None, **kwargs):
    return PrivateTokenClient(
        SecretStr(KEY),
        SecretStr(SECRET),
        limiter=limiter or clock.limiter(),
        transport=httpx.MockTransport(handler or (lambda r: response(clock, r.method))),
        clock=lambda: clock.wall,
        monotonic=lambda: clock.mono,
        **kwargs,
    )


def test_exact_token_signatures_body_and_safe_url():
    clock, seen = Clock(), []

    def handler(request):
        seen.append((request, clock.mono))
        assert str(request.url) == TOKEN_ENDPOINT
        assert request.headers["API-KEY"] == KEY
        body = (
            b"{}"
            if request.method == "POST"
            else json.dumps({"token": TOKEN}, separators=(",", ":")).encode()
        )
        assert request.content == body
        signed = (request.headers["API-TIMESTAMP"] + request.method + "/v1/ws-auth").encode()
        if request.method == "POST":
            signed += body
        assert (
            request.headers["API-SIGN"]
            == hmac.new(SECRET.encode(), signed, hashlib.sha256).hexdigest()
        )
        assert request.headers["Accept-Encoding"] == "identity"
        assert request.extensions["timeout"] == dict(connect=5, read=5, write=5, pool=5)
        return response(clock, request.method)

    with client(clock, handler) as tokens:
        assert seen == []
        tokens.acquire()
        url = tokens.connection_url()
        assert url.get_secret_value() == STREAM_ENDPOINT + TOKEN
        assert TOKEN not in repr(url)
        tokens.maintain()
        assert len(seen) == 1
        clock.advance(3000)
        tokens.maintain()
        assert tokens.connection_url() == url
        assert tokens.status()["token_owned"]
    assert [r.method for r, _ in seen] == ["POST", "PUT", "DELETE"]
    assert seen[2][1] - seen[1][1] >= 1.1 - 1e-10
    assert all("API-KEY" not in r.headers and "API-SIGN" not in r.headers for r, _ in seen)
    assert not tokens.status()["token_cleanup_unknown"]
    assert not tokens.status()["token_owned"]
    tokens.close()
    with pytest.raises(StreamError, match="new_stream_token_client_required"):
        tokens.acquire()


@pytest.mark.parametrize("status", [301, 307, 401, 403, 429, 500])
def test_http_failure_never_retries_or_follows_redirect_and_stops_shared_limiter(status):
    clock, calls = Clock(), []
    limiter = clock.limiter()

    def handler(req):
        calls.append(req)
        return httpx.Response(status, headers={"location": "https://example.test/"}, text=TOKEN)

    tokens = client(clock, handler, limiter=limiter)
    with pytest.raises(StreamError, match="stream_token_http_failed"):
        tokens.acquire()
    assert len(calls) == 1
    assert tokens.status()["token_cleanup_unknown"]
    with pytest.raises(StreamError, match="new_stream_token_client_required"):
        tokens.acquire()
    other = client(clock, limiter=limiter)
    with pytest.raises(StreamError, match="private_stream_stopped"):
        other.acquire()
    tokens.close()
    other.close()


@pytest.mark.parametrize(
    "body",
    [
        b'{"status":0,"status":0,"data":"fixture-token","responsetime":"2026-10-02T00:00:00Z"}',
        b"[]",
        b'{"status":true,"data":"fixture-token","responsetime":"2026-10-02T00:00:00Z"}',
        b'{"status":0,"data":NaN,"responsetime":"2026-10-02T00:00:00Z"}',
        b'{"status":1,"messages":["fixture-secret"]}',
        b'{"status":0,"data":"../evil","responsetime":"2026-10-02T00:00:00Z"}',
        b'{"status":0,"data":"a?secret=b","responsetime":"2026-10-02T00:00:00Z"}',
        b'{"status":0,"data":123,"responsetime":"2026-10-02T00:00:00Z"}',
        b'{"status":0,"data":"fixture-token","responsetime":"2026-10-02T00:00:00Z","extra":1}',
        b'{"status":0,"data":"fixture-token","responsetime":"2026-10-01T00:00:00Z"}',
        b'{"status":0,"data":"fixture-token","responsetime":"2026-10-03T00:00:00Z"}',
    ],
)
def test_malformed_or_untrusted_token_response_is_never_usable(body):
    tokens = client(
        Clock(),
        lambda r: httpx.Response(200, content=body, headers={"content-type": "application/json"}),
    )
    with pytest.raises(StreamError) as caught:
        tokens.acquire()
    assert TOKEN not in str(caught.value) and SECRET not in str(caught.value)
    with pytest.raises(StreamError, match="stream_token_not_usable"):
        tokens.connection_url()
    tokens.close()


@pytest.mark.parametrize("field", ["wall", "mono", "both"])
def test_expiry_cannot_be_extended_but_owned_token_is_deleted(field):
    clock, calls = Clock(), []

    def handler(r):
        calls.append(r.method)
        return response(clock, r.method)

    tokens = client(clock, handler)
    tokens.acquire()
    if field in {"wall", "both"}:
        clock.wall += timedelta(seconds=3570)
    if field in {"mono", "both"}:
        clock.mono += 3570
    with pytest.raises(StreamError, match="stream_token_expired"):
        tokens.maintain()
    tokens.close()
    assert calls == ["POST", "DELETE"]


@pytest.mark.parametrize("renewal", ["http", "bad_response", "deadline"])
def test_renewal_failure_latches_stop_and_does_not_claim_cleanup(renewal):
    clock, calls = Clock(), []

    def handler(r):
        calls.append(r.method)
        if r.method == "PUT":
            if renewal == "http":
                raise httpx.ReadTimeout(TOKEN, request=r)
            if renewal == "deadline":
                clock.advance(11)
            return response(clock, r.method, status=1 if renewal == "bad_response" else 0)
        return response(clock, r.method)

    tokens = client(clock, handler)
    tokens.acquire()
    clock.advance(3000)
    with pytest.raises(StreamError) as caught:
        tokens.maintain()
    assert TOKEN not in str(caught.value)
    assert tokens.status()["token_client_failed"]
    with pytest.raises(StreamError, match="stream_token_cleanup_failed"):
        tokens.close()
    assert tokens.status()["token_cleanup_unknown"]
    assert calls == ["POST", "PUT"]


def test_old_expiry_fences_renewal_after_wait_and_after_response():
    clock, calls = Clock(), []

    def handler(r):
        calls.append(r.method)
        if r.method == "PUT":
            clock.advance(2)
        return response(clock, r.method)

    tokens = client(clock, handler)
    tokens.acquire()
    clock.advance(3569)
    with pytest.raises(StreamError, match="stream_token_expired_during_renewal"):
        tokens.maintain()
    with pytest.raises(StreamError):
        tokens.close()
    assert calls == ["POST", "PUT"]

    clock = Clock()
    tokens = client(clock, handler)
    tokens.acquire()
    clock.advance(3569)
    # A different paced operation finishes just before the expiry boundary.
    with tokens.limiter.slot():
        pass
    with pytest.raises(StreamError, match="stream_token_expired"):
        tokens.maintain()
    with pytest.raises(StreamError):
        tokens.close()
    assert calls == ["POST", "PUT", "POST"]


@pytest.mark.parametrize("field", ["wall", "mono"])
def test_clock_reversal_stops_client_and_scrubs_cleanup(field):
    clock = Clock()
    tokens = client(clock)
    tokens.acquire()
    if field == "wall":
        clock.wall -= timedelta(seconds=1)
    else:
        clock.mono -= 1
    with pytest.raises(StreamError):
        tokens.maintain()
    with pytest.raises(StreamError):
        tokens.close()
    assert tokens._key.get_secret_value() == tokens._secret.get_secret_value() == ""
    assert tokens.status()["token_cleanup_unknown"]


@pytest.mark.parametrize("value", ["raw-secret", SecretStr(""), SecretStr("line\nbreak")])
def test_explicit_header_safe_credentials_required(value):
    with pytest.raises(StreamError, match="explicit_stream_credentials_required"):
        PrivateTokenClient(value, SecretStr(SECRET), limiter=PrivateStreamLimiter())


def test_early_wake_and_limiter_stop_prevent_send():
    limiter = PrivateStreamLimiter(monotonic=lambda: 0, sleep=lambda _: None)
    with limiter.slot():
        pass
    with pytest.raises(StreamError, match="stream_limiter_wait_incomplete"), limiter.slot():
        pytest.fail("slot was incorrectly granted")
    with pytest.raises(StreamError, match="private_stream_stopped"), limiter.slot():
        pytest.fail("stopped slot was granted")


class FakeSocket:
    def __init__(self, clock):
        self.clock = clock
        self.messages, self.sent = [], []
        self.pongs = []
        self.closed = False

    def send(self, data):
        self.sent.append((self.clock.mono, json.loads(data)))

    def recv(self, *, timeout, decode):
        assert timeout == 1 and decode is False
        if self.messages:
            result = self.messages.pop(0)
            if isinstance(result, BaseException):
                raise result
            return result
        self.clock.advance(timeout)
        raise TimeoutError

    def ping(self, *, ack_on_close):
        assert not ack_on_close
        pong = threading.Event()
        self.pongs.append(pong)
        return pong

    def close(self):
        self.closed = True


def frame(**changes):
    return json.dumps(
        {
            "channel": "positionEvents",
            "positionId": 401,
            "symbol": "USD_JPY",
            "side": "BUY",
            "size": "400",
            "orderdSize": "0",
            "price": "150",
            "lossGain": "-40",
            "totalSwap": "0",
            "timestamp": NOW.isoformat(),
            "msgType": "UPR",
            **changes,
        }
    ).encode()


def setup(tmp_path, *, handler=None):
    clock = Clock()
    journal = EventJournal.create(tmp_path / "journal", "synthetic")
    capture = JournaledEventCapture(
        journal, clock=lambda: clock.wall, monotonic_ns=lambda: int(clock.mono * 1e9)
    )
    tokens = client(clock, handler)
    sock = FakeSocket(clock)
    urls = []

    def connector(url):
        urls.append(url)
        return sock

    receiver = PrivateStreamReceiver(
        tokens, capture, connector=connector, monotonic=lambda: clock.mono
    )
    return clock, journal, capture, tokens, sock, receiver, urls


def start(journal, receiver):
    receiver.start(expected_head=journal.inspect()["head"])


def test_subscription_pacing_sequence_journal_and_clean_end(tmp_path):
    _, journal, _, _, sock, receiver, urls = setup(tmp_path)
    assert not receiver.status()["stream_running"]
    assert urls == []
    start(journal, receiver)
    assert isinstance(urls[0], SecretStr)
    assert [item["channel"] for _, item in sock.sent] == list(CHANNELS)
    assert all(item["command"] == "subscribe" for _, item in sock.sent)
    assert all(b[0] - a[0] >= 1.1 - 1e-10 for a, b in zip(sock.sent, sock.sent[1:], strict=False))
    sock.messages = [frame(), frame()]
    assert receiver.step() and receiver.step()
    assert receiver.status()["receive_sequence"] == 2
    assert journal.inspect()["unacknowledged_records"] == ()
    assert not receiver.status()["complete"] and not receiver.status()["live_enabled"]
    assert all(secret not in repr(receiver.status()) for secret in (KEY, SECRET, TOKEN))
    receiver.close()
    assert sock.closed and not journal.inspect()["session_open"]
    assert receiver.status()["account"]["phase"] == "DISCONNECTED"
    assert not receiver.status()["token_cleanup_unknown"]
    receiver.close()
    with pytest.raises(StreamError, match="new_private_stream_receiver_required"):
        start(journal, receiver)


def test_only_received_pong_extends_liveness_without_consuming_sequence(tmp_path):
    clock, journal, _, _, sock, receiver, _ = setup(tmp_path)
    start(journal, receiver)
    clock.advance(31)
    assert not receiver.step()
    assert len(sock.pongs) == 1
    before = journal.inspect()["records"]
    assert receiver.status()["receive_sequence"] == 0
    sock.pongs[0].set()
    assert not receiver.step()
    assert journal.inspect()["records"] == before + 2  # HEARTBEAT + ACK
    clock.advance(31)
    assert not receiver.step()
    clock.advance(15)
    with pytest.raises(StreamError, match="private_stream_pong_expired"):
        receiver.step()
    assert sock.closed and receiver.status()["account"]["phase"] == "DISCONNECTED"


@pytest.mark.parametrize(
    "payload",
    [
        b'{"error":"fixture-token-NOT-REAL"}',
        b"{}",
        b"x" * 16_385,
        frame(symbol="EUR_JPY"),
        frame(extra=TOKEN),
        "text-from-untrusted-adapter",
        RuntimeError(TOKEN),
    ],
)
def test_bad_data_or_transport_failure_ends_epoch_and_never_reconnects(tmp_path, payload):
    _, journal, _, _, sock, receiver, urls = setup(tmp_path)
    start(journal, receiver)
    sock.messages = [payload, frame()]
    with pytest.raises(StreamError) as caught:
        receiver.step()
    assert TOKEN not in str(caught.value)
    assert sock.closed and len(urls) == 1
    assert receiver.status()["account"]["phase"] == "DISCONNECTED"
    with pytest.raises(StreamError, match="private_stream_not_running"):
        receiver.step()
    assert receiver.status()["receive_sequence"] == (0 if isinstance(payload, Exception) else 1)
    assert TOKEN.encode() not in journal.path.read_bytes()


def test_stale_head_fails_before_token_or_socket_creation(tmp_path):
    _, journal, _, tokens, _, receiver, urls = setup(tmp_path)
    with pytest.raises(StreamError, match="private_stream_start_failed"):
        receiver.start(expected_head="f" * 64)
    assert not tokens.status()["token_acquire_attempted"] and urls == []
    assert journal.inspect()["records"] == 0


def test_subscription_failure_closes_socket_token_and_journal(tmp_path, monkeypatch):
    _, journal, _, tokens, sock, receiver, _ = setup(tmp_path)
    monkeypatch.setattr(sock, "send", lambda _: (_ for _ in ()).throw(RuntimeError(TOKEN)))
    with pytest.raises(StreamError, match="private_stream_start_failed"):
        start(journal, receiver)
    assert sock.closed and not journal.inspect()["session_open"]
    assert tokens.status()["token_client_closed"]
    assert not tokens.status()["token_cleanup_unknown"]


def test_journal_takeover_or_storage_failure_stops_stream(tmp_path):
    _, journal, _, _, sock, receiver, _ = setup(tmp_path)
    start(journal, receiver)
    other = EventJournal(journal.path.parent, "synthetic")
    other.start_session(
        expected_head=other.inspect()["head"],
        at=NOW + timedelta(seconds=10),
        monotonic_ns=10_000_000_000,
    )
    with pytest.raises(StreamError, match="private_stream_capture_invalid"):
        receiver.step()
    assert sock.closed and receiver.status()["stream_cleanup_failed"]


def test_event_receipt_continues_during_rest_collection_and_invalidates_it(tmp_path):
    clock, journal, _, _, sock, receiver, _ = setup(tmp_path)
    start(journal, receiver)
    entered, release = threading.Event(), threading.Event()

    def collect():
        entered.set()
        assert release.wait(5)
        return replay(demo_transcript(clock.wall))

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(receiver.resync, collect)
        assert entered.wait(5)
        sock.messages.append(frame())
        assert receiver.step()
        release.set()
        with pytest.raises(SyncError, match="changed_during_collection"):
            future.result(timeout=5)
    receiver.close()


def test_expiry_during_rest_never_returns_usable_assessment(tmp_path):
    clock, journal, _, _, sock, receiver, _ = setup(tmp_path)
    start(journal, receiver)
    # Keep the capture live while approaching token expiry.
    clock.advance(3560)
    # The monitor has its own 75s deadline: renewing heartbeat first is not
    # allowed to restore an expired observation. Test the token fence before REST.
    clock.advance(10)
    with pytest.raises(StreamError, match="private_stream_token_invalid"):
        receiver.resync(lambda: pytest.fail("expired token reached REST"))
    assert sock.closed


def test_run_honors_stop_and_closes_owned_resources(tmp_path):
    _, journal, _, _, sock, receiver, _ = setup(tmp_path)
    start(journal, receiver)
    stop = threading.Event()
    stop.set()
    receiver.run(stop)
    assert sock.closed and not journal.inspect()["session_open"]


def test_production_connector_options_and_redacted_errors(monkeypatch, caplog):
    calls = []

    def fake_connect(uri, **options):
        calls.append((uri, options))
        options["logger"].error(TOKEN)
        raise RuntimeError(uri)

    monkeypatch.setattr("trading.private_stream.connect", fake_connect)
    # SSLContext creation may probe OS trust stores, so provide a context without IO.
    monkeypatch.setattr(
        "trading.private_stream.ssl.create_default_context",
        lambda: ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT),
    )
    with caplog.at_level(logging.DEBUG), pytest.raises(StreamError) as caught:
        open_private_socket(SecretStr(STREAM_ENDPOINT + TOKEN))
    assert TOKEN not in str(caught.value) and TOKEN not in caplog.text
    uri, options = calls[0]
    assert uri == STREAM_ENDPOINT + TOKEN
    assert options["proxy"] is None and options["compression"] is None
    assert options["max_size"] == 16_384 and options["max_queue"] == 4
    assert options["open_timeout"] == options["close_timeout"] == 5
    assert options["ssl"].verify_mode == ssl.CERT_REQUIRED
    assert options["ssl"].check_hostname
    assert options["logger"].disabled and not options["logger"].propagate


@pytest.mark.parametrize(
    "url",
    [
        STREAM_ENDPOINT + TOKEN,
        SecretStr("ws://evil.test/x"),
        SecretStr(STREAM_ENDPOINT + "../x"),
        SecretStr(STREAM_ENDPOINT + "x?y=z"),
    ],
)
def test_connector_refuses_arbitrary_url_before_network(url):
    with pytest.raises(StreamError, match="invalid_private_stream_url"):
        open_private_socket(url)


@pytest.mark.parametrize(
    "headers",
    [
        {"content-type": "text/plain"},
        {"content-type": "application/json", "content-encoding": "gzip"},
        {"content-type": "application/json", "content-length": "4097"},
        {"content-type": "application/json", "content-length": "invalid"},
        {"content-type": "application/json", "content-length": "0"},
    ],
)
def test_token_response_metadata_is_bounded(headers):
    clock = Clock()
    tokens = client(clock, lambda r: httpx.Response(200, content=b"{}", headers=headers))
    with pytest.raises(StreamError):
        tokens.acquire()
    assert tokens.status()["token_client_failed"]
    tokens.close()


def test_chunked_response_limit_cleanup_and_redaction():
    clock = Clock()

    class Body(httpx.SyncByteStream):
        closed = False

        def __iter__(self):
            yield b"x" * 3000
            yield b"x" * 2000

        def close(self):
            self.closed = True

    body = Body()
    tokens = client(
        clock,
        lambda r: httpx.Response(200, stream=body, headers={"content-type": "application/json"}),
    )
    with pytest.raises(StreamError, match="stream_token_response_too_large"):
        tokens.acquire()
    assert body.closed
    tokens.close()


def test_late_pong_cannot_restore_liveness(tmp_path):
    clock, journal, _, _, sock, receiver, _ = setup(tmp_path)
    start(journal, receiver)
    clock.advance(31)
    receiver.step()
    sock.pongs[0].set()
    clock.advance(15)
    with pytest.raises(StreamError, match="private_stream_pong_expired"):
        receiver.step()
    assert receiver.status()["account"]["phase"] == "DISCONNECTED"


def test_socket_cleanup_failure_still_ends_capture_and_deletes_token(tmp_path, monkeypatch):
    _, journal, _, tokens, sock, receiver, _ = setup(tmp_path)
    start(journal, receiver)
    monkeypatch.setattr(sock, "close", lambda: (_ for _ in ()).throw(RuntimeError(TOKEN)))
    with pytest.raises(StreamError, match="private_stream_cleanup_failed"):
        receiver.close()
    assert not journal.inspect()["session_open"]
    assert tokens.status()["token_client_closed"]
    assert not tokens.status()["token_cleanup_unknown"]


def test_token_renewal_during_quiet_stream_and_failed_renewal_ends_epoch(tmp_path):
    clock, journal, capture, tokens, sock, receiver, _ = setup(tmp_path)
    start(journal, receiver)
    # Keepalive pongs maintain transport liveness, independently of account data.
    for _ in range(95):
        clock.advance(31)
        receiver.step()
        sock.pongs[-1].set()
        receiver.step()
    assert clock.mono > 3000
    assert tokens._renew > 3000
    assert receiver.status()["receive_sequence"] == 0
    assert capture.status()["phase"] == "NEEDS_RESYNC"
    tokens.limiter.stop()
    with pytest.raises(StreamError, match="private_stream_stopped"):
        receiver.step()
    assert sock.closed and not journal.inspect()["session_open"]
    assert receiver.status()["token_cleanup_unknown"]


def test_valid_rest_assessment_and_close_race(tmp_path):
    clock, journal, _, _, sock, receiver, _ = setup(tmp_path)
    start(journal, receiver)
    result = receiver.resync(lambda: replay(demo_transcript(clock.wall)))
    assert result.structural_match and not result.complete and not result.live_enabled
    assert receiver.status()["account"]["phase"] == "OBSERVED_UNVERIFIED"
    entered, release = threading.Event(), threading.Event()

    def collect():
        entered.set()
        assert release.wait(5)
        return replay(demo_transcript(clock.wall))

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(receiver.resync, collect)
        assert entered.wait(5)
        receiver.close()
        release.set()
        with pytest.raises(SyncError):
            future.result(timeout=5)
    assert sock.closed and receiver.status()["account"]["phase"] == "DISCONNECTED"


def test_ack_failure_keeps_uncertainty_and_closes_transport(tmp_path, monkeypatch):
    _, journal, _, _, sock, receiver, _ = setup(tmp_path)
    start(journal, receiver)
    monkeypatch.setattr(journal, "acknowledge", lambda *a: (_ for _ in ()).throw(JournalError()))
    sock.messages.append(frame())
    with pytest.raises(StreamError, match="private_stream_receive_failed"):
        receiver.step()
    assert sock.closed
    assert journal.inspect()["unacknowledged_records"] == (2,)
    assert "journal_delivery_outcome_unknown" in receiver.status()["account"]["blockers"]
    assert not journal.replay()["complete"]


def test_offline_demo_writes_serializable_report_without_credentials_and_never_overwrites(tmp_path):
    directory = tmp_path / "demo"
    result = demo(directory)
    assert json.loads(json.dumps(result)) == json.loads(
        (directory / "report.json").read_text(encoding="utf-8")
    )
    assert [r["method"] for r in result["token_requests"]] == ["POST", "PUT", "DELETE"]
    assert result["steps"][-1]["stream_closed"]
    assert result["steps"][-1]["stream_reason"] == "stream_token_expired"
    assert result["steps"][2]["receive_sequence"] == 1
    assert result["offline_only"] and not result["live_enabled"]
    saved = (directory / "report.json").read_bytes()
    for secret in ("synthetic-not-a-real-token", "synthetic-key", "synthetic-secret"):
        assert (
            secret.encode() not in saved
            and secret.encode() not in (directory / "event-journal.sqlite").read_bytes()
        )
    with pytest.raises((JournalError, FileExistsError)):
        demo(directory)
    assert (directory / "report.json").read_bytes() == saved
