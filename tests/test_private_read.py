import hashlib
import hmac
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from pydantic import SecretStr

from trading.account_read_lab import demo_transcript
from trading.account_reader import AccountReader, CollectionError
from trading.broker_contracts import RequestPlan
from trading.private_read import (
    ENDPOINT,
    AccountReadLimiter,
    PrivateReadClient,
    PrivateReadError,
)

NOW = datetime(2026, 9, 30, tzinfo=UTC)
KEY = "fixture-key-NOT-REAL"
SECRET = "fixture-secret-NOT-REAL"
ASSETS = RequestPlan("GET", "/v1/account/assets")


class Clock:
    def __init__(self):
        self.value = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.value

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.value += seconds

    def now(self):
        return NOW + timedelta(seconds=self.value)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("real network attempted")

    monkeypatch.setattr(socket, "socket", forbidden)


def success():
    return httpx.Response(200, json={"status": 0, "data": [], "responsetime": NOW.isoformat()})


def make_client(handler=None, *, clock=None, limiter=None, api_key=KEY, **kwargs):
    clock = clock or Clock()
    limiter = limiter or AccountReadLimiter(monotonic=clock.monotonic, sleep=clock.sleep)
    return PrivateReadClient(
        SecretStr(api_key),
        SecretStr(SECRET),
        limiter=limiter,
        transport=httpx.MockTransport(handler or (lambda req: success())),
        clock=clock.now,
        monotonic=clock.monotonic,
        **kwargs,
    )


@pytest.mark.parametrize(
    "plan",
    [
        ASSETS,
        RequestPlan("GET", "/v1/openPositions", query=(("count", "100"), ("prevId", "401"))),
        RequestPlan("GET", "/v1/activeOrders", query=(("count", "100"),)),
        RequestPlan("GET", "/v1/orders", query=(("orderId", "201"),)),
        RequestPlan("GET", "/v1/executions", query=(("orderId", "201"),)),
    ],
)
def test_only_official_get_and_exact_signature(plan):
    captured = []

    def handler(request):
        captured.append(request)
        assert request.method == "GET"
        assert str(request.url).split("?", 1)[0] == ENDPOINT + plan.path
        assert list(request.url.params.multi_items()) == list(plan.query)
        assert request.content == b""
        assert request.headers["API-KEY"] == KEY
        timestamp = str(int(NOW.timestamp() * 1000))
        expected = hmac.new(
            SECRET.encode(), (timestamp + "GET" + plan.path).encode(), hashlib.sha256
        ).hexdigest()
        assert request.headers["API-SIGN"] == expected
        assert request.headers["API-TIMESTAMP"] == timestamp
        assert request.headers["Accept-Encoding"] == "identity"
        assert request.extensions["timeout"] == dict(connect=5, read=5, write=5, pool=5)
        return success()

    with make_client(handler) as client:
        assert client.get(plan)["status"] == 0
        assert KEY not in repr(client) and SECRET not in repr(client)
    assert "API-KEY" not in captured[0].headers
    assert "API-SIGN" not in captured[0].headers


@pytest.mark.parametrize(
    "plan",
    [
        RequestPlan("POST", "/v1/order", b"{}"),
        RequestPlan("GET", "/v1/order"),
        RequestPlan("GET", "https://evil.test/v1/account/assets"),
        RequestPlan("GET", "/v1/account/assets/../order"),
        RequestPlan("GET", "/v1/account/assets?x=1"),
        RequestPlan("GET", "/v1/account/assets", b"{}"),
        RequestPlan("GET", "/v1/account/assets", query=(("x", "1"),)),
        RequestPlan("GET", "/v1/orders", query=(("orderId", "1,2"),)),
        RequestPlan("GET", "/v1/orders", query=(("orderId", "1"), ("orderId", "2"))),
        RequestPlan("GET", "/v1/orders", query=(("orderId", "1&x=2"),)),
        RequestPlan("GET", "/v1/orders", query=(("orderId", "-1"),)),
        RequestPlan("GET", "/v1/orders", query=(("orderId", "01"),)),
        RequestPlan("GET", "/v1/openPositions", query=(("count", "101"),)),
        RequestPlan("GET", "/v1/openPositions", query=(("symbol", "USD_JPY"),)),
        RequestPlan("GET", "/v1/openPositions"),
        RequestPlan("GET", "/v1/orders", query=(("orderId", True),)),
        RequestPlan("GET", "/v1/orders", query=[["orderId", "1"]]),
    ],
)
def test_invalid_plans_never_reach_transport(plan):
    def handler(request):
        pytest.fail("invalid plan sent")

    with make_client(handler) as client, pytest.raises(PrivateReadError):
        client.get(plan)


@pytest.mark.parametrize(
    "value",
    [
        "raw-string",
        SecretStr(""),
        SecretStr("key\nvalue"),
        SecretStr("key value"),
        SecretStr("日本語"),
    ],
)
def test_credentials_must_be_explicit_and_header_safe(value):
    with pytest.raises(PrivateReadError) as caught:
        PrivateReadClient(value, SecretStr(SECRET), limiter=AccountReadLimiter())
    assert SECRET not in str(caught.value)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"timeout_seconds": 0},
        {"timeout_seconds": True},
        {"timeout_seconds": float("nan")},
        {"max_response_bytes": 0},
    ],
)
def test_invalid_limits(kwargs):
    with pytest.raises(PrivateReadError):
        make_client(**kwargs)


def test_constructor_disables_ambient_settings(monkeypatch):
    seen = []
    real_client = httpx.Client

    def factory(**kwargs):
        seen.append(kwargs)
        return real_client(**kwargs)

    monkeypatch.setenv("HTTPS_PROXY", "http://unsafe.test")
    monkeypatch.setenv("SSL_CERT_FILE", "nonexistent.pem")
    monkeypatch.setattr(httpx, "Client", factory)
    with make_client() as client:
        client.get(ASSETS)
    assert seen[0]["trust_env"] is False
    assert seen[0]["verify"] is True
    assert seen[0]["follow_redirects"] is False


def test_shared_account_limit_across_two_keys_and_concurrent_reads():
    clock = Clock()
    limiter = AccountReadLimiter(monotonic=clock.monotonic, sleep=clock.sleep)
    starts = []
    handler_lock = threading.Lock()

    def handler(request):
        assert handler_lock.acquire(blocking=False)
        try:
            starts.append(clock.monotonic())
            # Signature generated after any rate-limit wait, never before it.
            assert int(request.headers["API-TIMESTAMP"]) == int(clock.now().timestamp() * 1000)
            return success()
        finally:
            handler_lock.release()

    with (
        make_client(handler, clock=clock, limiter=limiter) as a,
        make_client(handler, clock=clock, limiter=limiter, api_key="second-fixture-key") as b,
        ThreadPoolExecutor(max_workers=4) as pool,
    ):
        list(pool.map(lambda i: (a if i % 2 else b).get(ASSETS), range(12)))
    assert starts == [i * 0.25 for i in range(12)]


def test_slow_requests_cannot_cause_bunched_starts():
    clock = Clock()
    starts = []

    def handler(request):
        starts.append(clock.monotonic())
        clock.sleep(0.24)
        return success()

    with make_client(handler, clock=clock) as client:
        client.get(ASSETS)
        client.get(ASSETS)
    assert starts == [0, 0.49]


def test_overflowing_json_number_rejected():
    with (
        make_client(
            lambda r: httpx.Response(
                200,
                content=b'{"status":0,"data":1e999}',
                headers={"Content-Type": "application/json"},
            )
        ) as client,
        pytest.raises(PrivateReadError, match="nonfinite"),
    ):
        client.get(ASSETS)


def test_signing_wall_clock_reversal_stops_other_clients():
    clock = Clock()
    limiter = AccountReadLimiter(monotonic=clock.monotonic, sleep=clock.sleep)
    with make_client(clock=clock, limiter=limiter) as client:
        client.get(ASSETS)
        client._clock = lambda: NOW - timedelta(seconds=1)
        with pytest.raises(PrivateReadError, match="signing_clock_moved_backwards"):
            client.get(ASSETS)
        with pytest.raises(PrivateReadError, match="account_reads_stopped"):
            client.get(ASSETS)


def test_early_wake_cannot_bypass_limiter():
    limiter = AccountReadLimiter(monotonic=lambda: 0, sleep=lambda seconds: None)
    with make_client(limiter=limiter) as client:
        client.get(ASSETS)
        with pytest.raises(PrivateReadError, match="limiter_wait_incomplete"):
            client.get(ASSETS)


@pytest.mark.parametrize("status", [401, 403, 429])
def test_auth_and_rate_limit_latch_shared_stop(status):
    clock = Clock()
    limiter = AccountReadLimiter(monotonic=clock.monotonic, sleep=clock.sleep)
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(status, headers={"Retry-After": "60"}, text=SECRET)

    with (
        make_client(handler, clock=clock, limiter=limiter) as a,
        make_client(handler, clock=clock, limiter=limiter) as b,
    ):
        with pytest.raises(PrivateReadError, match="authentication_or_rate_limit_stop"):
            a.get(ASSETS)
        clock.sleep(1000)
        with pytest.raises(PrivateReadError, match="account_reads_stopped"):
            b.get(ASSETS)
    assert len(calls) == 1


@pytest.mark.parametrize("status", [301, 302, 307, 308, 400, 404, 500, 503])
def test_status_errors_do_not_follow_redirect_or_retry(status):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(status, headers={"Location": "https://evil.test"}, text=SECRET)

    with make_client(handler) as client, pytest.raises(PrivateReadError) as caught:
        client.get(ASSETS)
    assert len(calls) == 1 and SECRET not in str(caught.value)


@pytest.mark.parametrize(
    "payload",
    [
        b'{"status":0,"status":1,"data":[]}',
        b'{"status":0,"data":NaN}',
        b'{"status":true,"data":[]}',
        b'{"status":1,"message":"secret"}',
        b'{"status":0}',
        b"[]",
        b"not-json",
    ],
)
def test_malformed_or_error_json(payload):
    with (
        make_client(
            lambda r: httpx.Response(
                200, content=payload, headers={"Content-Type": "application/json"}
            )
        ) as client,
        pytest.raises(PrivateReadError),
    ):
        client.get(ASSETS)


def test_cookies_not_reused_or_retained():
    def handler(request):
        assert "cookie" not in request.headers
        result = success()
        result.headers["Set-Cookie"] = "secretcookie=x; Path=/"
        return result

    with make_client(handler) as client:
        client.get(ASSETS)
        client.get(ASSETS)
        assert not list(client._client.cookies)


@pytest.mark.parametrize("error", [httpx.ReadTimeout, httpx.ConnectError, RuntimeError])
def test_errors_are_redacted_and_not_retried(error, capsys):
    requests = []

    def handler(request):
        requests.append(request)
        raise error(KEY + SECRET)

    with make_client(handler) as client, pytest.raises(PrivateReadError) as caught:
        client.get(ASSETS)
    assert str(caught.value) == "private_read_failed"
    assert caught.value.__suppress_context__
    assert len(requests) == 1
    assert "api-key" not in requests[0].headers
    assert capsys.readouterr().out == ""


class Chunks(httpx.SyncByteStream):
    def __init__(self, chunks, clock=None, close_error=False):
        self.chunks = chunks
        self.clock = clock
        self.closed = False
        self.close_error = close_error

    def __iter__(self):
        for chunk in self.chunks:
            if self.clock:
                self.clock.sleep(6)
            yield chunk

    def close(self):
        self.closed = True
        if self.close_error:
            raise RuntimeError(SECRET)


@pytest.mark.parametrize("case", ["large", "length", "short", "encoding", "type", "slow", "close"])
def test_bounded_stream_and_cleanup(case):
    clock = Clock()
    stream = Chunks(
        [b'{"status":0,', b'"data":[]}'],
        clock if case == "slow" else None,
        close_error=case == "close",
    )
    headers = {"Content-Type": "application/json"}
    if case == "length":
        headers["Content-Length"] = "9999999"
    elif case == "short":
        headers["Content-Length"] = "100"
    elif case == "encoding":
        headers["Content-Encoding"] = "br"
    elif case == "type":
        headers["Content-Type"] = "text/html"

    def handler(request):
        return httpx.Response(200, stream=stream, headers=headers)

    with (
        make_client(handler, clock=clock, max_response_bytes=10 if case == "large" else 1000) as c,
        pytest.raises(PrivateReadError) as caught,
    ):
        c.get(ASSETS)
    assert stream.closed
    assert SECRET not in str(caught.value)


def test_close_is_idempotent_and_prevents_reuse():
    client = make_client()
    client.close()
    client.close()
    assert client._api_key.get_secret_value() == client._secret.get_secret_value() == ""
    with pytest.raises(PrivateReadError, match="client_closed"):
        client.get(ASSETS)


def test_clock_reversal_stops_reads():
    clock = Clock()
    limiter = AccountReadLimiter(monotonic=clock.monotonic, sleep=clock.sleep)
    with make_client(clock=clock, limiter=limiter) as client:
        client.get(ASSETS)
        clock.value = -1
        with pytest.raises(PrivateReadError, match="clock_invalid"):
            client.get(ASSETS)
        with pytest.raises(PrivateReadError, match="account_reads_stopped"):
            client.get(ASSETS)


def test_entire_account_collector_over_mock_http():
    clock = Clock()
    transcript = demo_transcript(NOW)
    calls = []

    def handler(request):
        exchange = transcript.exchanges[len(calls)]
        assert request.url.path == "/private" + exchange.path
        assert tuple(request.url.params.multi_items()) == exchange.query
        calls.append(request)
        payload = dict(exchange.response, responsetime=clock.now().isoformat())
        return httpx.Response(200, json=payload)

    with make_client(handler, clock=clock) as client:
        report = AccountReader(client, clock=clock.now).collect_account()
    assert len(calls) == 12
    assert report.positions[0].units == 400
    assert report.active_orders[0].units == 1000
    assert not report.live_enabled and not report.atomic_snapshot_verified


def test_collector_aborts_on_middle_page_failure():
    calls = []

    def handler(request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(200, json=demo_transcript(NOW).exchanges[0].response)
        raise httpx.ReadTimeout(SECRET)

    with make_client(handler) as client, pytest.raises(CollectionError):
        AccountReader(client, clock=lambda: NOW).collect_account()
    assert len(calls) == 2
