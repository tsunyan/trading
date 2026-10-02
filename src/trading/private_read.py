"""Explicit-credential GMO FX GET transport. No order submission or credential loading.

Production endpoint/auth/rate reference: https://api.coin.z.com/fxdocs/ (2026-09-30).
Tests use httpx.MockTransport; constructing this object never sends a request.
"""

import json
import math
import re
import threading
import time
from collections.abc import Callable
from contextlib import contextmanager
from datetime import UTC, datetime

import httpx
from pydantic import SecretStr

from trading.broker_contracts import RequestPlan, response_data, sign_request
from trading.wire_validation import unique_object

ENDPOINT = "https://forex-api.coin.z.com/private"


class PrivateReadError(ValueError):
    """Only fixed local reason codes; no public request/response attributes."""


class AccountReadLimiter:
    """Share ONE instance across all clients/keys for an account in this process.

    Serializes complete requests and spaces starts by >=250ms (<=4/s), below the
    documented 6 GET/s. Cannot coordinate other processes or manual/API activity.
    HTTP 401/403/429 and explicit API failures (integer status != 0) latch a stop,
    including API errors returned with HTTP 200; there is no auto-reset.
    """

    def __init__(
        self,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self._monotonic = monotonic
        self._sleep = sleep
        self._lock = threading.RLock()
        self._next_at = None
        self._last_at = None
        self._stopped = False

    def stop(self):
        with self._lock:
            self._stopped = True

    def _now(self):
        now = self._monotonic()
        if not math.isfinite(now) or (self._last_at is not None and now < self._last_at):
            self._stopped = True
            raise PrivateReadError("limiter_clock_invalid")
        self._last_at = now
        return now

    @contextmanager
    def slot(self):
        with self._lock:
            if self._stopped:
                raise PrivateReadError("account_reads_stopped")
            now = self._now()
            if self._next_at is not None and now < self._next_at:
                self._sleep(self._next_at - now)
                now = self._now()
                if now < self._next_at:
                    self._stopped = True
                    raise PrivateReadError("limiter_wait_incomplete")
            try:
                yield
            finally:
                # Pace from completion as well, so slow signing/transport cannot
                # cause two actual network starts to bunch up after a wait.
                self._next_at = self._now() + 0.25


def validate_read_plan(plan: RequestPlan) -> None:
    """Narrower than the offline signer: exactly the collector's GET requests."""
    if (
        not isinstance(plan, RequestPlan)
        or plan.method != "GET"
        or type(plan.path) is not str
        or type(plan.body) is not bytes
        or plan.body
    ):
        raise PrivateReadError("get_only")
    if not isinstance(plan.query, tuple):
        raise PrivateReadError("invalid_read_query")
    query = {}
    for pair in plan.query:
        if (
            not isinstance(pair, tuple)
            or len(pair) != 2
            or not all(isinstance(v, str) for v in pair)
            or pair[0] in query
        ):
            raise PrivateReadError("invalid_read_query")
        key, value = pair
        if not re.fullmatch(r"[1-9][0-9]{0,19}", value):
            raise PrivateReadError("invalid_read_query")
        query[key] = value
    if plan.path == "/v1/account/assets":
        valid = not query
    elif plan.path in {"/v1/openPositions", "/v1/activeOrders"}:
        valid = set(query) in ({"count"}, {"count", "prevId"}) and int(query["count"]) <= 100
    elif plan.path in {"/v1/orders", "/v1/executions"}:
        valid = set(query) == {"orderId"}
    else:
        raise PrivateReadError("read_endpoint_not_allowed")
    if not valid:
        raise PrivateReadError("invalid_read_query")


def _credentials(value):
    if not isinstance(value, SecretStr):
        raise PrivateReadError("explicit_secretstr_required")
    raw = value.get_secret_value()
    if not re.fullmatch(r"[\x21-\x7e]{1,512}", raw):
        raise PrivateReadError("invalid_credentials")
    return value


def _unique_object(pairs):
    try:
        return unique_object(pairs)
    except ValueError:
        raise PrivateReadError("duplicate_json_key") from None


def _invalid_constant(value):
    raise PrivateReadError("nonfinite_json_number")


def _finite_float(value):
    result = float(value)
    if not math.isfinite(result):
        raise PrivateReadError("nonfinite_json_number")
    return result


class PrivateReadClient:
    """No live-order interface, arbitrary URL, env loader, hooks or external Client.

    An optional low-level transport is a trusted test seam, NOT a sandbox boundary.
    Default TLS verification is on. Environment proxies/cert overrides are off.
    """

    def __init__(
        self,
        api_key: SecretStr,
        secret: SecretStr,
        *,
        limiter: AccountReadLimiter,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = time.monotonic,
        timeout_seconds: float = 5,
        max_response_bytes: int = 2_000_000,
    ):
        self._api_key = _credentials(api_key)
        self._secret = _credentials(secret)
        if not isinstance(limiter, AccountReadLimiter):
            raise PrivateReadError("shared_account_limiter_required")
        if (
            type(timeout_seconds) not in {int, float}
            or not math.isfinite(timeout_seconds)
            or not 0 < timeout_seconds <= 10
            or type(max_response_bytes) is not int
            or not 1 <= max_response_bytes <= 2_000_000
        ):
            raise PrivateReadError("invalid_transport_limits")
        self._limiter = limiter
        self._clock = clock
        self._monotonic = monotonic
        self._timeout = httpx.Timeout(timeout_seconds)
        self._max_elapsed = timeout_seconds * 2
        self._max_bytes = max_response_bytes
        self._last_timestamp = None
        self._lock = threading.RLock()
        self._closed = False
        self._client = httpx.Client(
            transport=transport,
            trust_env=False,
            verify=True,
            follow_redirects=False,
            timeout=self._timeout,
            limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
        )

    def __enter__(self):
        if self._closed:
            raise PrivateReadError("client_closed")
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        with self._lock:
            try:
                self._client.close()
            except Exception:
                self._limiter.stop()
                raise PrivateReadError("client_cleanup_failed") from None
            finally:
                self._api_key = SecretStr("")
                self._secret = SecretStr("")
                self._closed = True

    def get(self, plan: RequestPlan) -> dict:
        validate_read_plan(plan)
        # Client lock also protects close(); limiter lock is shared across keys.
        with self._lock, self._limiter.slot():
            if self._closed:
                raise PrivateReadError("client_closed")
            request = response = None
            try:
                stamp = self._clock()
                if not isinstance(stamp, datetime) or stamp.utcoffset() is None:
                    raise PrivateReadError("invalid_signing_clock")
                timestamp = int(stamp.timestamp() * 1000)
                if self._last_timestamp is not None and timestamp < self._last_timestamp:
                    self._limiter.stop()
                    raise PrivateReadError("signing_clock_moved_backwards")
                self._last_timestamp = timestamp
                signed = sign_request(plan, self._api_key, self._secret, timestamp)
                request = httpx.Request(
                    "GET",
                    ENDPOINT + plan.path,
                    params=plan.query,
                    headers={
                        **signed.headers,
                        "Accept": "application/json",
                        "Accept-Encoding": "identity",
                    },
                    extensions={"timeout": self._timeout.as_dict()},
                )
                started = self._monotonic()
                if not math.isfinite(started):
                    raise PrivateReadError("invalid_transport_clock")
                response = self._client.send(request, stream=True, follow_redirects=False)
                if response.status_code in {401, 403, 429}:
                    self._limiter.stop()
                    raise PrivateReadError("authentication_or_rate_limit_stop")
                if response.status_code != 200:
                    raise PrivateReadError("unexpected_http_status")
                encoding = response.headers.get("content-encoding", "identity").lower()
                if encoding != "identity":
                    raise PrivateReadError("encoded_response_not_allowed")
                if (
                    response.headers.get("content-type", "").split(";", 1)[0].strip()
                    != "application/json"
                ):
                    raise PrivateReadError("expected_json_response")
                length = response.headers.get("content-length")
                if length is not None and (
                    not length.isascii() or not length.isdigit() or int(length) > self._max_bytes
                ):
                    raise PrivateReadError("invalid_response_length")
                content = bytearray()
                for chunk in response.iter_bytes():
                    elapsed = self._monotonic() - started
                    if not math.isfinite(elapsed) or not 0 <= elapsed <= self._max_elapsed:
                        raise PrivateReadError("response_deadline_exceeded")
                    if len(content) + len(chunk) > self._max_bytes:
                        raise PrivateReadError("response_too_large")
                    content.extend(chunk)
                elapsed = self._monotonic() - started
                if not math.isfinite(elapsed) or not 0 <= elapsed <= self._max_elapsed:
                    raise PrivateReadError("response_deadline_exceeded")
                if length is not None and len(content) != int(length):
                    raise PrivateReadError("response_length_mismatch")
                payload = json.loads(
                    content,
                    object_pairs_hook=_unique_object,
                    parse_constant=_invalid_constant,
                    parse_float=_finite_float,
                )
                # The FX docs list error codes but do not specify the error body
                # shape. Conservatively stop on every explicit API failure rather
                # than guess where authentication/rate-limit codes are nested.
                if (
                    isinstance(payload, dict)
                    and type(payload.get("status")) is int
                    and payload["status"] != 0
                ):
                    self._limiter.stop()
                    raise PrivateReadError("api_error_stop")
                # API errors are not valid snapshots, even with HTTP 200.
                response_data(payload)
                return payload
            except PrivateReadError:
                raise
            except Exception:
                # httpx errors contain request objects; never expose their text.
                raise PrivateReadError("private_read_failed") from None
            finally:
                cleanup_failed = False
                try:
                    if response is not None:
                        response.close()
                except Exception:
                    cleanup_failed = True
                finally:
                    self._client.cookies.clear()
                    if request is not None:
                        for header in ("API-KEY", "API-SIGN"):
                            request.headers.pop(header, None)
                if cleanup_failed:
                    self._limiter.stop()
                    raise PrivateReadError("response_cleanup_failed") from None
