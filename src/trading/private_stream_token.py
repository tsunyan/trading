"""Explicit GMO FX WebSocket token ownership; no order or GET transport.

Source: https://api.coin.z.com/fxdocs/#ws-auth-post (2026-10-02).
Only POST includes its JSON body in the signature; PUT/DELETE exclude it.
"""

import hashlib
import hmac
import json
import math
import re
import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import httpx
from pydantic import SecretStr

from trading.wire_validation import clock_skew, timestamp_string, unique_object

TOKEN_ENDPOINT = "https://forex-api.coin.z.com/private/v1/ws-auth"
STREAM_ENDPOINT = "wss://forex-api.coin.z.com/ws/private/v1/"


class StreamError(ValueError):
    """Fixed local codes only; never expose network exceptions or credentials."""


class PrivateStreamLimiter:
    """Share across token clients and subscriptions for one account / outbound IP.

    All operations are serialized and spaced by 1.1s from completion. This only
    coordinates this process; other applications / machines need external control.
    Stops are latched, with no automatic reset.
    """

    def __init__(self, *, monotonic=time.monotonic, sleep=time.sleep):
        self._mono, self._sleep = monotonic, sleep
        self._lock = threading.RLock()
        self._last = self._next = None
        self._stopped = False

    def stop(self):
        with self._lock:
            self._stopped = True

    def check(self):
        with self._lock:
            if self._stopped:
                raise StreamError("private_stream_stopped")
            try:
                now = self._mono()
                if (
                    type(now) not in {int, float}
                    or not math.isfinite(now)
                    or now < 0
                    or (self._last is not None and now < self._last)
                ):
                    raise ValueError
            except Exception:
                self._stopped = True
                raise StreamError("stream_limiter_clock_invalid") from None
            self._last = now
            return now

    @contextmanager
    def slot(self):
        with self._lock:
            now = self.check()
            if self._next is not None and now < self._next:
                try:
                    self._sleep(self._next - now)
                except Exception:
                    self.stop()
                    raise StreamError("stream_limiter_wait_failed") from None
                if self.check() < self._next:
                    self.stop()
                    raise StreamError("stream_limiter_wait_incomplete")
            try:
                yield
            finally:
                # An error may have stopped the limiter; still pace any future
                # operations without replacing the original error on exit.
                if not self._stopped:
                    self._next = self.check() + 1.1


def _secret(value, *, token=False):
    pattern = r"[A-Za-z0-9_-]{1,512}" if token else r"[\x21-\x7e]{1,512}"
    if not isinstance(value, SecretStr) or not re.fullmatch(pattern, value.get_secret_value()):
        raise StreamError(
            "invalid_stream_token" if token else "explicit_stream_credentials_required"
        )
    return value


class PrivateTokenClient:
    """Own at most one token. No acquire retry, token import, or credential loader.

    Local expiry is 30s earlier than the documented 60 minutes and measured from
    request start, with renewal at 50 minutes. Both wall and monotonic deadlines
    apply. All ambiguous outcomes stop this client and its shared limiter.
    """

    def __init__(
        self,
        api_key: SecretStr,
        secret: SecretStr,
        *,
        limiter: PrivateStreamLimiter,
        transport: httpx.BaseTransport | None = None,
        clock=lambda: datetime.now(UTC),
        monotonic=time.monotonic,
        clock_skew_ms=0,
    ):
        self._key, self._secret = _secret(api_key), _secret(secret)
        if not isinstance(limiter, PrivateStreamLimiter):
            raise StreamError("shared_stream_limiter_required")
        self.limiter = limiter
        self._skew = clock_skew(clock_skew_ms)
        self._clock, self._mono = clock, monotonic
        self._lock = threading.RLock()
        self._last_wall = self._last_mono = None
        self._token = None
        self._expires = self._expires_wall = self._renew = None
        self._attempted = self._closed = self._failed = False
        self._cleanup_unknown = False
        self._client = httpx.Client(
            transport=transport,
            trust_env=False,
            verify=True,
            follow_redirects=False,
            timeout=5,
            limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
        )

    def __enter__(self):
        if self._closed:
            raise StreamError("stream_token_client_closed")
        return self

    def __exit__(self, *args):
        self.close()

    def _now(self):
        try:
            wall, mono = self._clock(), self._mono()
            if (
                not isinstance(wall, datetime)
                or wall.utcoffset() is None
                or type(mono) not in {int, float}
                or not math.isfinite(mono)
                or mono < 0
                or (self._last_wall is not None and wall < self._last_wall)
                or (self._last_mono is not None and mono < self._last_mono)
            ):
                raise ValueError
        except Exception:
            self._failed = True
            self.limiter.stop()
            raise StreamError("stream_token_clock_invalid") from None
        self._last_wall, self._last_mono = wall, mono
        return wall, mono

    def _request(self, method):
        # Never accepts a path, URL, arbitrary body, or externally supplied token.
        with self.limiter.slot():
            wall, mono = self._now()
            if method == "PUT" and (mono >= self._expires or wall >= self._expires_wall):
                raise StreamError("stream_token_expired")
            body = (
                b"{}"
                if method == "POST"
                else json.dumps(
                    {"token": self._token.get_secret_value()}, separators=(",", ":")
                ).encode("ascii")
            )
            stamp = str(int(wall.timestamp() * 1000))
            if int(stamp) <= 0:
                raise StreamError("stream_token_clock_invalid")
            signed = (stamp + method + "/v1/ws-auth").encode("ascii")
            if method == "POST":
                signed += body
            signature = hmac.new(
                self._secret.get_secret_value().encode(), signed, hashlib.sha256
            ).hexdigest()
            request = httpx.Request(
                method,
                TOKEN_ENDPOINT,
                content=body,
                headers={
                    "API-KEY": self._key.get_secret_value(),
                    "API-SIGN": signature,
                    "API-TIMESTAMP": stamp,
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Accept-Encoding": "identity",
                },
                extensions={"timeout": httpx.Timeout(5).as_dict()},
            )
            response = None
            try:
                # Until validated, POST / DELETE may have taken effect remotely.
                if method in {"POST", "DELETE"}:
                    self._cleanup_unknown = True
                response = self._client.send(request, stream=True, follow_redirects=False)
                if response.status_code != 200:
                    raise StreamError("stream_token_http_failed")
                if response.headers.get("content-encoding", "identity").lower() != "identity":
                    raise StreamError("stream_token_encoding_invalid")
                if response.headers.get("content-type", "").split(";", 1)[0].strip() != (
                    "application/json"
                ):
                    raise StreamError("stream_token_json_required")
                length = response.headers.get("content-length")
                if length is not None and (
                    not length.isascii() or not length.isdigit() or int(length) > 4096
                ):
                    raise StreamError("stream_token_response_too_large")
                content = bytearray()
                for chunk in response.iter_bytes():
                    if self._now()[1] - mono > 10:
                        raise StreamError("stream_token_response_deadline")
                    if len(content) + len(chunk) > 4096:
                        raise StreamError("stream_token_response_too_large")
                    content.extend(chunk)
                end_wall, end_mono = self._now()
                if end_mono - mono > 10:
                    raise StreamError("stream_token_response_deadline")
                if length is not None and int(length) != len(content):
                    raise StreamError("stream_token_response_length_invalid")
                payload = json.loads(content, object_pairs_hook=unique_object)
                required = {"status", "responsetime", *(("data",) if method == "POST" else ())}
                if (
                    not isinstance(payload, dict)
                    or set(payload) != required
                    or type(payload["status"]) is not int
                    or payload["status"] != 0
                ):
                    raise StreamError("stream_token_response_invalid")
                observed = timestamp_string(payload["responsetime"])
                if not wall - self._skew <= observed <= end_wall + self._skew:
                    raise StreamError("stream_token_response_stale")
                if method == "POST":
                    if not isinstance(payload["data"], str):
                        raise StreamError("invalid_stream_token")
                    self._token = _secret(SecretStr(payload["data"]), token=True)
                if method in {"POST", "PUT"}:
                    self._expires, self._renew = mono + 3570, mono + 3000
                    self._expires_wall = wall + timedelta(seconds=3570)
                    self._cleanup_unknown = False
                else:
                    self._token = None
                    self._cleanup_unknown = False
            except StreamError:
                raise
            except Exception:
                raise StreamError("stream_token_request_failed") from None
            finally:
                try:
                    if response is not None:
                        response.close()
                except Exception:
                    raise StreamError("stream_token_cleanup_failed") from None
                finally:
                    self._client.cookies.clear()
                    for header in ("API-KEY", "API-SIGN"):
                        request.headers.pop(header, None)

    def acquire(self):
        with self._lock:
            if self._attempted or self._closed or self._failed:
                raise StreamError("new_stream_token_client_required")
            self._attempted = True
            try:
                self._request("POST")
            except BaseException:
                self._failed = True
                self.limiter.stop()
                raise

    def _usable(self):
        if self._closed or self._failed or self._token is None:
            raise StreamError("stream_token_not_usable")
        self.limiter.check()
        wall, mono = self._now()
        if mono >= self._expires or wall >= self._expires_wall:
            self._failed = True
            raise StreamError("stream_token_expired")
        return wall, mono

    def connection_url(self):
        with self._lock:
            self._usable()
            return SecretStr(STREAM_ENDPOINT + self._token.get_secret_value())

    def maintain(self):
        """Called by the receive loop even when no account events arrive."""
        with self._lock:
            wall, mono = self._usable()
            if mono >= self._renew or wall >= self._expires_wall - timedelta(seconds=570):
                old_mono, old_wall = self._expires, self._expires_wall
                try:
                    self._request("PUT")
                    # A response arriving after the old deadline cannot revive it.
                    wall, mono = self._now()
                    if mono >= old_mono or wall >= old_wall:
                        raise StreamError("stream_token_expired_during_renewal")
                except BaseException:
                    self._failed = True
                    self.limiter.stop()
                    raise

    def status(self):
        with self._lock:
            return {
                "token_acquire_attempted": self._attempted,
                "token_owned": self._token is not None,
                "token_client_failed": self._failed,
                "token_client_closed": self._closed,
                "token_cleanup_unknown": self._cleanup_unknown,
            }

    def close(self):
        with self._lock:
            if self._closed:
                return
            failed = False
            try:
                if self._token is not None:
                    self._cleanup_unknown = True
                    self._request("DELETE")
            except Exception:
                failed = True
                self._failed = True
                self.limiter.stop()
            finally:
                try:
                    self._client.close()
                except Exception:
                    failed = True
                finally:
                    self._token = None
                    self._key = self._secret = SecretStr("")
                    self._closed = True
            if failed:
                raise StreamError("stream_token_cleanup_failed") from None
