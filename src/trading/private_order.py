"""One-shot GMO order transport using an enabled, bound live journal and risk gate."""

import hashlib
import json
import math
import threading
import time
from datetime import UTC, datetime

import httpx
from pydantic import SecretStr

from trading.broker_contracts import OrderIntent, sign_request
from trading.live_journal import LiveOrderJournal
from trading.order_journal import OrderBlocked
from trading.order_receipts import (
    MAX_RECEIPT_BYTES,
    parse_cancellation_receipt,
    parse_submission_receipt,
)
from trading.post_control import PostControlError

ENDPOINT = "https://forex-api.coin.z.com/private"


class OrderTransportError(ValueError):
    """Fixed local reasons only; no httpx request/response or remote text."""


def _credential(value):
    if not isinstance(value, SecretStr) or not value.get_secret_value():
        raise OrderTransportError("explicit_order_credentials_required")
    text = value.get_secret_value()
    if not text.isascii() or any(ord(c) < 33 or ord(c) == 127 for c in text):
        raise OrderTransportError("invalid_order_credentials")
    return value


class PrivateOrderClient:
    """No arbitrary plan/host or automatic retries. Low-level transport is trusted.

    Construction never loads keys or sends. Approval and normalized complete
    account evidence must already be supplied to the dedicated journal by the
    trusted operator/provider; this client never promotes a read-only report.
    """

    def __init__(
        self,
        api_key,
        secret,
        *,
        journal,
        transport=None,
        clock=lambda: datetime.now(UTC),
        monotonic=time.monotonic,
        timeout_seconds=5,
        clock_skew_ms=0,
    ):
        if not isinstance(journal, LiveOrderJournal):
            raise OrderTransportError("dedicated_live_journal_required")
        journal.snapshot()  # Validate the file/mode/binding/code before retaining keys.
        if (
            type(timeout_seconds) not in {int, float}
            or not math.isfinite(timeout_seconds)
            or not 0 < timeout_seconds <= 10
            or type(clock_skew_ms) is not int
            or not 0 <= clock_skew_ms <= 1000
        ):
            raise OrderTransportError("invalid_order_transport_limits")
        self._api_key, self._secret = _credential(api_key), _credential(secret)
        self.journal, self.posts = journal, journal.posts
        self._clock, self._mono = clock, monotonic
        self._timeout = httpx.Timeout(timeout_seconds)
        self._max_elapsed, self._skew = timeout_seconds * 2, clock_skew_ms
        self._lock, self._closed = threading.RLock(), False
        self._last_timestamp = None
        self._client = httpx.Client(
            transport=(
                transport
                if transport is not None
                else httpx.HTTPTransport(verify=True, trust_env=False, retries=0)
            ),
            trust_env=False,
            verify=True,
            follow_redirects=False,
            timeout=self._timeout,
            limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
        )

    def __enter__(self):
        if self._closed:
            raise OrderTransportError("order_client_closed")
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        with self._lock:
            try:
                self._client.close()
            except Exception:
                self.posts.stop("order_cleanup_failed")
                raise OrderTransportError("order_client_cleanup_failed") from None
            finally:
                self._api_key, self._secret = SecretStr(""), SecretStr("")
                self._closed = True

    def _now(self):
        stamp = self._clock()
        if not isinstance(stamp, datetime) or stamp.utcoffset() is None:
            raise OrderTransportError("invalid_order_signing_clock")
        return stamp

    def _elapsed(self, started):
        now = self._mono()
        if type(now) not in {int, float} or not math.isfinite(now):
            raise OrderTransportError("invalid_order_transport_clock")
        elapsed = now - started
        if not 0 <= elapsed <= self._max_elapsed:
            raise OrderTransportError("order_response_deadline_exceeded")
        return elapsed

    def _http(self, client_id, plan, *, cancellation_authorization=None):
        request = response = None
        try:
            started_at = self._now()
            timestamp = int(started_at.timestamp() * 1000)
            if self._last_timestamp is not None and timestamp < self._last_timestamp:
                raise OrderTransportError("order_signing_clock_moved_backwards")
            self._last_timestamp = timestamp
            signed = sign_request(plan, self._api_key, self._secret, timestamp)
            request = httpx.Request(
                "POST",
                ENDPOINT + plan.path,
                content=plan.body,
                headers={
                    **signed.headers,
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Accept-Encoding": "identity",
                },
                extensions={"timeout": self._timeout.as_dict()},
            )
            started = self._mono()
            if type(started) not in {int, float} or not math.isfinite(started) or started < 0:
                raise OrderTransportError("invalid_order_transport_clock")
            is_cancel = plan.path == "/v1/cancelOrders"
            if is_cancel:
                self.journal.validate_cancel_dispatch(
                    client_id, plan, authorization_sha256=cancellation_authorization
                )
            else:
                self.journal.validate_dispatch(client_id, plan)
            if self._elapsed(started) > 1:
                raise OrderTransportError("order_dispatch_deadline_exceeded")
            response = self._client.send(request, stream=True, follow_redirects=False)
            if response.status_code != 200:
                raise OrderTransportError("unexpected_order_http_status")
            if response.headers.get("content-encoding", "identity").lower() != "identity":
                raise OrderTransportError("encoded_order_response_not_allowed")
            if (
                response.headers.get("content-type", "").split(";", 1)[0].strip()
                != "application/json"
            ):
                raise OrderTransportError("expected_order_json_response")
            length = response.headers.get("content-length")
            if length is not None and (
                not length.isascii() or not length.isdigit() or int(length) > MAX_RECEIPT_BYTES
            ):
                raise OrderTransportError("invalid_order_response_length")
            content = bytearray()
            for chunk in response.iter_bytes():
                self._elapsed(started)
                if len(content) + len(chunk) > MAX_RECEIPT_BYTES:
                    raise OrderTransportError("order_response_too_large")
                content.extend(chunk)
            self._elapsed(started)
            if length is not None and len(content) != int(length):
                raise OrderTransportError("order_response_length_mismatch")
            received_at = self._now()
            # Close and strip headers before interpreting/persisting acceptance.
            response.close()
            response = None
            self._client.cookies.clear()
            for header in ("API-KEY", "API-SIGN"):
                request.headers.pop(header, None)
            if is_cancel:
                return parse_cancellation_receipt(
                    client_id,
                    json.loads(plan.body)["rootOrderIds"][0],
                    bytes(content),
                    started_at=started_at,
                    received_at=received_at,
                    clock_skew_ms=self._skew,
                )
            with self.journal._transaction() as conn:
                intent = OrderIntent.model_validate_json(
                    self.journal._row(conn, client_id)["intent_json"]
                )
            return parse_submission_receipt(
                intent,
                bytes(content),
                started_at=started_at,
                received_at=received_at,
                clock_skew_ms=self._skew,
            )
        finally:
            try:
                if response is not None:
                    response.close()
            finally:
                try:
                    self._client.cookies.clear()
                finally:
                    if request is not None:
                        for header in ("API-KEY", "API-SIGN"):
                            request.headers.pop(header, None)

    def _unknown(self, client_id):
        try:
            self.journal.unknown(client_id)
        except Exception:
            pass  # The committed SUBMITTING claim also refuses replay.
        try:
            self.journal.halt()
        except Exception:
            pass  # Missing/corrupt storage still refuses future dispatch.

    def submit(self, client_id, *, quote):
        with self._lock:
            if self._closed:
                raise OrderTransportError("order_client_closed")
            try:
                plan = self.journal.request(client_id)
            except Exception:
                raise OrderTransportError("order_preflight_refused") from None
            kind = "order" if plan.path == "/v1/order" else "close_order"
            refused = False
            claimed = False
            entered = False
            try:
                with self.posts.operation(
                    kind, request_sha256=hashlib.sha256(plan.body).hexdigest()
                ):
                    entered = True
                    try:
                        current = self.journal.begin_submission(
                            client_id, quote=quote, now=self._now()
                        )
                    except OrderBlocked:
                        # No HTTP call occurred. The local risk refusal is known;
                        # commit normal completion of the POST pacing slot only.
                        refused = True
                    else:
                        claimed = True
                        if current != plan:
                            raise OrderTransportError("order_plan_changed")
                        receipt = self._http(client_id, current)
                        self.journal.acknowledge_submission(receipt)
                if refused:
                    raise OrderTransportError("order_preflight_refused")
                return receipt
            except OrderTransportError:
                if entered and not refused:
                    self._unknown(client_id)
                raise
            except PostControlError:
                if entered and not refused:
                    self._unknown(client_id)
                raise OrderTransportError(
                    "order_submission_unknown" if claimed else "order_post_control_refused"
                ) from None
            except BaseException as error:
                if entered and not refused:
                    self._unknown(client_id)
                if not isinstance(error, Exception):
                    raise
                raise OrderTransportError("order_submission_unknown") from None

    def cancel(self, client_id, *, authorization_sha256=None):
        """One cancellation attempt for a positively identified active order."""
        with self._lock:
            if self._closed:
                raise OrderTransportError("order_client_closed")
            try:
                plan = self.journal.cancel_request(
                    client_id, authorization_sha256=authorization_sha256
                )
            except Exception:
                raise OrderTransportError("cancel_preflight_refused") from None
            entered = claimed = refused = False
            try:
                with self.posts.operation(
                    "cancel", request_sha256=hashlib.sha256(plan.body).hexdigest()
                ):
                    entered = True
                    try:
                        current = (
                            self.journal.begin_cancel(client_id)
                            if authorization_sha256 is None
                            else self.journal.begin_cancel(
                                client_id, authorization_sha256=authorization_sha256
                            )
                        )
                    except OrderBlocked:
                        refused = True  # Known refusal before consuming a cancel attempt or HTTP.
                    else:
                        claimed = True
                        if current != plan:
                            raise OrderTransportError("cancel_plan_changed")
                        receipt = self._http(
                            client_id, current, cancellation_authorization=authorization_sha256
                        )
                        self.journal.acknowledge_cancel(receipt)
                if refused:
                    raise OrderTransportError("cancel_preflight_refused")
                return receipt
            except OrderTransportError:
                if entered and not refused:
                    self._unknown(client_id)
                raise
            except PostControlError:
                if entered and not refused:
                    self._unknown(client_id)
                raise OrderTransportError(
                    "cancel_submission_unknown" if claimed else "cancel_post_control_refused"
                ) from None
            except BaseException as error:
                if entered and not refused:
                    self._unknown(client_id)
                if not isinstance(error, Exception):
                    raise
                raise OrderTransportError("cancel_submission_unknown") from None
