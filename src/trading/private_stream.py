"""Single-epoch, journal-first private receiver. No automatic reconnect or orders."""

import json
import logging
import math
import re
import ssl
import threading
import time

from pydantic import SecretStr
from websockets.sync.client import connect

from trading.account_sync import SyncError
from trading.event_capture import JournaledEventCapture
from trading.event_journal import JournalError
from trading.private_stream_token import STREAM_ENDPOINT, PrivateTokenClient, StreamError

CHANNELS = ("executionEvents", "orderEvents", "positionEvents")


def open_private_socket(url: SecretStr):
    """Fixed origin, bounded library buffering, verified TLS, no proxy or logs.

    The synchronous websockets 15 connector rejects redirects. The library
    handles fragmentation and replies to server pings. This receiver sends its
    own ping and only records a heartbeat after the corresponding pong arrives.
    """
    if not isinstance(url, SecretStr) or not re.fullmatch(
        re.escape(STREAM_ENDPOINT) + r"[A-Za-z0-9_-]{1,512}", url.get_secret_value()
    ):
        raise StreamError("invalid_private_stream_url")
    logger = logging.Logger("private-stream-silent")
    logger.disabled = True
    logger.propagate = False
    try:
        return connect(
            url.get_secret_value(),
            ssl=ssl.create_default_context(),
            proxy=None,
            compression=None,
            open_timeout=5,
            close_timeout=5,
            max_size=16_384,
            max_queue=4,
            ping_interval=None,
            logger=logger,
        )
    except Exception:
        raise StreamError("private_stream_connect_failed") from None


class PrivateStreamReceiver:
    """Explicit start / step / close for a freshly supplied capture and token client.

    One caller owns the receive loop. resync() releases the receiver lock before
    REST so receiving can continue. A connector is a trusted offline test seam,
    not a sandbox. Neither construction nor status() starts network work.
    """

    def __init__(
        self,
        tokens: PrivateTokenClient,
        capture: JournaledEventCapture,
        *,
        connector=open_private_socket,
        monotonic=time.monotonic,
    ):
        if not isinstance(tokens, PrivateTokenClient) or not isinstance(
            capture, JournaledEventCapture
        ):
            raise StreamError("private_stream_dependencies_required")
        self._tokens, self._capture = tokens, capture
        self._connector, self._mono = connector, monotonic
        self._lock = threading.RLock()
        self._socket = self._pong = None
        self._ping_at = self._last_mono = None
        self._sequence = 0
        self._started = self._capture_started = self._closed = False
        self._running = self._cleanup_failed = False
        self._reason = "not_started"

    def _now(self):
        # Token client validates both clocks on every receive-loop iteration.
        # Validate this independently supplied ping clock as well.
        try:
            now = self._mono()
            if (
                type(now) not in {int, float}
                or not math.isfinite(now)
                or now < 0
                or (self._last_mono is not None and now < self._last_mono)
            ):
                raise ValueError
        except Exception:
            raise StreamError("private_stream_clock_invalid") from None
        self._last_mono = now
        return now

    def start(self, *, expected_head):
        with self._lock:
            if self._started or self._closed:
                raise StreamError("new_private_stream_receiver_required")
            self._started = True
            self._reason = "starting"
            try:
                # Reject stale journal ownership before creating a remote token.
                self._capture.start_session(expected_head=expected_head)
                self._capture_started = True
                self._tokens.acquire()
                self._socket = self._connector(self._tokens.connection_url())
                for channel in CHANNELS:
                    self._tokens.maintain()
                    with self._tokens.limiter.slot():
                        self._socket.send(json.dumps({"command": "subscribe", "channel": channel}))
                self._ping_at = self._now()
                self._running = True
                # A sent subscription is not an acknowledgement or coverage proof.
                self._reason = "subscriptions_sent_unverified"
            except BaseException as error:
                self._reason = "private_stream_start_failed"
                self._shutdown()
                if not isinstance(error, Exception):
                    raise
                raise StreamError(self._reason) from None

    def step(self):
        """Receive at most one bounded message; timeout still services token / pong.

        Sequence is assigned when the complete data message crosses the library
        receive boundary. Control frames do not consume it. No application queue
        drops or skips messages; the library uses bounded backpressure.
        """
        with self._lock:
            if not self._running or self._closed:
                raise StreamError("private_stream_not_running")
            try:
                self._service(record_heartbeat=True)
                try:
                    payload = self._socket.recv(timeout=1, decode=False)
                except TimeoutError:
                    return False
                self._sequence += 1
                # Recheck expiry after recv, before accepting the notification.
                self._tokens.maintain()
                if type(payload) is not bytes or not 1 <= len(payload) <= 16_384:
                    raise StreamError("private_stream_message_invalid")
                # Malformed / unsupported data is rejected by the existing
                # journal parser, never treated as an ignorable control message.
                self._capture.ingest(self._sequence, payload)
                return True
            except BaseException as error:
                self._reason = (
                    str(error)
                    if isinstance(error, StreamError)
                    else "private_stream_receive_failed"
                )
                self._shutdown()
                if not isinstance(error, Exception):
                    raise
                raise StreamError(self._reason) from None

    def _service(self, *, record_heartbeat):
        self._tokens.maintain()
        now = self._now()
        if self._pong is not None:
            if now - self._ping_at >= 15:
                raise StreamError("private_stream_pong_expired")
            if self._pong.is_set():
                if record_heartbeat:
                    self._capture.heartbeat()
                self._pong = None
        if self._pong is None and now - self._ping_at >= 30:
            self._pong = self._socket.ping(ack_on_close=False)
            self._ping_at = now
        try:
            current = self._capture.check_live()
        except JournalError:
            raise StreamError("private_stream_capture_invalid") from None
        if current["phase"] == "DISCONNECTED":
            raise StreamError("private_stream_capture_invalid")

    def maintenance(self, *, record_heartbeat=True):
        """Service tokens/pong without dequeuing data; bounded backpressure remains.

        If heartbeat recording is paused, monitor liveness is not advanced by a
        pong. This is only a short final-collection pause, never an unlimited lease.
        """
        if type(record_heartbeat) is not bool:
            raise StreamError("invalid_stream_maintenance_option")
        with self._lock:
            if not self._running or self._closed:
                raise StreamError("private_stream_not_running")
            try:
                self._service(record_heartbeat=record_heartbeat)
            except BaseException as error:
                self._reason = (
                    str(error)
                    if isinstance(error, StreamError)
                    else "private_stream_receive_failed"
                )
                self._shutdown()
                if not isinstance(error, Exception):
                    raise
                raise StreamError(self._reason) from None

    @property
    def journal(self):
        return self._capture.journal

    @property
    def limiter(self):
        return self._tokens.limiter

    def rollover_ready(self, cash_book):
        with self._lock:
            if not self._running or self._closed:
                raise StreamError("private_stream_not_running")
            try:
                self._capture.assert_rollover_ready(cash_book)
            except SyncError as error:
                if str(error) in {
                    "rollover_collection_in_progress",
                    "rollover_execution_not_booked",
                }:
                    return False
                raise
            return True

    def resync(
        self,
        collect,
        *,
        collect_orders=None,
        cash_book=None,
        collect_reservations=None,
        reservation_book=None,
        collect_quote=None,
        valuation_policy=None,
        valuation_book=None,
        incremental_cash=None,
    ):
        with self._lock:
            if not self._running or self._closed:
                raise StreamError("private_stream_not_running")
            try:
                self._tokens.connection_url()  # Check validity without renewing across REST.
            except StreamError:
                self._reason = "private_stream_token_invalid"
                self._shutdown()
                raise StreamError(self._reason) from None
        # Capture / monitor perform the final generation and revision fences.
        try:
            result = self._capture.resync(
                collect,
                collect_orders=collect_orders,
                cash_book=cash_book,
                collect_reservations=collect_reservations,
                reservation_book=reservation_book,
                collect_quote=collect_quote,
                valuation_policy=valuation_policy,
                valuation_book=valuation_book,
                incremental_cash=incremental_cash,
            )
        except BaseException as error:
            if isinstance(error, SyncError) and str(error) in {
                "stream_changed_during_collection",
                "capture_changed_during_collection",
                "resync_already_running",
            }:
                with self._lock:
                    # A normal arriving event invalidates the attempt, not the
                    # connection. Storage/delivery/posting failures still close it.
                    if self._running and not self._closed:
                        try:
                            if self._capture.check_live()["phase"] != "DISCONNECTED":
                                raise error
                        except JournalError:
                            pass
            if cash_book is not None or reservation_book is not None or valuation_book is not None:
                with self._lock:
                    self._reason = (
                        "private_stream_cash_sync_failed"
                        if cash_book is not None
                        else "private_stream_reservation_sync_failed"
                        if reservation_book is not None
                        else "private_stream_valuation_sync_failed"
                    )
                    self._shutdown()
            raise
        with self._lock:
            if not self._running or self._closed:
                raise StreamError("private_stream_not_running")
            try:
                self._tokens.connection_url()
                current = self._capture.check_live()
            except (StreamError, JournalError) as error:
                self._reason = (
                    "private_stream_token_invalid"
                    if isinstance(error, StreamError)
                    else "private_stream_capture_invalid"
                )
                self._shutdown()
                raise StreamError(self._reason) from None
            # step() may deliver an event after the capture's own final fence and
            # before this lock. A committed cash posting stays safe to repeat; only
            # the stale assessment is withheld.
            if (current["epoch"], current["revision"]) != (result.epoch, result.revision):
                raise SyncError("capture_changed_during_collection")
        return result

    def run(self, stop_event: threading.Event):
        """Caller runs this loop; no daemon, auto-start, or reconnect is created."""
        try:
            while not stop_event.is_set():
                self.step()
        finally:
            self.close()

    def _shutdown(self):
        if self._closed:
            return
        self._closed = True
        self._running = False
        # Invalidate account observations before potentially slow network cleanup.
        if self._capture_started:
            try:
                self._capture.disconnect()
            except Exception:
                self._cleanup_failed = True
        if self._socket is not None:
            try:
                self._socket.close()
            except Exception:
                self._cleanup_failed = True
            finally:
                self._socket = self._pong = None
        try:
            self._tokens.close()
        except Exception:
            self._cleanup_failed = True

    def close(self):
        with self._lock:
            if self._closed:
                return
            self._reason = "closed"
            self._shutdown()
            if self._cleanup_failed:
                raise StreamError("private_stream_cleanup_failed")

    def close_for_rollover(self, cash_book=None):
        """Close a clean receipted boundary. Failure never starts a new connection."""
        with self._lock:
            if not self._running or self._closed:
                raise StreamError("private_stream_not_running")
            try:
                head = self._capture.end_for_rollover(cash_book)
                self._capture_started = False  # END has already invalidated the monitor.
                self._reason = "planned_rollover"
                self._shutdown()
                if self._cleanup_failed or self._tokens.status()["token_cleanup_unknown"]:
                    raise StreamError("private_stream_cleanup_failed")
                return head
            except BaseException as error:
                self._reason = "private_stream_rollover_failed"
                self._shutdown()
                if not isinstance(error, Exception):
                    raise
                raise StreamError(self._reason) from None

    def status(self):
        with self._lock:
            return {
                "stream_started": self._started,
                "stream_running": self._running,
                "stream_closed": self._closed,
                "stream_reason": self._reason,
                "stream_cleanup_failed": self._cleanup_failed,
                "receive_sequence": self._sequence,
                **self._tokens.status(),
                "account": self._capture.status(),
                "complete": False,
                "live_enabled": False,
            }
