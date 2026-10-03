"""Explicit long-running diagnostic receiver with owned, bounded REST workers."""

import math
import queue
import threading
import time

from pydantic import Field

from trading.account_sync import SyncError
from trading.broker_contracts import Contract
from trading.execution_cash_book import ExecutionCashBook
from trading.private_stream import PrivateStreamReceiver
from trading.private_stream_token import PrivateStreamLimiter
from trading.segmented_journal import SegmentedEventJournal
from trading.stream_control import StreamControl

RETRYABLE = frozenset({"stream_changed_during_collection", "capture_changed_during_collection"})
SYNC_FAILURES = frozenset(
    {
        "private_stream_cash_sync_failed",
        "private_stream_reservation_sync_failed",
        "private_stream_valuation_sync_failed",
    }
)
# step() may record a completed pong and then one data frame before returning.
FRAME_AND_END_BYTES = 110_000 + 4 * 1024


class SupervisorError(ValueError):
    """Fixed local reason codes; no callback exceptions or credentials."""


class SupervisorPolicy(Contract):
    sync_interval_seconds: int = Field(default=15, strict=True, ge=1, le=25)
    sync_timeout_seconds: int = Field(default=35, strict=True, ge=1, le=60)
    sync_retry_seconds: int = Field(default=1, strict=True, ge=1, le=10)
    max_sync_retries: int = Field(default=5, strict=True, ge=0, le=20)
    max_connection_seconds: int = Field(default=28_800, strict=True, ge=60, le=86_400)
    rotation_margin_records: int = Field(default=32, strict=True, ge=3, le=256)
    rotation_margin_bytes: int = Field(
        default=256_000, strict=True, ge=FRAME_AND_END_BYTES, le=1_000_000
    )
    rotation_margin_events: int = Field(default=50, strict=True, ge=1, le=100)
    join_timeout_seconds: float = Field(default=2.0, ge=0, le=10)


class PrivateStreamSupervisor:
    """Construction/status never connect. start()/run() are explicit authorization.

    The supplied factory owns explicit credentials and builds a fresh Receiver
    using the given journal and shared stream limiter. Collection is a trusted
    callback, normally a bounded AccountReader. No thread is killed or replaced
    on a deadline: observations close and OS ownership stays until it exits.

    Only planned rollover reconnects automatically. Transport, storage, posting,
    deadline and exhausted-fence-retry failures persist a stop. A new instance
    cannot restart a stale RUNNING/STOPPED control without explicit recovery.
    """

    def __init__(
        self,
        control,
        journal,
        cash_book,
        limiter,
        receiver_factory,
        collect,
        *,
        collect_orders,
        policy=None,
        monotonic=time.monotonic,
        **sync_options,
    ):
        if not isinstance(control, StreamControl) or not isinstance(journal, SegmentedEventJournal):
            raise SupervisorError("explicit_supervisor_stores_required")
        if not isinstance(cash_book, ExecutionCashBook) or not isinstance(
            limiter, PrivateStreamLimiter
        ):
            raise SupervisorError("explicit_supervisor_dependencies_required")
        if not all(callable(f) for f in (receiver_factory, collect, collect_orders)):
            raise SupervisorError("explicit_supervisor_callbacks_required")
        allowed = {
            "collect_reservations",
            "reservation_book",
            "collect_quote",
            "valuation_policy",
            "valuation_book",
            "incremental_cash",
        }
        if sync_options.keys() - allowed:
            raise SupervisorError("invalid_supervisor_sync_options")
        self.control, self.journal, self.cash_book = control, journal, cash_book
        self.limiter, self._factory, self._collect = limiter, receiver_factory, collect
        self.policy = SupervisorPolicy.model_validate((policy or SupervisorPolicy()).model_dump())
        self._options = {"collect_orders": collect_orders, "cash_book": cash_book, **sync_options}
        self._mono, self._last_mono = monotonic, None
        self._lock = threading.RLock()
        self._lease = self._owner = self._receiver = None
        self._worker = None
        self._results = queue.Queue(maxsize=1)
        self._worker_started = self._connection_started = self._next_sync = self._last_launch = None
        self._connection = self._retries = 0
        self._started = self._running = self._closed = False
        self._reason = "not_started"
        self._last_result = None
        self._receive_paused = False
        control.check_binding(journal, cash_book)
        if journal.capacity()["max_records"] < 6:
            raise SupervisorError("supervisor_journal_capacity_too_small")

    def _now(self):
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
            raise SupervisorError("clock_invalid") from None
        self._last_mono = now
        return now

    def _open(self):
        stream = self._factory(self.journal, self.limiter)
        if not isinstance(stream, PrivateStreamReceiver):
            raise SupervisorError("invalid_supervisor_receiver_factory")
        self._receiver = stream  # Cleanup owns even a factory that violates the binding.
        if stream.journal is not self.journal or stream.limiter is not self.limiter:
            raise SupervisorError("supervisor_receiver_binding_mismatch")
        if (
            stream.status()["stream_started"]
            or stream.status()["account"]["journal_session_started"]
        ):
            raise SupervisorError("fresh_supervisor_receiver_required")
        stream.start(expected_head=self.journal.head())
        self._connection += 1
        self._connection_started = self._now()
        self._next_sync = self._connection_started
        self._last_result = None  # Never carry an old connection's REST proof.
        self._retries = 0

    def start(self, *, expected_revision, expected_head):
        with self._lock:
            if self._started or self._closed:
                raise SupervisorError("new_supervisor_required")
            # Refusals before claiming ownership do not poison a READY control.
            self.control.check_binding(self.journal, self.cash_book)
            lease = self.control.ownership()
            lease.__enter__()
            self._lease = lease
            try:
                state = self.control.begin(
                    self.journal, expected_revision=expected_revision, expected_head=expected_head
                )
            except BaseException:
                self._release()
                raise
            self._owner = state["owner"]
            self._started = True
            try:
                self._now()
                if self.journal.inspect()["records"]:
                    self.journal = self.journal.rotate(expected_head=expected_head)
                    self.control.update(self._owner, journal=self.journal)
                self._open()
                self._running = True
                self._reason = "running"
                self._launch(self._now())
            except BaseException as error:
                self._abort("startup_failed")
                if not isinstance(error, Exception):
                    raise
                raise SupervisorError("startup_failed") from None

    def _launch(self, now):
        if self._worker is not None:
            raise SupervisorError("supervisor_worker_already_running")
        stream, connection = self._receiver, self._connection
        self._worker_started = self._last_launch = now

        def collect():
            # Hold a reference to this supervisor until completion, including its
            # OS owner when cleanup has timed out. No daemon hides a live worker.
            try:
                result = stream.resync(self._collect, **self._options)
                outcome = ("ok", result)
            except BaseException as error:
                retry = isinstance(error, SyncError) and str(error) in RETRYABLE
                outcome = ("retry" if retry else "failed", None)
            try:
                finished = self._mono()
            except Exception:
                finished = None
            self._results.put_nowait((stream, connection, finished, outcome))

        self._worker = threading.Thread(target=collect, name="trading-private-rest", daemon=False)
        try:
            self._worker.start()
        except Exception:
            if self._worker.ident is None:
                self._worker = None  # A failed OS thread creation has nothing to join.
            raise

    def _drain(self, now):
        if self._worker is None:
            return
        if self._worker.is_alive():
            if now - self._worker_started >= self.policy.sync_timeout_seconds:
                raise SupervisorError("sync_deadline")
            return
        self._worker.join()
        self._worker = None
        try:
            stream, connection, finished, outcome = self._results.get_nowait()
        except queue.Empty:
            raise SupervisorError("sync_failed") from None
        if stream is not self._receiver or connection != self._connection:
            raise SupervisorError("sync_failed")
        if (
            type(finished) not in {int, float}
            or not math.isfinite(finished)
            or finished < self._worker_started
        ):
            raise SupervisorError("clock_invalid")
        if finished - self._worker_started >= self.policy.sync_timeout_seconds:
            raise SupervisorError("sync_deadline")
        kind, result = outcome
        if kind == "failed":
            raise SupervisorError("sync_failed")
        if kind == "ok":
            current = stream._capture.check_live()
            if current["phase"] == "DISCONNECTED":
                raise SupervisorError("sync_failed")
            if current["epoch"] != result.epoch or current["revision"] != result.revision:
                kind = "retry"  # A later notification/expiry invalidated this queued result.
        if kind == "retry":
            self._retries += 1
            self.control.update(self._owner, retry=True)
            if self._retries > self.policy.max_sync_retries:
                raise SupervisorError("sync_failed")
            self._last_result = None
            self._next_sync = now + self.policy.sync_retry_seconds
        else:
            if any(m != "balance_change_unverified" for m in result.mismatches):
                raise SupervisorError("sync_failed")
            inventory = stream._capture.compare_account_inventory(
                self.cash_book, expected_revision=result.revision
            )
            if (
                inventory["halted"]
                or not inventory["balance"]["balance_match"]
                or (
                    inventory["positions"] is not None
                    and not inventory["positions"]["position_match"]
                )
            ):
                raise SupervisorError("sync_failed")
            for diagnostic, match in (
                (result.position_reservations, "reservation_match"),
                (result.account_valuation, "diagnostics_match"),
            ):
                if diagnostic is not None and (
                    diagnostic["head"] != inventory["head"]
                    or not diagnostic[match]
                    or diagnostic.get("halted", False)
                ):
                    raise SupervisorError("sync_failed")
            prior_head = (
                result.execution_cash["head"]
                if result.execution_cash is not None
                else result.booked_cash_head
            )
            if prior_head is not None and prior_head != inventory["head"]:
                raise SupervisorError("sync_failed")
            self._retries = 0
            self.control.update(self._owner, success=True)
            self._last_result = result.model_copy(update={"account_inventory": inventory})
            self._next_sync = now + self.policy.sync_interval_seconds

    def step(self):
        with self._lock:
            if not self._running:
                raise SupervisorError("supervisor_not_running")
            failure = "stream_failed"
            try:
                now = self._now()
                failure = "sync_failed"
                self._drain(now)
                budget = self.journal.capacity()
                account = self._receiver._capture.check_live()
                if account["phase"] == "DISCONNECTED":
                    raise SupervisorError("stream_failed")
                if (
                    self._worker is None
                    and self._last_result is not None
                    and account["received_sequence"] > self._last_result.received_sequence
                    and now - self._last_launch >= self.policy.sync_retry_seconds
                ):
                    self._next_sync = min(self._next_sync, now)
                record_margin = min(
                    self.policy.rotation_margin_records, max(3, budget["max_records"] // 4)
                )
                event_margin = min(
                    self.policy.rotation_margin_events, max(1, account["max_session_events"] // 4)
                )
                due = (
                    budget["records_remaining"] <= record_margin
                    or budget["bytes_remaining"] <= self.policy.rotation_margin_bytes
                    or account["received_sequence"] >= account["max_session_events"] - event_margin
                    or now - self._connection_started >= self.policy.max_connection_seconds
                )
                hard = (
                    budget["records_remaining"] < 5
                    or budget["bytes_remaining"] < FRAME_AND_END_BYTES
                    or account["received_sequence"] >= account["max_session_events"] - 1
                )
                due = due or hard
                self._receive_paused = due and hard
                if due and self._worker is None:
                    failure = "rollover_failed"
                    if self._receiver.rollover_ready(self.cash_book):
                        head = self._receiver.close_for_rollover(self.cash_book)
                        self.journal = self.journal.rotate(expected_head=head)
                        self.control.update(self._owner, journal=self.journal)
                        self._open()
                        self._receive_paused = False
                        self._launch(self._now())
                        return False
                    if now >= self._next_sync:
                        self._launch(now)
                if self._worker is None and now >= self._next_sync:
                    self._launch(now)
                failure = "stream_failed"
                if self._receive_paused:
                    self._receiver.maintenance(record_heartbeat=False)
                    return False
                return self._receiver.step()
            except BaseException as error:
                reason = str(error) if isinstance(error, SupervisorError) else failure
                if reason == "stream_failed" and self._receiver is not None:
                    # resync closes the capture before its worker queues the
                    # outcome. Preserve that known failure while the worker is
                    # still alive; a transport-only failure remains distinct.
                    try:
                        if self._receiver.status()["stream_reason"] in SYNC_FAILURES:
                            reason = "sync_failed"
                    except Exception:
                        pass
                self._abort(reason)
                if not isinstance(error, Exception):
                    raise
                raise SupervisorError(reason) from None

    def _release(self):
        if self._lease is not None:
            lease, self._lease = self._lease, None
            lease.__exit__(None, None, None)

    def _abort(self, reason):
        self._running = False
        self._reason = reason
        if self._owner is not None and self._lease is not None:
            try:
                self.control.finish(self._owner, self.journal, reason=reason, cleanup_unknown=True)
            except Exception:
                pass  # RUNNING remains a durable refusal even if storage is unavailable.
        if self._receiver is not None:
            try:
                self._receiver.close()
            except Exception:
                pass
        if self._worker is not None:
            try:
                self._worker.join(timeout=self.policy.join_timeout_seconds)
            except RuntimeError:
                # An interruption during Thread.start() can leave startup uncertain.
                # Keep ownership until a later close can join, or the process exits.
                return
            if self._worker.is_alive():
                return  # Keep the OS owner. close() can finish joining later.
            self._worker = None
        unknown = True
        if self._receiver is not None:
            try:
                state = self._receiver.status()
                unknown = state["token_cleanup_unknown"] or state["stream_cleanup_failed"]
            except Exception:
                pass
        if self._owner is not None and self._lease is not None:
            try:
                self.control.finish(
                    self._owner, self.journal, reason=reason, cleanup_unknown=unknown
                )
            except Exception:
                pass
        self._closed = True
        self._release()

    def close(self):
        with self._lock:
            if self._closed:
                return
            if not self._running:
                self._abort(self._reason if self._started else "closed")
                if self._worker is not None:
                    raise SupervisorError("worker_not_joined")
                return
            try:
                if self._worker is not None:
                    self._worker.join(timeout=self.policy.join_timeout_seconds)
                    if self._worker.is_alive():
                        raise SupervisorError("worker_not_joined")
                    self._drain(self._now())
                self._receiver.close_for_rollover(self.cash_book)
                self.control.finish(self._owner, self.journal)
                self._running = False
                self._closed = True
                self._reason = "closed"
                self._release()
            except BaseException as error:
                reason = str(error) if isinstance(error, SupervisorError) else "stream_failed"
                self._abort(reason)
                if not isinstance(error, Exception):
                    raise
                raise SupervisorError(reason) from None

    def run(self, stop_event, *, expected_revision, expected_head):
        self.start(expected_revision=expected_revision, expected_head=expected_head)
        try:
            while not stop_event.is_set():
                self.step()
                stop_event.wait(0.02)  # Also pace final-collection pauses without busy-spinning.
        finally:
            self.close()

    def status(self):
        with self._lock:
            return {
                "started": self._started,
                "running": self._running,
                "closed": self._closed,
                "reason": self._reason,
                "connection": self._connection,
                "rest_worker_alive": self._worker is not None and self._worker.is_alive(),
                "owner_retained": self._lease is not None,
                "receive_paused": self._receive_paused,
                "consecutive_sync_retries": self._retries,
                "control": self.control.snapshot(),
                "stream": self._receiver.status() if self._receiver is not None else None,
                "complete": False,
                "live_enabled": False,
            }
