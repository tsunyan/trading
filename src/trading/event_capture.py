"""Record-before-delivery adapter. No receiver, credentials, or automatic restart."""

import threading
import time
from datetime import UTC, datetime

from trading.account_sync import AccountSyncMonitor, SyncError
from trading.event_journal import EventJournal, JournalError


class JournaledEventCapture:
    """Own one process-local monitor and one explicitly started journal epoch.

    Private record/ack internals must not be bypassed by callers. Resync results
    remain diagnostic and are deliberately NOT persisted as reusable account proof.
    """

    def __init__(
        self,
        journal: EventJournal,
        *,
        clock=lambda: datetime.now(UTC),
        monotonic_ns=time.monotonic_ns,
    ):
        self._journal = journal
        self._clock, self._mono = clock, monotonic_ns
        self._monitor = AccountSyncMonitor(clock=clock, monotonic=lambda: monotonic_ns() / 1e9)
        self._session = self._monitor_session = None
        self._failed = False
        self._ended = False
        self._lock = threading.RLock()

    def _poison(self):
        if self._failed:
            return
        self._failed = True
        if self._monitor_session is not None:
            try:
                self._monitor.disconnect(self._monitor_session)
            except SyncError:
                pass

    def _stamp(self):
        try:
            return self._journal._stamp(self._clock(), self._mono())
        except Exception:
            self._poison()
            raise JournalError("invalid_capture_clock") from None

    def _ready(self):
        if self._failed or self._ended or self._session is None:
            raise JournalError("capture_not_ready")
        try:
            return self._journal.current(self._session)
        except JournalError:
            self._poison()
            raise

    def start_session(self, *, expected_head):
        """Explicit compare-and-swap epoch boundary, never automatic takeover."""
        with self._lock:
            if self._failed or self._session is not None:
                raise JournalError("new_capture_object_required")
            at, mono = self._stamp()
            self._session = self._journal.start_session(
                expected_head=expected_head, at=at, monotonic_ns=mono
            )
            try:
                self._monitor_session = self._monitor.start_session()
            except BaseException:
                self._poison()
                raise

    def _deliver(self, kind, *, sequence=None, payload=None):
        with self._lock:
            self._ready()
            at, mono = self._stamp()
            try:
                record_id = self._journal.record(
                    self._session,
                    kind,
                    at=at,
                    monotonic_ns=mono,
                    sequence=sequence,
                    payload=payload,
                )
            except BaseException:
                self._poison()
                raise
            try:
                if kind == "EVENT":
                    self._monitor.ingest(self._monitor_session, sequence, payload)
                elif kind == "HEARTBEAT":
                    self._monitor.heartbeat(self._monitor_session)
                else:
                    self._monitor.disconnect(self._monitor_session)
                    self._ended = True
                if kind != "END":
                    self._journal.acknowledge(self._session, record_id)
            except BaseException as error:
                self._poison()
                try:
                    self._journal.fail_delivery(self._session)
                except JournalError:
                    pass  # A pending capture is already a durable uncertainty marker.
                if not isinstance(error, Exception):
                    raise
                raise JournalError("capture_delivery_failed") from None

    def ingest(self, receive_sequence, payload: bytes):
        self._deliver("EVENT", sequence=receive_sequence, payload=payload)

    def heartbeat(self):
        self._deliver("HEARTBEAT")

    def disconnect(self):
        self._deliver("END")

    def status(self):
        with self._lock:
            view = None
            try:
                if self._session is None or self._ended:
                    view = self._journal.inspect()
                else:
                    view = self._ready()
            except JournalError:
                self._poison()
            result = self._monitor.status()
            result.update(
                capture_failed=self._failed, journal_session_started=self._session is not None
            )
            result["journal_epoch"] = view["epoch"] if view else None
            result["journal_unacknowledged_records"] = (
                view["unacknowledged_records"] if view else None
            )
            result["blockers"] = list(dict.fromkeys((*result["blockers"], *self._blockers(view))))
            return result

    @staticmethod
    def _blockers(view):
        return (
            "journal_is_not_broker_history_proof",
            *(("journal_epoch_gap_not_repaired",) if view and view["epoch"] > 1 else ()),
            *(
                ("journal_delivery_outcome_unknown",)
                if view and view["unacknowledged_records"]
                else ()
            ),
        )

    def resync(self, collect):
        with self._lock:
            self._ready()
            session = self._session
        # Never hold the capture lock across REST: arriving events must be committed
        # and delivered while collection is in progress, invalidating that attempt.
        result = self._monitor.resync(self._monitor_session, collect)
        with self._lock:
            view = self._ready()
            current = self._monitor.status()
            if session != self._session or current["revision"] != result.revision:
                raise SyncError("capture_changed_during_collection")
            # Journal presence never proves continuity or authenticates the account.
            return result.model_copy(
                update={
                    "blockers": tuple(
                        dict.fromkeys(
                            (
                                *result.blockers,
                                *self._blockers(view),
                            )
                        )
                    )
                }
            )
