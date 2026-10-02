"""Record-before-delivery adapter. No receiver, credentials, or automatic restart."""

import threading
import time
from datetime import UTC, datetime

from trading.account_sync import AccountSyncMonitor, SyncError
from trading.event_journal import EventJournal, JournalError, _JournalBusy
from trading.execution_cash_book import ExecutionCashBook


class JournaledEventCapture:
    """Own one process-local monitor and one explicitly started journal epoch.

    Private record/ack internals must not be bypassed by callers. Resync results
    remain diagnostic and are never persisted as reusable account proof. Explicit
    cash integration saves only individual fills and their comparison evidence.
    Reservation integration returns non-persistent diagnostics for this revision.
    Valuation integration requires freshly collected quotes and a declared model.
    """

    def __init__(
        self,
        journal: EventJournal,
        *,
        clock=lambda: datetime.now(UTC),
        monotonic_ns=time.monotonic_ns,
        clock_skew_ms=0,
    ):
        self._journal = journal
        self._clock, self._mono = clock, monotonic_ns
        self._monitor = AccountSyncMonitor(
            clock=clock, monotonic=lambda: monotonic_ns() / 1e9, clock_skew_ms=clock_skew_ms
        )
        self._clock_skew_ms = clock_skew_ms
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

    def _check_ready(self):
        if self._failed or self._ended or self._session is None:
            raise JournalError("capture_not_ready")

    def _ready(self):
        self._check_ready()
        try:
            return self._journal.current(self._session)
        except _JournalBusy:
            raise  # Lock contention after retries; nothing was observed to be wrong.
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
                expected_head=expected_head,
                at=at,
                monotonic_ns=mono,
                clock_skew_ms=self._clock_skew_ms,
            )
            try:
                self._monitor_session = self._monitor.start_session()
            except BaseException:
                self._poison()
                raise

    def _deliver(self, kind, *, sequence=None, payload=None):
        with self._lock:
            # record() verifies the journal and session within its transaction.
            self._check_ready()
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
            view, busy = None, False
            try:
                if self._session is None or self._ended or self._failed:
                    view = self._journal.inspect()
                else:
                    view = self._ready()
            except _JournalBusy:
                busy = True  # A diagnostic read must not stop a healthy capture.
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
            result["blockers"] = list(
                dict.fromkeys(
                    (
                        *result["blockers"],
                        *self._blockers(view),
                        *(("journal_busy_state_unknown",) if busy else ()),
                    )
                )
            )
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

    def _check_cash_book(self, book):
        if not isinstance(book, ExecutionCashBook):
            raise SyncError("execution_cash_book_required")
        if book.scope != self._journal.scope:
            raise SyncError("execution_cash_scope_mismatch")

    def apply_execution_cash(self, book: ExecutionCashBook, *, expected_revision):
        """Explicitly post a current matched batch; never use a returned aggregate."""
        with self._lock:
            self._check_cash_book(book)
            self._ready()
            try:
                # Hold the journal's write reservation as well as the local
                # capture lock: another process cannot take over this epoch
                # between its check and the separate cash book commit.
                with self._journal.guard_session(self._session):
                    return self._monitor.apply_execution_cash(
                        self._monitor_session, book, expected_revision=expected_revision
                    )
            except BaseException:
                self._poison()
                raise

    def compare_position_reservations(self, book: ExecutionCashBook, *, expected_revision):
        with self._lock:
            self._check_cash_book(book)
            self._ready()
            try:
                with self._journal.guard_session(self._session):
                    return self._monitor.compare_position_reservations(
                        self._monitor_session, book, expected_revision=expected_revision
                    )
            except BaseException:
                self._poison()
                raise

    def compare_account_valuation(self, book: ExecutionCashBook, *, expected_revision):
        with self._lock:
            self._check_cash_book(book)
            self._ready()
            try:
                with self._journal.guard_session(self._session):
                    return self._monitor.compare_account_valuation(
                        self._monitor_session, book, expected_revision=expected_revision
                    )
            except BaseException:
                self._poison()
                raise

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
    ):
        with self._lock:
            if cash_book is not None:
                self._check_cash_book(cash_book)
                if collect_orders is None:
                    raise SyncError("execution_cash_requires_order_collection")
            if reservation_book is not None:
                self._check_cash_book(reservation_book)
                if collect_reservations is None:
                    raise SyncError("reservations_require_order_collection")
                if cash_book is not None and cash_book.path != reservation_book.path:
                    raise SyncError("reservation_cash_book_mismatch")
            if (collect_quote is None) != (valuation_policy is None):
                raise SyncError("valuation_requires_quote_and_policy")
            if valuation_book is not None:
                self._check_cash_book(valuation_book)
                if collect_quote is None:
                    raise SyncError("valuation_requires_quote_and_policy")
                if any(
                    other is not None and other.path != valuation_book.path
                    for other in (cash_book, reservation_book)
                ):
                    raise SyncError("valuation_cash_book_mismatch")
            self._ready()
            session = self._session
        # Never hold the capture lock across REST: arriving events must be committed
        # and delivered while collection is in progress, invalidating that attempt.
        result = self._monitor.resync(
            self._monitor_session,
            collect,
            collect_orders=collect_orders,
            collect_reservations=collect_reservations,
            collect_quote=collect_quote,
            valuation_policy=valuation_policy,
        )
        with self._lock:
            view = self._ready()
            current = self._monitor.status()
            if session != self._session or current["revision"] != result.revision:
                raise SyncError("capture_changed_during_collection")
            execution_cash = None
            reconciliation = result.execution_reconciliation
            if (
                cash_book is not None
                and reconciliation is not None
                and not reconciliation.unverified_execution_ids
                and not any(m != "balance_change_unverified" for m in result.mismatches)
            ):
                execution_cash = self.apply_execution_cash(
                    cash_book, expected_revision=result.revision
                )
            reservations = None
            if reservation_book is not None:
                reservations = self.compare_position_reservations(
                    reservation_book, expected_revision=result.revision
                )
                if execution_cash is not None and reservations["head"] != execution_cash["head"]:
                    self._poison()
                    raise SyncError("cash_book_changed_during_reservation_comparison")
            valuation = None
            if valuation_book is not None:
                valuation = self.compare_account_valuation(
                    valuation_book, expected_revision=result.revision
                )
                if any(
                    prior is not None and prior["head"] != valuation["head"]
                    for prior in (execution_cash, reservations)
                ):
                    self._poison()
                    raise SyncError("cash_book_changed_during_valuation_comparison")
            # Journal presence never proves continuity or authenticates the account.
            return result.model_copy(
                update={
                    "execution_cash": execution_cash,
                    "position_reservations": reservations,
                    "account_valuation": valuation,
                    "blockers": tuple(
                        dict.fromkeys(
                            (
                                *result.blockers,
                                *self._blockers(view),
                                *(reservations["blockers"] if reservations else ()),
                                *(valuation["blockers"] if valuation else ()),
                            )
                        )
                    ),
                }
            )
