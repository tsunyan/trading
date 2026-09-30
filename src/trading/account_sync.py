"""Bounded, process-local event/REST reconciliation diagnostics, never a trade gate."""

import math
import threading
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Literal

from pydantic import AwareDatetime, TypeAdapter

from trading.account_events import AccountEvent, EventError, parse_event
from trading.account_reader import ASSETS, ORDERS, POSITIONS, AccountReadReport
from trading.broker_contracts import Contract

TIME = TypeAdapter(AwareDatetime)
BLOCKERS = (
    *AccountReadReport.model_fields["blockers"].default,
    "broker_event_continuity_unproven",
    "asset_event_coverage_unproven",
)


class SyncError(ValueError):
    """Fixed local reason code only."""


class SyncAssessment(Contract):
    epoch: int
    revision: int
    received_sequence: int
    structural_match: bool
    mismatches: tuple[str, ...]
    unverified_execution_ids: tuple[int, ...]
    report: AccountReadReport
    blockers: tuple[str, ...]
    complete: Literal[False] = False
    live_enabled: Literal[False] = False


def _position(item):
    # MTM P&L, accumulated swap and timestamps are not structural equality.
    return (item.symbol, item.side, item.units, item.ordered_units, item.price)


def _order(item):
    # units means total size; never convert it to unfilled quantity.
    return (
        item.root_order_id,
        item.client_id,
        item.symbol,
        item.side,
        item.effect,
        item.kind,
        item.units,
        item.price,
        item.status,
    )


class AccountSyncMonitor:
    """Single-process monitor for an explicitly supplied event source and reader.

    Reconnect means a NEW observation baseline, not restoration of missing history.
    Nothing is persisted, and no API keys, network transport or order journal are used.
    The caller must feed every captured data frame with a consecutive LOCAL ordinal.
    """

    def __init__(
        self,
        *,
        clock=lambda: datetime.now(UTC),
        monotonic=time.monotonic,
        max_idle_seconds=75,
        max_collection_seconds=30,
        max_observation_seconds=30,
        max_session_events=1000,
    ):
        for value, upper in (
            (max_idle_seconds, 300),
            (max_collection_seconds, 300),
            (max_observation_seconds, 300),
            (max_session_events, 100_000),
        ):
            if type(value) is not int or not 1 <= value <= upper:
                raise ValueError("invalid_sync_limits")
        self._clock, self._monotonic = clock, monotonic
        self._idle_limit, self._collection_limit = max_idle_seconds, max_collection_seconds
        self._observation_limit = max_observation_seconds
        self._observed_at = None
        self._capacity = max_session_events
        self._lock = threading.RLock()
        self._session = None
        self._epoch = self._revision = self._sequence = 0
        self._last_wall = self._last_mono = self._last_seen = None
        self._connected = self._observed = False
        self._gap = False
        self._reason = "not_connected"
        self._ticket = None
        self._baseline = None
        self._positions: dict[int, AccountEvent] = {}
        self._orders: dict[int, AccountEvent] = {}
        self._executions: dict[int, str] = {}

    def _invalidate(self, reason, *, disconnect=False):
        self._revision += 1
        self._observed = False
        self._reason = reason
        if disconnect:
            self._connected = False
            self._gap = True

    def _now(self):
        try:
            wall = TIME.validate_python(self._clock())
            mono = self._monotonic()
            if (
                type(mono) not in {int, float}
                or not math.isfinite(mono)
                or mono < 0
                or (self._last_wall is not None and wall < self._last_wall)
                or (self._last_mono is not None and mono < self._last_mono)
            ):
                raise ValueError
        except Exception:
            self._invalidate("sync_clock_invalid", disconnect=True)
            raise SyncError("sync_clock_invalid") from None
        self._last_wall, self._last_mono = wall, mono
        if self._connected and mono - self._last_seen > self._idle_limit:
            self._invalidate("stream_liveness_expired", disconnect=True)
        if (
            self._connected
            and self._observed
            and mono - self._observed_at > self._observation_limit
        ):
            self._invalidate("rest_observation_expired")
        return wall, mono

    def _current(self, session):
        if session != self._session or self._session is None:
            raise SyncError("stale_stream_session")
        wall, mono = self._now()
        if not self._connected:
            raise SyncError("stream_not_connected")
        return wall, mono

    def start_session(self):
        """Explicitly start a new capture epoch. This opens NO network connection."""
        with self._lock:
            wall, mono = self._now()
            if self._epoch:
                self._gap = True
            self._session = uuid.uuid4().hex
            self._epoch += 1
            self._sequence = 0
            self._last_seen = mono
            self._connected = True
            self._ticket = self._baseline = None
            self._positions.clear()
            self._orders.clear()
            self._executions.clear()
            self._invalidate("new_stream_baseline_required")
            return self._session

    def disconnect(self, session):
        with self._lock:
            if session != self._session or self._session is None:
                raise SyncError("stale_stream_session")
            self._invalidate("stream_disconnected", disconnect=True)

    def heartbeat(self, session):
        """Record an actually observed transport ping/pong; not an empty-event guess."""
        with self._lock:
            _, self._last_seen = self._current(session)
            # Heartbeats never repair gaps, clear pending changes, or re-enable a session.

    def ingest(self, session, receive_sequence: int, payload: bytes):
        with self._lock:
            now, mono = self._current(session)
            if type(receive_sequence) is not int or receive_sequence != self._sequence + 1:
                self._invalidate("local_receive_gap_or_reorder", disconnect=True)
                raise SyncError("local_receive_gap_or_reorder")
            if receive_sequence > self._capacity:
                self._invalidate("event_capacity_exceeded", disconnect=True)
                raise SyncError("event_capacity_exceeded")
            try:
                event = parse_event(payload, now)
            except EventError:
                self._invalidate("event_rejected", disconnect=True)
                raise SyncError("event_rejected") from None
            if event.channel == "executionEvents":
                prior = self._executions.get(event.entity_id)
                if prior is not None and prior != event.payload_sha256:
                    self._invalidate("execution_identity_conflict", disconnect=True)
                    raise SyncError("execution_identity_conflict")
                self._executions[event.entity_id] = event.payload_sha256
            elif event.channel == "positionEvents":
                self._positions[event.entity_id] = event
            else:
                self._orders[event.entity_id] = event
            self._sequence, self._last_seen = receive_sequence, mono
            # Even an identical duplicate arriving during collection invalidates it.
            # Never deduplicate positions/orders globally: A -> B -> A can be real.
            self._invalidate("event_received")

    def status(self):
        with self._lock:
            try:
                self._now()
            except SyncError:
                pass
            phase = (
                "DISCONNECTED"
                if not self._connected
                else "COLLECTING"
                if self._ticket
                else "OBSERVED_UNVERIFIED"
                if self._observed
                else "NEEDS_RESYNC"
            )
            return {
                "phase": phase,
                "reason": self._reason,
                "epoch": self._epoch,
                "revision": self._revision,
                "received_sequence": self._sequence,
                "resync_required": not self._observed,
                "history_gap_observed": self._gap,
                "pending_positions": len(self._positions),
                "pending_orders": len(self._orders),
                "unverified_executions": len(self._executions),
                "complete": False,
                "live_enabled": False,
                "blockers": list(self._blockers()),
            }

    def _blockers(self):
        return (
            *BLOCKERS,
            *(("history_gap_not_repaired",) if self._gap else ()),
            *(("execution_events_not_reconciled",) if self._executions else ()),
        )

    def _compare(self, report):
        problems = []
        for name, rows, pending, identify, project in (
            ("position", report.positions, self._positions, "position_id", _position),
            ("order", report.active_orders, self._orders, "order_id", _order),
        ):
            current = {getattr(item, identify): project(item) for item in rows}
            if len(current) != len(rows):
                raise SyncError("invalid_sync_report")
            for identity, event in sorted(pending.items()):
                item = event.position if name == "position" else event.order
                expected = None if event.removed else project(item)
                if current.get(identity) != expected:
                    problems.append(f"{name}_event_mismatch:{identity}")
            if self._baseline is not None:
                old_rows = (
                    self._baseline.positions if name == "position" else self._baseline.active_orders
                )
                previous = {getattr(item, identify): project(item) for item in old_rows}
                for identity in sorted(current.keys() | previous.keys()):
                    if current.get(identity) != previous.get(identity) and identity not in pending:
                        problems.append(f"{name}_change_without_event:{identity}")
        if self._baseline is not None and report.assets.balance != self._baseline.assets.balance:
            problems.append("balance_change_unverified")
        return tuple(problems)

    def resync(self, session, collect: Callable[[], AccountReadReport]) -> SyncAssessment:
        """Collect once outside the lock, then fence concurrent changes at acceptance.

        Supply AccountReader.collect_account (or an offline transcript replay). No
        retry, network creation, ledger mutation, stop reset, or trade permission.
        """
        with self._lock:
            started, start_mono = self._current(session)
            if self._ticket is not None:
                raise SyncError("resync_already_running")
            ticket = uuid.uuid4().hex
            self._ticket = ticket
            self._observed = False
            self._reason = "collecting"
            revision = self._revision
        try:
            try:
                report = collect()
                # Detach/revalidate even model_construct/model_copy inputs.
                if not isinstance(report, AccountReadReport):
                    raise ValueError
                report = AccountReadReport.model_validate(report.model_dump())
            except Exception:
                raise SyncError("sync_collection_failed") from None
            with self._lock:
                now, mono = self._current(session)
                if ticket != self._ticket or self._revision != revision:
                    raise SyncError("stream_changed_during_collection")
                if (
                    mono - start_mono > self._collection_limit
                    or (now - started).total_seconds() > self._collection_limit
                ):
                    raise SyncError("sync_collection_expired")
                observations = report.observations
                if (
                    not observations
                    or {o.path for o in observations} != {ASSETS, POSITIONS, ORDERS}
                    or any(
                        not started <= o.response_at <= o.received_at <= now for o in observations
                    )
                    or any(
                        b.received_at < a.received_at or b.response_at < a.response_at
                        for a, b in zip(observations, observations[1:], strict=False)
                    )
                ):
                    raise SyncError("stale_or_invalid_sync_report")
                mismatches = self._compare(report)
                result = SyncAssessment(
                    epoch=self._epoch,
                    revision=revision,
                    received_sequence=self._sequence,
                    structural_match=not mismatches,
                    mismatches=mismatches,
                    unverified_execution_ids=tuple(sorted(self._executions)),
                    report=report,
                    blockers=tuple(dict.fromkeys((*self._blockers(), *report.blockers))),
                )
                if not mismatches:
                    self._baseline = report
                    self._positions.clear()
                    self._orders.clear()
                    self._observed = True
                    self._observed_at = mono
                self._reason = "observed_unverified" if self._observed else "rest_event_mismatch"
                return result
        except BaseException as error:
            with self._lock:
                # An old completion must not overwrite a new session's result.
                if self._ticket == ticket and self._session == session:
                    if self._connected and self._revision == revision:
                        self._invalidate("sync_attempt_failed")
                    if not isinstance(error, Exception):
                        self._invalidate("sync_interrupted", disconnect=True)
            raise
        finally:
            with self._lock:
                if self._ticket == ticket:
                    self._ticket = None
