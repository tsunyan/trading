"""Explicit read-only CLI composition; no strategy promotion or order transport."""

import argparse
import hashlib
import json
import os
import re
import signal
import threading
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from trading.account_reader import AccountReader
from trading.broker_contracts import Contract
from trading.credential_store import CredentialVault
from trading.event_capture import JournaledEventCapture
from trading.execution_cash_book import ExecutionCashBatch, ExecutionCashBook
from trading.execution_reconciliation import reconcile_executions
from trading.known_orders import KnownOrder, KnownOrderCatalog
from trading.live_journal import LiveOrderJournal
from trading.live_monitor_target import LiveMonitorBinding
from trading.live_order_catalog import LiveOrderCatalogSource
from trading.post_control import PersistentPostLimiter, PostBusyError
from trading.private_read import PrivateReadClient
from trading.private_stream import PrivateStreamReceiver
from trading.private_stream_token import PrivateStreamLimiter, PrivateTokenClient
from trading.private_supervisor import PrivateStreamSupervisor, SupervisorError, SupervisorPolicy
from trading.read_control import PersistentReadLimiter
from trading.segmented_journal import SegmentedEventJournal
from trading.stream_control import StreamControl, StreamControlError
from trading.wire_validation import unique_object

MAX_PLAN_BYTES = 262_144


class PrivateSyncError(ValueError):
    """Fixed reason codes only, including untrusted configuration failures."""


class SyncPlan(Contract):
    version: Literal[1] = 1
    scope: str = Field(pattern=r"^[A-Za-z0-9_-]{1,64}$")
    read_control_directory: Path
    read_control_instance: str = Field(pattern=r"^[a-f0-9]{32}$")
    cash_directory: Path
    credential_reference: str = Field(pattern=r"^[a-f0-9]{32}$")
    known_orders: tuple[KnownOrder, ...] = Field(default=(), max_length=1000)
    max_records: int = Field(default=1024, strict=True, ge=6, le=20_000)
    read_timeout_seconds: int = Field(default=5, strict=True, ge=1, le=5)
    collection_limit_seconds: int = Field(default=30, strict=True, ge=1, le=30)
    supervisor: SupervisorPolicy = Field(default_factory=SupervisorPolicy)

    @model_validator(mode="after")
    def coherent(self):
        ids = [o.order_id for o in self.known_orders]
        clients = [o.intent.client_id for o in self.known_orders]
        if len(set(ids)) != len(ids) or len(set(clients)) != len(clients):
            raise ValueError("duplicate known order")
        if self.collection_limit_seconds >= self.supervisor.sync_timeout_seconds:
            raise ValueError("collector deadline must precede supervisor deadline")
        return self


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(plan):
    return hashlib.sha256(_canonical(plan.model_dump(mode="json")).encode()).hexdigest()


def _catalog_manifest(plan, control_instance, catalog_instance):
    manifest = {
        "version": 2,
        "plan": plan.model_dump(mode="json"),
        "sha256": _digest(plan),
        "control_instance": control_instance,
        "catalog_instance": catalog_instance,
    }
    manifest["manifest_sha256"] = hashlib.sha256(_canonical(manifest).encode()).hexdigest()
    return manifest


def _read_json(path):
    try:
        with Path(path).open("rb") as source:
            body = source.read(MAX_PLAN_BYTES + 1)
        if len(body) > MAX_PLAN_BYTES:
            raise ValueError
        return json.loads(body, object_pairs_hook=unique_object)
    except Exception:
        raise PrivateSyncError("invalid_sync_plan") from None


def load_plan(path):
    path = Path(path).resolve()
    try:
        plan = SyncPlan.model_validate(_read_json(path))
        return SyncPlan.model_validate(
            {
                **plan.model_dump(),
                **{
                    key: (path.parent / getattr(plan, key)).resolve()
                    for key in ("read_control_directory", "cash_directory")
                },
            }
        )
    except Exception:
        raise PrivateSyncError("invalid_sync_plan") from None


class _ReaderOwner:
    """One REST worker; deferred close never blocks its caller on a hung callback."""

    def __init__(self, client, plan, *, clock, monotonic, order_lookup=None, order_verify=None):
        self.client, self.plan = client, plan
        self.clock, self.monotonic = clock, monotonic
        self._lock = threading.Lock()
        self._active = 0
        self._closing = False
        self._deadline = None
        self._orders = {o.order_id: o.intent for o in plan.known_orders}
        self._lookup = order_lookup
        self._verify = order_verify

    def get(self, request):
        if self._deadline is None or self.monotonic() >= self._deadline:
            raise PrivateSyncError("sync_collection_deadline")
        result = self.client.get(request)
        if self.monotonic() >= self._deadline:
            raise PrivateSyncError("sync_collection_deadline")
        return result

    def _call(self, collect):
        with self._lock:
            if self._closing:
                raise PrivateSyncError("sync_reader_closed")
            self._active += 1
        try:
            return collect()
        finally:
            with self._lock:
                self._active -= 1
                close = self._closing and self._active == 0
            if close:
                self.client.close()

    def orders(self, ids, *, standalone=False):
        if standalone:
            self._deadline = self.monotonic() + self.plan.collection_limit_seconds
        # Validate every requested ID before the first order HTTP request.
        orders = self._lookup(ids) if self._lookup is not None else self._orders
        if any(identity not in orders for identity in ids):
            raise PrivateSyncError("sync_order_intent_missing")
        reader = AccountReader(self, clock=self.clock)
        reports = self._call(
            lambda: tuple(reader.collect_order(orders[identity], identity) for identity in ids)
        )
        if self._verify is not None:
            self._verify(reports)
            if self.monotonic() >= self._deadline:
                raise PrivateSyncError("sync_collection_deadline")
        return reports

    def reservations(self):
        """Discover active IDs, then read declared intents before the final account."""
        self._deadline = self.monotonic() + self.plan.collection_limit_seconds

        def collect():
            discovery = AccountReader(self, clock=self.clock).collect_account()
            ids = tuple(sorted(o.order_id for o in discovery.active_orders))
            if len(ids) > 1000:
                raise PrivateSyncError("sync_reservation_collection_capacity")
            # orders validates every ID before the first individual order GET.
            return self.orders(ids)

        return self._call(collect)

    def account(self, *, continuation=False):
        # Reservation discovery, final account and execution orders share one budget.
        if not continuation:
            self._deadline = self.monotonic() + self.plan.collection_limit_seconds
        return self._call(lambda: AccountReader(self, clock=self.clock).collect_account())

    def close(self):
        with self._lock:
            if self._closing:
                return
            self._closing = True
            close = self._active == 0
        if close:
            self.client.close()


class PrivateSyncWorkspace:
    """Reopen/status/recovery never load keys. Production dependencies are explicit.

    Low-level transports, clocks, vault and connector arguments are trusted test
    seams only. They cannot be selected from the saved plan or CLI arguments.
    """

    def __init__(self, directory, *, clock=lambda: datetime.now(UTC), monotonic=time.monotonic):
        self.directory = Path(directory).resolve()
        self.clock, self.monotonic = clock, monotonic
        try:
            manifest = _read_json(self.directory / "sync-plan.json")
            base = {"plan", "sha256", "control_instance"}
            self._catalog_bound = set(manifest) != base
            if self._catalog_bound:
                if (
                    set(manifest) != base | {"version", "catalog_instance", "manifest_sha256"}
                    or type(manifest["version"]) is not int
                    or manifest["version"] != 2
                    or manifest["manifest_sha256"]
                    != hashlib.sha256(
                        _canonical(
                            {k: v for k, v in manifest.items() if k != "manifest_sha256"}
                        ).encode()
                    ).hexdigest()
                ):
                    raise ValueError
            elif set(manifest) != base:
                raise ValueError
            self._manifest_hash = hashlib.sha256(_canonical(manifest).encode()).hexdigest()
            self.plan = SyncPlan.model_validate(manifest["plan"])
            if (
                _digest(self.plan) != manifest["sha256"]
                or not self.plan.read_control_directory.is_absolute()
                or not self.plan.cash_directory.is_absolute()
            ):
                raise ValueError
            self.plan_sha256 = manifest["sha256"]
            self.journal = SegmentedEventJournal(self.directory / "journal", self.plan.scope)
            self.book = ExecutionCashBook(self.plan.cash_directory, self.plan.scope)
            self.control = StreamControl(self.directory / "control")
            if self.control.snapshot()["instance"] != manifest["control_instance"]:
                raise ValueError
            self.control.check_binding(self.journal, self.book)
            self.catalog = self._catalog() if (self.directory / "catalog").exists() else None
            if self._catalog_bound and (
                self.catalog is None
                or self.catalog.snapshot()["instance"] != manifest["catalog_instance"]
            ):
                raise ValueError
        except Exception:
            raise PrivateSyncError("sync_workspace_invalid") from None

    @classmethod
    def create(cls, directory, plan, **clocks):
        directory = Path(directory).resolve()
        if directory.exists():
            raise FileExistsError("sync_directory_already_exists")
        plan = SyncPlan.model_validate(plan.model_dump())
        if not all(p.is_absolute() for p in (plan.read_control_directory, plan.cash_directory)):
            raise PrivateSyncError("absolute_sync_paths_required")
        reads = PersistentReadLimiter(plan.read_control_directory, plan.scope)
        state = reads.status()
        book = ExecutionCashBook(plan.cash_directory, plan.scope)
        if (
            state["instance_id"] != plan.read_control_instance
            or state["blocked"]
            or book.snapshot()["halted"]
        ):
            raise PrivateSyncError("sync_dependencies_blocked")
        if reads.stream_binding() is not None:
            raise PrivateSyncError("sync_supervisor_already_bound")
        directory.mkdir(parents=True, exist_ok=False)
        # A failed initialization may retain its partial stores for inspection.
        # Never recursively erase or silently adopt an existing runtime directory.
        journal = SegmentedEventJournal.create(
            directory / "journal", plan.scope, max_records=plan.max_records
        )
        control = StreamControl.create(directory / "control", journal, book)
        catalog = KnownOrderCatalog.create(
            directory / "catalog",
            plan.scope,
            control.snapshot()["instance"],
            _digest(plan),
            plan.known_orders,
            clock=clocks.get("clock"),
        )
        manifest = _catalog_manifest(
            plan, control.snapshot()["instance"], catalog.snapshot()["instance"]
        )
        body = _canonical(manifest).encode()
        if len(body) > MAX_PLAN_BYTES:
            raise PrivateSyncError("sync_plan_too_large")
        with (directory / "sync-plan.json").open("xb") as output:
            output.write(body)
            output.flush()
            os.fsync(output.fileno())
        reads.bind_stream(manifest["control_instance"])
        return cls(directory, **clocks)

    def _reads(self, **clocks):
        reads = PersistentReadLimiter(self.plan.read_control_directory, self.plan.scope, **clocks)
        if reads.status()["instance_id"] != self.plan.read_control_instance:
            raise PrivateSyncError("sync_read_control_changed")
        if reads.stream_binding() != self.control.snapshot()["instance"]:
            raise PrivateSyncError("sync_supervisor_binding_mismatch")
        return reads

    def _catalog(self):
        return KnownOrderCatalog(
            self.directory / "catalog",
            self.plan.scope,
            self.control.snapshot()["instance"],
            self.plan_sha256,
            self.plan.known_orders,
            clock=self.clock,
        )

    def confirm_read_binding_history(
        self,
        *,
        expected_plan_sha256,
        expected_revision,
        expected_head,
        legacy_binding_confirmed=False,
    ):
        """Explicitly mark the same legacy association; never create, rebind or release a stop."""
        self._check_plan(expected_plan_sha256)
        if legacy_binding_confirmed is not True:
            raise PrivateSyncError("sync_legacy_binding_confirmation_required")
        if type(expected_revision) is not int or expected_revision < 0:
            raise PrivateSyncError("invalid_sync_revision")
        with self.control.ownership():
            self._check_plan(expected_plan_sha256)
            state = self.control.snapshot()
            if state["revision"] != expected_revision or self.journal.head() != expected_head:
                raise PrivateSyncError("sync_checkpoint_changed")
            if state["phase"] not in {"READY", "STOPPED"}:
                raise PrivateSyncError("sync_legacy_binding_requires_idle_owner")
            reads = PersistentReadLimiter(self.plan.read_control_directory, self.plan.scope)
            if reads.status()["instance_id"] != self.plan.read_control_instance:
                raise PrivateSyncError("sync_read_control_changed")
            reads.bind_stream(state["instance"], legacy_binding_confirmed=True)
        return self.status()

    def initialize_catalog(self, *, expected_plan_sha256, expected_revision, expected_head):
        self._check_plan(expected_plan_sha256)
        if type(expected_revision) is not int or expected_revision < 0:
            raise PrivateSyncError("invalid_sync_revision")
        self._reads()
        with self.control.ownership():
            state = self.control.snapshot()
            if state["revision"] != expected_revision or self.journal.head() != expected_head:
                raise PrivateSyncError("sync_checkpoint_changed")
            if self._catalog_bound:
                raise PrivateSyncError("sync_catalog_already_initialized")
            if self.catalog is None:
                self.catalog = KnownOrderCatalog.create(
                    self.directory / "catalog",
                    self.plan.scope,
                    state["instance"],
                    self.plan_sha256,
                    self.plan.known_orders,
                    clock=self.clock,
                )
            view = self.catalog.snapshot()
            if view["records"] != len(self.plan.known_orders):
                raise PrivateSyncError("sync_unbound_catalog_requires_review")
            current = _read_json(self.directory / "sync-plan.json")
            if hashlib.sha256(_canonical(current).encode()).hexdigest() != self._manifest_hash:
                raise PrivateSyncError("sync_manifest_changed")
            manifest = _catalog_manifest(self.plan, state["instance"], view["instance"])
            body = _canonical(manifest).encode()
            if len(body) > MAX_PLAN_BYTES:
                raise PrivateSyncError("sync_plan_too_large")
            temporary = self.directory / ("sync-plan-" + uuid.uuid4().hex + ".tmp")
            try:
                with temporary.open("xb") as output:
                    output.write(body)
                    output.flush()
                    os.fsync(output.fileno())
                os.replace(temporary, self.directory / "sync-plan.json")
            finally:
                temporary.unlink(missing_ok=True)
            self._manifest_hash = hashlib.sha256(_canonical(manifest).encode()).hexdigest()
            self._catalog_bound = True
        return self.status()

    def register_order(
        self,
        order,
        *,
        expected_plan_sha256,
        expected_catalog_head,
        source_ref,
        intent_confirmed=False,
    ):
        self._check_plan(expected_plan_sha256)
        self._reads()
        if self.catalog is None or not self._catalog_bound:
            raise PrivateSyncError("sync_catalog_initialization_required")
        return self.catalog.register(
            order,
            source_ref=source_ref,
            expected_head=expected_catalog_head,
            intent_confirmed=intent_confirmed,
        )

    def _order_lookup(self, reads, posts):
        if posts is not None and posts.execution_binding() is not None:
            if self.catalog is None or not self._catalog_bound:
                raise PrivateSyncError("sync_live_catalog_required")
            source = LiveOrderCatalogSource(posts, self.catalog, clock=self.clock)
            return source.lookup, source.verify_reports, source.halt
        return (self.catalog.lookup if self.catalog is not None else None), None, None

    def register_live_orders(self, *, expected_plan_sha256, expected_catalog_head):
        """Local refresh from the permanently bound execution journal; no new declarations."""
        self._check_plan(expected_plan_sha256)
        if self.catalog is None or not self._catalog_bound:
            raise PrivateSyncError("sync_live_catalog_required")
        reads = self._reads()
        posts = self._posts(reads)
        if posts is None:
            raise PrivateSyncError("sync_live_binding_required")
        if expected_catalog_head is None:
            raise PrivateSyncError("sync_catalog_checkpoint_required")
        return LiveOrderCatalogSource(posts, self.catalog, clock=self.clock).refresh(
            expected_head=expected_catalog_head
        )

    def status(self):
        self._check_manifest()
        catalog = self.catalog.snapshot() if self.catalog is not None else None
        reads = self._reads()
        posts = self._posts(reads)
        post, post_owner = None, None
        live = None
        if posts is not None:
            post = posts.snapshot()
            if post["phase"] == "IN_FLIGHT":
                try:
                    with posts._ownership():
                        post, post_owner = posts.snapshot(), False
                except PostBusyError:
                    post, post_owner = posts.snapshot(), True
            binding = posts.execution_binding()
            if binding is not None:
                target = LiveMonitorBinding(
                    scope=self.plan.scope,
                    sync_instance=self.control.snapshot()["instance"],
                    read_instance=self.plan.read_control_instance,
                    post_instance=post["instance"],
                    live_instance=binding["instance"],
                    read_directory=str(reads.path.parent),
                    post_directory=str(posts.path.parent),
                    live_directory=binding["path"],
                )
                live = {
                    "binding": target.model_dump(),
                    "status": LiveOrderJournal(
                        binding["path"], posts, clock=self.clock
                    ).monitoring_status(),
                }
        return {
            "plan_sha256": self.plan_sha256,
            "control": self.control.snapshot(),
            "journal": self.journal.inspect(),
            "reads": reads.status(),
            "posts": post,
            "post_owner_present": post_owner,
            "live": live,
            "cash": self.book.snapshot(),
            "catalog": catalog,
            "catalog_bound": self._catalog_bound,
            "known_order_count": catalog["records"]
            if catalog is not None
            else len(self.plan.known_orders),
            "complete": False,
            "live_enabled": False,
        }

    def _posts(self, reads, *, sleep=time.sleep):
        binding = reads.post_binding()
        if binding is None:
            return None
        return PersistentPostLimiter(
            Path(binding["path"]),
            reads,
            wall_ns=lambda: int(self.clock().timestamp() * 1e9),
            monotonic=self.monotonic,
            sleep=sleep,
        )

    def _check_plan(self, expected):
        self._check_manifest()
        if expected != self.plan_sha256 or _digest(self.plan) != self.plan_sha256:
            raise PrivateSyncError("sync_plan_changed")

    def _check_manifest(self):
        current = _read_json(self.directory / "sync-plan.json")
        if hashlib.sha256(_canonical(current).encode()).hexdigest() != self._manifest_hash:
            raise PrivateSyncError("sync_manifest_changed")

    def recover(self, *, expected_plan_sha256, **checks):
        self._check_plan(expected_plan_sha256)
        self._reads()  # Association is checked even when GETs remain stopped.
        self.journal = self.control.recover(self.journal, self.book, **checks)
        return self.status()

    def reconcile_stopped(
        self,
        *,
        expected_plan_sha256,
        expected_revision,
        expected_head,
        expected_reason,
        read_only_confirmed=False,
        vault=None,
        read_transport=None,
        read_clocks=None,
    ):
        """Explicit GET proof and idempotent booking; keep the stopped state intact."""
        self._check_plan(expected_plan_sha256)
        if read_only_confirmed is not True:
            raise PrivateSyncError("sync_read_permission_confirmation_required")
        if type(expected_revision) is not int or expected_revision < 0:
            raise PrivateSyncError("invalid_sync_revision")
        if self.catalog is not None and not self._catalog_bound:
            raise PrivateSyncError("sync_catalog_initialization_required")
        with self.control.ownership():
            self._check_plan(expected_plan_sha256)
            self.control.check_binding(self.journal, self.book)
            before = self.control.snapshot()
            if before["phase"] == "READY":
                raise PrivateSyncError("sync_reconciliation_stop_required")
            if before["revision"] != expected_revision or before["reason"] != expected_reason:
                raise PrivateSyncError("sync_checkpoint_changed")
            events = self.journal.recovery_events(expected_head=expected_head)
            if self.book.snapshot()["halted"]:
                raise PrivateSyncError("sync_dependencies_blocked")
            ids = tuple(sorted({event.execution_order_id for event in events}))
            if len(ids) > 1000:
                raise PrivateSyncError("sync_reconciliation_capacity")
            reads = self._reads(**(read_clocks or {}))
            if reads.status()["blocked"]:
                raise PrivateSyncError("sync_dependencies_blocked")
            lookup, verify, stop_live = self._order_lookup(reads, self._posts(reads))
            # Verified live identities are refreshed before credential access or GETs.
            if lookup is not None:
                lookup(ids)
            elif any(i not in {o.order_id for o in self.plan.known_orders} for i in ids):
                raise PrivateSyncError("sync_order_intent_missing")
            booking = None
            if events:
                vault = vault if vault is not None else CredentialVault()
                credentials = vault.load(reads, self.plan.credential_reference)
                client = PrivateReadClient(
                    credentials.api_key,
                    credentials.secret,
                    limiter=reads,
                    transport=read_transport,
                    clock=self.clock,
                    monotonic=self.monotonic,
                    timeout_seconds=self.plan.read_timeout_seconds,
                )
                owner = _ReaderOwner(
                    client,
                    self.plan,
                    clock=self.clock,
                    monotonic=self.monotonic,
                    order_lookup=lookup,
                    order_verify=verify,
                )
                try:
                    deadline = self.monotonic() + self.plan.collection_limit_seconds
                    reports = owner.orders(ids, standalone=True)
                    by_order = {r.evidence.order_id: r for r in reports}
                    unique = {}
                    # Receipt times can differ. Check every saved variant against
                    # REST before deduplicating execution IDs for cash posting.
                    for event in events:
                        if self.monotonic() >= deadline:
                            raise PrivateSyncError("sync_collection_deadline")
                        match = reconcile_executions(
                            (event,), (by_order[event.execution_order_id],)
                        )
                        if match.unverified_execution_ids or match.mismatches:
                            raise PrivateSyncError("sync_reconciliation_mismatch")
                        unique.setdefault(event.entity_id, event)
                    batch = ExecutionCashBatch(events=tuple(unique.values()), reports=reports)
                    self._check_plan(expected_plan_sha256)
                    if self.control.snapshot() != before:
                        raise PrivateSyncError("sync_checkpoint_changed")
                    with self.journal.guard_recovery_events(expected_head=expected_head):
                        if self.monotonic() >= deadline:
                            raise PrivateSyncError("sync_collection_deadline")
                        booking = self.book.apply(batch)
                except BaseException:
                    if stop_live is not None:
                        stop_live()
                    raise
                finally:
                    try:
                        owner.close()
                    except BaseException:
                        if stop_live is not None:
                            stop_live()
                        raise
            return {
                **self.status(),
                "reconciled_notice_count": len(events),
                "booking": booking,
                "recovery_required": True,
            }

    def run(
        self,
        stop_event,
        *,
        duration_seconds,
        expected_plan_sha256,
        expected_revision,
        expected_head,
        read_only_confirmed=False,
        vault=None,
        read_transport=None,
        token_transport=None,
        connector=None,
        read_clocks=None,
        stream_sleep=time.sleep,
    ):
        self._check_plan(expected_plan_sha256)
        if read_only_confirmed is not True:
            raise PrivateSyncError("sync_read_permission_confirmation_required")
        if type(duration_seconds) is not int or not 1 <= duration_seconds <= 604_800:
            raise PrivateSyncError("sync_duration_invalid")
        if self.catalog is not None and not self._catalog_bound:
            raise PrivateSyncError("sync_catalog_initialization_required")
        if stop_event.is_set():
            return self.status()
        reads = self._reads(**(read_clocks or {}))
        if reads.status()["blocked"]:
            raise PrivateSyncError("sync_dependencies_blocked")
        vault = vault if vault is not None else CredentialVault()
        limiter = self._posts(reads, sleep=stream_sleep)
        if limiter is not None and limiter.snapshot()["blocked"]:
            raise PrivateSyncError("sync_dependencies_blocked")
        if limiter is None:
            limiter = PrivateStreamLimiter(monotonic=self.monotonic, sleep=stream_sleep)
        lookup, verify, stop_live = self._order_lookup(
            reads, limiter if isinstance(limiter, PersistentPostLimiter) else None
        )
        owner = None

        def factory(journal, shared):
            nonlocal owner
            if stop_event.is_set():
                raise PrivateSyncError("sync_start_cancelled")
            if lookup is not None:
                lookup(())  # Check/export the bound source before native credential access.
            credentials = vault.load(reads, self.plan.credential_reference)
            if stop_event.is_set():
                raise PrivateSyncError("sync_start_cancelled")
            if owner is None:
                client = PrivateReadClient(
                    credentials.api_key,
                    credentials.secret,
                    limiter=reads,
                    transport=read_transport,
                    clock=self.clock,
                    monotonic=self.monotonic,
                    timeout_seconds=self.plan.read_timeout_seconds,
                )
                owner = _ReaderOwner(
                    client,
                    self.plan,
                    clock=self.clock,
                    monotonic=self.monotonic,
                    order_lookup=lookup,
                    order_verify=verify,
                )
            capture = JournaledEventCapture(
                journal, clock=self.clock, monotonic_ns=lambda: int(self.monotonic() * 1e9)
            )
            tokens = PrivateTokenClient(
                credentials.api_key,
                credentials.secret,
                limiter=shared,
                transport=token_transport,
                clock=self.clock,
                monotonic=self.monotonic,
            )
            options = {} if connector is None else {"connector": connector}
            try:
                return PrivateStreamReceiver(tokens, capture, monotonic=self.monotonic, **options)
            except BaseException:
                tokens.close()
                raise

        position_aware = bool(self.book.snapshot().get("position_accounting_applied"))
        reservation_options = (
            {"collect_reservations": lambda: owner.reservations(), "reservation_book": self.book}
            if position_aware
            else {}
        )
        runner = PrivateStreamSupervisor(
            self.control,
            self.journal,
            self.book,
            limiter,
            factory,
            lambda: owner.account(continuation=position_aware),
            collect_orders=lambda ids: owner.orders(ids),
            policy=self.plan.supervisor,
            monotonic=self.monotonic,
            **reservation_options,
        )
        try:
            runner.start(expected_revision=expected_revision, expected_head=expected_head)
            deadline = self.monotonic() + duration_seconds
            while not stop_event.is_set() and self.monotonic() < deadline:
                runner.step()
                stop_event.wait(0.02)
        finally:
            try:
                runner.close()
            finally:
                self.journal = runner.journal
                if owner is not None:
                    try:
                        owner.close()
                    except BaseException:
                        if stop_live is not None:
                            stop_live()
                        raise
                if stop_live is not None:
                    try:
                        if self.control.snapshot()["phase"] == "STOPPED":
                            stop_live()
                    except (ValueError, OSError):
                        stop_live()
        return self.status()


class SyncParser(argparse.ArgumentParser):
    def error(self, message):
        # Unknown arguments and validation text can contain accidentally pasted keys.
        self.exit(2, "Invalid private sync arguments.\n")


def main(argv=None):
    parser = SyncParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=(
            "init",
            "status",
            "run",
            "continue",
            "recover",
            "init-orders",
            "register-order",
            "register-live-orders",
            "reconcile-stopped",
            "confirm-read-binding",
        ),
    )
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--expected-plan-sha256")
    parser.add_argument("--expected-revision", type=int)
    parser.add_argument("--expected-head")
    parser.add_argument("--expected-reason")
    parser.add_argument("--duration-seconds", type=int)
    parser.add_argument("--read-only-confirmed", action="store_true")
    parser.add_argument("--acknowledge-token-uncertainty", action="store_true")
    parser.add_argument("--order-file", type=Path)
    parser.add_argument("--source-ref")
    parser.add_argument("--intent-confirmed", action="store_true")
    parser.add_argument("--expected-catalog-head")
    parser.add_argument("--legacy-binding-confirmed", action="store_true")
    args = parser.parse_args(argv)
    stop_event = threading.Event()
    previous = {}
    try:
        if args.command == "init":
            if args.plan is None:
                raise PrivateSyncError("sync_plan_required")
            workspace = PrivateSyncWorkspace.create(args.directory, load_plan(args.plan))
            result = workspace.status()
        else:
            if args.plan is not None:
                raise PrivateSyncError("saved_sync_plan_required")
            workspace = PrivateSyncWorkspace(args.directory)
            if args.command == "confirm-read-binding":
                result = workspace.confirm_read_binding_history(
                    expected_plan_sha256=args.expected_plan_sha256,
                    expected_revision=args.expected_revision,
                    expected_head=args.expected_head,
                    legacy_binding_confirmed=args.legacy_binding_confirmed,
                )
            elif args.command == "init-orders":
                result = workspace.initialize_catalog(
                    expected_plan_sha256=args.expected_plan_sha256,
                    expected_revision=args.expected_revision,
                    expected_head=args.expected_head,
                )
            elif args.command == "register-order":
                try:
                    order = KnownOrder.model_validate(_read_json(args.order_file))
                except Exception:
                    raise PrivateSyncError("invalid_known_order_file") from None
                result = workspace.register_order(
                    order,
                    expected_plan_sha256=args.expected_plan_sha256,
                    expected_catalog_head=args.expected_catalog_head,
                    source_ref=args.source_ref,
                    intent_confirmed=args.intent_confirmed,
                )
            elif args.command == "register-live-orders":
                result = workspace.register_live_orders(
                    expected_plan_sha256=args.expected_plan_sha256,
                    expected_catalog_head=args.expected_catalog_head,
                )
            elif args.command == "reconcile-stopped":
                result = workspace.reconcile_stopped(
                    expected_plan_sha256=args.expected_plan_sha256,
                    expected_revision=args.expected_revision,
                    expected_head=args.expected_head,
                    expected_reason=args.expected_reason,
                    read_only_confirmed=args.read_only_confirmed,
                )
            elif args.command in {"run", "continue"}:
                if threading.current_thread() is threading.main_thread():
                    for sig in (signal.SIGINT, signal.SIGTERM):
                        previous[sig] = signal.signal(sig, lambda *_: stop_event.set())
                if args.command == "continue":
                    # Only a clean READY end continues on its own saved checkpoint. STOPPED
                    # and an abandoned RUNNING still require the explicit recovery path.
                    if any(
                        value is not None
                        for value in (
                            args.expected_plan_sha256,
                            args.expected_revision,
                            args.expected_head,
                        )
                    ):
                        raise PrivateSyncError("continue_uses_saved_checkpoint")
                    control = workspace.control.snapshot()
                    if control["phase"] != "READY":
                        raise PrivateSyncError("continue_requires_ready")
                    args.expected_plan_sha256 = workspace.plan_sha256
                    args.expected_revision = control["revision"]
                    args.expected_head = workspace.journal.head()
                result = workspace.run(
                    stop_event,
                    duration_seconds=args.duration_seconds,
                    expected_plan_sha256=args.expected_plan_sha256,
                    expected_revision=args.expected_revision,
                    expected_head=args.expected_head,
                    read_only_confirmed=args.read_only_confirmed,
                )
            elif args.command == "recover":
                result = workspace.recover(
                    expected_plan_sha256=args.expected_plan_sha256,
                    expected_revision=args.expected_revision,
                    expected_head=args.expected_head,
                    expected_reason=args.expected_reason,
                    acknowledge_token_uncertainty=args.acknowledge_token_uncertainty,
                )
            else:
                result = workspace.status()
        print(json.dumps({"ok": True, **result}, default=str))
    except Exception as error:
        # These types carry fixed local codes only; anything else stays generic.
        reason = str(error)
        known = isinstance(error, (PrivateSyncError, StreamControlError, SupervisorError))
        suffix = f" reason={reason}" if known and re.fullmatch(r"[a-z0-9_]{1,64}", reason) else ""
        parser.exit(2, f"Private sync failed; inspect local control state.{suffix}\n")
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    main()
