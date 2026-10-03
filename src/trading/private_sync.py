"""Explicit read-only CLI composition; no strategy promotion or order transport."""

import argparse
import hashlib
import json
import os
import signal
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from trading.account_reader import AccountReader
from trading.broker_contracts import Contract, OrderIntent
from trading.credential_store import CredentialVault
from trading.event_capture import JournaledEventCapture
from trading.execution_cash_book import ExecutionCashBook
from trading.private_read import PrivateReadClient
from trading.private_stream import PrivateStreamReceiver
from trading.private_stream_token import PrivateStreamLimiter, PrivateTokenClient
from trading.private_supervisor import PrivateStreamSupervisor, SupervisorPolicy
from trading.read_control import PersistentReadLimiter
from trading.segmented_journal import SegmentedEventJournal
from trading.stream_control import StreamControl
from trading.wire_validation import unique_object

MAX_PLAN_BYTES = 262_144


class PrivateSyncError(ValueError):
    """Fixed reason codes only, including untrusted configuration failures."""


class KnownOrder(Contract):
    order_id: int = Field(strict=True, gt=0)
    intent: OrderIntent


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

    def __init__(self, client, plan, *, clock, monotonic):
        self.client, self.plan = client, plan
        self.clock, self.monotonic = clock, monotonic
        self._lock = threading.Lock()
        self._active = 0
        self._closing = False
        self._deadline = None
        self._orders = {o.order_id: o.intent for o in plan.known_orders}

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

    def orders(self, ids):
        # Validate every requested ID before the first order HTTP request.
        if any(identity not in self._orders for identity in ids):
            raise PrivateSyncError("sync_order_intent_missing")
        reader = AccountReader(self, clock=self.clock)
        return self._call(
            lambda: tuple(
                reader.collect_order(self._orders[identity], identity) for identity in ids
            )
        )

    def account(self):
        # Monitor collection starts with the account, followed by known orders.
        # Both callbacks share one budget; periodic attempts get a fresh budget.
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
            if set(manifest) != {"plan", "sha256", "control_instance"}:
                raise ValueError
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
        manifest = {
            "plan": plan.model_dump(mode="json"),
            "sha256": _digest(plan),
            "control_instance": control.snapshot()["instance"],
        }
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

    def status(self):
        return {
            "plan_sha256": self.plan_sha256,
            "control": self.control.snapshot(),
            "journal": self.journal.inspect(),
            "reads": self._reads().status(),
            "cash": self.book.snapshot(),
            "known_order_count": len(self.plan.known_orders),
            "complete": False,
            "live_enabled": False,
        }

    def _check_plan(self, expected):
        if expected != self.plan_sha256 or _digest(self.plan) != self.plan_sha256:
            raise PrivateSyncError("sync_plan_changed")

    def recover(self, *, expected_plan_sha256, **checks):
        self._check_plan(expected_plan_sha256)
        self._reads()  # Association is checked even when GETs remain stopped.
        self.journal = self.control.recover(self.journal, self.book, **checks)
        return self.status()

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
        if stop_event.is_set():
            return self.status()
        reads = self._reads(**(read_clocks or {}))
        if reads.status()["blocked"]:
            raise PrivateSyncError("sync_dependencies_blocked")
        vault = vault if vault is not None else CredentialVault()
        limiter = PrivateStreamLimiter(monotonic=self.monotonic, sleep=stream_sleep)
        owner = None

        def factory(journal, shared):
            nonlocal owner
            if stop_event.is_set():
                raise PrivateSyncError("sync_start_cancelled")
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
                owner = _ReaderOwner(client, self.plan, clock=self.clock, monotonic=self.monotonic)
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

        runner = PrivateStreamSupervisor(
            self.control,
            self.journal,
            self.book,
            limiter,
            factory,
            lambda: owner.account(),
            collect_orders=lambda ids: owner.orders(ids),
            policy=self.plan.supervisor,
            monotonic=self.monotonic,
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
                    owner.close()
        return self.status()


class SyncParser(argparse.ArgumentParser):
    def error(self, message):
        # Unknown arguments and validation text can contain accidentally pasted keys.
        self.exit(2, "Invalid private sync arguments.\n")


def main(argv=None):
    parser = SyncParser(description=__doc__)
    parser.add_argument("command", choices=("init", "status", "run", "recover"))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--expected-plan-sha256")
    parser.add_argument("--expected-revision", type=int)
    parser.add_argument("--expected-head")
    parser.add_argument("--expected-reason")
    parser.add_argument("--duration-seconds", type=int)
    parser.add_argument("--read-only-confirmed", action="store_true")
    parser.add_argument("--acknowledge-token-uncertainty", action="store_true")
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
            if args.command == "run":
                if threading.current_thread() is threading.main_thread():
                    for sig in (signal.SIGINT, signal.SIGTERM):
                        previous[sig] = signal.signal(sig, lambda *_: stop_event.set())
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
    except Exception:
        parser.exit(2, "Private sync failed; inspect local control state.\n")
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    main()
