"""Original sync/watchdog prerequisites for a registered live dispatch environment."""

import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from pydantic import Field, model_validator

from trading.broker_contracts import Contract
from trading.stream_control import StreamControlError


class LiveOperationsError(ValueError):
    """Fixed local reason codes; no credentials, account amounts or network."""


class OperationsBinding(Contract):
    sync_directory: str = Field(min_length=1, max_length=2048)
    sync_instance: str = Field(pattern=r"^[a-f0-9]{32}$")
    plan_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    monitor_instance: str = Field(pattern=r"^[a-f0-9]{32}$")
    max_sync_age_seconds: int = Field(default=120, strict=True, ge=1, le=86400)
    max_watchdog_age_seconds: int = Field(default=120, strict=True, ge=1, le=86400)

    @model_validator(mode="after")
    def canonical_directory(self):
        path = Path(self.sync_directory)
        if not path.is_absolute() or str(path.resolve()) != self.sync_directory:
            raise ValueError("invalid_operations_binding_path")
        return self


class LiveOperations:
    def __init__(self, binding, target, *, clock, monotonic):
        self.binding = OperationsBinding.model_validate(binding.model_dump())
        self.target, self.clock, self.monotonic = target, clock, monotonic

    @classmethod
    def capture(
        cls,
        directory,
        target,
        *,
        expected_plan_sha256,
        expected_monitor_instance,
        max_sync_age_seconds=120,
        max_watchdog_age_seconds=120,
        clock,
        monotonic,
    ):
        from trading.private_sync import PrivateSyncWorkspace

        workspace = PrivateSyncWorkspace(directory, clock=clock, monotonic=monotonic)
        binding = OperationsBinding(
            sync_directory=str(workspace.directory),
            sync_instance=workspace.control.snapshot()["instance"],
            plan_sha256=expected_plan_sha256,
            monitor_instance=expected_monitor_instance,
            max_sync_age_seconds=max_sync_age_seconds,
            max_watchdog_age_seconds=max_watchdog_age_seconds,
        )
        result = cls(
            binding,
            {**target, "sync_instance": binding.sync_instance},
            clock=clock,
            monotonic=monotonic,
        )
        result.enrollment_check()
        return binding

    def enrollment_check(self):
        # Lazy imports avoid a live journal -> sync -> live journal import cycle.
        from trading.private_operations import PrivateOperations
        from trading.private_sync import PrivateSyncWorkspace

        workspace = PrivateSyncWorkspace(
            self.binding.sync_directory, clock=self.clock, monotonic=self.monotonic
        )
        monitor = PrivateOperations(
            workspace.directory, clock=self.clock, monotonic=self.monotonic
        ).status(require_lock=True)
        reads = workspace._reads()
        if (
            not workspace._catalog_bound
            or workspace.plan_sha256 != self.binding.plan_sha256
            or workspace.control.snapshot()["instance"] != self.binding.sync_instance
            or str(reads.path.parent) != self.target["read_directory"]
            or reads.status()["instance_id"] != self.target["read_instance"]
            or reads.post_binding()
            != {"instance": self.target["post_instance"], "path": self.target["post_directory"]}
            or monitor["monitor_instance"] != self.binding.monitor_instance
            or monitor["control_instance"] != self.binding.sync_instance
            or monitor["plan_sha256"] != self.binding.plan_sha256
            or monitor["live_binding"] != self.target
            or self.binding.max_sync_age_seconds > monitor["stale_seconds"]
            or self.binding.max_watchdog_age_seconds > monitor["stale_seconds"]
        ):
            raise LiveOperationsError("live_operations_binding_changed")
        return workspace, monitor, reads

    @staticmethod
    def _fresh(stamp, now, limit):
        return (
            isinstance(stamp, datetime)
            and stamp.utcoffset() is not None
            and 0 <= (now - stamp).total_seconds() <= limit
        )

    @contextmanager
    def sync_idle(self):
        """Hold the sync's OS lease so no sync process runs, or can start, meanwhile.

        Restart and claim resolution advance the POST generation, which refuses every
        older handle, including a running sync's; that sync would stop and the watchdog
        would then stop the just-restarted orders. The sync must be idle (READY or
        STOPPED) and is started again, with new handles, afterwards.
        """
        from trading.private_sync import PrivateSyncWorkspace

        try:
            workspace = PrivateSyncWorkspace(
                self.binding.sync_directory, clock=self.clock, monotonic=self.monotonic
            )
            if workspace.control.snapshot()["instance"] != self.binding.sync_instance:
                raise LiveOperationsError("live_operations_binding_changed")
            lease = workspace.control.ownership()
            lease.__enter__()
        except LiveOperationsError:
            raise
        except StreamControlError as error:
            if str(error) == "stream_owner_busy":
                raise LiveOperationsError("live_sync_running") from None
            raise LiveOperationsError("live_operations_unavailable") from None
        except (ValueError, OSError, KeyError, TypeError, sqlite3.Error):
            raise LiveOperationsError("live_operations_unavailable") from None
        try:
            if workspace.control.snapshot()["phase"] == "RUNNING":
                # RUNNING without an owner is an interrupted sync: recover it first.
                raise LiveOperationsError("live_sync_recovery_required")
            yield
        finally:
            lease.__exit__(None, None, None)

    def require_healthy(self, now):
        from trading.private_operations import CONDITIONS

        try:
            workspace, monitor, reads = self.enrollment_check()
            checkpoint = monitor["watchdog_checkpoint"]
            allowed = {"private_live_entry_halted", "private_live_approval_invalid"}
            if (
                checkpoint is None
                or checkpoint["live_instance"] != self.target["live_instance"]
                or not self._fresh(
                    checkpoint["checked_at"], now, self.binding.max_watchdog_age_seconds
                )
                or (set(monitor["conditions"]) & CONDITIONS) - allowed
            ):
                raise LiveOperationsError("live_watchdog_unhealthy")
            # A free OS lease proves there is no current sync owner. Never start/recover it here.
            try:
                with workspace.control.ownership():
                    raise LiveOperationsError("live_sync_owner_missing")
            except StreamControlError as error:
                if str(error) != "stream_owner_busy":
                    raise
            control = workspace.control.snapshot()
            stamp = control["last_sync_wall_ns"]
            now_ns = int(now.timestamp() * 1e9)
            if (
                control["phase"] != "RUNNING"
                or control["generation"] != checkpoint["generation"]
                or control["sync_successes"] < checkpoint["sync_successes"]
                or control["revision"] < checkpoint["control_revision"]
                or checkpoint["sync_successes"] <= checkpoint["generation_start_successes"]
                or stamp is None
                or not 0 <= now_ns - stamp <= self.binding.max_sync_age_seconds * 1_000_000_000
            ):
                raise LiveOperationsError("live_sync_unhealthy")
            if (
                reads.status()["blocked"]
                or workspace.book.snapshot()["halted"]
                or workspace.journal.inspect()["rejected_frames"]
            ):
                raise LiveOperationsError("live_sync_dependencies_unhealthy")
            # Re-probe after the sample: a worker dying during these reads is refused too.
            try:
                with workspace.control.ownership():
                    raise LiveOperationsError("live_sync_owner_missing")
            except StreamControlError as error:
                if str(error) != "stream_owner_busy":
                    raise
            current = workspace.control.snapshot()
            if (
                current["phase"] != "RUNNING"
                or current["generation"] != control["generation"]
                or current["revision"] < control["revision"]
                or current["sync_successes"] < control["sync_successes"]
            ):
                raise LiveOperationsError("live_sync_checkpoint_changed")
        except LiveOperationsError:
            raise
        except (ValueError, OSError, KeyError, TypeError, sqlite3.Error):
            raise LiveOperationsError("live_operations_unavailable") from None
