"""Persistable original live target for a local watchdog; no keys, HTTP or recovery."""

from pathlib import Path

from pydantic import Field, model_validator

from trading.broker_contracts import Contract
from trading.live_journal import MONITOR_STOP_REASONS, LiveOrderJournal
from trading.post_control import PersistentPostLimiter
from trading.read_control import PersistentReadLimiter

LIVE_STOP_REASONS = MONITOR_STOP_REASONS


class LiveMonitorError(ValueError):
    """Fixed local reason codes only."""


class LiveMonitorBinding(Contract):
    scope: str = Field(pattern=r"^[A-Za-z0-9_-]{1,64}$")
    sync_instance: str = Field(pattern=r"^[a-f0-9]{32}$")
    read_instance: str = Field(pattern=r"^[a-f0-9]{32}$")
    post_instance: str = Field(pattern=r"^[a-f0-9]{32}$")
    live_instance: str = Field(pattern=r"^[a-f0-9]{32}$")
    read_directory: str = Field(min_length=1, max_length=2048)
    post_directory: str = Field(min_length=1, max_length=2048)
    live_directory: str = Field(min_length=1, max_length=2048)

    @model_validator(mode="after")
    def canonical_paths(self):
        for path in (self.read_directory, self.post_directory, self.live_directory):
            if not Path(path).is_absolute() or str(Path(path).resolve()) != path:
                raise ValueError("invalid_live_monitor_path")
        return self


class LiveMonitorTarget:
    def __init__(self, binding, *, clock, monotonic):
        self.binding = LiveMonitorBinding.model_validate(binding.model_dump())
        self.reads = PersistentReadLimiter(binding.read_directory, binding.scope)
        if (
            self.reads.status()["instance_id"] != binding.read_instance
            or self.reads.stream_binding() != binding.sync_instance
            or self.reads.post_binding()
            != {"instance": binding.post_instance, "path": binding.post_directory}
        ):
            raise LiveMonitorError("live_monitor_binding_changed")
        self.posts = PersistentPostLimiter(
            binding.post_directory,
            self.reads,
            wall_ns=lambda: int(clock().timestamp() * 1e9),
            monotonic=monotonic,
        )
        if self.posts.snapshot()[
            "instance"
        ] != binding.post_instance or self.posts.execution_binding() != {
            "instance": binding.live_instance,
            "path": binding.live_directory,
        }:
            raise LiveMonitorError("live_monitor_binding_changed")
        self.journal = LiveOrderJournal(binding.live_directory, self.posts, clock=clock)
        if self.journal.monitoring_status()["instance"] != binding.live_instance:
            raise LiveMonitorError("live_monitor_binding_changed")

    def stop(self, monitor_instance, reasons):
        reasons = frozenset(reasons)
        if not reasons or reasons - LIVE_STOP_REASONS:
            raise LiveMonitorError("invalid_live_monitor_stop_reason")
        return self.journal.halt_for_monitor(monitor_instance, reasons)
