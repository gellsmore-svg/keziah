"""Open the queue backend selected by configuration."""

from __future__ import annotations

from keziah.clock import Clock
from keziah.config import Settings
from keziah.queue.backend import Limits
from keziah.queue.hybrid import HybridBackend
from keziah.queue.memory import MemoryBackend
from keziah.queue.sqlite import SQLiteBackend


def open_backend(settings: Settings, clock: Clock) -> MemoryBackend | SQLiteBackend | HybridBackend:
    limits = Limits(
        max_queued_jobs=settings.scheduler.max_queued_jobs,
        max_batch_size=settings.scheduler.max_batch_size,
        max_active_jobs_per_client=settings.scheduler.max_active_jobs_per_client,
    )
    mode = settings.queue.mode
    if mode == "memory":
        return MemoryBackend(clock, limits)
    db = SQLiteBackend(
        settings.queue.sqlite_path,
        clock,
        busy_timeout_ms=settings.queue.busy_timeout_ms,
        limits=limits,
    )
    if mode == "disk":
        return db
    return HybridBackend(db, settings)
