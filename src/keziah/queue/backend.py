"""Queue backend contract.

Application code talks to this surface. SQLite details stay inside the disk
and hybrid implementations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from keziah.config import SchedulerSettings
from keziah.scheduler import PickerState
from keziah.types import Batch, Job


@dataclass(slots=True)
class Limits:
    max_queued_jobs: int = 100_000
    max_batch_size: int = 200_000
    max_active_jobs_per_client: int = 100_000


@dataclass(slots=True)
class Maintenance:
    requeued: list[str] = field(default_factory=list)
    finished: list[str] = field(default_factory=list)
    recovered: int = 0
    promoted: int = 0
    dead_lettered: int = 0
    expired_cancelled: int = 0
    deadlines: int = 0


class QueueBackend(Protocol):
    def enqueue(self, job: Job) -> Job: ...

    def begin_batch(self, batch: Batch) -> Batch: ...

    def stage_jobs(self, batch_id: str, jobs: list[Job]) -> int: ...

    def activate_batch(self, batch_id: str) -> Batch: ...

    def abort_batch(self, batch_id: str) -> None: ...

    def claim_next(
        self,
        model: str,
        picker: PickerState,
        now_iso: str,
        now_epoch: float,
        owner: str,
        lease_until: str,
        settings: SchedulerSettings,
    ) -> tuple[Job | None, PickerState]: ...

    def claim_compatible(
        self,
        *,
        model: str,
        questions_hash: str,
        exclude: set[str],
        limit: int,
        now_iso: str,
        owner: str,
        lease_until: str,
    ) -> list[Job]: ...

    def extend_lease(self, job_id: str, owner: str, lease_until: str, now_iso: str) -> bool: ...

    def mark_running(self, job_id: str, owner: str, lease_until: str, now_iso: str) -> bool: ...

    def complete(
        self,
        job_id: str,
        owner: str,
        *,
        result: dict[str, Any],
        model_version: str | None,
        queue_ms: int | None,
        execution_ms: int | None,
        total_ms: int | None,
        now_iso: str,
    ) -> bool: ...

    def fail(
        self,
        job_id: str,
        owner: str,
        *,
        error: dict[str, Any],
        dead_letter: bool,
        now_iso: str,
    ) -> bool: ...

    def retry(
        self,
        job_id: str,
        owner: str,
        *,
        error: dict[str, Any],
        not_before: str,
        resolved_model: str,
        fallback: dict[str, Any] | None,
        now_iso: str,
    ) -> bool: ...

    def finish_cancel(self, job_id: str, owner: str, now_iso: str) -> bool: ...

    def cancel(self, job_id: str, now_iso: str) -> Job: ...

    def cancel_batch(self, batch_id: str, now_iso: str) -> int: ...

    def maintenance(self, now_iso: str) -> Maintenance: ...

    def get_job(self, job_id: str) -> Job | None: ...

    def get_jobs(
        self,
        *,
        state: str | None = None,
        batch_id: str | None = None,
        client_id: str | None = None,
        model: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]: ...

    def get_batch(self, batch_id: str) -> Batch | None: ...

    def get_batch_results(self, batch_id: str, *, offset: int = 0, limit: int = 100) -> list[Job]: ...

    def get_events(self, job_id: str, *, limit: int = 100) -> list[dict[str, Any]]: ...

    def get_stats(self) -> dict[str, Any]: ...

    def cleanup(self, *, success_before: str, failure_before: str, event_before: str) -> dict[str, int]: ...

    def requeue(self, job_id: str, now_iso: str) -> Job: ...

    def queue_depth(self) -> int: ...

    def pending_count(self) -> int: ...

    def next_wake_s(self, now_iso: str) -> float | None: ...

    def close(self) -> None: ...
