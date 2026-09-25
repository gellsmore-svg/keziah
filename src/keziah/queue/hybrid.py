"""Hybrid mode. SQLite stays authoritative. RAM only orders ready work."""

from __future__ import annotations

import threading
from typing import Any

from keziah.config import SchedulerSettings, Settings
from keziah.queue.common import candidate_from_job
from keziah.queue.ready import ReadyIndex
from keziah.queue.sqlite import SQLiteBackend
from keziah.scheduler import PickerState
from keziah.types import QUEUED, Batch, Job


class HybridBackend:
    """Delegates every durable transition to SQLite.

    ``claim_next`` chooses from the ready index, then claims that exact row.
    A failed claim drops the hint. ``reconcile`` rebuilds the index from SQLite
    so a missed update or an external writer cannot leave the two views apart
    for long.
    """

    def __init__(self, db: SQLiteBackend, settings: Settings) -> None:
        self.db = db
        self.settings = settings
        self.index = ReadyIndex()
        self._lock = threading.Lock()
        self._reconciled_at = 0.0
        self.reconcile()

    def reconcile(self) -> int:
        with self._lock:
            candidates = self.db.ready_candidates()
            self.index.rebuild(candidates)
            self._reconciled_at = self.db.clock.monotonic()
            return len(candidates)

    def maybe_reconcile(self) -> None:
        interval = self.settings.queue.reconcile_interval_s
        if interval <= 0:
            return
        if self.db.clock.monotonic() - self._reconciled_at >= interval:
            self.reconcile()

    def enqueue(self, job: Job) -> Job:
        stored = self.db.enqueue(job)
        if stored.state == QUEUED:
            with self._lock:
                self.index.add(candidate_from_job(stored))
        return stored

    def begin_batch(self, batch: Batch) -> Batch:
        return self.db.begin_batch(batch)

    def stage_jobs(self, batch_id: str, jobs: list[Job]) -> int:
        return self.db.stage_jobs(batch_id, jobs)

    def activate_batch(self, batch_id: str, **kwargs: Any) -> Batch:
        stored = self.db.activate_batch(batch_id, **kwargs)
        # Activation can make thousands of rows runnable at once. Rebuilding the
        # hint from SQLite is simpler than threading each id back through, and
        # SQLite remains the authority if the rebuild is stale for a moment.
        with self._lock:
            self.index.rebuild(self.db.ready_candidates())
            self._reconciled_at = self.db.clock.monotonic()
        return stored

    def abort_batch(self, batch_id: str) -> None:
        self.db.abort_batch(batch_id)

    def claim_next(
        self,
        model: str,
        picker: PickerState,
        now_iso: str,
        now_epoch: float,
        owner: str,
        lease_until: str,
        settings: SchedulerSettings,
    ) -> tuple[Job | None, PickerState]:
        self.maybe_reconcile()
        base = picker
        for _ in range(64):
            with self._lock:
                choice, proposed = self.index.propose(model, base, now_epoch, settings)
                if choice is None:
                    return None, base
                job_id = choice.job_id
            job = self.db.claim_by_id(job_id, owner, lease_until, now_iso)
            with self._lock:
                self.index.remove(job_id)
            if job is not None:
                return job, proposed
        return None, base

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
    ) -> list[Job]:
        claimed = self.db.claim_compatible(
            model=model,
            questions_hash=questions_hash,
            exclude=exclude,
            limit=limit,
            now_iso=now_iso,
            owner=owner,
            lease_until=lease_until,
        )
        with self._lock:
            for job in claimed:
                self.index.remove(job.job_id)
        return claimed

    def maintenance(self, now_iso: str) -> Any:
        report = self.db.maintenance(now_iso)
        with self._lock:
            for job_id in report.finished:
                self.index.remove(job_id)
            for job_id in report.requeued:
                job = self.db.get_job(job_id)
                if job is not None and job.state == QUEUED:
                    self.index.add(candidate_from_job(job))
                else:
                    self.index.remove(job_id)
        return report

    def cancel(self, job_id: str, now_iso: str) -> Job:
        job = self.db.cancel(job_id, now_iso)
        if job.state != QUEUED:
            with self._lock:
                self.index.remove(job_id)
        return job

    def cancel_batch(self, batch_id: str, now_iso: str) -> int:
        changed = self.db.cancel_batch(batch_id, now_iso)
        self.reconcile()
        return changed

    def requeue(self, job_id: str, now_iso: str) -> Job:
        job = self.db.requeue(job_id, now_iso)
        if job.state == QUEUED:
            with self._lock:
                self.index.add(candidate_from_job(job))
        return job

    def __getattr__(self, name: str) -> Any:
        return getattr(self.db, name)
