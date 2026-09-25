"""Native in-memory queue. Nothing survives process exit."""

from __future__ import annotations

import threading
from typing import Any

from dataclasses import replace

from keziah.clock import Clock, isoformat
from keziah.config import SchedulerSettings
from keziah.errors import BackpressureError, ConflictError, NotFoundError, ValidationError
from keziah.queue.backend import Limits, Maintenance
from keziah.queue.common import (
    apply_claim,
    candidate_from_job,
    claimable,
    clone_job,
    empty_counts,
    event,
    fill_counts,
    owned_running,
)
from keziah.queue.ready import ReadyIndex
from keziah.scheduler import PickerState
from keziah.types import (
    BATCH_ABORTED,
    BATCH_ACTIVE,
    BATCH_STAGING,
    CANCELLED,
    DEAD_LETTER,
    FAILED,
    INCOMPLETE_STATES,
    LEASED,
    QUEUED,
    RETRY_WAIT,
    RUNNING,
    STAGED,
    SUCCEEDED,
    Batch,
    Job,
)


class MemoryBackend:
    def __init__(self, clock: Clock, limits: Limits | None = None) -> None:
        self.clock = clock
        self.limits = limits or Limits()
        self._lock = threading.Lock()
        self._jobs: dict[str, Job] = {}
        self._batches: dict[str, Batch] = {}
        self._idemp: dict[tuple[str, str], str] = {}
        self._batch_idemp: dict[tuple[str, str], str] = {}
        self._events: list[dict[str, Any]] = []
        self._event_id = 0
        self._index = ReadyIndex()
        self._closed = False

    def enqueue(self, job: Job) -> Job:
        with self._lock:
            self._check()
            existing = self._existing_job(job)
            if existing is not None:
                return existing
            self._reserve_capacity(job.client_id, 1)
            self._remember_job(clone_job(job))
            self._emit("SUBMITTED", job.created_at, job_id=job.job_id, batch_id=job.batch_id, detail={})
            if job.state == QUEUED:
                self._index.add(candidate_from_job(job))
            return clone_job(job)

    def find_batch_idempotency(self, client_id: str, key: str) -> Batch | None:
        with self._lock:
            found = self._batch_idemp.get((client_id, key))
            if found is None:
                return None
            return self._batch_view(found)

    def begin_batch(self, batch: Batch) -> Batch:
        with self._lock:
            self._check()
            existing = self._existing_batch(batch)
            if existing is not None:
                return existing
            if batch.job_count > self.limits.max_batch_size:
                raise BackpressureError(
                    f"batch size {batch.job_count} exceeds {self.limits.max_batch_size}",
                    code="batch_too_large",
                )
            stored = replace(batch, idempotency_key=None, idempotency_hash=None, counts=dict(batch.counts))
            self._batches[stored.batch_id] = stored
            self._emit("SUBMITTED", stored.created_at, job_id=None, batch_id=stored.batch_id, detail={"status": stored.status})
            return self._batch_view(batch.batch_id)

    def stage_jobs(self, batch_id: str, jobs: list[Job]) -> int:
        with self._lock:
            self._check()
            batch = self._batches.get(batch_id)
            if batch is None:
                raise NotFoundError(f"batch {batch_id} not found")
            if batch.status != BATCH_STAGING:
                raise ValidationError(f"batch {batch_id} is {batch.status}, not staging")
            if batch.job_count + len(jobs) > self.limits.max_batch_size:
                raise BackpressureError(
                    f"batch would exceed {self.limits.max_batch_size} jobs",
                    code="batch_too_large",
                )
            if jobs:
                self._reserve_capacity(jobs[0].client_id, len(jobs))
            for job in jobs:
                if job.batch_id != batch_id or job.state != STAGED:
                    raise ValidationError("staged jobs must belong to the staging batch")
                dup = self._existing_job(job)
                if dup is not None:
                    raise ConflictError("job idempotency key reused inside a batch", details={"job_id": dup.job_id})
                self._remember_job(clone_job(job))
                self._emit("SUBMITTED", job.created_at, job_id=job.job_id, batch_id=batch_id, detail={"staged": True})
            batch.job_count += len(jobs)
            return batch.job_count

    def activate_batch(
        self,
        batch_id: str,
        *,
        idempotency_key: str | None = None,
        idempotency_hash: str | None = None,
    ) -> Batch:
        with self._lock:
            self._check()
            batch = self._require_batch(batch_id)
            if idempotency_key:
                found = self._batch_idemp.get((batch.client_id, idempotency_key))
                if found is not None and found != batch_id:
                    other = self._batches[found]
                    if other.idempotency_hash != idempotency_hash:
                        raise ConflictError(
                            "batch idempotency key was already used with a different payload",
                            details={"batch_id": found},
                        )
                    self._abort_unlocked(batch_id)
                    view = self._batch_view(found)
                    view.replayed = True
                    return view
            if batch.status == BATCH_ACTIVE:
                return self._batch_view(batch_id)
            if batch.status != BATCH_STAGING:
                raise ValidationError(f"batch {batch_id} is {batch.status}")
            now = isoformat(self.clock.now())
            batch.status = BATCH_ACTIVE
            batch.activated_at = now
            if idempotency_key:
                batch.idempotency_key = idempotency_key
                batch.idempotency_hash = idempotency_hash
                self._batch_idemp[(batch.client_id, idempotency_key)] = batch_id
            for job in self._jobs_for_batch(batch_id):
                if job.state == STAGED:
                    job.state = QUEUED
                    job.updated_at = now
                    self._index.add(candidate_from_job(job))
            self._emit("SUBMITTED", now, job_id=None, batch_id=batch_id, detail={"activated": True, "jobs": batch.job_count})
            return self._batch_view(batch_id)

    def abort_batch(self, batch_id: str) -> None:
        with self._lock:
            self._check()
            self._abort_unlocked(batch_id)

    def _abort_unlocked(self, batch_id: str) -> None:
        batch = self._batches.get(batch_id)
        if batch is None or batch.status != BATCH_STAGING:
            return
        for job in self._jobs_for_batch(batch_id):
            self._jobs.pop(job.job_id, None)
            if job.idempotency_key:
                self._idemp.pop((job.client_id, job.idempotency_key), None)
            self._index.remove(job.job_id)
        batch.status = BATCH_ABORTED
        batch.job_count = 0
        if batch.idempotency_key:
            self._batch_idemp.pop((batch.client_id, batch.idempotency_key), None)
            batch.idempotency_key = None
            batch.idempotency_hash = None

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
        with self._lock:
            self._check()
            base = picker
            for _ in range(64):
                choice, proposed = self._index.propose(model, base, now_epoch, settings)
                if choice is None:
                    return None, base
                job = self._jobs.get(choice.job_id)
                active = self._batch_is_active(job.batch_id) if job is not None else False
                if job is None or not claimable(job, now_iso, batch_active=active):
                    self._index.remove(choice.job_id)
                    continue
                apply_claim(job, owner, lease_until, now_iso)
                self._index.remove(job.job_id)
                self._emit("CLAIMED", now_iso, job_id=job.job_id, batch_id=job.batch_id, detail={"attempt": job.attempt_count})
                return clone_job(job), proposed
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
        if limit <= 0:
            return []
        with self._lock:
            self._check()
            matches: list[Job] = []
            for job in self._jobs.values():
                if job.job_id in exclude or job.resolved_model != model or job.questions_hash != questions_hash:
                    continue
                if not claimable(job, now_iso, batch_active=self._batch_is_active(job.batch_id)):
                    continue
                matches.append(job)
            matches.sort(key=lambda item: (item.created_at, item.job_id))
            claimed: list[Job] = []
            for job in matches[:limit]:
                apply_claim(job, owner, lease_until, now_iso)
                self._index.remove(job.job_id)
                self._emit("CLAIMED", now_iso, job_id=job.job_id, batch_id=job.batch_id, detail={"attempt": job.attempt_count, "native_batch": True})
                claimed.append(clone_job(job))
            return claimed

    def extend_lease(self, job_id: str, owner: str, lease_until: str, now_iso: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.lease_owner != owner or job.state not in {LEASED, RUNNING}:
                return False
            job.lease_expires_at = lease_until
            job.updated_at = now_iso
            return True

    def mark_running(self, job_id: str, owner: str, lease_until: str, now_iso: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.state != LEASED or job.lease_owner != owner:
                return False
            job.state = RUNNING
            job.started_at = job.started_at or now_iso
            job.lease_expires_at = lease_until
            job.updated_at = now_iso
            self._emit("STARTED", now_iso, job_id=job_id, batch_id=job.batch_id, detail={"attempt": job.attempt_count})
            return True

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
    ) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or not owned_running(job, owner):
                return False
            job.state = SUCCEEDED
            job.result = dict(result)
            job.model_version = model_version
            job.queue_ms = queue_ms
            job.execution_ms = execution_ms
            job.total_ms = total_ms
            job.finished_at = now_iso
            job.updated_at = now_iso
            job.lease_owner = None
            job.lease_expires_at = None
            job.error = None
            self._emit("SUCCEEDED", now_iso, job_id=job_id, batch_id=job.batch_id, detail={})
            return True

    def fail(
        self,
        job_id: str,
        owner: str,
        *,
        error: dict[str, Any],
        dead_letter: bool,
        now_iso: str,
    ) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or not owned_running(job, owner):
                return False
            self._finish_failure(job, error, dead_letter, now_iso)
            return True

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
    ) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or not owned_running(job, owner):
                return False
            job.state = RETRY_WAIT
            job.error = dict(error)
            job.not_before = not_before
            job.resolved_model = resolved_model
            job.fallback = None if fallback is None else dict(fallback)
            job.lease_owner = None
            job.lease_expires_at = None
            job.updated_at = now_iso
            self._index.remove(job_id)
            self._emit("RETRY_SCHEDULED", now_iso, job_id=job_id, batch_id=job.batch_id, detail={"not_before": not_before})
            return True

    def finish_cancel(self, job_id: str, owner: str, now_iso: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.lease_owner != owner or job.state not in {LEASED, RUNNING}:
                return False
            self._mark_cancelled(job, now_iso)
            return True

    def cancel(self, job_id: str, now_iso: str) -> Job:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise NotFoundError(f"job {job_id} not found")
            if job.state in {QUEUED, RETRY_WAIT, STAGED, LEASED}:
                self._mark_cancelled(job, now_iso)
            elif job.state == RUNNING:
                job.cancel_requested = True
                job.updated_at = now_iso
            return clone_job(job)

    def cancel_batch(self, batch_id: str, now_iso: str) -> int:
        with self._lock:
            if batch_id not in self._batches:
                raise NotFoundError(f"batch {batch_id} not found")
            changed = 0
            for job in self._jobs_for_batch(batch_id):
                if job.state in {QUEUED, RETRY_WAIT, STAGED, LEASED}:
                    self._mark_cancelled(job, now_iso)
                    changed += 1
                elif job.state == RUNNING and not job.cancel_requested:
                    job.cancel_requested = True
                    job.updated_at = now_iso
                    changed += 1
            return changed

    def maintenance(self, now_iso: str) -> Maintenance:
        with self._lock:
            report = Maintenance()
            for job in list(self._jobs.values()):
                if job.state in {LEASED, RUNNING} and job.lease_expires_at is not None and job.lease_expires_at <= now_iso:
                    self._expire_lease(job, now_iso, report)
                elif job.state == RETRY_WAIT and (job.not_before is None or job.not_before <= now_iso):
                    job.state = QUEUED
                    job.updated_at = now_iso
                    self._index.add(candidate_from_job(job))
                    self._emit("PROMOTED", now_iso, job_id=job.job_id, batch_id=job.batch_id, detail={})
                    report.promoted += 1
                    report.requeued.append(job.job_id)
                elif job.state == QUEUED and job.deadline_at is not None and job.deadline_at <= now_iso:
                    self._finish_failure(job, {"code": "deadline", "message": "deadline passed before execution", "retryable": False}, False, now_iso, force=True)
                    report.deadlines += 1
                    report.finished.append(job.job_id)
                elif job.state in {QUEUED, RETRY_WAIT, STAGED} and job.cancel_requested:
                    self._mark_cancelled(job, now_iso)
                    report.expired_cancelled += 1
                    report.finished.append(job.job_id)
            return report

    def get_job(self, job_id: str) -> Job | None:
        with self._lock:
            job = self._jobs.get(job_id)
            return None if job is None else clone_job(job)

    def get_jobs(
        self,
        *,
        state: str | None = None,
        batch_id: str | None = None,
        client_id: str | None = None,
        model: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        with self._lock:
            rows = [job for job in self._jobs.values() if self._match(job, state, batch_id, client_id, model)]
            rows.sort(key=lambda job: (job.created_at, job.job_id))
            return [clone_job(job) for job in rows[offset : offset + limit]]

    def get_batch(self, batch_id: str) -> Batch | None:
        with self._lock:
            if batch_id not in self._batches:
                return None
            return self._batch_view(batch_id)

    def get_batch_results(self, batch_id: str, *, offset: int = 0, limit: int = 100) -> list[Job]:
        with self._lock:
            if batch_id not in self._batches:
                raise NotFoundError(f"batch {batch_id} not found")
            rows = self._jobs_for_batch(batch_id)
            rows.sort(key=lambda job: (job.batch_ordinal if job.batch_ordinal is not None else 1 << 30, job.job_id))
            return [clone_job(job) for job in rows[offset : offset + limit]]

    def get_events(self, job_id: str, *, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            rows = [dict(item) for item in self._events if item["job_id"] == job_id]
            return rows[-limit:]

    def get_stats(self) -> dict[str, Any]:
        with self._lock:
            by_state = empty_counts()
            by_model: dict[str, int] = {}
            by_class: dict[str, int] = {}
            for job in self._jobs.values():
                by_state[job.state] = by_state.get(job.state, 0) + 1
                if job.state in INCOMPLETE_STATES:
                    by_model[job.resolved_model] = by_model.get(job.resolved_model, 0) + 1
                if job.state == QUEUED:
                    by_class[job.scheduling_class] = by_class.get(job.scheduling_class, 0) + 1
            depth = by_state["queued"] + by_state["retry_wait"] + by_state["staged"]
            return {
                "jobs_by_state": by_state,
                "jobs_by_model": by_model,
                "queued_by_class": by_class,
                "queue_depth": depth,
                "active": by_state["leased"] + by_state["running"],
                "event_count": len(self._events),
            }

    def cleanup(self, *, success_before: str, failure_before: str, event_before: str) -> dict[str, int]:
        with self._lock:
            removed = 0
            for job_id, job in list(self._jobs.items()):
                if job.state in INCOMPLETE_STATES or job.finished_at is None:
                    continue
                cutoff = success_before if job.state == SUCCEEDED else failure_before
                if job.finished_at < cutoff:
                    self._forget_job(job)
                    removed += 1
            before = len(self._events)
            self._events = [item for item in self._events if item["at"] >= event_before or item["job_id"] in self._jobs]
            return {"jobs": removed, "events": before - len(self._events)}

    def requeue(self, job_id: str, now_iso: str) -> Job:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise NotFoundError(f"job {job_id} not found")
            if job.state not in {FAILED, DEAD_LETTER}:
                raise ValidationError(f"job {job_id} is {job.state} and cannot be requeued")
            job.state = QUEUED
            job.attempt_count = 0
            job.error = None
            job.result = None
            job.started_at = None
            job.finished_at = None
            job.lease_owner = None
            job.lease_expires_at = None
            job.not_before = None
            job.cancel_requested = False
            job.queue_ms = None
            job.execution_ms = None
            job.total_ms = None
            job.updated_at = now_iso
            self._index.add(candidate_from_job(job))
            self._emit("REQUEUED", now_iso, job_id=job_id, batch_id=job.batch_id, detail={})
            return clone_job(job)

    def queue_depth(self) -> int:
        with self._lock:
            return sum(1 for job in self._jobs.values() if job.state in INCOMPLETE_STATES)

    def pending_count(self) -> int:
        return self.queue_depth()

    def next_wake_s(self, now_iso: str) -> float | None:
        with self._lock:
            soon: str | None = None
            for job in self._jobs.values():
                moment = None
                if job.state == RETRY_WAIT:
                    moment = job.not_before
                elif job.state in {LEASED, RUNNING}:
                    moment = job.lease_expires_at
                if moment is None or moment <= now_iso:
                    continue
                if soon is None or moment < soon:
                    soon = moment
            if soon is None:
                return None
            from keziah.clock import parse_iso

            return max(0.0, (parse_iso(soon) - parse_iso(now_iso)).total_seconds())

    def release_leases(self, owner_prefix: str, now_iso: str) -> list[str]:
        """Return this worker's unfinished leases to the queue before shutdown."""
        with self._lock:
            report = Maintenance()
            released: list[str] = []
            for job in list(self._jobs.values()):
                if job.state not in {LEASED, RUNNING}:
                    continue
                if not job.lease_owner or not job.lease_owner.startswith(owner_prefix):
                    continue
                self._expire_lease(job, now_iso, report)
                released.append(job.job_id)
            return released

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._jobs.clear()
            self._batches.clear()
            self._index = ReadyIndex()

    def _existing_job(self, job: Job) -> Job | None:
        if not job.idempotency_key:
            return None
        found = self._idemp.get((job.client_id, job.idempotency_key))
        if found is None:
            return None
        current = self._jobs[found]
        if current.idempotency_hash != job.idempotency_hash:
            raise ConflictError(
                "idempotency key was already used with a different payload",
                details={"job_id": current.job_id},
            )
        replay = clone_job(current)
        replay.replayed = True
        return replay

    def _existing_batch(self, batch: Batch) -> Batch | None:
        if not batch.idempotency_key:
            return None
        found = self._batch_idemp.get((batch.client_id, batch.idempotency_key))
        if found is None:
            return None
        current = self._batches[found]
        if current.idempotency_hash != batch.idempotency_hash:
            raise ConflictError(
                "batch idempotency key was already used with a different payload",
                details={"batch_id": current.batch_id},
            )
        view = self._batch_view(current.batch_id)
        view.replayed = True
        return view

    def _reserve_capacity(self, client_id: str, additional: int) -> None:
        depth = sum(1 for job in self._jobs.values() if job.state in INCOMPLETE_STATES)
        if depth + additional > self.limits.max_queued_jobs:
            raise BackpressureError(
                f"queue is full ({self.limits.max_queued_jobs})",
                code="queue_full",
            )
        client_jobs = sum(
            1 for job in self._jobs.values() if job.client_id == client_id and job.state in INCOMPLETE_STATES
        )
        if client_jobs + additional > self.limits.max_active_jobs_per_client:
            raise BackpressureError(
                f"client {client_id or '(anonymous)'} has too many active jobs",
                code="client_full",
            )

    def _remember_job(self, job: Job) -> None:
        self._jobs[job.job_id] = job
        if job.idempotency_key:
            self._idemp[(job.client_id, job.idempotency_key)] = job.job_id

    def _forget_job(self, job: Job) -> None:
        self._jobs.pop(job.job_id, None)
        self._index.remove(job.job_id)
        if job.idempotency_key:
            self._idemp.pop((job.client_id, job.idempotency_key), None)

    def _batch_is_active(self, batch_id: str | None) -> bool:
        if batch_id is None:
            return True
        batch = self._batches.get(batch_id)
        return batch is not None and batch.status == BATCH_ACTIVE

    def _jobs_for_batch(self, batch_id: str) -> list[Job]:
        return [job for job in self._jobs.values() if job.batch_id == batch_id]

    def _batch_view(self, batch_id: str) -> Batch:
        batch = replace(self._batches[batch_id])
        return fill_counts(batch, self._jobs_for_batch(batch_id))

    def _require_batch(self, batch_id: str) -> Batch:
        batch = self._batches.get(batch_id)
        if batch is None:
            raise NotFoundError(f"batch {batch_id} not found")
        return batch

    def _emit(self, kind: str, now_iso: str, *, job_id: str | None, batch_id: str | None, detail: dict[str, Any]) -> None:
        self._event_id += 1
        row = event(kind, now_iso, job_id=job_id, batch_id=batch_id, detail=detail)
        row["id"] = self._event_id
        self._events.append(row)

    def _mark_cancelled(self, job: Job, now_iso: str) -> None:
        job.state = CANCELLED
        job.finished_at = now_iso
        job.updated_at = now_iso
        job.lease_owner = None
        job.lease_expires_at = None
        job.cancel_requested = True
        self._index.remove(job.job_id)
        self._emit("CANCELLED", now_iso, job_id=job.job_id, batch_id=job.batch_id, detail={})

    def _finish_failure(self, job: Job, error: dict[str, Any], dead_letter: bool, now_iso: str, *, force: bool = False) -> None:
        if not force and job.state != RUNNING:
            return
        job.state = DEAD_LETTER if dead_letter else FAILED
        job.error = dict(error)
        job.finished_at = now_iso
        job.updated_at = now_iso
        job.lease_owner = None
        job.lease_expires_at = None
        self._index.remove(job.job_id)
        kind = "DEAD_LETTERED" if dead_letter else "FAILED"
        self._emit(kind, now_iso, job_id=job.job_id, batch_id=job.batch_id, detail={"code": error.get("code")})

    def _expire_lease(self, job: Job, now_iso: str, report: Maintenance) -> None:
        self._emit("LEASE_EXPIRED", now_iso, job_id=job.job_id, batch_id=job.batch_id, detail={"attempt": job.attempt_count})
        job.lease_owner = None
        job.lease_expires_at = None
        job.updated_at = now_iso
        if job.cancel_requested:
            self._mark_cancelled(job, now_iso)
            report.expired_cancelled += 1
            report.finished.append(job.job_id)
            return
        if job.attempt_count >= job.max_attempts:
            self._finish_failure(
                job,
                {"code": "lease_expired", "message": "lease expired after the last attempt", "retryable": True},
                True,
                now_iso,
                force=True,
            )
            report.dead_lettered += 1
            report.finished.append(job.job_id)
            return
        job.state = QUEUED
        self._index.add(candidate_from_job(job))
        report.recovered += 1
        report.requeued.append(job.job_id)

    def _match(self, job: Job, state: str | None, batch_id: str | None, client_id: str | None, model: str | None) -> bool:
        if state is not None and job.state != state:
            return False
        if batch_id is not None and job.batch_id != batch_id:
            return False
        if client_id is not None and job.client_id != client_id:
            return False
        if model is not None and job.resolved_model != model and job.requested_model != model:
            return False
        return True

    def _check(self) -> None:
        if self._closed:
            raise ValidationError("queue is closed")
