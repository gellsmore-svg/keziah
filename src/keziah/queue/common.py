"""Shared record helpers used by every backend."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from keziah.clock import parse_iso
from keziah.types import (
    CANCELLED,
    DEAD_LETTER,
    FAILED,
    QUEUED,
    RUNNING,
    Batch,
    Candidate,
    Job,
)


def clone_job(job: Job) -> Job:
    return replace(
        job,
        payload=dict(job.payload),
        result=None if job.result is None else dict(job.result),
        error=None if job.error is None else dict(job.error),
        fallback=None if job.fallback is None else dict(job.fallback),
    )


def candidate_from_job(job: Job) -> Candidate:
    return Candidate(
        job_id=job.job_id,
        scheduling_class=job.scheduling_class,
        priority=job.priority,
        created_at=parse_iso(job.created_at).timestamp(),
        client_id=job.client_id or "",
        model=job.resolved_model,
        questions_hash=job.questions_hash,
    )


def claimable(job: Job, now_iso: str, *, batch_active: bool) -> bool:
    if job.state != QUEUED or job.cancel_requested:
        return False
    if not batch_active:
        return False
    if job.not_before is not None and job.not_before > now_iso:
        return False
    if job.deadline_at is not None and job.deadline_at <= now_iso:
        return False
    return True


def apply_claim(job: Job, owner: str, lease_until: str, now_iso: str) -> None:
    job.state = "leased"
    job.lease_owner = owner
    job.lease_expires_at = lease_until
    job.attempt_count += 1
    job.updated_at = now_iso


def owned_running(job: Job, owner: str) -> bool:
    return job.state == RUNNING and job.lease_owner == owner


def empty_counts() -> dict[str, int]:
    return {
        "staged": 0,
        "queued": 0,
        "leased": 0,
        "running": 0,
        "succeeded": 0,
        "retry_wait": 0,
        "failed": 0,
        "dead_letter": 0,
        "cancelled": 0,
    }


def fill_counts(batch: Batch, jobs: list[Job]) -> Batch:
    counts = empty_counts()
    for job in jobs:
        counts[job.state] = counts.get(job.state, 0) + 1
    batch.counts = counts
    return batch


def terminal_retention_state(state: str) -> str:
    if state == "succeeded":
        return "success"
    if state in {FAILED, DEAD_LETTER, CANCELLED}:
        return "failure"
    return ""


def event(kind: str, now_iso: str, *, job_id: str | None, batch_id: str | None, detail: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "job_id": job_id,
        "batch_id": batch_id,
        "at": now_iso,
        "kind": kind,
        "detail": detail or {},
    }


