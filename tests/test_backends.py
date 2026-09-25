"""Memory, SQLite, and hybrid queues share one contract."""

from __future__ import annotations

import threading
from datetime import timedelta

import pytest

from keziah.clock import ManualClock, isoformat
from keziah.config import SchedulerSettings
from keziah.queue.sqlite import SQLiteBackend
from keziah.scheduler import PickerState
from keziah.service import Keziah
from keziah.types import BATCH_STAGING, STAGED, Batch, Job
from tests.conftest import make_settings

pytestmark = pytest.mark.parametrize("mode", ["memory", "disk", "hybrid"])


def _backend(tmp_path, mode, clock=None):
    clock = clock or ManualClock()
    settings = make_settings(tmp_path, mode)
    service = Keziah(settings=settings, clock=clock, start_workers=False)
    return service, service.backend, clock


def _job(clock: ManualClock, *, model: str = "mock", scheduling_class: str = "normal", priority: int = 0, client: str = "app") -> Job:
    now = isoformat(clock.now())
    return Job(
        job_id=f"job_{scheduling_class}_{priority}_{client}_{now}_{threading.get_ident()}_{id(clock)}",
        batch_id=None,
        batch_ordinal=None,
        idempotency_key=None,
        idempotency_hash=None,
        client_id=client,
        created_at=now,
        updated_at=now,
        started_at=None,
        finished_at=None,
        not_before=None,
        priority=priority,
        scheduling_class=scheduling_class,
        requested_model=model,
        resolved_model=model,
        model_version=None,
        state="queued",
        payload={"state": {"n": 1}, "questions": {"ok": {"type": "noul", "instructions": "yes?"}}, "parameters": {}},
        questions_hash="hash",
        attempt_count=0,
        max_attempts=3,
        lease_owner=None,
        lease_expires_at=None,
        execution_timeout_ms=None,
        deadline_at=None,
        result=None,
        error=None,
        queue_ms=None,
        execution_ms=None,
        total_ms=None,
        cancel_requested=False,
        fallback=None,
    )


def test_enqueue_claim_complete(tmp_path, mode) -> None:
    service, backend, clock = _backend(tmp_path, mode)
    job = _job(clock)
    job.job_id = "job_one"
    stored = backend.enqueue(job)
    assert stored.state == "queued"
    claimed, _picker = backend.claim_next(
        "mock", PickerState(), isoformat(clock.now()), clock.now().timestamp(), "worker", isoformat(clock.now() + timedelta(seconds=30)), SchedulerSettings()
    )
    assert claimed is not None
    assert claimed.attempt_count == 1
    assert backend.mark_running(claimed.job_id, "worker", isoformat(clock.now() + timedelta(seconds=30)), isoformat(clock.now()))
    assert backend.complete(
        claimed.job_id,
        "worker",
        result={"ok": True},
        model_version="mock-1",
        queue_ms=1,
        execution_ms=1,
        total_ms=2,
        now_iso=isoformat(clock.now()),
    )
    finished = backend.get_job("job_one")
    assert finished is not None and finished.state == "succeeded"
    # A stale owner cannot overwrite the terminal result.
    assert not backend.complete(
        "job_one",
        "other",
        result={"ok": False},
        model_version=None,
        queue_ms=0,
        execution_ms=0,
        total_ms=0,
        now_iso=isoformat(clock.now()),
    )
    finished = backend.get_job("job_one")
    assert finished is not None and finished.result == {"ok": True}
    service.shutdown()


def test_only_one_claim_wins(tmp_path, mode) -> None:
    service, backend, clock = _backend(tmp_path, mode)
    job = _job(clock)
    job.job_id = "job_race"
    backend.enqueue(job)
    winners: list[str] = []
    barrier = threading.Barrier(8)

    def race(index: int) -> None:
        barrier.wait()
        claimed, _picker = backend.claim_next(
            "mock",
            PickerState(),
            isoformat(clock.now()),
            clock.now().timestamp(),
            f"worker-{index}",
            isoformat(clock.now() + timedelta(seconds=30)),
            SchedulerSettings(),
        )
        if claimed is not None:
            winners.append(claimed.lease_owner or "")

    threads = [threading.Thread(target=race, args=(index,)) for index in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(winners) == 1
    service.shutdown()


def test_lease_expiry_requeues_and_preserves_attempt(tmp_path, mode) -> None:
    service, backend, clock = _backend(tmp_path, mode)
    job = _job(clock)
    job.job_id = "job_lease"
    backend.enqueue(job)
    claimed, _picker = backend.claim_next(
        "mock",
        PickerState(),
        isoformat(clock.now()),
        clock.now().timestamp(),
        "worker",
        isoformat(clock.now() + timedelta(seconds=5)),
        SchedulerSettings(),
    )
    assert claimed is not None
    clock.advance(10)
    report = backend.maintenance(isoformat(clock.now()))
    assert report.recovered == 1
    restored = backend.get_job("job_lease")
    assert restored is not None
    assert restored.state == "queued"
    assert restored.attempt_count == 1
    assert restored.lease_owner is None
    service.shutdown()


def test_expired_lease_dead_letters_when_attempts_are_exhausted(tmp_path, mode) -> None:
    service, backend, clock = _backend(tmp_path, mode)
    job = _job(clock)
    job.job_id = "job_dead"
    job.max_attempts = 1
    backend.enqueue(job)
    backend.claim_next(
        "mock",
        PickerState(),
        isoformat(clock.now()),
        clock.now().timestamp(),
        "worker",
        isoformat(clock.now() + timedelta(seconds=1)),
        SchedulerSettings(),
    )
    clock.advance(5)
    report = backend.maintenance(isoformat(clock.now()))
    assert report.dead_lettered == 1
    dead = backend.get_job("job_dead")
    assert dead is not None and dead.state == "dead_letter"
    service.shutdown()


def test_staged_batch_is_invisible_until_activation(tmp_path, mode) -> None:
    service, backend, clock = _backend(tmp_path, mode)
    now = isoformat(clock.now())
    batch = Batch(
        batch_id="batch_stage",
        client_id="app",
        status=BATCH_STAGING,
        requested_model="mock",
        job_count=0,
        created_at=now,
        activated_at=None,
        idempotency_key=None,
        idempotency_hash=None,
        deadline_at=None,
    )
    backend.begin_batch(batch)
    job = _job(clock)
    job.job_id = "job_staged"
    job.batch_id = "batch_stage"
    job.batch_ordinal = 0
    job.state = STAGED
    backend.stage_jobs("batch_stage", [job])
    claimed, _picker = backend.claim_next(
        "mock", PickerState(), now, clock.now().timestamp(), "worker", isoformat(clock.now() + timedelta(seconds=30)), SchedulerSettings()
    )
    assert claimed is None
    activated = backend.activate_batch("batch_stage")
    assert activated.status == "active"
    assert activated.job_count == 1
    claimed, _picker = backend.claim_next(
        "mock", PickerState(), isoformat(clock.now()), clock.now().timestamp(), "worker", isoformat(clock.now() + timedelta(seconds=30)), SchedulerSettings()
    )
    assert claimed is not None
    assert claimed.job_id == "job_staged"
    service.shutdown()


def test_results_keep_input_order(tmp_path, mode) -> None:
    service, backend, clock = _backend(tmp_path, mode)
    now = isoformat(clock.now())
    backend.begin_batch(
        Batch("batch_order", "app", BATCH_STAGING, "mock", 0, now, None, None, None, None)
    )
    jobs = []
    for ordinal in range(5):
        job = _job(clock, priority=5 - ordinal)
        job.job_id = f"job_{ordinal}"
        job.batch_id = "batch_order"
        job.batch_ordinal = ordinal
        job.state = STAGED
        jobs.append(job)
    backend.stage_jobs("batch_order", jobs)
    backend.activate_batch("batch_order")
    rows = backend.get_batch_results("batch_order", offset=0, limit=10)
    assert [row.batch_ordinal for row in rows] == [0, 1, 2, 3, 4]
    service.shutdown()


def test_idempotency_replay_and_conflict(tmp_path, mode) -> None:
    from keziah.errors import ConflictError

    service, backend, clock = _backend(tmp_path, mode)
    first = _job(clock)
    first.job_id = "job_same"
    first.idempotency_key = "key-1"
    first.idempotency_hash = "hash-a"
    stored = backend.enqueue(first)
    again = _job(clock)
    again.job_id = "job_other"
    again.idempotency_key = "key-1"
    again.idempotency_hash = "hash-a"
    replay = backend.enqueue(again)
    assert replay.replayed
    assert replay.job_id == stored.job_id
    conflict = _job(clock)
    conflict.job_id = "job_conflict"
    conflict.idempotency_key = "key-1"
    conflict.idempotency_hash = "hash-b"
    with pytest.raises(ConflictError):
        backend.enqueue(conflict)
    service.shutdown()


def test_cancel_queued_and_cleanup_keeps_unfinished(tmp_path, mode) -> None:
    service, backend, clock = _backend(tmp_path, mode)
    queued = _job(clock)
    queued.job_id = "job_keep"
    done = _job(clock)
    done.job_id = "job_done"
    backend.enqueue(queued)
    backend.enqueue(done)
    claimed, _picker = backend.claim_next(
        "mock", PickerState(), isoformat(clock.now()), clock.now().timestamp(), "worker", isoformat(clock.now() + timedelta(seconds=30)), SchedulerSettings()
    )
    # claim_next may pick either. Complete whichever we got, cancel the other if still queued.
    assert claimed is not None
    backend.mark_running(claimed.job_id, "worker", isoformat(clock.now() + timedelta(seconds=30)), isoformat(clock.now()))
    backend.complete(
        claimed.job_id,
        "worker",
        result={"ok": True},
        model_version="m",
        queue_ms=0,
        execution_ms=0,
        total_ms=0,
        now_iso=isoformat(clock.now()),
    )
    other_id = "job_keep" if claimed.job_id == "job_done" else "job_done"
    other = backend.get_job(other_id)
    assert other is not None
    if other.state == "queued":
        backend.cancel(other_id, isoformat(clock.now()))
    clock.advance(10)
    # Re-enqueue a fresh unfinished job after cleanup window and ensure it survives.
    fresh = _job(clock)
    fresh.job_id = "job_fresh"
    fresh.created_at = isoformat(clock.now())
    fresh.updated_at = fresh.created_at
    backend.enqueue(fresh)
    removed = backend.cleanup(
        success_before=isoformat(clock.now()),
        failure_before=isoformat(clock.now()),
        event_before=isoformat(clock.now() - timedelta(days=1)),
    )
    assert removed["jobs"] >= 1
    fresh_job = backend.get_job("job_fresh")
    assert fresh_job is not None and fresh_job.state == "queued"
    service.shutdown()


def test_sqlite_migration_adds_events(tmp_path, mode) -> None:
    if mode != "disk":
        return
    clock = ManualClock()
    path = tmp_path / "migrate.db"
    first = SQLiteBackend(str(path), clock, migrate_to=1)
    assert first.schema_version == 1
    first.close()
    second = SQLiteBackend(str(path), clock)
    assert second.schema_version == 2
    # v2 can record an event by enqueueing through the service path's columns.
    job = _job(clock)
    job.job_id = "job_migrated"
    second.enqueue(job)
    assert second.get_events("job_migrated")[0]["kind"] == "SUBMITTED"
    second.close()
