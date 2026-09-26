"""In-process Keziah runtime.

The HTTP server and the Python API share this object. A background thread
owns the asyncio dispatcher. Callers block on ``wait`` without running an
HTTP server.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import random
import threading
import time
import uuid
from collections.abc import Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Any, Callable

from keziah.clock import Clock, isoformat, parse_iso
from keziah.config import Settings, load_settings
from keziah.errors import (
    BackpressureError,
    KeziahError,
    ModelUnavailableError,
    NotFoundError,
    PermanentInferenceError,
    RetryableInferenceError,
    Timeout,
    ValidationError,
)
from keziah.jsonutil import canonical_json, payload_hash
from keziah.queue import open_backend
from keziah.registry import Registry
from keziah.results import BatchReceipt, Result
from keziah.scheduler import PickerState
from keziah.telemetry.logging import get_logger
from keziah.telemetry.metrics import Metrics
from keziah.types import (
    BATCH_STAGING,
    SCHEDULING_CLASSES,
    TERMINAL_STATES,
    Batch,
    Job,
    SystemOneRequest,
    normalise_questions,
    validate_state,
)

log = get_logger("service")
Callback = Callable[[Result], None]


class Keziah:
    def __init__(
        self,
        mode: str | None = None,
        *,
        config: str | None = None,
        config_path: str | None = None,
        db_path: str | None = None,
        settings: Settings | None = None,
        clock: Clock | None = None,
        adapters: dict[str, Any] | None = None,
        start_workers: bool = True,
    ) -> None:
        resolved: Settings
        if settings is None:
            overrides: dict[str, Any] = {}
            if mode is not None:
                overrides["queue"] = {"mode": mode}
            if db_path is not None:
                overrides.setdefault("queue", {})["sqlite_path"] = db_path
            resolved = load_settings(config or config_path, overrides=overrides or None)
        elif mode is not None or db_path is not None:
            data = settings.model_dump()
            if mode is not None:
                data["queue"]["mode"] = mode
            if db_path is not None:
                data["queue"]["sqlite_path"] = db_path
            resolved = Settings.model_validate(data)
        else:
            resolved = settings
        self.settings = resolved
        self.clock = clock or Clock()
        self.backend = open_backend(resolved, self.clock)
        self.registry = Registry(resolved, adapters)
        self.metrics = Metrics()
        self.picker = PickerState()
        self.worker_id = f"keziah-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self._rng = random.Random()
        self._accepting = True
        self._stop = False
        self._started = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._wake: asyncio.Event | None = None
        self._ready = threading.Event()
        self._stopped = threading.Event()
        self._tasks: set[asyncio.Task[None]] = set()
        self._executors: dict[str, ThreadPoolExecutor] = {}
        self._slot_lock = threading.Lock()
        self._active: dict[str, int] = {}
        self._inflight = 0
        self._events_lock = threading.Lock()
        self._events: dict[str, threading.Event] = {}
        self._callbacks: dict[str, Callback] = {}
        self._lost: set[tuple[str, str]] = set()
        if start_workers:
            self.start()

    def __enter__(self) -> Keziah:
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.shutdown()

    def start(self) -> None:
        if self._started:
            return
        self._stop = False
        self._accepting = True
        self._ready.clear()
        self._thread = threading.Thread(target=self._thread_main, name="keziah-dispatch", daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout=5):
            raise KeziahError("dispatcher did not start")
        self._started = True
        for model_id, spec in self.settings.models.items():
            if spec.enabled and spec.preload:
                adapter = self.registry.adapter(model_id)
                warm = getattr(adapter, "warm", None)
                if warm is not None:
                    warm()

    def shutdown(self, grace_s: float | None = None) -> None:
        if getattr(self, "_shutdown_done", False):
            return
        self._shutdown_done = True
        self._accepting = False
        self._stop = True
        grace = self.settings.scheduler.shutdown_grace_s if grace_s is None else grace_s
        self._signal()
        if self._started:
            self._stopped.wait(timeout=max(grace, 0.5))
        deadline = time.monotonic() + max(0.0, grace)
        while time.monotonic() < deadline and self._inflight > 0:
            time.sleep(0.01)
        try:
            self.backend.release_leases(self.worker_id, isoformat(self.clock.now()))
        except Exception:
            log.warning("could not release leases during shutdown", exc_info=True)
        loop = self._loop
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)
        if self._thread is not None and self._thread.is_alive() and threading.current_thread() is not self._thread:
            self._thread.join(timeout=2)
        for pool in self._executors.values():
            pool.shutdown(wait=False, cancel_futures=True)
        try:
            self.backend.close()
        except Exception:
            log.warning("queue close failed", exc_info=True)
        self._started = False

    def submit(
        self,
        *,
        model: str,
        state: Any,
        questions: dict[str, Any],
        client_id: str = "",
        priority: int = 0,
        scheduling_class: str | None = None,
        idempotency_key: str | None = None,
        parameters: dict[str, Any] | None = None,
        execution_timeout_ms: int | None = None,
        deadline_at: str | None = None,
        max_attempts: int | None = None,
        callback: Callback | None = None,
    ) -> str:
        job = self._build_job(
            model=model,
            state=state,
            questions=questions,
            client_id=client_id,
            priority=priority,
            scheduling_class=scheduling_class,
            idempotency_key=idempotency_key,
            parameters=parameters,
            execution_timeout_ms=execution_timeout_ms,
            deadline_at=deadline_at,
            max_attempts=max_attempts,
            batch_id=None,
            batch_ordinal=None,
        )
        stored = self.backend.enqueue(job)
        if callback is not None:
            if stored.state in TERMINAL_STATES:
                callback(self._to_result(stored))
            else:
                self._callbacks[stored.job_id] = callback
                fresh = self.backend.get_job(stored.job_id)
                if fresh is not None and fresh.state in TERMINAL_STATES:
                    self._deliver(stored.job_id)
        if not stored.replayed:
            self.metrics.inc(
                "keziah_jobs_submitted_total",
                model=stored.resolved_model,
                scheduling_class=stored.scheduling_class,
            )
        self._signal()
        return stored.job_id

    def submit_batch(
        self,
        jobs: Iterable[dict[str, Any]],
        *,
        model: str | None = None,
        client_id: str = "",
        priority: int = 0,
        scheduling_class: str | None = None,
        idempotency_key: str | None = None,
        deadline_at: str | None = None,
        max_attempts: int | None = None,
    ) -> BatchReceipt:
        self._ensure_accepting()
        scheduling_class = self._class(scheduling_class)
        materialised: Sequence[dict[str, Any]] | None
        if isinstance(jobs, Sequence) and not isinstance(jobs, (str, bytes)):
            materialised = jobs
            if len(materialised) > self.settings.scheduler.max_batch_size:
                raise BackpressureError(
                    f"batch size {len(materialised)} exceeds {self.settings.scheduler.max_batch_size}",
                    code="batch_too_large",
                )
        else:
            materialised = None
        if materialised is not None and idempotency_key:
            digest = _hash_jobs(materialised, model=model, priority=priority, scheduling_class=scheduling_class)
            existing = self.backend.find_batch_idempotency(client_id, idempotency_key)
            if existing is not None:
                if existing.idempotency_hash == digest:
                    existing.replayed = True
                    return _receipt(existing)
                raise keziah_conflict(existing.batch_id)
        batch_id = _new_id("batch")
        now = isoformat(self.clock.now())
        batch = Batch(
            batch_id=batch_id,
            client_id=client_id,
            status=BATCH_STAGING,
            requested_model=model,
            job_count=0,
            created_at=now,
            activated_at=None,
            idempotency_key=None,
            idempotency_hash=None,
            deadline_at=_normalise_deadline(deadline_at),
        )
        self.backend.begin_batch(batch)
        hasher = hashlib.sha256()
        total_bytes = 0
        ordinal = 0
        chunk: list[Job] = []
        source: Iterable[dict[str, Any]] = materialised if materialised is not None else jobs
        try:
            for raw in source:
                if not isinstance(raw, dict):
                    raise ValidationError(f"job {ordinal} must be an object")
                built = self._build_job(
                    model=str(raw.get("model") or model or ""),
                    state=raw.get("state"),
                    questions=raw.get("questions") or {},
                    client_id=str(raw.get("client_id") or client_id),
                    priority=raw.get("priority", priority),
                    scheduling_class=str(raw.get("scheduling_class") or scheduling_class),
                    idempotency_key=raw.get("idempotency_key"),
                    parameters=raw.get("parameters") or {},
                    execution_timeout_ms=raw.get("execution_timeout_ms"),
                    deadline_at=raw.get("deadline_at") or deadline_at,
                    max_attempts=raw.get("max_attempts", max_attempts),
                    batch_id=batch_id,
                    batch_ordinal=ordinal,
                )
                encoded = canonical_json(
                    {
                        "ordinal": ordinal,
                        "model": built.requested_model,
                        "payload": built.payload,
                        "priority": built.priority,
                        "class": built.scheduling_class,
                    }
                ).encode("utf-8")
                total_bytes += len(encoded)
                if total_bytes > self.settings.scheduler.max_batch_bytes:
                    raise BackpressureError("batch input is too large", code="batch_too_large")
                hasher.update(encoded)
                chunk.append(built)
                ordinal += 1
                if len(chunk) >= 500:
                    self.backend.stage_jobs(batch_id, chunk)
                    chunk = []
                if ordinal > self.settings.scheduler.max_batch_size:
                    raise BackpressureError(
                        f"batch size exceeds {self.settings.scheduler.max_batch_size}",
                        code="batch_too_large",
                    )
            if chunk:
                self.backend.stage_jobs(batch_id, chunk)
            if ordinal == 0:
                raise ValidationError("batch is empty")
            digest = hasher.hexdigest()
            if materialised is not None and idempotency_key:
                # Same bytes the pre-check hashed. Recompute with the shared helper
                # when the caller passed a list so replay detection stays stable.
                digest = _hash_jobs(materialised, model=model, priority=priority, scheduling_class=scheduling_class)
            activated = self.backend.activate_batch(
                batch_id,
                idempotency_key=idempotency_key,
                idempotency_hash=digest if idempotency_key else None,
            )
        except Exception:
            try:
                self.backend.abort_batch(batch_id)
            except Exception:
                log.warning("failed to abort staging batch", extra={"batch_id": batch_id})
            raise
        if not activated.replayed:
            self.metrics.inc("keziah_jobs_submitted_total", amount=activated.job_count)
            self.metrics.observe("keziah_batch_size", float(activated.job_count))
            self._signal()
        return _receipt(activated)

    def wait(self, job_id: str, timeout: float | None = None) -> Result:
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            job = self.backend.get_job(job_id)
            if job is None:
                raise NotFoundError(f"job {job_id} not found")
            if job.state in TERMINAL_STATES:
                return self._to_result(job)
            event = self._event_for(job_id)
            # Drop a signal from an earlier attempt, then look again so a
            # completion that landed during clear is not missed.
            event.clear()
            job = self.backend.get_job(job_id)
            if job is not None and job.state in TERMINAL_STATES:
                return self._to_result(job)
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                raise Timeout(f"timed out waiting for {job_id}")
            if not event.wait(remaining):
                job = self.backend.get_job(job_id)
                if job is not None and job.state in TERMINAL_STATES:
                    return self._to_result(job)
                raise Timeout(f"timed out waiting for {job_id}")

    def wait_batch(self, batch_id: str, timeout: float | None = None) -> list[Result]:
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            batch = self.get_batch(batch_id)
            if batch["completion"] in {"complete", "aborted"}:
                return [self._to_result(job) for job in self._all_batch_jobs(batch_id)]
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                raise Timeout(f"timed out waiting for batch {batch_id}")
            time.sleep(0.01 if remaining is None else min(0.05, remaining))

    def get(self, job_id: str) -> Result:
        job = self.backend.get_job(job_id)
        if job is None:
            raise NotFoundError(f"job {job_id} not found")
        return self._to_result(job)

    def get_job_record(self, job_id: str) -> dict[str, Any]:
        job = self.backend.get_job(job_id)
        if job is None:
            raise NotFoundError(f"job {job_id} not found")
        return _job_dict(job)

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        batch = self.backend.get_batch(batch_id)
        if batch is None:
            raise NotFoundError(f"batch {batch_id} not found")
        return _batch_dict(batch)

    def get_results(self, batch_id: str, *, offset: int = 0, limit: int = 100) -> list[Result]:
        return [self._to_result(job) for job in self.backend.get_batch_results(batch_id, offset=offset, limit=limit)]

    def cancel(self, job_id: str) -> Result:
        job = self.backend.cancel(job_id, isoformat(self.clock.now()))
        if job.state == "cancelled":
            self.metrics.inc("keziah_jobs_cancelled_total")
            self._deliver(job.job_id)
        self._signal()
        return self._to_result(job)

    def cancel_batch(self, batch_id: str) -> int:
        changed = self.backend.cancel_batch(batch_id, isoformat(self.clock.now()))
        if changed:
            self.metrics.inc("keziah_jobs_cancelled_total", amount=changed)
        self._signal()
        return changed

    def requeue(self, job_id: str) -> Result:
        job = self.backend.requeue(job_id, isoformat(self.clock.now()))
        self._signal()
        return self._to_result(job)

    def cleanup(self) -> dict[str, int]:
        now = self.clock.now()
        retention = self.settings.retention
        return self.backend.cleanup(
            success_before=isoformat(now - timedelta(seconds=retention.success_seconds)),
            failure_before=isoformat(now - timedelta(seconds=retention.failure_seconds)),
            event_before=isoformat(now - timedelta(seconds=retention.event_seconds)),
        )

    def models(self) -> list[dict[str, Any]]:
        return [_model_dict(item) for item in self.registry.describe(self.active_slots())]

    def show(self, model_id: str) -> dict[str, Any]:
        resolved = self.registry.resolve(model_id).resolved if model_id in self.settings.aliases or model_id in self.settings.model_groups or model_id in self.settings.models else model_id
        # Concrete id, alias, or group. Unknown concrete names still raise from describe lookup.
        if model_id in self.settings.model_groups or (
            model_id in self.settings.aliases and self.registry.follow(model_id) in self.settings.model_groups
        ):
            resolution = self.registry.resolve(model_id)
            resolved = resolution.resolved
        elif model_id in self.settings.aliases:
            resolved = self.registry.follow(model_id)
        else:
            resolved = model_id
        info = {item.id: item for item in self.registry.describe(self.active_slots())}
        if resolved not in info:
            raise ModelUnavailableError(f"unknown model {model_id!r}")
        payload = _model_dict(info[resolved])
        payload["requested"] = model_id
        spec = self.settings.models[resolved]
        payload["native_batch_size"] = spec.max_native_batch_size
        payload["native_batch_wait_ms"] = spec.max_native_batch_wait_ms
        health = self.registry.health(resolved)
        payload["health_detail"] = health.detail
        return payload

    def stats(self) -> dict[str, Any]:
        raw = self.backend.get_stats()
        raw["models"] = self.models()
        raw["worker_id"] = self.worker_id
        raw["mode"] = self.settings.queue.mode
        return raw

    def active_slots(self) -> dict[str, int]:
        with self._slot_lock:
            return dict(self._active)

    def render_metrics(self) -> str:
        stats = self.backend.get_stats()
        capacity = {model_id: spec.max_concurrency for model_id, spec in self.settings.models.items()}
        text = self.metrics.render(
            queue_depth=float(stats.get("queue_depth", 0)),
            active=self.active_slots(),
            capacity=capacity,
        )
        extra = ["# TYPE keziah_jobs_by_model gauge"]
        for model, count in sorted(stats.get("jobs_by_model", {}).items()):
            extra.append(f'keziah_jobs_by_model{{model="{model}"}} {count}')
        extra.append("# TYPE keziah_jobs_by_scheduling_class gauge")
        for name, count in sorted(stats.get("queued_by_class", {}).items()):
            extra.append(f'keziah_jobs_by_scheduling_class{{scheduling_class="{name}"}} {count}')
        return text + "\n".join(extra) + "\n"

    def liveness(self) -> dict[str, Any]:
        return {"status": "live"}

    def readiness(self) -> dict[str, Any]:
        if self._stop or not self._started:
            return {"status": "not_ready", "reason": "dispatcher stopped"}
        try:
            self.backend.get_stats()
        except Exception as exc:
            return {"status": "not_ready", "reason": type(exc).__name__}
        return {"status": "ready", "mode": self.settings.queue.mode}

    def _build_job(
        self,
        *,
        model: str,
        state: Any,
        questions: dict[str, Any],
        client_id: str,
        priority: int,
        scheduling_class: str | None,
        idempotency_key: str | None,
        parameters: dict[str, Any] | None,
        execution_timeout_ms: int | None,
        deadline_at: str | None,
        max_attempts: int | None,
        batch_id: str | None,
        batch_ordinal: int | None,
    ) -> Job:
        self._ensure_accepting()
        if not model:
            raise ValidationError("model is required")
        scheduling_class = self._class(scheduling_class)
        if not isinstance(priority, int) or isinstance(priority, bool) or not -1_000_000 <= priority <= 1_000_000:
            raise ValidationError("priority must be an integer")
        validate_state(state)
        normalised = normalise_questions(questions, max_questions=self.settings.scheduler.max_questions)
        parameters = dict(parameters or {})
        deadline_at = _normalise_deadline(deadline_at)
        if execution_timeout_ms is not None and (
            isinstance(execution_timeout_ms, bool) or not isinstance(execution_timeout_ms, int)
        ):
            raise ValidationError("execution_timeout_ms must be an integer")
        resolution = self.registry.resolve(model)
        payload = {"state": state, "questions": normalised, "parameters": parameters}
        size = len(canonical_json(payload).encode("utf-8"))
        if size > self.settings.scheduler.max_request_bytes:
            raise BackpressureError("request is too large", code="request_too_large")
        if max_attempts is None:
            attempts = self.settings.scheduler.default_max_attempts
        elif isinstance(max_attempts, bool) or not isinstance(max_attempts, int):
            raise ValidationError("max_attempts must be an integer")
        else:
            attempts = max_attempts
        if attempts < 1:
            raise ValidationError("max_attempts must be >= 1")
        now = isoformat(self.clock.now())
        key = idempotency_key.strip() if isinstance(idempotency_key, str) and idempotency_key.strip() else None
        identity = {
            "model": resolution.requested,
            "payload": payload,
            "priority": priority,
            "scheduling_class": scheduling_class,
        }
        fallback = None
        if resolution.fallback_used:
            fallback = {
                "used": True,
                "reason": resolution.fallback_reason,
                "requested": resolution.requested,
                "resolved": resolution.resolved,
            }
        return Job(
            job_id=_new_id("job"),
            batch_id=batch_id,
            batch_ordinal=batch_ordinal,
            idempotency_key=key,
            idempotency_hash=payload_hash(identity) if key else None,
            client_id=client_id or "",
            created_at=now,
            updated_at=now,
            started_at=None,
            finished_at=None,
            not_before=None,
            priority=priority,
            scheduling_class=scheduling_class,
            requested_model=resolution.requested,
            resolved_model=resolution.resolved,
            model_version=None,
            state="queued" if batch_id is None else "staged",
            payload=payload,
            questions_hash=_coalesce_hash(normalised, parameters),
            attempt_count=0,
            max_attempts=attempts,
            lease_owner=None,
            lease_expires_at=None,
            execution_timeout_ms=execution_timeout_ms,
            deadline_at=deadline_at,
            result=None,
            error=None,
            queue_ms=None,
            execution_ms=None,
            total_ms=None,
            cancel_requested=False,
            fallback=fallback,
        )

    def _class(self, value: str | None) -> str:
        chosen = value or self.settings.scheduler.default_class
        if chosen not in SCHEDULING_CLASSES:
            raise ValidationError(f"scheduling_class must be one of {', '.join(SCHEDULING_CLASSES)}")
        return chosen

    def _ensure_accepting(self) -> None:
        if not self._accepting:
            raise ValidationError("keziah is shutting down and is not accepting work", code="shutting_down")

    def _thread_main(self) -> None:
        assert self._thread is not None
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        loop.create_task(self._run())
        loop.run_forever()
        loop.close()

    async def _run(self) -> None:
        self._wake = asyncio.Event()
        self._ready.set()
        try:
            await self._dispatch_loop()
        finally:
            self._stopped.set()

    async def _dispatch_loop(self) -> None:
        while not self._stop:
            if self._wake is not None:
                self._wake.clear()
            started = 0
            try:
                now = isoformat(self.clock.now())
                report = self.backend.maintenance(now)
                for job_id in getattr(report, "finished", ()) or ():
                    self._deliver(job_id)
                started = await self._dispatch_available()
            except Exception:
                log.exception("dispatcher iteration failed")
            if self._stop:
                break
            if started == 0:
                delay = self.settings.scheduler.recovery_interval_s
                sooner = self.backend.next_wake_s(isoformat(self.clock.now()))
                if sooner is not None:
                    delay = min(delay, max(sooner, 0.0))
                try:
                    assert self._wake is not None
                    await asyncio.wait_for(self._wake.wait(), timeout=max(delay, 0.001))
                except TimeoutError:
                    pass

    async def _dispatch_available(self) -> int:
        if self._stop:
            return 0
        started = 0
        now_dt = self.clock.now()
        now_iso = isoformat(now_dt)
        now_epoch = now_dt.timestamp()
        lease_until = isoformat(now_dt + timedelta(seconds=self.settings.scheduler.lease_seconds))
        for model_id, spec in self.settings.models.items():
            if not spec.enabled:
                continue
            while self._try_reserve(model_id):
                owner = self._owner()
                job, self.picker = self.backend.claim_next(
                    model_id,
                    self.picker,
                    now_iso,
                    now_epoch,
                    owner,
                    lease_until,
                    self.settings.scheduler,
                )
                if job is None:
                    self._release(model_id)
                    break
                jobs = [job]
                adapter = self.registry.adapter(model_id)
                if spec.max_native_batch_size > 1 and adapter.capabilities.supports_native_batch:
                    jobs.extend(
                        self.backend.claim_compatible(
                            model=model_id,
                            questions_hash=job.questions_hash,
                            exclude={job.job_id},
                            limit=spec.max_native_batch_size - 1,
                            now_iso=now_iso,
                            owner=owner,
                            lease_until=lease_until,
                        )
                    )
                task = asyncio.create_task(self._execute(jobs, owner, model_id))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)
                started += 1
                self.metrics.inc("keziah_jobs_started_total", amount=len(jobs), model=model_id)
        return started

    async def _execute(self, jobs: list[Job], owner: str, model_id: str) -> None:
        heartbeat: asyncio.Task[None] | None = None
        try:
            now_iso = isoformat(self.clock.now())
            lease_until = isoformat(self.clock.now() + timedelta(seconds=self.settings.scheduler.lease_seconds))
            runnable: list[Job] = []
            for job in jobs:
                current = self.backend.get_job(job.job_id)
                if current is None or current.cancel_requested:
                    self.backend.finish_cancel(job.job_id, owner, now_iso)
                    self._deliver(job.job_id)
                    continue
                if current.deadline_at is not None and current.deadline_at <= now_iso:
                    if self.backend.mark_running(job.job_id, owner, lease_until, now_iso):
                        self.backend.fail(
                            job.job_id,
                            owner,
                            error={"code": "deadline", "message": "deadline passed before execution", "retryable": False},
                            dead_letter=False,
                            now_iso=now_iso,
                        )
                        self.metrics.inc("keziah_jobs_failed_total", model=job.resolved_model)
                        self._deliver(job.job_id)
                    continue
                if not self.backend.mark_running(job.job_id, owner, lease_until, now_iso):
                    continue
                fresh = self.backend.get_job(job.job_id) or current
                try:
                    target, fallback = self._execution_target(fresh)
                except Exception as exc:
                    if self._settle_failure(fresh, owner, _as_inference_error(exc)):
                        self._deliver(fresh.job_id)
                    continue
                if target != model_id:
                    # The slot belongs to model_id. Run the fallback through its own cap.
                    fresh.fallback = fallback
                    self.backend.release_to_queue(
                        fresh.job_id,
                        owner,
                        now_iso,
                        resolved_model=target,
                        decrement_attempt=True,
                    )
                    continue
                runnable.append(fresh)
            if not runnable:
                return
            heartbeat = asyncio.create_task(self._heartbeat([job.job_id for job in runnable], owner))
            try:
                responses = await self._infer_group(runnable, model_id)
            except Exception as exc:
                classified = _as_inference_error(exc)
                for job in runnable:
                    if (job.job_id, owner) in self._lost:
                        continue
                    if self._settle_failure(job, owner, classified):
                        self._deliver(job.job_id)
                return
            for job, response in zip(runnable, responses, strict=True):
                if (job.job_id, owner) in self._lost:
                    continue
                fresh = self.backend.get_job(job.job_id)
                if fresh is not None and fresh.cancel_requested:
                    self.backend.finish_cancel(job.job_id, owner, isoformat(self.clock.now()))
                    self.metrics.inc("keziah_jobs_cancelled_total")
                    self._deliver(job.job_id)
                    continue
                self._settle_success(job, owner, response)
                self._deliver(job.job_id)
        except Exception:
            log.exception("execution failed", extra={"model": model_id, "worker": self.worker_id})
        finally:
            if heartbeat is not None:
                try:
                    heartbeat.cancel()
                except RuntimeError:
                    pass
            for job in jobs:
                self._lost.discard((job.job_id, owner))
            self._release(model_id)
            try:
                self._signal()
            except RuntimeError:
                pass

    async def _infer_group(self, jobs: list[Job], model_id: str) -> list[Any]:
        spec = self.settings.models[model_id]
        adapter = self.registry.adapter(model_id)
        prepared: list[tuple[Job, SystemOneRequest, str]] = []
        for job in jobs:
            # Retargeting happens before this group is formed, on the model's own slot.
            request = SystemOneRequest(
                state=job.payload["state"],
                questions=job.payload["questions"],
                model=model_id,
                parameters=dict(job.payload.get("parameters") or {}),
                job_id=job.job_id,
            )
            prepared.append((job, request, model_id))
        timeout_s = self._timeout_s(jobs[0])
        targets = {item[2] for item in prepared}
        if len(jobs) > 1 and len(targets) == 1 and hasattr(adapter, "infer_many"):
            requests = [item[1] for item in prepared]
            try:
                if spec.execution == "thread" and hasattr(adapter, "infer_many_blocking"):
                    fut = asyncio.get_running_loop().run_in_executor(
                        self._executor(model_id), adapter.infer_many_blocking, requests
                    )
                    return await asyncio.wait_for(fut, timeout_s * len(jobs))
                return await asyncio.wait_for(adapter.infer_many(requests), timeout_s * len(jobs))
            except PermanentInferenceError:
                raise
            except Exception:
                # A native batch failure falls back to single calls so one bad
                # grouping does not hide a per-job result.
                log.warning("native batch failed; retrying jobs individually", extra={"model": model_id})
        responses = []
        for _job, request, target in prepared:
            target_spec = self.settings.models.get(target, spec)
            target_adapter = self.registry.adapter(target) if target in self.settings.models else adapter
            responses.append(await self._infer_one(target_adapter, target_spec, target, request, self._timeout_s(_job)))
        return responses

    async def _infer_one(self, adapter: Any, spec: Any, model_id: str, request: SystemOneRequest, timeout_s: float) -> Any:
        try:
            if spec.execution == "thread":
                fut = asyncio.get_running_loop().run_in_executor(self._executor(model_id), adapter.infer_blocking, request)
                return await asyncio.wait_for(fut, timeout_s)
            return await asyncio.wait_for(adapter.infer(request), timeout_s)
        except TimeoutError as exc:
            raise RetryableInferenceError("execution timed out", code="execution_timeout") from exc

    def _execution_target(self, job: Job) -> tuple[str, dict[str, Any] | None]:
        health = self.registry.health(job.resolved_model)
        if health.ok:
            return job.resolved_model, job.fallback
        if self.registry.is_group(job.requested_model):
            resolution = self.registry.resolve(job.requested_model)
            if resolution.resolved != job.resolved_model and self.registry.health(resolution.resolved).ok:
                return resolution.resolved, {
                    "used": True,
                    "reason": resolution.fallback_reason or "primary_unavailable",
                    "requested": job.requested_model,
                    "resolved": resolution.resolved,
                    "from": job.resolved_model,
                }
            if self.registry.health(resolution.resolved).permanent:
                raise PermanentInferenceError(
                    self.registry.health(resolution.resolved).detail or "model unavailable",
                    code="model_unavailable",
                )
        if health.permanent:
            raise PermanentInferenceError(health.detail or "model unavailable", code="model_unavailable")
        # A cached transient probe is not a reason to spend the job's attempts.
        return job.resolved_model, job.fallback

    def _settle_success(self, job: Job, owner: str, response: Any) -> None:
        now = self.clock.now()
        now_iso = isoformat(now)
        queue_ms = max(0, int((now - parse_iso(job.created_at)).total_seconds() * 1000))
        inference_ms = int(float(getattr(response, "inference_seconds", 0.0)) * 1000)
        execution_ms = inference_ms
        total_ms = queue_ms + execution_ms
        fallback = job.fallback or {}
        result = {
            "job_id": job.job_id,
            "batch_id": job.batch_id,
            "batch_ordinal": job.batch_ordinal,
            "requested_model": job.requested_model,
            "resolved_model": job.resolved_model,
            "adapter": self.settings.models[job.resolved_model].adapter if job.resolved_model in self.settings.models else None,
            "model_version": response.model_version,
            "fallback_used": bool(fallback.get("used")),
            "fallback_reason": fallback.get("reason"),
            "attempts": job.attempt_count,
            "timings": {
                "queue_ms": queue_ms,
                "inference_ms": inference_ms,
                "execution_ms": execution_ms,
                "total_ms": total_ms,
            },
            "request": job.payload,
            "response": {"answers": response.answers, "usage": response.usage},
            "finished_at": now_iso,
        }
        committed = False
        started = time.perf_counter()
        committed = self.backend.complete(
            job.job_id,
            owner,
            result=result,
            model_version=response.model_version,
            queue_ms=queue_ms,
            execution_ms=execution_ms,
            total_ms=total_ms,
            now_iso=now_iso,
        )
        commit_ms = int((time.perf_counter() - started) * 1000)
        if committed:
            result["timings"]["commit_ms"] = commit_ms
            self.metrics.inc("keziah_jobs_succeeded_total", model=job.resolved_model)
            self.metrics.observe("keziah_queue_latency_seconds", queue_ms / 1000.0, model=job.resolved_model)
            self.metrics.observe("keziah_inference_latency_seconds", inference_ms / 1000.0, model=job.resolved_model)
            self.metrics.observe("keziah_end_to_end_latency_seconds", total_ms / 1000.0, model=job.resolved_model)
        else:
            log.warning("completion lost the lease", extra={"job_id": job.job_id, "worker": self.worker_id})

    def _settle_failure(self, job: Job, owner: str, exc: Exception) -> bool:
        now_iso = isoformat(self.clock.now())
        retryable = isinstance(exc, RetryableInferenceError)
        code = getattr(exc, "code", "error")
        error = {"code": code, "message": str(exc), "retryable": retryable}
        deadline_hit = job.deadline_at is not None and job.deadline_at <= now_iso
        if not retryable or job.attempt_count >= job.max_attempts or deadline_hit:
            dead = bool(retryable)
            self.backend.fail(job.job_id, owner, error=error, dead_letter=dead, now_iso=now_iso)
            if dead:
                self.metrics.inc("keziah_dead_letter_total", model=job.resolved_model)
            else:
                self.metrics.inc("keziah_jobs_failed_total", model=job.resolved_model)
            log.warning(
                "job failed",
                extra={"job_id": job.job_id, "model": job.resolved_model, "attempt": job.attempt_count, "worker": self.worker_id},
            )
            return True
        delay = exc.retry_after_s if isinstance(exc, RetryableInferenceError) and exc.retry_after_s is not None else _backoff(
            job.attempt_count, self.settings, self._rng
        )
        not_before = isoformat(self.clock.now() + timedelta(seconds=delay))
        resolved, fallback = self._retry_target(job)
        self.backend.retry(
            job.job_id,
            owner,
            error=error,
            not_before=not_before,
            resolved_model=resolved,
            fallback=fallback,
            now_iso=now_iso,
        )
        self.metrics.inc("keziah_jobs_retried_total", model=job.resolved_model)
        return False

    def _retry_target(self, job: Job) -> tuple[str, dict[str, Any] | None]:
        if not self.registry.is_group(job.requested_model):
            return job.resolved_model, job.fallback
        try:
            resolution = self.registry.resolve(job.requested_model)
        except KeziahError:
            return job.resolved_model, job.fallback
        fallback = job.fallback
        if resolution.fallback_used:
            fallback = {
                "used": True,
                "reason": resolution.fallback_reason,
                "requested": job.requested_model,
                "resolved": resolution.resolved,
                "from": job.resolved_model,
            }
        return resolution.resolved, fallback

    async def _heartbeat(self, job_ids: list[str], owner: str) -> None:
        interval = max(0.05, self.settings.scheduler.lease_seconds * self.settings.scheduler.heartbeat_fraction)
        while True:
            await asyncio.sleep(interval)
            now = self.clock.now()
            lease_until = isoformat(now + timedelta(seconds=self.settings.scheduler.lease_seconds))
            now_iso = isoformat(now)
            for job_id in job_ids:
                if not self.backend.extend_lease(job_id, owner, lease_until, now_iso):
                    self._lost.add((job_id, owner))

    def _timeout_s(self, job: Job) -> float:
        ms = job.execution_timeout_ms
        if ms is None:
            ms = self.settings.scheduler.default_execution_timeout_ms
        return max(0.001, ms / 1000.0)

    def _try_reserve(self, model_id: str) -> bool:
        cap = self.settings.models[model_id].max_concurrency
        with self._slot_lock:
            current = self._active.get(model_id, 0)
            if current >= cap:
                return False
            self._active[model_id] = current + 1
            self._inflight += 1
            return True

    def _release(self, model_id: str) -> None:
        with self._slot_lock:
            self._active[model_id] = max(0, self._active.get(model_id, 1) - 1)
            self._inflight = max(0, self._inflight - 1)

    def _executor(self, model_id: str) -> ThreadPoolExecutor:
        pool = self._executors.get(model_id)
        if pool is None:
            cap = self.settings.models[model_id].max_concurrency
            pool = ThreadPoolExecutor(max_workers=cap, thread_name_prefix=f"keziah-{model_id}")
            self._executors[model_id] = pool
        return pool

    def _owner(self) -> str:
        return f"{self.worker_id}:{uuid.uuid4().hex[:8]}"

    def _event_for(self, job_id: str) -> threading.Event:
        with self._events_lock:
            event = self._events.get(job_id)
            if event is None:
                event = threading.Event()
                self._events[job_id] = event
            return event

    def _deliver(self, job_id: str) -> None:
        with self._events_lock:
            event = self._events.get(job_id)
        if event is not None:
            event.set()
        callback = self._callbacks.pop(job_id, None)
        if callback is None:
            return
        try:
            callback(self.get(job_id))
        except Exception:
            log.warning("in-process callback failed", extra={"job_id": job_id})

    def _signal(self) -> None:
        loop = self._loop
        wake = self._wake
        if loop is not None and wake is not None and loop.is_running():
            loop.call_soon_threadsafe(wake.set)

    def _to_result(self, job: Job) -> Result:
        stored = job.result or {}
        timings = stored.get("timings") if isinstance(stored.get("timings"), dict) else {}
        if not timings:
            timings = {
                "queue_ms": job.queue_ms,
                "execution_ms": job.execution_ms,
                "total_ms": job.total_ms,
            }
        response = stored.get("response") if isinstance(stored.get("response"), dict) else None
        fallback = job.fallback or {}
        if stored:
            fallback_used = bool(stored.get("fallback_used", fallback.get("used", False)))
            fallback_reason = stored.get("fallback_reason", fallback.get("reason"))
            resolved = stored.get("resolved_model", job.resolved_model)
            version = stored.get("model_version", job.model_version)
            adapter = stored.get("adapter")
            attempts = int(stored.get("attempts", job.attempt_count))
            raw_request = stored.get("request")
            request = raw_request if isinstance(raw_request, dict) else job.payload
        else:
            fallback_used = bool(fallback.get("used"))
            fallback_reason = fallback.get("reason")
            resolved = job.resolved_model
            version = job.model_version
            adapter = self.settings.models[resolved].adapter if resolved in self.settings.models else None
            attempts = job.attempt_count
            request = job.payload
        return Result(
            job_id=job.job_id,
            status=job.state,
            batch_id=job.batch_id,
            batch_ordinal=job.batch_ordinal,
            requested_model=job.requested_model,
            resolved_model=resolved,
            adapter=adapter,
            model_version=version,
            fallback_used=fallback_used,
            fallback_reason=fallback_reason,
            attempts=attempts,
            timings=timings,
            request=request,
            response=response,
            error=job.error,
            finished_at=job.finished_at or stored.get("finished_at"),
            client_id=job.client_id,
            scheduling_class=job.scheduling_class,
            priority=job.priority,
        )

    def _all_batch_jobs(self, batch_id: str) -> list[Job]:
        rows: list[Job] = []
        offset = 0
        while True:
            page = self.backend.get_batch_results(batch_id, offset=offset, limit=1000)
            if not page:
                return rows
            rows.extend(page)
            if len(page) < 1000:
                return rows
            offset += 1000


def _normalise_deadline(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValidationError("deadline_at must be a timezone-aware timestamp", code="invalid_request")
    try:
        moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValidationError("deadline_at must be a timezone-aware timestamp", code="invalid_request") from exc
    if moment.tzinfo is None:
        raise ValidationError("deadline_at must include a timezone", code="invalid_request")
    return isoformat(moment)


def _coalesce_hash(questions: dict[str, Any], parameters: dict[str, Any]) -> str:
    """Jobs coalesce only when questions and inference parameters match.

    ``max_len`` and ``head_max_len`` are not per item on Laya's predict_batch,
    so a job that sets either one never shares a native batch.
    """
    body: dict[str, Any] = {"questions": questions, "parameters": parameters}
    if parameters.get("max_len") is not None or parameters.get("head_max_len") is not None:
        body["solo"] = uuid.uuid4().hex
    return payload_hash(body)


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _backoff(attempt: int, settings: Settings, rng: random.Random) -> float:
    scheduler = settings.scheduler
    raw = min(scheduler.retry_max_s, scheduler.retry_base_s * (scheduler.retry_factor ** max(0, attempt - 1)))
    if scheduler.retry_jitter > 0:
        jitter = scheduler.retry_jitter
        raw *= 1.0 - jitter + 2.0 * jitter * rng.random()
    return max(0.0, raw)


def _as_inference_error(exc: Exception) -> Exception:
    if isinstance(exc, (PermanentInferenceError, RetryableInferenceError)):
        return exc
    if isinstance(exc, TimeoutError):
        return RetryableInferenceError("execution timed out", code="execution_timeout")
    return RetryableInferenceError(f"{type(exc).__name__}: {exc}", code="transient")


def _hash_jobs(jobs: Sequence[dict[str, Any]], *, model: str | None, priority: int, scheduling_class: str) -> str:
    hasher = hashlib.sha256()
    for ordinal, raw in enumerate(jobs):
        hasher.update(
            canonical_json(
                {
                    "ordinal": ordinal,
                    "model": raw.get("model") or model,
                    "state": raw.get("state"),
                    "questions": raw.get("questions"),
                    "parameters": raw.get("parameters") or {},
                    "priority": raw.get("priority", priority),
                    "class": raw.get("scheduling_class", scheduling_class),
                }
            ).encode("utf-8")
        )
    return hasher.hexdigest()


def _receipt(batch: Batch) -> BatchReceipt:
    return BatchReceipt(
        batch_id=batch.batch_id,
        job_count=batch.job_count,
        status=batch.status,
        created_at=batch.created_at,
        completion=batch.completion,
        idempotent_replay=batch.replayed,
        counts=dict(batch.counts),
    )


def _job_dict(job: Job) -> dict[str, Any]:
    return {
        "job_id": job.job_id,
        "batch_id": job.batch_id,
        "batch_ordinal": job.batch_ordinal,
        "client_id": job.client_id,
        "state": job.state,
        "requested_model": job.requested_model,
        "resolved_model": job.resolved_model,
        "model_version": job.model_version,
        "priority": job.priority,
        "scheduling_class": job.scheduling_class,
        "attempt_count": job.attempt_count,
        "max_attempts": job.max_attempts,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
        "lease_owner": job.lease_owner,
        "lease_expires_at": job.lease_expires_at,
        "cancel_requested": job.cancel_requested,
        "fallback": job.fallback,
        "error": job.error,
        "result": job.result,
        "payload": job.payload,
        "deadline_at": job.deadline_at,
        "execution_timeout_ms": job.execution_timeout_ms,
        "idempotent_replay": job.replayed,
    }


def _batch_dict(batch: Batch) -> dict[str, Any]:
    counts = dict(batch.counts)
    return {
        "batch_id": batch.batch_id,
        "client_id": batch.client_id,
        "status": batch.status,
        "completion": batch.completion,
        "requested_model": batch.requested_model,
        "job_count": batch.job_count,
        "created_at": batch.created_at,
        "activated_at": batch.activated_at,
        "deadline_at": batch.deadline_at,
        "counts": counts,
        "queued": counts.get("queued", 0) + counts.get("retry_wait", 0) + counts.get("staged", 0),
        "running": counts.get("running", 0) + counts.get("leased", 0),
        "succeeded": counts.get("succeeded", 0),
        "failed": counts.get("failed", 0),
        "dead_letter": counts.get("dead_letter", 0),
        "cancelled": counts.get("cancelled", 0),
        "idempotent_replay": batch.replayed,
    }


def _model_dict(info: Any) -> dict[str, Any]:
    return {
        "id": info.id,
        "adapter": info.adapter,
        "available": info.available,
        "local": info.local,
        "version": info.version,
        "capabilities": list(info.capabilities),
        "max_concurrency": info.max_concurrency,
        "active": info.active,
        "available_slots": info.available_slots,
        "execution": info.execution,
        "health": info.health,
        "endpoint": info.endpoint,
        "supports_native_batch": info.supports_native_batch,
        "enabled": info.enabled,
    }


def keziah_conflict(batch_id: str) -> Any:
    from keziah.errors import ConflictError

    return ConflictError(
        "batch idempotency key was already used with a different payload",
        details={"batch_id": batch_id},
    )
