"""Service behaviour with the mock adapter: retries, fairness, fallback, recovery."""

from __future__ import annotations

import threading
import time

import pytest

from keziah.adapters.mock import MockAdapter
from keziah.clock import ManualClock, isoformat
from keziah.config import ModelGroupSettings, ModelSettings
from keziah.errors import BackpressureError, ConflictError, ModelUnknownError, PermanentInferenceError, RetryableInferenceError
from keziah.service import Keziah
from keziah.types import AdapterCapabilities, ModelHealth
from tests.conftest import make_settings


def test_single_and_batch_round_trip(tmp_path, questions) -> None:
    settings = make_settings(tmp_path, "memory")
    with Keziah(settings=settings) as service:
        job_id = service.submit(model="mock", state={"message": "hello"}, questions=questions, client_id="app-a")
        result = service.wait(job_id, timeout=5)
        assert result.status == "succeeded"
        assert result.response is not None
        assert set(result.response["answers"]) == set(questions)
        assert result.requested_model == "mock"
        assert result.resolved_model == "mock"
        receipt = service.submit_batch(
            [
                {"state": {"message": "one"}, "questions": questions, "priority": 0},
                {"state": {"message": "two"}, "questions": questions, "priority": 10, "scheduling_class": "interactive"},
            ],
            model="mock",
        )
        rows = service.wait_batch(receipt.batch_id, timeout=5)
        assert [row.batch_ordinal for row in rows] == [0, 1]
        assert [row.status for row in rows] == ["succeeded", "succeeded"]
        replay = service.submit(
            model="mock",
            state={"message": "hello"},
            questions=questions,
            client_id="app-a",
            idempotency_key="same",
        )
        # First call had no key. A keyed call is new. Repeating it replays.
        again = service.submit(
            model="mock",
            state={"message": "hello"},
            questions=questions,
            client_id="app-a",
            idempotency_key="same",
        )
        assert replay == again
        with pytest.raises(ConflictError):
            service.submit(
                model="mock",
                state={"message": "different"},
                questions=questions,
                client_id="app-a",
                idempotency_key="same",
            )


def test_retry_then_dead_letter_and_requeue(tmp_path, questions) -> None:
    settings = make_settings(tmp_path, "disk", max_attempts=2)
    adapter = MockAdapter(
        script=[
            RetryableInferenceError("connection reset", code="connection"),
            RetryableInferenceError("still down", code="connection"),
            None,
        ]
    )
    with Keziah(settings=settings, adapters={"mock": adapter}) as service:
        job_id = service.submit(model="mock", state="x", questions=questions)
        result = service.wait(job_id, timeout=5)
        assert result.status == "dead_letter"
        assert result.attempts == 2
        service.requeue(job_id)
        recovered = service.wait(job_id, timeout=5)
        assert recovered.status == "succeeded"


def test_permanent_failure_is_not_retried(tmp_path, questions) -> None:
    settings = make_settings(tmp_path, "memory", max_attempts=4)
    adapter = MockAdapter(script=[PermanentInferenceError("bad question", code="invalid_request")])
    with Keziah(settings=settings, adapters={"mock": adapter}) as service:
        job_id = service.submit(model="mock", state="x", questions=questions)
        result = service.wait(job_id, timeout=5)
        assert result.status == "failed"
        assert result.attempts == 1
        assert adapter.calls == 1


def test_unknown_model_and_no_silent_fallback(tmp_path, questions) -> None:
    settings = make_settings(
        tmp_path,
        "memory",
        extra_models={"other": ModelSettings(adapter="mock", execution="async", max_concurrency=2)},
    )
    down = _DownAdapter()
    other = MockAdapter("other")
    with Keziah(settings=settings, adapters={"mock": MockAdapter("mock"), "down": down, "other": other}) as service:
        pass
    settings = make_settings(
        tmp_path,
        "memory",
        extra_models={"down": ModelSettings(adapter="mock", execution="async", max_concurrency=1)},
    )
    down = _DownAdapter()
    mock = MockAdapter("mock")
    with Keziah(settings=settings, adapters={"mock": mock, "down": down}) as service:
        with pytest.raises(ModelUnknownError):
            service.submit(model="missing", state="x", questions=questions)
        job_id = service.submit(model="down", state="x", questions=questions)
        result = service.wait(job_id, timeout=5)
        assert result.status == "failed"
        assert result.resolved_model == "down"
        assert mock.calls == 0


def test_explicit_group_fallback(tmp_path, questions) -> None:
    settings = make_settings(
        tmp_path,
        "hybrid",
        extra_models={"down": ModelSettings(adapter="mock", execution="async", max_concurrency=1)},
        groups={"emotion": ModelGroupSettings(primary="down", fallbacks=["mock"])},
    )
    down = _DownAdapter()
    mock = MockAdapter("mock")
    with Keziah(settings=settings, adapters={"mock": mock, "down": down}) as service:
        job_id = service.submit(model="emotion", state={"message": "fallback"}, questions=questions)
        result = service.wait(job_id, timeout=5)
        assert result.status == "succeeded"
        assert result.requested_model == "emotion"
        assert result.resolved_model == "mock"
        assert result.fallback_used is True
        assert mock.calls == 1


def test_concurrency_limit(tmp_path, questions) -> None:
    settings = make_settings(tmp_path, "memory", concurrency=2, execution="thread")
    gate = threading.Event()
    adapter = MockAdapter(gate=gate)
    with Keziah(settings=settings, adapters={"mock": adapter}) as service:
        ids = [service.submit(model="mock", state={"n": index}, questions=questions) for index in range(5)]
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and adapter.entered < 2:
            time.sleep(0.01)
        assert adapter.entered == 2
        running = [service.get(job_id).status for job_id in ids]
        assert running.count("running") + running.count("leased") <= 2
        gate.set()
        results = [service.wait(job_id, timeout=5) for job_id in ids]
        assert {item.status for item in results} == {"succeeded"}


def test_interactive_jumps_the_queue(tmp_path, questions) -> None:
    settings = make_settings(tmp_path, "memory", concurrency=1)
    order: list[str] = []
    lock = threading.Lock()

    def remember(result) -> None:
        with lock:
            order.append(result.scheduling_class)

    service = Keziah(settings=settings, start_workers=False)
    try:
        for _ in range(12):
            service.submit(model="mock", state="b", questions=questions, scheduling_class="bulk", callback=remember)
        service.submit(model="mock", state="i", questions=questions, scheduling_class="interactive", callback=remember)
        service.start()
        service.wait_batch  # attribute exists; wait each job via stats instead
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and len(order) < 13:
            time.sleep(0.01)
        assert len(order) == 13
        assert order[0] == "interactive"
        assert "bulk" in order
    finally:
        service.shutdown()


def test_two_clients_both_make_progress(tmp_path, questions) -> None:
    settings = make_settings(tmp_path, "memory", concurrency=1)
    order: list[str] = []
    lock = threading.Lock()

    def remember(result) -> None:
        with lock:
            order.append(result.client_id)

    service = Keziah(settings=settings, start_workers=False)
    try:
        for index in range(8):
            service.submit(model="mock", state={"n": index}, questions=questions, client_id="alpha", callback=remember)
            service.submit(model="mock", state={"n": index}, questions=questions, client_id="beta", callback=remember)
        service.start()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and len(order) < 16:
            time.sleep(0.01)
        assert order.count("alpha") == 8
        assert order.count("beta") == 8
        longest = 1
        current = 1
        for previous, client in zip(order, order[1:]):
            current = current + 1 if client == previous else 1
            longest = max(longest, current)
        assert longest <= 3
    finally:
        service.shutdown()


def test_cancel_queued_work(tmp_path, questions) -> None:
    settings = make_settings(tmp_path, "disk", concurrency=1, execution="thread")
    gate = threading.Event()
    adapter = MockAdapter(gate=gate)
    with Keziah(settings=settings, adapters={"mock": adapter}) as service:
        first = service.submit(model="mock", state="block", questions=questions)
        second = service.submit(model="mock", state="later", questions=questions)
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and service.get(first).status not in {"leased", "running"}:
            time.sleep(0.01)
        cancelled = service.cancel(second)
        assert cancelled.status == "cancelled"
        gate.set()
        assert service.wait(first, timeout=5).status == "succeeded"
        assert service.get(second).status == "cancelled"


def test_restart_recovers_leased_work(tmp_path, questions) -> None:
    settings = make_settings(tmp_path, "hybrid", concurrency=1, execution="thread", lease_seconds=30)
    gate = threading.Event()
    adapter = MockAdapter(gate=gate)
    service = Keziah(settings=settings, adapters={"mock": adapter})
    job_id = service.submit(model="mock", state="crash", questions=questions)
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline and service.get(job_id).status not in {"leased", "running"}:
        time.sleep(0.01)
    service.shutdown(grace_s=0)
    gate.set()
    with Keziah(settings=settings, adapters={"mock": MockAdapter()}) as restarted:
        result = restarted.wait(job_id, timeout=5)
        assert result.status == "succeeded"
        assert result.attempts >= 1


def test_backpressure(tmp_path, questions) -> None:
    settings = make_settings(tmp_path, "memory", max_queued=1, concurrency=1, execution="thread")
    gate = threading.Event()
    with Keziah(settings=settings, adapters={"mock": MockAdapter(gate=gate)}) as service:
        service.submit(model="mock", state="a", questions=questions)
        with pytest.raises(BackpressureError):
            service.submit(model="mock", state="b", questions=questions)
        gate.set()


def test_bad_batch_is_not_visible(tmp_path, questions) -> None:
    settings = make_settings(tmp_path, "sqlite" if False else "disk")
    with Keziah(settings=settings) as service:
        with pytest.raises(Exception):
            service.submit_batch(
                [
                    {"state": "ok", "questions": questions},
                    {"state": "bad", "questions": {}},
                ],
                model="mock",
            )
        assert service.stats()["jobs_by_state"].get("queued", 0) == 0
        assert service.stats()["jobs_by_state"].get("succeeded", 0) == 0


def test_cleanup_does_not_delete_queued(tmp_path, questions) -> None:
    clock = ManualClock()
    settings = make_settings(tmp_path, "disk")
    settings.retention.success_seconds = 1
    service = Keziah(settings=settings, clock=clock, start_workers=False)
    try:
        finished = service.submit(model="mock", state="done", questions=questions)
        service.start()
        # The manual clock does not move, but the mock does not need it.
        # Release the gate only if a worker is blocked. start_workers False then start uses async mock without gate wait if gate is None.
        result = service.wait(finished, timeout=5)
        assert result.status == "succeeded"
        clock.advance(5)
        service._stop = True
        service._signal()
        removed = service.cleanup()
        assert removed["jobs"] >= 1
        held = service.backend.enqueue  # queue is open; insert through submit after re-enabling accept
        service._accepting = True
        service._stop = True
        # Direct backend insert stays queued because the dispatcher has been asked to stop.
        from keziah.service import _new_id
        from keziah.types import Job

        now = isoformat(clock.now())
        queued = Job(
            job_id=_new_id("job"),
            batch_id=None,
            batch_ordinal=None,
            idempotency_key=None,
            idempotency_hash=None,
            client_id="app",
            created_at=now,
            updated_at=now,
            started_at=None,
            finished_at=None,
            not_before=None,
            priority=0,
            scheduling_class="bulk",
            requested_model="mock",
            resolved_model="mock",
            model_version=None,
            state="queued",
            payload={"state": "stay", "questions": questions, "parameters": {}},
            questions_hash="stay",
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
        service.backend.enqueue(queued)
        removed_again = service.cleanup()
        assert removed_again["jobs"] == 0
        kept = service.backend.get_job(queued.job_id)
        assert kept is not None and kept.state == "queued"
        assert held
    finally:
        service.shutdown()


def test_batch_idempotency(tmp_path, questions) -> None:
    settings = make_settings(tmp_path, "hybrid")
    with Keziah(settings=settings) as service:
        jobs = [{"state": {"n": index}, "questions": questions} for index in range(4)]
        first = service.submit_batch(jobs, model="mock", idempotency_key="batch-1")
        second = service.submit_batch(jobs, model="mock", idempotency_key="batch-1")
        assert second.idempotent_replay
        assert second.batch_id == first.batch_id
        with pytest.raises(ConflictError):
            service.submit_batch([{"state": "other", "questions": questions}], model="mock", idempotency_key="batch-1")


class _DownAdapter:
    def __init__(self) -> None:
        self.model_id = "down"
        self.calls = 0
        self.capabilities = AdapterCapabilities(local=True, supports_native_batch=False, recommended_concurrency=1)

    def health_sync(self) -> ModelHealth:
        return ModelHealth(ok=False, permanent=True, detail="unavailable")

    async def health(self) -> ModelHealth:
        return self.health_sync()

    def infer_blocking(self, request):
        self.calls += 1
        raise AssertionError("down adapter should not run")

    async def infer(self, request):
        return self.infer_blocking(request)

    async def shutdown(self) -> None:
        return None
