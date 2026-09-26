"""Regression tests for the 2026-09-26 high findings."""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from keziah.adapters.laya import LayaAdapter
from keziah.adapters.mock import MockAdapter
from keziah.api.app import create_app
from keziah.config import ModelGroupSettings, ModelSettings, SchedulerSettings
from keziah.scheduler import PickerState, select
from keziah.service import Keziah
from keziah.types import Candidate, ModelHealth
from tests.conftest import QUESTIONS, make_settings


def test_aged_normal_does_not_outrank_fresh_interactive() -> None:
    settings = SchedulerSettings()
    state = PickerState()
    now = 1000.0
    counts = {"interactive": 0, "normal": 0}
    for _ in range(1000):
        choice, state = select(
            state,
            "m",
            [
                Candidate("i", "interactive", 0, now, "a", "m", "h"),
                Candidate("n", "normal", 0, now - 120, "b", "m", "h"),
            ],
            now,
            settings,
        )
        assert choice is not None
        counts[choice.scheduling_class] += 1
    assert counts["interactive"] > 500
    assert counts["normal"] > 0


def test_transient_health_failure_does_not_dead_letter(tmp_path) -> None:
    class Flaky(MockAdapter):
        def __init__(self) -> None:
            super().__init__("remote")
            self.health_ok = False

        def health_sync(self) -> ModelHealth:
            return ModelHealth(ok=self.health_ok, permanent=False, detail="probe 5xx")

        def infer_blocking(self, request):  # type: ignore[no-untyped-def]
            self.health_ok = True
            return super().infer_blocking(request)

    adapter = Flaky()
    settings = make_settings(tmp_path, extra_models={"remote": ModelSettings(adapter="mock", max_concurrency=4)})
    settings.scheduler.retry_base_s = 0.05
    with Keziah(settings=settings, adapters={"remote": adapter}) as keziah:
        ids = [keziah.submit(model="remote", state=f"s{i}", questions=QUESTIONS) for i in range(5)]
        results = [keziah.wait(job_id, timeout=5) for job_id in ids]
    assert {item.status for item in results} == {"succeeded"}
    assert adapter.calls == 5


def test_one_lost_lease_does_not_block_the_next_attempt(tmp_path) -> None:
    gate = threading.Event()
    settings = make_settings(tmp_path, lease_seconds=0.4, max_attempts=3)
    adapter = MockAdapter("mock", gate=gate)
    with Keziah(settings=settings, adapters={"mock": adapter}) as keziah:
        job_id = keziah.submit(model="mock", state="x", questions=QUESTIONS)
        owner = ""
        for _ in range(50):
            job = keziah.backend.get_job(job_id)
            if job is not None and job.lease_owner and job.state == "running":
                owner = job.lease_owner
                break
            time.sleep(0.02)
        assert owner
        keziah._lost.add((job_id, owner))
        gate.set()
        result = keziah.wait(job_id, timeout=10)
        assert result.status == "succeeded"
        assert keziah._lost == set()


def test_maintenance_deadline_wakes_wait_and_callback(tmp_path) -> None:
    gate = threading.Event()
    adapter = MockAdapter("mock", gate=gate)
    settings = make_settings(tmp_path, concurrency=1)
    with Keziah(settings=settings, adapters={"mock": adapter}) as keziah:
        blocker = keziah.submit(model="mock", state="block", questions=QUESTIONS)
        time.sleep(0.2)
        fired: list[object] = []
        deadline = (datetime.now(timezone.utc) + timedelta(milliseconds=300)).isoformat()
        job_id = keziah.submit(
            model="mock",
            state="late",
            questions=QUESTIONS,
            deadline_at=deadline,
            callback=fired.append,
        )
        started = time.monotonic()
        result = keziah.wait(job_id, timeout=3)
        elapsed = time.monotonic() - started
        gate.set()
        keziah.wait(blocker, timeout=5)
    assert result.status == "failed"
    assert result.error and result.error.get("code") == "deadline"
    assert elapsed < 1.5
    assert fired


def test_deadline_offset_runs_and_garbage_is_rejected(tmp_path) -> None:
    from keziah.errors import ValidationError

    settings = make_settings(tmp_path)
    with Keziah(settings=settings) as keziah:
        future = datetime.now(timezone(timedelta(hours=-5))) + timedelta(minutes=30)
        result = keziah.wait(
            keziah.submit(model="mock", state="x", questions=QUESTIONS, deadline_at=future.isoformat()),
            timeout=5,
        )
        assert result.status == "succeeded"
        past = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        expired = keziah.wait(
            keziah.submit(model="mock", state="old", questions=QUESTIONS, deadline_at=past),
            timeout=5,
        )
        assert expired.status == "failed"
        assert expired.error and expired.error.get("code") == "deadline"
        try:
            keziah.submit(model="mock", state="y", questions=QUESTIONS, deadline_at="not a date")
        except ValidationError:
            pass
        else:
            raise AssertionError("garbage deadline was accepted")


def test_laya_batch_forwards_per_item_parameters() -> None:
    seen: list[list[dict]] = []

    class FakeRouter:
        def predict_batch(self, requests):  # type: ignore[no-untyped-def]
            seen.append([{key: value for key, value in item.items() if key not in {"state", "questions"}} for item in requests])
            return [{"answers": {"q": {"type": "noul", "noul": 0.5}}} for _ in requests]

    adapter = LayaAdapter("laya")
    adapter._router = FakeRouter()
    from keziah.types import SystemOneRequest

    questions = {"q": {"type": "noul", "instructions": "i"}}
    adapter.infer_many_blocking(
        [
            SystemOneRequest("a", questions, "laya", {"checkpoint": "multilingual", "lang": "fr"}),
            SystemOneRequest("b", questions, "laya", {"checkpoint": "english", "lang": "en"}),
        ]
    )
    assert seen == [[{"model": "multilingual", "lang": "fr"}, {"model": "english", "lang": "en"}]]


def test_different_checkpoints_are_not_coalesced(tmp_path) -> None:
    from keziah.service import _coalesce_hash

    questions = {"q": {"type": "noul", "instructions": "i"}}
    left = _coalesce_hash(questions, {"checkpoint": "multilingual", "lang": "fr"})
    right = _coalesce_hash(questions, {"checkpoint": "english", "lang": "en"})
    assert left != right
    solo_a = _coalesce_hash(questions, {"max_len": 128})
    solo_b = _coalesce_hash(questions, {"max_len": 128})
    assert solo_a != solo_b


def test_shutdown_releases_the_attempt(tmp_path) -> None:
    gate = threading.Event()
    settings = make_settings(tmp_path, mode="disk", max_attempts=1)
    keziah = Keziah(settings=settings, adapters={"mock": MockAdapter("mock", gate=gate)})
    job_id = keziah.submit(model="mock", state="x", questions=QUESTIONS)
    time.sleep(0.3)
    keziah.shutdown(grace_s=0.2)
    gate.set()
    reopened = Keziah(settings=make_settings(tmp_path, mode="disk", max_attempts=1), start_workers=False)
    try:
        job = reopened.backend.get_job(job_id)
        assert job is not None
        assert job.state == "queued"
        assert job.attempt_count == 0
    finally:
        reopened.shutdown()
    finished = Keziah(settings=make_settings(tmp_path, mode="disk", max_attempts=1))
    try:
        result = finished.wait(job_id, timeout=5)
        assert result.status == "succeeded"
    finally:
        finished.shutdown()


def test_fallback_respects_its_concurrency_cap(tmp_path) -> None:
    class Toggle(MockAdapter):
        ok = True

        def health_sync(self) -> ModelHealth:
            return ModelHealth(ok=type(self).ok, permanent=False)

    peak = {"now": 0, "max": 0}
    lock = threading.Lock()

    class Counting(MockAdapter):
        async def infer(self, request):  # type: ignore[no-untyped-def]
            import asyncio

            with lock:
                peak["now"] += 1
                peak["max"] = max(peak["max"], peak["now"])
            await asyncio.sleep(0.05)
            with lock:
                peak["now"] -= 1
            return await super().infer(request)

    settings = make_settings(
        tmp_path,
        extra_models={
            "prim": ModelSettings(adapter="mock", max_concurrency=6),
            "fb": ModelSettings(adapter="mock", max_concurrency=1),
        },
        groups={"grp": ModelGroupSettings(primary="prim", fallbacks=["fb"])},
    )
    with Keziah(settings=settings, adapters={"prim": Toggle("prim"), "fb": Counting("fb")}, start_workers=False) as keziah:
        ids = [keziah.submit(model="grp", state=f"s{i}", questions=QUESTIONS) for i in range(6)]
        Toggle.ok = False
        keziah.start()
        results = [keziah.wait(job_id, timeout=10) for job_id in ids]
    assert peak["max"] <= 1
    assert {item.status for item in results} == {"succeeded"}


def test_malformed_batches_are_422(tmp_path) -> None:
    settings = make_settings(tmp_path)
    keziah = Keziah(settings=settings)
    try:
        with TestClient(create_app(service=keziah), raise_server_exceptions=False) as client:
            missing = client.post("/v1/batches", json={"jobs": [{"state": "x"}]})
            invalid = client.post("/v1/batches", content=b"{not json", headers={"content-type": "application/json"})
            ndjson = client.post(
                "/v1/batches?model=mock",
                content=b'{"state":"x","questions":{"q":{"type":"noul","instructions":"i"}},"priority":"high"}\n',
                headers={"content-type": "application/x-ndjson"},
            )
        for response in (missing, invalid, ndjson):
            assert response.status_code == 422
            assert response.json()["detail"]["code"] == "invalid_request"
    finally:
        keziah.shutdown()
