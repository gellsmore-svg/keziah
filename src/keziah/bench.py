"""Benchmark harness.

Reports measured timings for this process. It does not invent a performance target.
The default model is the in-process mock, so the numbers describe queue overhead
rather than Laya or Jev.
"""

from __future__ import annotations

import statistics
import tempfile
import time
from pathlib import Path
from typing import Any

from keziah.adapters.mock import MockAdapter
from keziah.config import ModelSettings, QueueSettings, SchedulerSettings, Settings
from keziah.service import Keziah
from keziah.types import SystemOneRequest


def run_benchmark(
    *,
    jobs: int = 100,
    model: str = "mock",
    mode: str = "memory",
    concurrency: int = 4,
    settings: Settings | None = None,
) -> dict[str, Any]:
    if jobs < 1:
        raise ValueError("jobs must be >= 1")
    questions = {
        "department": {
            "type": "choice",
            "instructions": "Which team?",
            "options": ["billing", "technical", "sales"],
        }
    }
    state = {"message": "benchmark"}
    adapter = MockAdapter(model, supports_native_batch=False)
    raw_samples = []
    request = SystemOneRequest(state=state, questions=questions, model=model)
    for _ in range(min(jobs, 50)):
        started = time.perf_counter()
        adapter.infer_blocking(request)
        raw_samples.append((time.perf_counter() - started) * 1000.0)

    own_settings = settings if _mock_only(settings, model) else None
    if own_settings is None:
        directory = Path(tempfile.mkdtemp(prefix="keziah-bench-"))
        own_settings = Settings(
            queue=QueueSettings(mode=mode, sqlite_path=str(directory / "bench.db")),  # type: ignore[arg-type]
            scheduler=SchedulerSettings(
                retry_jitter=0.0,
                recovery_interval_s=0.05,
                default_max_attempts=1,
            ),
            models={
                model: ModelSettings(
                    adapter="mock",
                    execution="async",
                    max_concurrency=concurrency,
                    local=True,
                    supports_native_batch=False,
                    max_native_batch_size=1,
                )
            },
        )
    submit_samples: list[float] = []
    started_all = time.perf_counter()
    with Keziah(settings=own_settings, adapters={model: adapter}) as service:
        ids: list[str] = []
        for index in range(jobs):
            t0 = time.perf_counter()
            job_id = service.submit(
                model=model,
                state={"message": "benchmark", "n": index},
                questions=questions,
                scheduling_class="batch",
            )
            submit_samples.append((time.perf_counter() - t0) * 1000.0)
            ids.append(job_id)
        results = [service.wait(job_id, timeout=30) for job_id in ids]
        elapsed = time.perf_counter() - started_all
    e2e = [float((item.timings or {}).get("total_ms") or 0) for item in results]
    queue_ms = [float((item.timings or {}).get("queue_ms") or 0) for item in results]
    infer_ms = [float((item.timings or {}).get("inference_ms") or 0) for item in results]
    failed = [item.job_id for item in results if item.status != "succeeded"]
    return {
        "mode": own_settings.queue.mode,
        "model": model,
        "jobs": jobs,
        "concurrency": concurrency,
        "failed": len(failed),
        "raw_adapter_latency_ms": _summary(raw_samples),
        "submit_latency_ms": _summary(submit_samples),
        "e2e_latency_ms": _summary(e2e),
        "queue_latency_ms": _summary(queue_ms),
        "inference_latency_ms": _summary(infer_ms),
        "throughput_jobs_per_second": round(jobs / elapsed, 3) if elapsed else None,
        "wall_seconds": round(elapsed, 6),
    }


def _mock_only(settings: Settings | None, model: str) -> bool:
    if settings is None:
        return False
    spec = settings.models.get(model)
    return spec is not None and spec.adapter == "mock" and list(settings.models) == [model]


def _summary(samples: list[float]) -> dict[str, float | None]:
    if not samples:
        return {"mean": None, "p50": None, "p99": None}
    ordered = sorted(samples)

    def pct(fraction: float) -> float:
        index = min(len(ordered) - 1, max(0, int(round(fraction * (len(ordered) - 1)))))
        return round(ordered[index], 4)

    return {"mean": round(statistics.fmean(samples), 4), "p50": pct(0.50), "p99": pct(0.99)}
