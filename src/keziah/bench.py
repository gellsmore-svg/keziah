"""Benchmark harness.

Reports measured timings for this process. It does not invent a performance target.
``mock`` is the default and stays on this machine. ``laya`` uses the local Laya
adapter when that package is installed. ``jev`` uses the configured HTTP adapter
and does nothing useful without an API key.
"""

from __future__ import annotations

import os
import statistics
import tempfile
import time
from pathlib import Path
from typing import Any

from keziah.adapters import build_adapter
from keziah.config import ModelSettings, QueueSettings, SchedulerSettings, Settings
from keziah.service import Keziah
from keziah.types import SystemOneRequest


def run_benchmark(
    *,
    jobs: int = 100,
    model: str = "mock",
    mode: str = "memory",
    concurrency: int | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    if jobs < 1:
        raise ValueError("jobs must be >= 1")
    spec = _spec_for(model, settings, concurrency)
    # Criteria, not an options list: Laya rejects a choice that has no criteria,
    # and the raw adapter call does not pass through the queue normaliser.
    questions = {
        "department": {
            "type": "choice",
            "instructions": "Which team should handle this?",
            "criteria": {
                "billing": "invoices, payments, refunds",
                "technical": "bugs, outages, errors",
                "sales": "pricing, contracts, new accounts",
            },
        }
    }
    directory = Path(tempfile.mkdtemp(prefix="keziah-bench-"))
    # One model only. A full config would also construct Laya and Jev and
    # spend their startup on a mock run.
    bench_settings = Settings(
        queue=QueueSettings(mode=mode, sqlite_path=str(directory / "bench.db")),  # type: ignore[arg-type]
        scheduler=SchedulerSettings(
            retry_jitter=0.0,
            recovery_interval_s=0.05,
            default_max_attempts=1,
            default_execution_timeout_ms=max(spec.timeout_ms, 120_000),
            lease_seconds=180.0,
        ),
        models={model: spec},
    )
    adapter = build_adapter(model, spec)
    warmup_s = _warm(adapter)
    request = SystemOneRequest(state={"message": "benchmark"}, questions=questions, model=model)
    raw_samples: list[float] = []
    raw_count = jobs if spec.adapter != "mock" else min(jobs, 50)
    for _ in range(raw_count):
        started = time.perf_counter()
        adapter.infer_blocking(request)
        raw_samples.append((time.perf_counter() - started) * 1000.0)

    submit_samples: list[float] = []
    wait_s = 30.0 if spec.adapter == "mock" else 180.0
    started_all = time.perf_counter()
    with Keziah(settings=bench_settings, adapters={model: adapter}) as service:
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
        results = [service.wait(job_id, timeout=wait_s) for job_id in ids]
        elapsed = time.perf_counter() - started_all
    e2e = [float((item.timings or {}).get("total_ms") or 0) for item in results]
    queue_ms = [float((item.timings or {}).get("queue_ms") or 0) for item in results]
    infer_ms = [float((item.timings or {}).get("inference_ms") or 0) for item in results]
    failed = [item for item in results if item.status != "succeeded"]
    raw_mean = _summary(raw_samples)["mean"]
    e2e_mean = _summary(e2e)["mean"]
    overhead = None
    if raw_mean and e2e_mean is not None and raw_mean > 0:
        overhead = round(e2e_mean / raw_mean, 3)
    return {
        "mode": bench_settings.queue.mode,
        "model": model,
        "adapter": spec.adapter,
        "jobs": jobs,
        "concurrency": spec.max_concurrency,
        "failed": len(failed),
        "failure_codes": [((item.error or {}).get("code")) for item in failed],
        "warmup_seconds": None if warmup_s is None else round(warmup_s, 4),
        "raw_adapter_latency_ms": _summary(raw_samples),
        "submit_latency_ms": _summary(submit_samples),
        "e2e_latency_ms": _summary(e2e),
        "queue_latency_ms": _summary(queue_ms),
        "inference_latency_ms": _summary(infer_ms),
        "e2e_over_raw": overhead,
        "throughput_jobs_per_second": round(jobs / elapsed, 3) if elapsed else None,
        "wall_seconds": round(elapsed, 6),
    }


def _spec_for(model: str, settings: Settings | None, concurrency: int | None) -> ModelSettings:
    if settings is not None and model in settings.models:
        spec = settings.models[model].model_copy(deep=True)
    elif model == "laya":
        spec = ModelSettings(
            adapter="laya",
            execution="thread",
            max_concurrency=1,
            local=True,
            supports_native_batch=False,
            max_native_batch_size=1,
            preload=True,
            device=os.environ.get("LAYA_DEVICE") or "cpu",
            timeout_ms=120_000,
        )
    elif model == "jev":
        spec = ModelSettings(
            adapter="jev",
            execution="async",
            max_concurrency=4,
            local=False,
            endpoint="https://api.typesafe.ai",
            api_key_env="TYPESAFE_API_KEY",
            model_name="jev-latest",
            timeout_ms=30_000,
        )
    else:
        spec = ModelSettings(
            adapter="mock",
            execution="async",
            max_concurrency=4,
            local=True,
            supports_native_batch=False,
            max_native_batch_size=1,
        )
    if concurrency is not None:
        spec = spec.model_copy(update={"max_concurrency": concurrency})
    elif spec.adapter == "laya":
        spec = spec.model_copy(update={"max_concurrency": 1, "execution": "thread", "preload": True})
    if spec.adapter == "laya" and not spec.device:
        spec = spec.model_copy(update={"device": os.environ.get("LAYA_DEVICE") or "cpu"})
    return spec


def _warm(adapter: Any) -> float | None:
    warm = getattr(adapter, "warm", None)
    if warm is None:
        return None
    started = time.perf_counter()
    warm()
    return time.perf_counter() - started


def _summary(samples: list[float]) -> dict[str, float | None]:
    if not samples:
        return {"mean": None, "p50": None, "p99": None}
    ordered = sorted(samples)

    def pct(fraction: float) -> float:
        index = min(len(ordered) - 1, max(0, int(round(fraction * (len(ordered) - 1)))))
        return round(ordered[index], 4)

    return {"mean": round(statistics.fmean(samples), 4), "p50": pct(0.50), "p99": pct(0.99)}
