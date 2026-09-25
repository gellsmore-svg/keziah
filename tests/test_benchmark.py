"""Benchmark smoke test. It checks the report shape, not a made-up speed."""

from keziah.bench import run_benchmark


def test_benchmark_smoke(tmp_path) -> None:
    report = run_benchmark(jobs=20, mode="memory", concurrency=2)
    assert report["failed"] == 0
    assert report["jobs"] == 20
    assert report["throughput_jobs_per_second"] > 0
    assert "mean" in report["raw_adapter_latency_ms"]
    assert "mean" in report["submit_latency_ms"]
    assert "mean" in report["queue_latency_ms"]
    disk = run_benchmark(jobs=10, mode="disk", concurrency=2)
    hybrid = run_benchmark(jobs=10, mode="hybrid", concurrency=2)
    assert disk["failed"] == 0
    assert hybrid["failed"] == 0
    assert disk["mode"] == "disk"
    assert hybrid["mode"] == "hybrid"
