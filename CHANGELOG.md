# Changelog

## 0.1.1

- Jev health is a cached `GET /v1/models` probe, not a check that only looks for an API key, and not a call per job.
- `keziah benchmark --model laya` measures the local Laya checkpoint. The default remains the offline mock.

## 0.1.0

First release.

- Memory, SQLite, and hybrid queues for System-1 jobs.
- Single submission, transactional batch visibility, and streamed JSONL.
- Explicit model selection, aliases, and opt-in fallback groups.
- Adapters for Laya, Jev, a generic System-1 HTTP endpoint, and an offline mock.
- Per-model concurrency, scheduling classes, leases, retries, dead letters, cancellation, and idempotency.
- Embedded Python API, HTTP API with OpenAPI, a small client, and a CLI.
- Health, Prometheus-style metrics, structured logs, retention cleanup, and schema migrations.
- Tests that run without a GPU, Laya weights, or Jev credentials.
