# Deployment

Keziah is a single process. Install it with pip or pipx into a virtualenv. Docker is optional.

## Modes

```bash
keziah serve --mode memory
keziah serve --mode disk
keziah serve --mode hybrid
keziah serve --mode hybrid --host 0.0.0.0 --port 8766
```

`memory` is for tests and throwaway work. `disk` and `hybrid` use `~/.keziah/keziah.db` unless `KEZIAH_DB` or `queue.sqlite_path` says otherwise. Hybrid is the recommended local durable mode: SQLite is authoritative and the ready set is indexed in RAM.

Do not point two Keziah processes at the same SQLite file. Staging recovery assumes one owner. A future Redis or Postgres backend is the path for several writers, not a second process on this file.

## Network

The default bind is `127.0.0.1`. `0.0.0.0` is something you type on purpose. If that bind has no API key, the log says so at startup.

Set `KEZIAH_API_KEY` for any listener that other machines can reach. The client sends `Authorization: Bearer ...`. Logs redact bearer tokens and values that look like `api_key=...`. Request bodies are not logged, because a state can contain customer text.

TLS is not terminated inside Keziah 0.1. Put a reverse proxy in front if the connection leaves the machine. The proxy should pass `Authorization` through and should not log it either.

A model being down does not fail the process. `/health/live` answers when the process is up. `/health/ready` answers when the dispatcher and the queue are usable. Model health is on `/v1/models`.

## Shutdown

On SIGINT or SIGTERM the server stops accepting work, stops claiming, waits up to `scheduler.shutdown_grace_s` for calls already running, releases leases it still holds, closes adapters, and closes the database. The next start continues queued work.

## Docker

`Dockerfile` builds a non-root image. `docker-compose.yml` publishes `127.0.0.1:8766` and stores the database in a volume. Inside the container the process listens on `0.0.0.0` so the published port works. Set `KEZIAH_API_KEY` if you change the host publish away from loopback.

## Metrics and logs

Logs are JSON on stderr when `server.log_json` is true. Fields that show up when present: `request_id`, `job_id`, `batch_id`, `client_id`, `model`, `adapter`, `attempt`, `worker`.

Scrape `GET /metrics`. Useful series include queue depth, submit/start/success/fail/retry/cancel/dead-letter counters, latency histograms, and per-model capacity. Do not add a job id as a label.

## Benchmark

```bash
keziah benchmark --jobs 100 --mode memory
keziah benchmark --jobs 100 --mode hybrid --output bench.json
keziah benchmark --model laya --jobs 8 --mode memory --concurrency 1
```

The default model is `mock`. `--model laya` loads the local checkpoint once (`warmup_seconds` in the report) and then times both the adapter and the queue. `--model jev` calls the real API and needs a key; without one the jobs fail instead of falling back. The JSON report is what this process measured. It is not a pass/fail threshold.
