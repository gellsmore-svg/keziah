# Configuration

Precedence, highest first:

1. CLI flags (`--mode`, `--host`, `--port`, `--api-key`, `--config`)
2. Environment variables
3. YAML file (`--config`, or `KEZIAH_CONFIG`)
4. Built-in defaults

The sample file is `keziah.example.yaml`. A file that sets `models` replaces the default catalogue. It does not merge halfway with the built-in mock, Laya, and Jev entries. Aliases and groups are replaced the same way when those keys are present.

## Environment

| Variable | Effect |
| --- | --- |
| `KEZIAH_MODE` | `memory`, `disk`, or `hybrid` |
| `KEZIAH_DB` | SQLite path |
| `KEZIAH_HOST` | bind address |
| `KEZIAH_PORT` | bind port |
| `KEZIAH_API_KEY` | bearer token for the listener |
| `KEZIAH_LOG_LEVEL` | log level |
| `KEZIAH_CONFIG` | path to YAML |
| `TYPESAFE_API_KEY`, `JEV_API_KEY` | Jev credentials, read by the adapter |

Model blocks point at secrets with `api_key_env`. Put the secret in the environment, not in the YAML you commit.

## Scheduler knobs that matter

- `max_queued_jobs`, `max_batch_size`, `max_request_bytes`, `max_batch_bytes`, `max_active_jobs_per_client` reject extra work with a machine-readable `backpressure` error.
- `class_weights` default to interactive 8, normal 4, batch 2, bulk 1.
- `client_skip_weight` is how fast a skipped client catches a higher-priority neighbour.
- `starvation_seconds` and `starvation_boost_cap` raise a class's weight when its oldest job has waited.
- `disk_candidates_per_class` is the SQL window used in disk mode. Hybrid does not use that window; it tracks ready jobs in memory.
- `lease_seconds` and `recovery_interval_s` control crash recovery. The recovery interval is a fallback wake, not a hot poll.

## Defaults

With no file, Keziah configures `mock` (enabled, in-process), `laya` (enabled, unavailable until the package imports), and `jev` (enabled, unavailable until a key is set). The alias `default` points at `mock`.
