# HTTP API

FastAPI serves the API and writes OpenAPI at `/openapi.json` and `/docs`. OpenAPI is the standard description of *this* service. The routes are not an OpenAI-compatible chat API, and they are not Jev's own URL. Jev's URL is an adapter target. Keziah's URL is the queue.

Default base: `http://127.0.0.1:8766`.

If `KEZIAH_API_KEY` or `server.api_key` is set, send `Authorization: Bearer <token>`. `/health/live` stays open so a process probe does not need the token. Other routes, including `/metrics`, require it.

## Calls

`POST /v1/systemone` submits one decision and waits. The job still enters the queue and takes a slot. Body fields include `model`, `state`, `questions`, optional `parameters`, `client_id`, `priority`, `scheduling_class`, `idempotency_key`, `execution_timeout_ms`, `deadline_at`, `max_attempts`, and `timeout_s` (how long this HTTP call waits).

`POST /v1/jobs` returns `202` with `job_id`, `status`, and `created_at`.

`POST /v1/batches` accepts either:

```json
{"model": "laya", "jobs": [ {"state": "...", "questions": {}} ]}
```

or `Content-Type: application/x-ndjson`, one job per line. Query parameters `model`, `client_id`, `scheduling_class`, and `idempotency_key` apply to the stream.

`GET /v1/batches/{id}/results?offset=0&limit=100` returns results in `batch_ordinal` order.

`DELETE /v1/jobs/{id}` and `DELETE /v1/batches/{id}` cancel. `POST /v1/jobs/{id}/requeue` retries a failed or dead-letter job.

`GET /v1/models` and `GET /v1/models/{id}` discover configured models.

`GET /v1/queue`, `GET /v1/stats`, and `POST /v1/queue/cleanup` inspect and prune.

`GET /metrics` is Prometheus text. Labels are model id and scheduling class. Job ids are not labels.

## Errors

```json
{"detail": {"code": "queue_full", "message": "queue is full (100000)", "details": {}}}
```

| HTTP | Code | When |
| --- | --- | --- |
| 401 | `unauthorized` | bad or missing bearer token |
| 404 | `not_found` | unknown job or batch |
| 409 | `idempotency_conflict` | same key, different payload |
| 422 | `invalid_request`, `unknown_model`, `unsupported_question` | rejected before queueing |
| 429 | `backpressure` | over a configured limit |
| 503 | `model_unavailable`, `not_ready` | model disabled, or the queue is not ready |
| 504 | `timeout` | the synchronous call waited longer than `timeout_s` |

The synchronous call keeps running after the client leaves unless `server.cancel_on_disconnect` is true. The default is to finish accepted work.
