# Keziah

Keziah is a lightweight queue and execution service for **System-1 models**.

A System-1 call here is not a chat completion. It is a state plus one or more bounded questions, and the model returns typed probabilistic decisions:

- `choice` — one option, with a probability for every option
- `score` — a value on an ordered rubric, with a probability per level
- `noul` — the probability that a yes/no proposition holds

Several applications should not each load Laya, open their own Jev connection, and invent a worker pool. They submit work to Keziah. Keziah keeps one queue, one view of model capacity, and one place that retries and recovers.

```text
Application A ─┐
Application B ─┤
Application C ─┼──> Keziah ──> Laya
Application D ─┤            ├─> Jev
Application E ─┘            └─> future System-1 models
```

Hoglah is the System-2 sibling: prompts, chat, embeddings, and other LLM work. Keziah does not do that. Mahalath is an ontology system and is unrelated.

Version 0.1 is a single process. It can run inside one application, as a local daemon, or as a small network service. It is not a cluster.

## Install

Python 3.12+.

```bash
pip install keziah
```

From a checkout:

```bash
pip install -e ".[dev]"
```

Laya is optional (`pip install laya`, or `pip install keziah[laya]` when you want the local model). Jev needs `TYPESAFE_API_KEY` or `JEV_API_KEY`. Neither is required to run the queue. The built-in `mock` model is deterministic and offline.

## Run it

Memory, nothing survives exit:

```bash
keziah serve --mode memory
```

SQLite is the queue:

```bash
keziah serve --mode disk
```

SQLite remains the source of truth and RAM orders ready work. This is the usual local mode:

```bash
keziah serve --mode hybrid
```

The listener binds to `127.0.0.1:8766` unless you say otherwise. Binding to `0.0.0.0` is explicit, and you should set `KEZIAH_API_KEY` when you do.

```bash
keziah models
keziah show mock
```

## One decision

`examples/request.json`:

```json
{
  "state": {"message": "I've asked three times and nobody has fixed it."},
  "questions": {
    "frustration": {
      "type": "score",
      "instructions": "Rate the customer's frustration.",
      "criteria": ["calm", "frustrated", "very angry"]
    },
    "needs_human": {
      "type": "noul",
      "instructions": "Does this require human intervention?"
    }
  }
}
```

```bash
keziah submit examples/request.json --model mock --wait
```

With Laya installed, `--model laya` uses the local adapter. The process loads the router once and reuses it. With a Jev key, `--model jev` calls `POST /v1/systemone` on TypeSafe. Keziah does not switch a request for `laya` over to Jev because Laya is down. Fallback happens only when the caller names a configured model group.

Embedded, no HTTP server:

```python
from keziah import Keziah

with Keziah(mode="memory") as k:
    job_id = k.submit(
        model="mock",
        state={"message": "I've asked three times and nobody has fixed it."},
        questions={
            "frustration": {
                "type": "score",
                "instructions": "Rate the customer's frustration.",
                "criteria": ["calm", "frustrated", "very angry"],
            },
            "needs_human": {
                "type": "noul",
                "instructions": "Does this require human intervention?",
            },
        },
    )
    print(k.wait(job_id))
```

`mock` is the model that runs without weights. Replace it with `laya` or `jev` when that adapter is actually available.

## A batch

A batch is many independent states. Questions inside one state stay in that one inference call.

```python
batch = k.submit_batch(
    [
        {"state": {"message": "Message one"}, "questions": questions},
        {"state": {"message": "Message two"}, "questions": questions},
        {"model": "jev", "state": {"message": "Message three"}, "questions": questions},
    ],
    model="laya",
)
results = k.wait_batch(batch.batch_id)
```

The batch default is Laya. The third job overrides it. Results come back in input order (`batch_ordinal`) even when execution order differs.

JSONL is streamed, not loaded as one list:

```bash
keziah batch submit examples/requests.jsonl --model mock --embedded --wait
```

On disk and hybrid, jobs in a batch stay invisible to workers until the batch is activated in one transaction. A crash during staging aborts that batch on the next open. Workers never run a half-submitted batch.

## After a crash

Durable modes store jobs in SQLite (WAL, busy timeout, explicit transactions). A claimed job holds a lease. If the worker disappears, the lease expires, the job becomes eligible again, and `attempt_count` is kept. Shutdown also releases leases held by this process so the next start does not wait out the lease.

This is **at-least-once execution**, not exactly-once. There is a window where the model has finished and the process dies before the completion commit. That job runs again. Completion itself checks the lease token, so a stale worker cannot overwrite a result that a new owner already took.

Submission idempotency is separate. The same `Idempotency-Key` and the same payload return the original job. The same key with a different payload is a conflict (`409`).

## What you can ask it

```bash
keziah models
keziah show laya
keziah submit request.json --model laya --wait
keziah batch submit requests.jsonl --model laya --wait
keziah job <job-id>
keziah batch status <batch-id>
keziah wait <job-id>
keziah cancel <job-id>
keziah queue stats
keziah queue dead
keziah queue requeue <job-id>
keziah queue cleanup
keziah health
keziah benchmark --jobs 100 --mode memory
keziah benchmark --model laya --jobs 8 --concurrency 1
```

Add `--json` for machine-readable output. Commands other than `serve` talk to `http://127.0.0.1:8766` unless you pass `--embedded` or `--url`.

HTTP shape (OpenAPI at `/openapi.json`; this is the API description, not an OpenAI-compatible API):

```text
POST /v1/systemone
POST /v1/jobs
GET  /v1/jobs/{job_id}
DELETE /v1/jobs/{job_id}
POST /v1/batches
GET  /v1/batches/{batch_id}
GET  /v1/batches/{batch_id}/results
GET  /v1/models
GET  /v1/models/{model_id}
GET  /v1/queue
GET  /v1/stats
GET  /health/live
GET  /health/ready
GET  /metrics
```

`POST /v1/systemone` waits, but the work still takes a model slot. It does not run beside the queue.

```python
from keziah import Client

client = Client("http://127.0.0.1:8766")
print(client.models())
batch = client.submit_batch(jobs, model="mock")
print(client.wait_batch(batch.batch_id))
```

## Scheduling and capacity

Each model has its own concurrency limit and execution kind (`inline`, `thread`, or `async`). There is no unbounded pool. Five applications sharing one Keziah share Laya's configured slots instead of each starting their own.

Scheduling classes are `interactive`, `normal`, `batch`, and `bulk`. A weighted-credit picker prefers interactive work, and a client that has been skipped inside a class gains score so one caller cannot hold the class forever. Age raises a class's weight so an old bulk backlog is not stuck behind a permanent stream of interactive work. The rule is deterministic and covered by tests.

## Configuration

See `keziah.example.yaml` and `.env.example`. Precedence is CLI, environment, file, defaults.

## Further reading

- [Architecture](docs/architecture.md)
- [Queue semantics](docs/queue-semantics.md)
- [Batches](docs/batches.md)
- [Model adapters](docs/model-adapters.md)
- [Configuration](docs/configuration.md)
- [HTTP API](docs/api.md)
- [Deployment](docs/deployment.md)
- [A future Redis backend](docs/redis-backend.md)
- [Design review](docs/design-review.md)
- [Code and requirements review, 2026-09-26](docs/review-2026-09-26.md): open work items

## Licence

Apache-2.0. See `LICENSE`.
