# Architecture

Keziah is one process with five pieces:

```text
listener or embedded API
        │
        ▼
   Keziah service  ── model registry ── adapters (mock, laya, jev, http)
        │
        ▼
   scheduler (weighted credit, per client, per class)
        │
        ▼
   queue backend (memory | sqlite | sqlite + ready index)
```

The service is the same object whether you call it from Python or through FastAPI. The HTTP server does not have its own queue.

## Why it is small

System-1 calls are short. A second queue, a second copy of Laya, and a second worker pool in every application are the problem Keziah exists to remove. A distributed log, a workflow engine, or an LLM router would spend more time than the decision. Version 0.1 stays on one machine for that reason.

Hoglah already solved leases, idempotent submit, per-model slots, and SQLite-as-a-queue for LLM jobs. Keziah keeps those ideas where a fast decision still needs them:

- one conditional claim, so two workers cannot both own the live lease
- a lease token checked again on completion
- WAL SQLite and a busy timeout, not an ORM
- per-model capacity instead of one global pool
- an offline adapter so tests do not need a model

It does not keep Hoglah's chat/embeddings split, Kafka, RabbitMQ, Redis, MongoDB, label sequences, or dependency graph. Those belong to longer LLM work.

## Storage

| Mode | Authority | Ready ordering |
| --- | --- | --- |
| `memory` | process memory | in-process index |
| `disk` | SQLite | per-class SQL window, then the same picker |
| `hybrid` | SQLite | in-process index, rebuilt from SQLite on an interval and after batch activation |

Hybrid's index is a hint. `claim` is a conditional `UPDATE` on the row. If the hint is wrong, the claim fails and the id is dropped. A reconcile pass rebuilds the hint from rows that are actually queued. Durable status is never stored only in RAM.

Memory mode is a native structure, not SQLite `:memory:`, so a disposable run does not pay for a database.

## Execution

The dispatcher thread owns an asyncio loop. It wakes on submit, on completion, and on a slow timer (default 250 ms) that recovers leases and promotes retries. It does not poll SQLite every millisecond.

Each model has `max_concurrency` and an execution kind:

- `thread` — a bounded pool. Laya uses this and also a lock around `predict`, because one forward pass is the useful unit.
- `async` — the dispatcher awaits the adapter. Jev and the mock use this.
- `inline` — the dispatcher awaits the adapter on the loop. Useful for tiny tests.

A native batch, when the adapter declares `supports_native_batch`, claims several jobs that share a model and a question schema and runs them as one call. That call occupies one slot. The wait before coalescing defaults to 0 ms, so a lone interactive job is not held for company.

## Model names

A caller always passes a model name.

1. Aliases rename (`local` → `laya`). Cycles are rejected at startup.
2. A model group (`emotion` → Laya, else Jev) may select a later member when an earlier one is unavailable. The result records `fallback_used` and the reason.
3. A concrete name (`laya`) never changes model. If Laya is down, the job fails or retries against Laya.

Resolution for a group is done at submit and again when a retry is scheduled, so a model that recovers can be used on the next attempt.

## Failure

Retries use bounded exponential backoff with jitter. Connection failures, timeouts, HTTP 429/5xx, and a temporarily unavailable model are retryable. Validation errors and authentication failures are not. Exhausted retries become `dead_letter`. A non-retryable error becomes `failed`. Both stay readable. `keziah queue requeue` puts a failed or dead-letter job back to `queued` and clears the attempt count.

Cancellation of queued, waiting, staged, or leased work is immediate. A running call is marked cancel-requested. The worker does not start another attempt, and it discards the result if it notices the flag before committing. Keziah does not claim it can interrupt an arbitrary in-flight model call.
