# Batches

Two different batch shapes exist. They are not interchangeable.

## Questions on one state

One job, one model call, several decisions:

```json
{
  "state": {"message": "I've been waiting for three days."},
  "questions": {
    "frustration": {"type": "score", "instructions": "Rate frustration.", "criteria": ["calm", "angry"]},
    "needs_human": {"type": "noul", "instructions": "Needs a human?"}
  }
}
```

Keziah does not split those questions into jobs. Laya's `predict` and Jev's `/v1/systemone` both answer the map in one request.

## Many states

A Keziah batch is many jobs. Each job is one state and its questions. The batch has an id, a creation time, a job count, and counts by state. `completion` is `staging`, `open`, `complete`, or `aborted`.

`batch_ordinal` is assigned by the server from input order, starting at 0. Result reads use `ORDER BY batch_ordinal`.

A batch may set a default model. A job inside it may set `model` and override that default. One batch can therefore send most rows to Laya and a sample to Jev.

## Atomic visibility

Normal submission:

```text
insert batch as staging
insert jobs as staged, in chunks
one transaction marks the batch active and the jobs queued
```

Workers claim only `queued` jobs whose batch is `active` (or jobs with no batch). If the process dies while the batch is still `staging`, the next open deletes those jobs and marks the batch aborted. The caller never received success, and a retry with the same idempotency key can submit again.

If validation fails halfway, the service aborts the staging batch in the same way. Nothing from that attempt becomes runnable.

JSONL input is iterated line by line. A list that fits in memory is hashed up front so a repeated idempotency key can return the existing batch without staging a shadow copy. A pure stream hashes as it goes and resolves a conflicting key at activation.

`POST /v1/batches` accepts a JSON object or `application/x-ndjson`. Responses include job ids only when the batch has at most 1000 jobs. Larger batches are paged with `GET /v1/batches/{id}/results?offset=&limit=`.

## Native model batching

Some adapters can score several states in one call (`supports_native_batch`). Keziah may claim a few already-queued jobs that share the resolved model and the same question schema. `max_native_batch_wait_ms` defaults to 0, so this only groups work that is already waiting. It is not the same thing as multiple questions on one state.
