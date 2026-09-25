# Queue semantics

## State machine

Stored values are lowercase.

```text
staged ──activate──> queued
queued ──claim──> leased ──start──> running
running ──success──> succeeded
running ──retryable error, attempts left──> retry_wait ──due──> queued
running ──non-retryable error──> failed
running ──attempts exhausted──> dead_letter
queued | retry_wait | staged | leased ──cancel──> cancelled
running ──cancel requested──> cancelled when the attempt ends
```

`staged` exists only inside a batch that is not yet active. Workers do not claim it.

Every transition that changes durable state is one transaction (or one lock, in memory mode) and writes an event: `SUBMITTED`, `CLAIMED`, `STARTED`, `RETRY_SCHEDULED`, `LEASE_EXPIRED`, `SUCCEEDED`, `FAILED`, `CANCELLED`, `DEAD_LETTERED`, `PROMOTED`, `REQUEUED`.

## Delivery

Execution is at least once.

```text
model returns
process dies before the completion transaction commits
lease expires or the next process releases it
the job runs again
```

Exactly-once model execution is not claimed. The completion write succeeds only when the lease owner still matches, so the first worker cannot overwrite a result after someone else has taken the job.

Idempotency keys stop duplicate *submission*. They do not collapse a replayed execution.

## Leases

A claim stores `lease_owner` and `lease_expires_at` and increments `attempt_count`. The worker extends the lease while the call is in progress. Maintenance returns an expired lease to `queued`, or to `dead_letter` when the attempt budget is already spent, or to `cancelled` when cancellation was requested. Shutdown of a live process releases its own leases immediately so recovery does not wait for the timeout.

## Clocks and timeouts

These are different values:

| Name | Meaning |
| --- | --- |
| HTTP / `wait` timeout | how long the caller blocks |
| `deadline_at` | do not start the job after this instant |
| `execution_timeout_ms` | how long one model call may run |
| `lease_seconds` | how long a claim stays valid without a heartbeat |
| batch deadline | stored on the batch and copied onto jobs when the caller sets it |

An execution timeout is retryable until attempts run out. A deadline that passes before start is a terminal `failed`.

## Ordering

Jobs are not FIFO across the whole process. The picker chooses a class by weighted credit, then a job inside that class by priority, how often that client was skipped, and age. Input order of a batch is not execution order. `batch_ordinal` restores input order when results are read.

## Retention

`keziah queue cleanup` deletes succeeded jobs older than the success window and failed, dead-letter, and cancelled jobs older than the failure window. It does not delete queued, leased, running, retry-wait, or staged jobs. Events older than the event window go with them when their job is already gone.

## Schema

`schema_version` starts at 1 (jobs and batches) and 2 (events). Opening a database applies the missing steps. You do not delete the file to upgrade within 0.1.
