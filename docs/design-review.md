# Design review

This note records the review of the 0.1 design and of the code that was actually built. Scores are 0–10. They are judgments, not measurements, except where a test or the benchmark harness checks the behaviour.

## Final score vector

| | Topic | Score |
| --- | --- | --- |
| A | Transactional correctness | 8 |
| B | Crash recovery | 8 |
| C | Batch semantics | 8 |
| D | Model abstraction | 8 |
| E | Model discovery | 7 |
| F | Model selection | 8 |
| G | Resource governance | 8 |
| H | Scheduler fairness | 7 |
| I | System-1 throughput | 7 |
| J | Queue latency overhead | 7 |
| K | Persistence performance | 7 |
| L | API simplicity | 8 |
| M | Embedded-use simplicity | 8 |
| N | Observability | 7 |
| O | Security | 7 |
| P | Testability | 8 |
| Q | Maintainability | 7 |
| R | Future backend extensibility | 8 |
| S | Deployment simplicity | 8 |
| T | Scope discipline | 8 |

## Weaknesses found before coding, and what changed

A first sketch let an old bulk backlog jump the line on every dispatch once it crossed a starvation age. That would freeze interactive work behind a large import. The picker now adds credit to every class that has work. An old class receives an additive boost that stays below the base weight of every higher class, so interactive work still wins more often. Bulk work still receives turns. Tests check both.

A priority tuple `(priority, skips, age)` let a client with a one-point priority lead starve everyone else, because skips never overtook priority. Score is numeric: priority, skip count, and age have explicit weights, so a skipped client catches up.

Hybrid mode was at risk of two sources of truth. The ready index is a hint. Claims are conditional updates. Reconcile rebuilds the index from SQLite. Disk mode does not trust a long-lived index; it reads a per-class window so bulk jobs are not hidden behind an interactive `ORDER BY`.

`wait()` originally treated any wake as completion. A retry woke the caller and returned `retry_wait`. It now loops until a terminal state.

The synchronous HTTP path is a normal job plus a wait. It uses a worker thread so it does not block the dispatcher loop.

Laya's own server uses one inference worker and a lock around the forward pass. Keziah defaults Laya to concurrency 1 and holds the same kind of lock, instead of pretending a GPU checkpoint wants a large thread pool.

Redis, Kafka, callbacks-as-a-product, and an automatic model router were dropped. A group fallback is the only cross-model path, and only when the caller names the group.

## Weaknesses found in the code, and what changed

The first service tests showed `wait()` returning on retry and on a stale event after requeue. Both were fixed and covered.

Lease recovery, single-winner claims, staging invisibility, idempotency conflict, restart after a blocked worker, concurrency caps, and "no silent fallback" are tests, not comments.

Pyright and ruff are clean on `src` and `tests`. The suite uses the mock adapter only.

## Trade-offs that remain

- Execution is at least once. The crash window between model return and the completion commit is real.
- One process owns one SQLite file. That is the whole deployment story for 0.1.
- Disk mode's fairness looks at a bounded window per class. Hybrid is the mode that tracks every ready job.
- Native-batch companions share the slot of the first job. They do not each pass the fairness picker.
- Jev health is a cached `GET /v1/models`. It is live, and it is not repeated for every job. It is not a full decision call.
- Laya availability means the package imports. Weights load on first use or on `preload`.
- In-process callbacks exist. HTTP callbacks do not.
- TLS stops at a reverse proxy.
- The benchmark reports this machine's mock-adapter run. It does not set a pass threshold and it does not speak for Laya or Jev.

## Why the review stopped

Another pass would add a second database, a distributed lease, or a smarter router. Those do not fix a remaining correctness bug in the single-process queue. The open items above are either inherent (at least once) or explicitly deferred (Redis, HTTP callbacks, live Jev probes).
