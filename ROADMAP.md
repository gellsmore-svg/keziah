# Roadmap

Version 0.1 is a single-process queue. These are later possibilities, not commitments. They are not built yet because 0.1 does not need them.

- Redis Streams backend, using the mapping in `docs/redis-backend.md`.
- PostgreSQL backend for operators who already run it.
- Workers on more than one machine, still with one authority for each job.
- More System-1 adapters.
- Richer native batching for models that accept many states in one call.
- HTTP callbacks and server-sent completion events.
- A live queue view.
- Optional result cache, off by default, keyed by model version as well as the request.
- Adaptive concurrency, only after measurement shows a fixed limit is the wrong control.

Not planned: LLM orchestration, agent graphs, semantic routing, or an AI scheduler.
