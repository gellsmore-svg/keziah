# Redis backend (not in 0.1)

Redis is not required and is not implemented. The queue protocol is the extension point. A later backend would implement the same operations the memory and SQLite backends implement: enqueue, staged batch activation, conditional claim, lease extend, complete, fail, retry, cancel, recover, read, stats, cleanup.

A Streams design that preserves today's guarantees:

| Keziah operation | Redis sketch |
| --- | --- |
| enqueue | `XADD keziah:jobs *` with the job payload, plus a hash `keziah:job:{id}` for the authoritative record |
| claim | consumer group `XREADGROUP` on a ready stream, then `HSET` the lease with a compare on state, or a Lua script that moves the hash from `queued` to `leased` only if it is still queued |
| lease | hash fields `lease_owner`, `lease_expires_at`; `XAUTOCLAIM` or a periodic reclaim for entries whose hash lease has expired |
| complete | Lua: if `lease_owner` matches, set state `succeeded`, store the result, `XACK` |
| retry | Lua: set `retry_wait` and a `not_before` score in a zset; a promoter `XADD`s it back when due |
| batch activation | write jobs under a staging key, `RENAME` or a Lua script that flips the batch hash to `active` and only then publishes the ids |
| idempotency | `SET keziah:idemp:{client}:{key} {job_id} NX` plus a stored payload hash; same hash returns the id, different hash returns a conflict |

Redis would become the authority for that deployment, the way SQLite is the authority in disk and hybrid mode. The in-process ready index would remain a hint, rebuilt from the hashes after restart, not a second copy of status.

Do not add Redis beside SQLite for the same jobs. Two authorities is how a queue loses a completion or runs a job twice without a lease to explain it. Pick one backend per process.

Consumer groups, pending entries, and acknowledgement map cleanly onto claim, lease, and complete. They do not by themselves give exactly-once model execution. The crash window between "model returned" and "hash updated" remains, and the job must be allowed to run again.
