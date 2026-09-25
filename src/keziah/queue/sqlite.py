"""SQLite queue. WAL, explicit transactions, and a versioned schema.

SQLite is the authority in disk and hybrid modes. Claims are a single
conditional UPDATE so two workers cannot both own the active lease.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from keziah.clock import Clock, isoformat, parse_iso
from keziah.config import SchedulerSettings
from keziah.errors import BackpressureError, ConflictError, NotFoundError, ValidationError
from keziah.jsonutil import dumps, loads
from keziah.queue.backend import Limits, Maintenance
from keziah.queue.common import empty_counts
from keziah.scheduler import select
from keziah.scheduler import PickerState
from keziah.types import (
    BATCH_ABORTED,
    BATCH_ACTIVE,
    BATCH_STAGING,
    CANCELLED,
    DEAD_LETTER,
    FAILED,
    INCOMPLETE_STATES,
    LEASED,
    QUEUED,
    RETRY_WAIT,
    RUNNING,
    STAGED,
    SUCCEEDED,
    Batch,
    Candidate,
    Job,
)

SCHEMA_VERSION = 2

_INCOMPLETE_SQL = ",".join(f"'{name}'" for name in sorted(INCOMPLETE_STATES))


def _migrate_v1(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE batches (
            batch_id TEXT PRIMARY KEY,
            client_id TEXT NOT NULL,
            status TEXT NOT NULL,
            requested_model TEXT,
            job_count INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            activated_at TEXT,
            idempotency_key TEXT,
            idempotency_hash TEXT,
            deadline_at TEXT
        );
        CREATE UNIQUE INDEX idx_batches_idemp
            ON batches(client_id, idempotency_key)
            WHERE idempotency_key IS NOT NULL;
        CREATE TABLE jobs (
            job_id TEXT PRIMARY KEY,
            batch_id TEXT,
            batch_ordinal INTEGER,
            idempotency_key TEXT,
            idempotency_hash TEXT,
            client_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            not_before TEXT,
            priority INTEGER NOT NULL,
            scheduling_class TEXT NOT NULL,
            requested_model TEXT NOT NULL,
            resolved_model TEXT NOT NULL,
            model_version TEXT,
            state TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            questions_hash TEXT NOT NULL,
            attempt_count INTEGER NOT NULL,
            max_attempts INTEGER NOT NULL,
            lease_owner TEXT,
            lease_expires_at TEXT,
            execution_timeout_ms INTEGER,
            deadline_at TEXT,
            result_json TEXT,
            error_json TEXT,
            queue_ms INTEGER,
            execution_ms INTEGER,
            total_ms INTEGER,
            cancel_requested INTEGER NOT NULL DEFAULT 0,
            fallback_json TEXT
        );
        CREATE UNIQUE INDEX idx_jobs_idemp
            ON jobs(client_id, idempotency_key)
            WHERE idempotency_key IS NOT NULL;
        CREATE INDEX idx_jobs_state ON jobs(state, resolved_model, scheduling_class, priority DESC, created_at);
        CREATE INDEX idx_jobs_batch ON jobs(batch_id, batch_ordinal);
        CREATE INDEX idx_jobs_lease ON jobs(state, lease_expires_at);
        CREATE INDEX idx_jobs_finished ON jobs(state, finished_at);
        CREATE INDEX idx_jobs_not_before ON jobs(state, not_before);
        """
    )


def _migrate_v2(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id TEXT,
            batch_id TEXT,
            at TEXT NOT NULL,
            kind TEXT NOT NULL,
            detail_json TEXT
        );
        CREATE INDEX idx_events_job ON events(job_id, id);
        CREATE INDEX idx_events_at ON events(at);
        """
    )


MIGRATIONS = {1: _migrate_v1, 2: _migrate_v2}


class SQLiteBackend:
    def __init__(
        self,
        path: str,
        clock: Clock,
        *,
        busy_timeout_ms: int = 5000,
        limits: Limits | None = None,
        migrate_to: int | None = None,
    ) -> None:
        self.clock = clock
        self.limits = limits or Limits()
        self.path = Path(path).expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)}")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._lock = threading.Lock()
        self._closed = False
        self._migrate(migrate_to if migrate_to is not None else SCHEMA_VERSION)
        if self.schema_version >= SCHEMA_VERSION:
            self._abort_orphan_staging()

    @property
    def schema_version(self) -> int:
        row = self._conn.execute("SELECT version FROM schema_version").fetchone()
        return int(row["version"]) if row else 0

    def enqueue(self, job: Job) -> Job:
        def run() -> Job:
            existing = self._existing_job(job)
            if existing is not None:
                return existing
            self._reserve_capacity(job.client_id, 1)
            self._insert_job(job)
            self._emit("SUBMITTED", job.created_at, job_id=job.job_id, batch_id=job.batch_id, detail={})
            return self._require_job(job.job_id)

        return self._tx(run)

    def find_batch_idempotency(self, client_id: str, key: str) -> Batch | None:
        def run() -> Batch | None:
            row = self._conn.execute(
                "SELECT batch_id FROM batches WHERE client_id=? AND idempotency_key=?",
                (client_id, key),
            ).fetchone()
            if row is None:
                return None
            return self._batch_view(row["batch_id"])

        return self._auto(run)

    def begin_batch(self, batch: Batch) -> Batch:
        def run() -> Batch:
            existing = self._existing_batch(batch)
            if existing is not None:
                return existing
            self._conn.execute(
                """
                INSERT INTO batches (
                    batch_id, client_id, status, requested_model, job_count, created_at,
                    activated_at, idempotency_key, idempotency_hash, deadline_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    batch.batch_id,
                    batch.client_id,
                    batch.status,
                    batch.requested_model,
                    0,
                    batch.created_at,
                    None,
                    None,
                    None,
                    batch.deadline_at,
                ),
            )
            self._emit(
                "SUBMITTED",
                batch.created_at,
                job_id=None,
                batch_id=batch.batch_id,
                detail={"status": batch.status},
            )
            return self._batch_view(batch.batch_id)

        return self._tx(run)

    def stage_jobs(self, batch_id: str, jobs: list[Job]) -> int:
        def run() -> int:
            batch = self._batch_row(batch_id)
            if batch is None:
                raise NotFoundError(f"batch {batch_id} not found")
            if batch["status"] != BATCH_STAGING:
                raise ValidationError(f"batch {batch_id} is {batch['status']}, not staging")
            new_count = int(batch["job_count"]) + len(jobs)
            if new_count > self.limits.max_batch_size:
                raise BackpressureError(
                    f"batch would exceed {self.limits.max_batch_size} jobs",
                    code="batch_too_large",
                )
            if jobs:
                self._reserve_capacity(str(jobs[0].client_id), len(jobs))
            for job in jobs:
                if job.batch_id != batch_id or job.state != STAGED:
                    raise ValidationError("staged jobs must belong to the staging batch")
                dup = self._existing_job(job)
                if dup is not None:
                    raise ConflictError(
                        "job idempotency key reused inside a batch",
                        details={"job_id": dup.job_id},
                    )
                self._insert_job(job)
                self._emit(
                    "SUBMITTED",
                    job.created_at,
                    job_id=job.job_id,
                    batch_id=batch_id,
                    detail={"staged": True},
                )
            self._conn.execute("UPDATE batches SET job_count=? WHERE batch_id=?", (new_count, batch_id))
            return new_count

        return self._tx(run)

    def activate_batch(
        self,
        batch_id: str,
        *,
        idempotency_key: str | None = None,
        idempotency_hash: str | None = None,
    ) -> Batch:
        def run() -> Batch:
            batch = self._batch_row(batch_id)
            if batch is None:
                raise NotFoundError(f"batch {batch_id} not found")
            if idempotency_key:
                existing = self._conn.execute(
                    "SELECT batch_id, idempotency_hash FROM batches WHERE client_id=? AND idempotency_key=?",
                    (batch["client_id"], idempotency_key),
                ).fetchone()
                if existing is not None and existing["batch_id"] != batch_id:
                    if existing["idempotency_hash"] != idempotency_hash:
                        raise ConflictError(
                            "batch idempotency key was already used with a different payload",
                            details={"batch_id": existing["batch_id"]},
                        )
                    self._conn.execute("DELETE FROM jobs WHERE batch_id=?", (batch_id,))
                    self._conn.execute(
                        """
                        UPDATE batches
                           SET status=?, job_count=0, idempotency_key=NULL, idempotency_hash=NULL
                         WHERE batch_id=?
                        """,
                        (BATCH_ABORTED, batch_id),
                    )
                    view = self._batch_view(existing["batch_id"])
                    view.replayed = True
                    return view
            if batch["status"] == BATCH_ACTIVE:
                return self._batch_view(batch_id)
            if batch["status"] != BATCH_STAGING:
                raise ValidationError(f"batch {batch_id} is {batch['status']}")
            now = isoformat(self.clock.now())
            try:
                self._conn.execute(
                    """
                    UPDATE batches
                       SET status=?, activated_at=?, idempotency_key=?, idempotency_hash=?
                     WHERE batch_id=? AND status=?
                    """,
                    (BATCH_ACTIVE, now, idempotency_key, idempotency_hash, batch_id, BATCH_STAGING),
                )
            except sqlite3.IntegrityError:
                existing = self._conn.execute(
                    "SELECT batch_id, idempotency_hash FROM batches WHERE client_id=? AND idempotency_key=?",
                    (batch["client_id"], idempotency_key),
                ).fetchone()
                if existing is None or existing["idempotency_hash"] != idempotency_hash:
                    raise ConflictError(
                        "batch idempotency key was already used with a different payload",
                        details={"batch_id": None if existing is None else existing["batch_id"]},
                    )
                self._conn.execute("DELETE FROM jobs WHERE batch_id=?", (batch_id,))
                self._conn.execute(
                    """
                    UPDATE batches
                       SET status=?, job_count=0, idempotency_key=NULL, idempotency_hash=NULL
                     WHERE batch_id=?
                    """,
                    (BATCH_ABORTED, batch_id),
                )
                view = self._batch_view(existing["batch_id"])
                view.replayed = True
                return view
            self._conn.execute(
                "UPDATE jobs SET state=?, updated_at=? WHERE batch_id=? AND state=?",
                (QUEUED, now, batch_id, STAGED),
            )
            count = self._conn.execute("SELECT COUNT(*) AS n FROM jobs WHERE batch_id=?", (batch_id,)).fetchone()["n"]
            self._conn.execute("UPDATE batches SET job_count=? WHERE batch_id=?", (int(count), batch_id))
            self._emit(
                "SUBMITTED",
                now,
                job_id=None,
                batch_id=batch_id,
                detail={"activated": True, "jobs": int(count)},
            )
            return self._batch_view(batch_id)

        return self._tx(run)

    def abort_batch(self, batch_id: str) -> None:
        def run() -> None:
            batch = self._batch_row(batch_id)
            if batch is None or batch["status"] != BATCH_STAGING:
                return
            self._conn.execute("DELETE FROM jobs WHERE batch_id=?", (batch_id,))
            self._conn.execute(
                """
                UPDATE batches
                   SET status=?, job_count=0, idempotency_key=NULL, idempotency_hash=NULL
                 WHERE batch_id=?
                """,
                (BATCH_ABORTED, batch_id),
            )

        self._tx(run)

    def claim_by_id(self, job_id: str, owner: str, lease_until: str, now_iso: str) -> Job | None:
        def run() -> Job | None:
            if self._claim_sql(job_id, owner, lease_until, now_iso) != 1:
                return None
            job = self._require_job(job_id)
            self._emit(
                "CLAIMED",
                now_iso,
                job_id=job_id,
                batch_id=job.batch_id,
                detail={"attempt": job.attempt_count},
            )
            return job

        return self._tx(run)

    def claim_next(
        self,
        model: str,
        picker: PickerState,
        now_iso: str,
        now_epoch: float,
        owner: str,
        lease_until: str,
        settings: SchedulerSettings,
    ) -> tuple[Job | None, PickerState]:
        def run() -> tuple[Job | None, PickerState]:
            candidates = self._candidates(model, now_iso, settings.disk_candidates_per_class)
            skipped: set[str] = set()
            for _ in range(8):
                pool = [item for item in candidates if item.job_id not in skipped]
                choice, proposed = select(picker, model, pool, now_epoch, settings)
                if choice is None:
                    return None, picker
                if self._claim_sql(choice.job_id, owner, lease_until, now_iso) == 1:
                    job = self._require_job(choice.job_id)
                    self._emit(
                        "CLAIMED",
                        now_iso,
                        job_id=job.job_id,
                        batch_id=job.batch_id,
                        detail={"attempt": job.attempt_count},
                    )
                    return job, proposed
                skipped.add(choice.job_id)
            return None, picker

        return self._tx(run)

    def claim_compatible(
        self,
        *,
        model: str,
        questions_hash: str,
        exclude: set[str],
        limit: int,
        now_iso: str,
        owner: str,
        lease_until: str,
    ) -> list[Job]:
        if limit <= 0:
            return []

        def run() -> list[Job]:
            rows = self._conn.execute(
                """
                SELECT job_id FROM jobs
                 WHERE state=? AND resolved_model=? AND questions_hash=?
                   AND (not_before IS NULL OR not_before<=?)
                   AND (deadline_at IS NULL OR deadline_at>?)
                   AND cancel_requested=0
                   AND (batch_id IS NULL OR EXISTS (
                        SELECT 1 FROM batches b WHERE b.batch_id=jobs.batch_id AND b.status=?
                   ))
                 ORDER BY created_at ASC, job_id ASC
                 LIMIT ?
                """,
                (QUEUED, model, questions_hash, now_iso, now_iso, BATCH_ACTIVE, limit + len(exclude)),
            ).fetchall()
            claimed: list[Job] = []
            for row in rows:
                if row["job_id"] in exclude:
                    continue
                if len(claimed) >= limit:
                    break
                if self._claim_sql(row["job_id"], owner, lease_until, now_iso) != 1:
                    continue
                job = self._require_job(row["job_id"])
                self._emit(
                    "CLAIMED",
                    now_iso,
                    job_id=job.job_id,
                    batch_id=job.batch_id,
                    detail={"attempt": job.attempt_count, "native_batch": True},
                )
                claimed.append(job)
            return claimed

        return self._tx(run)

    def extend_lease(self, job_id: str, owner: str, lease_until: str, now_iso: str) -> bool:
        def run() -> bool:
            cur = self._conn.execute(
                """
                UPDATE jobs SET lease_expires_at=?, updated_at=?
                 WHERE job_id=? AND lease_owner=? AND state IN (?, ?)
                """,
                (lease_until, now_iso, job_id, owner, LEASED, RUNNING),
            )
            return cur.rowcount == 1

        return self._tx(run)

    def mark_running(self, job_id: str, owner: str, lease_until: str, now_iso: str) -> bool:
        def run() -> bool:
            cur = self._conn.execute(
                """
                UPDATE jobs
                   SET state=?, started_at=COALESCE(started_at, ?), lease_expires_at=?, updated_at=?
                 WHERE job_id=? AND lease_owner=? AND state=?
                """,
                (RUNNING, now_iso, lease_until, now_iso, job_id, owner, LEASED),
            )
            if cur.rowcount != 1:
                return False
            job = self._require_job(job_id)
            self._emit("STARTED", now_iso, job_id=job_id, batch_id=job.batch_id, detail={"attempt": job.attempt_count})
            return True

        return self._tx(run)

    def complete(
        self,
        job_id: str,
        owner: str,
        *,
        result: dict[str, Any],
        model_version: str | None,
        queue_ms: int | None,
        execution_ms: int | None,
        total_ms: int | None,
        now_iso: str,
    ) -> bool:
        def run() -> bool:
            cur = self._conn.execute(
                """
                UPDATE jobs
                   SET state=?, result_json=?, model_version=?, queue_ms=?, execution_ms=?, total_ms=?,
                       finished_at=?, updated_at=?, lease_owner=NULL, lease_expires_at=NULL, error_json=NULL
                 WHERE job_id=? AND lease_owner=? AND state=?
                """,
                (
                    SUCCEEDED,
                    dumps(result),
                    model_version,
                    queue_ms,
                    execution_ms,
                    total_ms,
                    now_iso,
                    now_iso,
                    job_id,
                    owner,
                    RUNNING,
                ),
            )
            if cur.rowcount != 1:
                return False
            job = self._require_job(job_id)
            self._emit("SUCCEEDED", now_iso, job_id=job_id, batch_id=job.batch_id, detail={})
            return True

        return self._tx(run)

    def fail(
        self,
        job_id: str,
        owner: str,
        *,
        error: dict[str, Any],
        dead_letter: bool,
        now_iso: str,
    ) -> bool:
        def run() -> bool:
            return self._fail_sql(job_id, owner, error, dead_letter, now_iso, require_owner=True)

        return self._tx(run)

    def retry(
        self,
        job_id: str,
        owner: str,
        *,
        error: dict[str, Any],
        not_before: str,
        resolved_model: str,
        fallback: dict[str, Any] | None,
        now_iso: str,
    ) -> bool:
        def run() -> bool:
            cur = self._conn.execute(
                """
                UPDATE jobs
                   SET state=?, error_json=?, not_before=?, resolved_model=?, fallback_json=?,
                       lease_owner=NULL, lease_expires_at=NULL, updated_at=?
                 WHERE job_id=? AND lease_owner=? AND state=?
                """,
                (
                    RETRY_WAIT,
                    dumps(error),
                    not_before,
                    resolved_model,
                    dumps(fallback) if fallback is not None else None,
                    now_iso,
                    job_id,
                    owner,
                    RUNNING,
                ),
            )
            if cur.rowcount != 1:
                return False
            job = self._require_job(job_id)
            self._emit(
                "RETRY_SCHEDULED",
                now_iso,
                job_id=job_id,
                batch_id=job.batch_id,
                detail={"not_before": not_before, "model": resolved_model},
            )
            return True

        return self._tx(run)

    def finish_cancel(self, job_id: str, owner: str, now_iso: str) -> bool:
        def run() -> bool:
            job = self._job_row(job_id)
            if job is None or job["lease_owner"] != owner or job["state"] not in {LEASED, RUNNING}:
                return False
            self._mark_cancelled_sql(job_id, job["batch_id"], now_iso)
            return True

        return self._tx(run)

    def cancel(self, job_id: str, now_iso: str) -> Job:
        def run() -> Job:
            row = self._job_row(job_id)
            if row is None:
                raise NotFoundError(f"job {job_id} not found")
            if row["state"] in {QUEUED, RETRY_WAIT, STAGED, LEASED}:
                self._mark_cancelled_sql(job_id, row["batch_id"], now_iso)
            elif row["state"] == RUNNING and not row["cancel_requested"]:
                self._conn.execute(
                    "UPDATE jobs SET cancel_requested=1, updated_at=? WHERE job_id=?",
                    (now_iso, job_id),
                )
            return self._require_job(job_id)

        return self._tx(run)

    def cancel_batch(self, batch_id: str, now_iso: str) -> int:
        def run() -> int:
            if self._batch_row(batch_id) is None:
                raise NotFoundError(f"batch {batch_id} not found")
            changed = 0
            rows = self._conn.execute("SELECT job_id, state, batch_id, cancel_requested FROM jobs WHERE batch_id=?", (batch_id,)).fetchall()
            for row in rows:
                if row["state"] in {QUEUED, RETRY_WAIT, STAGED, LEASED}:
                    self._mark_cancelled_sql(row["job_id"], batch_id, now_iso)
                    changed += 1
                elif row["state"] == RUNNING and not row["cancel_requested"]:
                    self._conn.execute(
                        "UPDATE jobs SET cancel_requested=1, updated_at=? WHERE job_id=?",
                        (now_iso, row["job_id"]),
                    )
                    changed += 1
            return changed

        return self._tx(run)

    def maintenance(self, now_iso: str) -> Maintenance:
        def run() -> Maintenance:
            report = Maintenance()
            expired = self._conn.execute(
                """
                SELECT job_id FROM jobs
                 WHERE state IN (?, ?) AND lease_expires_at IS NOT NULL AND lease_expires_at<=?
                """,
                (LEASED, RUNNING, now_iso),
            ).fetchall()
            for row in expired:
                self._expire_sql(row["job_id"], now_iso, report)
            due = self._conn.execute(
                """
                SELECT job_id FROM jobs
                 WHERE state=? AND (not_before IS NULL OR not_before<=?)
                """,
                (RETRY_WAIT, now_iso),
            ).fetchall()
            for row in due:
                self._conn.execute(
                    "UPDATE jobs SET state=?, updated_at=? WHERE job_id=? AND state=?",
                    (QUEUED, now_iso, row["job_id"], RETRY_WAIT),
                )
                job = self._require_job(row["job_id"])
                self._emit("PROMOTED", now_iso, job_id=job.job_id, batch_id=job.batch_id, detail={})
                report.promoted += 1
                report.requeued.append(job.job_id)
            late = self._conn.execute(
                """
                SELECT job_id FROM jobs
                 WHERE state=? AND deadline_at IS NOT NULL AND deadline_at<=?
                """,
                (QUEUED, now_iso),
            ).fetchall()
            for row in late:
                self._fail_sql(
                    row["job_id"],
                    "",
                    {"code": "deadline", "message": "deadline passed before execution", "retryable": False},
                    False,
                    now_iso,
                    require_owner=False,
                    from_state=QUEUED,
                )
                report.deadlines += 1
                report.finished.append(row["job_id"])
            flagged = self._conn.execute(
                "SELECT job_id, batch_id FROM jobs WHERE cancel_requested=1 AND state IN (?, ?, ?)",
                (QUEUED, RETRY_WAIT, STAGED),
            ).fetchall()
            for row in flagged:
                self._mark_cancelled_sql(row["job_id"], row["batch_id"], now_iso)
                report.expired_cancelled += 1
                report.finished.append(row["job_id"])
            return report

        return self._tx(run)

    def get_job(self, job_id: str) -> Job | None:
        row = self._auto(lambda: self._job_row(job_id))
        return None if row is None else self._to_job(row)

    def get_jobs(
        self,
        *,
        state: str | None = None,
        batch_id: str | None = None,
        client_id: str | None = None,
        model: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        clauses = ["1=1"]
        args: list[Any] = []
        if state is not None:
            clauses.append("state=?")
            args.append(state)
        if batch_id is not None:
            clauses.append("batch_id=?")
            args.append(batch_id)
        if client_id is not None:
            clauses.append("client_id=?")
            args.append(client_id)
        if model is not None:
            clauses.append("(resolved_model=? OR requested_model=?)")
            args.extend([model, model])
        args.extend([limit, offset])
        sql = f"SELECT * FROM jobs WHERE {' AND '.join(clauses)} ORDER BY created_at, job_id LIMIT ? OFFSET ?"

        def run() -> list[Job]:
            return [self._to_job(row) for row in self._conn.execute(sql, args).fetchall()]

        return self._auto(run)

    def get_batch(self, batch_id: str) -> Batch | None:
        def run() -> Batch | None:
            if self._batch_row(batch_id) is None:
                return None
            return self._batch_view(batch_id)

        return self._auto(run)

    def get_batch_results(self, batch_id: str, *, offset: int = 0, limit: int = 100) -> list[Job]:
        def run() -> list[Job]:
            if self._batch_row(batch_id) is None:
                raise NotFoundError(f"batch {batch_id} not found")
            rows = self._conn.execute(
                """
                SELECT * FROM jobs WHERE batch_id=?
                 ORDER BY COALESCE(batch_ordinal, 1000000000), job_id
                 LIMIT ? OFFSET ?
                """,
                (batch_id, limit, offset),
            ).fetchall()
            return [self._to_job(row) for row in rows]

        return self._auto(run)

    def get_events(self, job_id: str, *, limit: int = 100) -> list[dict[str, Any]]:
        def run() -> list[dict[str, Any]]:
            rows = self._conn.execute(
                "SELECT * FROM events WHERE job_id=? ORDER BY id ASC LIMIT ?",
                (job_id, limit),
            ).fetchall()
            return [
                {
                    "id": int(row["id"]),
                    "job_id": row["job_id"],
                    "batch_id": row["batch_id"],
                    "at": row["at"],
                    "kind": row["kind"],
                    "detail": loads(row["detail_json"]) or {},
                }
                for row in rows
            ]

        return self._auto(run)

    def get_stats(self) -> dict[str, Any]:
        def run() -> dict[str, Any]:
            by_state = empty_counts()
            for row in self._conn.execute("SELECT state, COUNT(*) AS n FROM jobs GROUP BY state"):
                by_state[row["state"]] = int(row["n"])
            by_model: dict[str, int] = {}
            for row in self._conn.execute(
                f"SELECT resolved_model AS model, COUNT(*) AS n FROM jobs WHERE state IN ({_INCOMPLETE_SQL}) GROUP BY resolved_model"
            ):
                by_model[row["model"]] = int(row["n"])
            by_class: dict[str, int] = {}
            for row in self._conn.execute(
                "SELECT scheduling_class AS cls, COUNT(*) AS n FROM jobs WHERE state=? GROUP BY scheduling_class",
                (QUEUED,),
            ):
                by_class[row["cls"]] = int(row["n"])
            events = int(self._conn.execute("SELECT COUNT(*) AS n FROM events").fetchone()["n"])
            depth = by_state.get("queued", 0) + by_state.get("retry_wait", 0) + by_state.get("staged", 0)
            return {
                "jobs_by_state": by_state,
                "jobs_by_model": by_model,
                "queued_by_class": by_class,
                "queue_depth": depth,
                "active": by_state.get("leased", 0) + by_state.get("running", 0),
                "event_count": events,
            }

        return self._auto(run)

    def cleanup(self, *, success_before: str, failure_before: str, event_before: str) -> dict[str, int]:
        def run() -> dict[str, int]:
            cur = self._conn.execute(
                """
                DELETE FROM jobs
                 WHERE finished_at IS NOT NULL
                   AND state NOT IN ({incomplete})
                   AND (
                        (state=? AND finished_at<?)
                        OR (state IN (?, ?, ?) AND finished_at<?)
                   )
                """.format(incomplete=_INCOMPLETE_SQL),
                (SUCCEEDED, success_before, FAILED, DEAD_LETTER, CANCELLED, failure_before),
            )
            ev = self._conn.execute("DELETE FROM events WHERE at<?", (event_before,))
            return {"jobs": cur.rowcount, "events": ev.rowcount}

        return self._tx(run)

    def requeue(self, job_id: str, now_iso: str) -> Job:
        def run() -> Job:
            row = self._job_row(job_id)
            if row is None:
                raise NotFoundError(f"job {job_id} not found")
            if row["state"] not in {FAILED, DEAD_LETTER}:
                raise ValidationError(f"job {job_id} is {row['state']} and cannot be requeued")
            self._conn.execute(
                """
                UPDATE jobs
                   SET state=?, attempt_count=0, error_json=NULL, result_json=NULL, started_at=NULL,
                       finished_at=NULL, lease_owner=NULL, lease_expires_at=NULL, not_before=NULL,
                       cancel_requested=0, queue_ms=NULL, execution_ms=NULL, total_ms=NULL, updated_at=?
                 WHERE job_id=?
                """,
                (QUEUED, now_iso, job_id),
            )
            job = self._require_job(job_id)
            self._emit("REQUEUED", now_iso, job_id=job_id, batch_id=job.batch_id, detail={})
            return job

        return self._tx(run)

    def queue_depth(self) -> int:
        return int(
            self._auto(
                lambda: self._conn.execute(
                    f"SELECT COUNT(*) AS n FROM jobs WHERE state IN ({_INCOMPLETE_SQL})"
                ).fetchone()["n"]
            )
        )

    def pending_count(self) -> int:
        return self.queue_depth()

    def ready_candidates(self) -> list[Candidate]:
        def run() -> list[Candidate]:
            rows = self._conn.execute(
                """
                SELECT job_id, scheduling_class, priority, created_at, client_id, resolved_model, questions_hash
                  FROM jobs
                 WHERE state=?
                   AND (batch_id IS NULL OR EXISTS (
                        SELECT 1 FROM batches b WHERE b.batch_id=jobs.batch_id AND b.status=?
                   ))
                """,
                (QUEUED, BATCH_ACTIVE),
            ).fetchall()
            return [self._candidate_row(row) for row in rows]

        return self._auto(run)

    def next_wake_s(self, now_iso: str) -> float | None:
        def run() -> float | None:
            row = self._conn.execute(
                """
                SELECT MIN(moment) AS moment FROM (
                    SELECT not_before AS moment FROM jobs
                     WHERE state=? AND not_before IS NOT NULL AND not_before>?
                    UNION ALL
                    SELECT lease_expires_at AS moment FROM jobs
                     WHERE state IN (?, ?) AND lease_expires_at IS NOT NULL AND lease_expires_at>?
                )
                """,
                (RETRY_WAIT, now_iso, LEASED, RUNNING, now_iso),
            ).fetchone()
            if row is None or row["moment"] is None:
                return None
            return max(0.0, (parse_iso(row["moment"]) - parse_iso(now_iso)).total_seconds())

        return self._auto(run)

    def release_leases(self, owner_prefix: str, now_iso: str) -> list[str]:
        def run() -> list[str]:
            rows = self._conn.execute(
                "SELECT job_id, lease_owner FROM jobs WHERE state IN (?, ?)",
                (LEASED, RUNNING),
            ).fetchall()
            report = Maintenance()
            released: list[str] = []
            for row in rows:
                owner = row["lease_owner"] or ""
                if not owner.startswith(owner_prefix):
                    continue
                self._expire_sql(row["job_id"], now_iso, report)
                released.append(row["job_id"])
            return released

        return self._tx(run)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._conn.close()

    def _claim_sql(self, job_id: str, owner: str, lease_until: str, now_iso: str) -> int:
        cur = self._conn.execute(
            """
            UPDATE jobs
               SET state=?, lease_owner=?, lease_expires_at=?, attempt_count=attempt_count+1, updated_at=?
             WHERE job_id=? AND state=?
               AND (not_before IS NULL OR not_before<=?)
               AND (deadline_at IS NULL OR deadline_at>?)
               AND cancel_requested=0
               AND (batch_id IS NULL OR EXISTS (
                    SELECT 1 FROM batches b WHERE b.batch_id=jobs.batch_id AND b.status=?
               ))
            """,
            (LEASED, owner, lease_until, now_iso, job_id, QUEUED, now_iso, now_iso, BATCH_ACTIVE),
        )
        return int(cur.rowcount)

    def _candidates(self, model: str, now_iso: str, limit: int) -> list[Candidate]:
        found: list[Candidate] = []
        for scheduling_class in ("interactive", "normal", "batch", "bulk"):
            rows = self._conn.execute(
                """
                SELECT job_id, scheduling_class, priority, created_at, client_id, resolved_model, questions_hash
                  FROM jobs
                 WHERE state=? AND resolved_model=? AND scheduling_class=?
                   AND (not_before IS NULL OR not_before<=?)
                   AND (deadline_at IS NULL OR deadline_at>?)
                   AND cancel_requested=0
                   AND (batch_id IS NULL OR EXISTS (
                        SELECT 1 FROM batches b WHERE b.batch_id=jobs.batch_id AND b.status=?
                   ))
                 ORDER BY priority DESC, created_at ASC, job_id ASC
                 LIMIT ?
                """,
                (QUEUED, model, scheduling_class, now_iso, now_iso, BATCH_ACTIVE, limit),
            ).fetchall()
            found.extend(self._candidate_row(row) for row in rows)
        return found

    def _expire_sql(self, job_id: str, now_iso: str, report: Maintenance) -> None:
        row = self._job_row(job_id)
        if row is None:
            return
        self._emit(
            "LEASE_EXPIRED",
            now_iso,
            job_id=job_id,
            batch_id=row["batch_id"],
            detail={"attempt": int(row["attempt_count"])},
        )
        if row["cancel_requested"]:
            self._mark_cancelled_sql(job_id, row["batch_id"], now_iso)
            report.expired_cancelled += 1
            report.finished.append(job_id)
            return
        if int(row["attempt_count"]) >= int(row["max_attempts"]):
            self._fail_sql(
                job_id,
                "",
                {"code": "lease_expired", "message": "lease expired after the last attempt", "retryable": True},
                True,
                now_iso,
                require_owner=False,
                from_state=str(row["state"]),
            )
            report.dead_lettered += 1
            report.finished.append(job_id)
            return
        self._conn.execute(
            """
            UPDATE jobs
               SET state=?, lease_owner=NULL, lease_expires_at=NULL, updated_at=?
             WHERE job_id=?
            """,
            (QUEUED, now_iso, job_id),
        )
        report.recovered += 1
        report.requeued.append(job_id)

    def _fail_sql(
        self,
        job_id: str,
        owner: str,
        error: dict[str, Any],
        dead_letter: bool,
        now_iso: str,
        *,
        require_owner: bool,
        from_state: str = RUNNING,
    ) -> bool:
        state = DEAD_LETTER if dead_letter else FAILED
        if require_owner:
            cur = self._conn.execute(
                """
                UPDATE jobs
                   SET state=?, error_json=?, finished_at=?, updated_at=?, lease_owner=NULL, lease_expires_at=NULL
                 WHERE job_id=? AND lease_owner=? AND state=?
                """,
                (state, dumps(error), now_iso, now_iso, job_id, owner, RUNNING),
            )
        else:
            cur = self._conn.execute(
                """
                UPDATE jobs
                   SET state=?, error_json=?, finished_at=?, updated_at=?, lease_owner=NULL, lease_expires_at=NULL
                 WHERE job_id=? AND state=?
                """,
                (state, dumps(error), now_iso, now_iso, job_id, from_state),
            )
        if cur.rowcount != 1:
            return False
        job = self._require_job(job_id)
        kind = "DEAD_LETTERED" if dead_letter else "FAILED"
        self._emit(kind, now_iso, job_id=job_id, batch_id=job.batch_id, detail={"code": error.get("code")})
        return True

    def _mark_cancelled_sql(self, job_id: str, batch_id: str | None, now_iso: str) -> None:
        self._conn.execute(
            """
            UPDATE jobs
               SET state=?, finished_at=?, updated_at=?, lease_owner=NULL, lease_expires_at=NULL, cancel_requested=1
             WHERE job_id=?
            """,
            (CANCELLED, now_iso, now_iso, job_id),
        )
        self._emit("CANCELLED", now_iso, job_id=job_id, batch_id=batch_id, detail={})

    def _insert_job(self, job: Job) -> None:
        self._conn.execute(
            """
            INSERT INTO jobs (
                job_id, batch_id, batch_ordinal, idempotency_key, idempotency_hash, client_id,
                created_at, updated_at, started_at, finished_at, not_before, priority, scheduling_class,
                requested_model, resolved_model, model_version, state, payload_json, questions_hash,
                attempt_count, max_attempts, lease_owner, lease_expires_at, execution_timeout_ms,
                deadline_at, result_json, error_json, queue_ms, execution_ms, total_ms,
                cancel_requested, fallback_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                job.job_id,
                job.batch_id,
                job.batch_ordinal,
                job.idempotency_key,
                job.idempotency_hash,
                job.client_id,
                job.created_at,
                job.updated_at,
                job.started_at,
                job.finished_at,
                job.not_before,
                job.priority,
                job.scheduling_class,
                job.requested_model,
                job.resolved_model,
                job.model_version,
                job.state,
                dumps(job.payload),
                job.questions_hash,
                job.attempt_count,
                job.max_attempts,
                job.lease_owner,
                job.lease_expires_at,
                job.execution_timeout_ms,
                job.deadline_at,
                dumps(job.result) if job.result is not None else None,
                dumps(job.error) if job.error is not None else None,
                job.queue_ms,
                job.execution_ms,
                job.total_ms,
                1 if job.cancel_requested else 0,
                dumps(job.fallback) if job.fallback is not None else None,
            ),
        )

    def _existing_job(self, job: Job) -> Job | None:
        if not job.idempotency_key:
            return None
        row = self._conn.execute(
            "SELECT * FROM jobs WHERE client_id=? AND idempotency_key=?",
            (job.client_id, job.idempotency_key),
        ).fetchone()
        if row is None:
            return None
        if row["idempotency_hash"] != job.idempotency_hash:
            raise ConflictError(
                "idempotency key was already used with a different payload",
                details={"job_id": row["job_id"]},
            )
        found = self._to_job(row)
        found.replayed = True
        return found

    def _existing_batch(self, batch: Batch) -> Batch | None:
        if not batch.idempotency_key:
            return None
        row = self._conn.execute(
            "SELECT * FROM batches WHERE client_id=? AND idempotency_key=?",
            (batch.client_id, batch.idempotency_key),
        ).fetchone()
        if row is None:
            return None
        if row["idempotency_hash"] != batch.idempotency_hash:
            raise ConflictError(
                "batch idempotency key was already used with a different payload",
                details={"batch_id": row["batch_id"]},
            )
        view = self._batch_view(row["batch_id"])
        view.replayed = True
        return view

    def _reserve_capacity(self, client_id: str, additional: int) -> None:
        depth = int(
            self._conn.execute(f"SELECT COUNT(*) AS n FROM jobs WHERE state IN ({_INCOMPLETE_SQL})").fetchone()["n"]
        )
        if depth + additional > self.limits.max_queued_jobs:
            raise BackpressureError(f"queue is full ({self.limits.max_queued_jobs})", code="queue_full")
        client_jobs = int(
            self._conn.execute(
                f"SELECT COUNT(*) AS n FROM jobs WHERE client_id=? AND state IN ({_INCOMPLETE_SQL})",
                (client_id,),
            ).fetchone()["n"]
        )
        if client_jobs + additional > self.limits.max_active_jobs_per_client:
            raise BackpressureError(
                f"client {client_id or '(anonymous)'} has too many active jobs",
                code="client_full",
            )

    def _emit(self, kind: str, now_iso: str, *, job_id: str | None, batch_id: str | None, detail: dict[str, Any]) -> None:
        self._conn.execute(
            "INSERT INTO events (job_id, batch_id, at, kind, detail_json) VALUES (?, ?, ?, ?, ?)",
            (job_id, batch_id, now_iso, kind, dumps(detail)),
        )

    def _batch_view(self, batch_id: str) -> Batch:
        row = self._batch_row(batch_id)
        if row is None:
            raise NotFoundError(f"batch {batch_id} not found")
        counts = empty_counts()
        for item in self._conn.execute("SELECT state, COUNT(*) AS n FROM jobs WHERE batch_id=? GROUP BY state", (batch_id,)):
            counts[item["state"]] = int(item["n"])
        batch = Batch(
            batch_id=row["batch_id"],
            client_id=row["client_id"],
            status=row["status"],
            requested_model=row["requested_model"],
            job_count=int(row["job_count"]),
            created_at=row["created_at"],
            activated_at=row["activated_at"],
            idempotency_key=row["idempotency_key"],
            idempotency_hash=row["idempotency_hash"],
            deadline_at=row["deadline_at"],
            counts=counts,
        )
        return batch

    def _to_job(self, row: sqlite3.Row) -> Job:
        return Job(
            job_id=row["job_id"],
            batch_id=row["batch_id"],
            batch_ordinal=row["batch_ordinal"],
            idempotency_key=row["idempotency_key"],
            idempotency_hash=row["idempotency_hash"],
            client_id=row["client_id"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            not_before=row["not_before"],
            priority=int(row["priority"]),
            scheduling_class=row["scheduling_class"],
            requested_model=row["requested_model"],
            resolved_model=row["resolved_model"],
            model_version=row["model_version"],
            state=row["state"],
            payload=loads(row["payload_json"]) or {},
            questions_hash=row["questions_hash"],
            attempt_count=int(row["attempt_count"]),
            max_attempts=int(row["max_attempts"]),
            lease_owner=row["lease_owner"],
            lease_expires_at=row["lease_expires_at"],
            execution_timeout_ms=row["execution_timeout_ms"],
            deadline_at=row["deadline_at"],
            result=loads(row["result_json"]),
            error=loads(row["error_json"]),
            queue_ms=row["queue_ms"],
            execution_ms=row["execution_ms"],
            total_ms=row["total_ms"],
            cancel_requested=bool(row["cancel_requested"]),
            fallback=loads(row["fallback_json"]),
        )

    def _candidate_row(self, row: sqlite3.Row) -> Candidate:
        return Candidate(
            job_id=row["job_id"],
            scheduling_class=row["scheduling_class"],
            priority=int(row["priority"]),
            created_at=parse_iso(row["created_at"]).timestamp(),
            client_id=row["client_id"] or "",
            model=row["resolved_model"],
            questions_hash=row["questions_hash"],
        )

    def _job_row(self, job_id: str) -> sqlite3.Row | None:
        return self._conn.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()

    def _require_job(self, job_id: str) -> Job:
        row = self._job_row(job_id)
        if row is None:
            raise NotFoundError(f"job {job_id} not found")
        return self._to_job(row)

    def _batch_row(self, batch_id: str) -> sqlite3.Row | None:
        return self._conn.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()

    def _abort_orphan_staging(self) -> None:
        def run() -> None:
            rows = self._conn.execute("SELECT batch_id FROM batches WHERE status=?", (BATCH_STAGING,)).fetchall()
            for row in rows:
                self._conn.execute("DELETE FROM jobs WHERE batch_id=?", (row["batch_id"],))
                self._conn.execute(
                    """
                    UPDATE batches
                       SET status=?, job_count=0, idempotency_key=NULL, idempotency_hash=NULL
                     WHERE batch_id=?
                    """,
                    (BATCH_ABORTED, row["batch_id"]),
                )

        self._tx(run)

    def _migrate(self, target: int) -> None:
        with self._lock:
            self._conn.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)")
            row = self._conn.execute("SELECT version FROM schema_version").fetchone()
            if row is None:
                self._conn.execute("INSERT INTO schema_version(version) VALUES (0)")
                current = 0
            else:
                current = int(row["version"])
            for version in range(1, target + 1):
                if version <= current:
                    continue
                MIGRATIONS[version](self._conn)
                self._conn.execute("UPDATE schema_version SET version=?", (version,))
                current = version

    def _tx(self, fn: Any) -> Any:
        last: Exception | None = None
        for attempt in range(6):
            try:
                return self._tx_once(fn)
            except sqlite3.OperationalError as exc:
                last = exc
                text = str(exc).lower()
                if "locked" not in text and "busy" not in text:
                    raise
                time.sleep(0.01 * (attempt + 1))
        assert last is not None
        raise last

    def _tx_once(self, fn: Any) -> Any:
        with self._lock:
            self._ensure()
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                result = fn()
            except Exception:
                self._conn.rollback()
                raise
            else:
                self._conn.commit()
                return result

    def _auto(self, fn: Any) -> Any:
        with self._lock:
            self._ensure()
            return fn()

    def _ensure(self) -> None:
        if self._closed:
            raise ValidationError("queue is closed")
