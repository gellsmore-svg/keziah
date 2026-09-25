"""Queue records and the System-1 request shape.

Wire values are lowercase. The state diagram in the docs uses the same names.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Job states.
STAGED = "staged"
QUEUED = "queued"
LEASED = "leased"
RUNNING = "running"
SUCCEEDED = "succeeded"
RETRY_WAIT = "retry_wait"
FAILED = "failed"
DEAD_LETTER = "dead_letter"
CANCELLED = "cancelled"

TERMINAL_STATES = frozenset({SUCCEEDED, FAILED, DEAD_LETTER, CANCELLED})
INCOMPLETE_STATES = frozenset({STAGED, QUEUED, LEASED, RUNNING, RETRY_WAIT})

# Batch submission visibility. Completion of the work is derived from job states.
BATCH_STAGING = "staging"
BATCH_ACTIVE = "active"
BATCH_ABORTED = "aborted"

SCHEDULING_CLASSES = ("interactive", "normal", "batch", "bulk")

# Question types. ``options`` on a choice is normalised to a criteria map.
QUESTION_TYPES = ("choice", "score", "noul")


def normalise_questions(questions: Any, *, max_questions: int = 64) -> dict[str, dict[str, Any]]:
    """Validate and normalise a question map. Raises ValidationError."""
    from keziah.errors import ValidationError

    if not isinstance(questions, dict) or not questions:
        raise ValidationError("questions must be a non-empty object")
    if len(questions) > max_questions:
        raise ValidationError(
            f"too many questions ({len(questions)} > {max_questions})",
            code="request_too_large",
        )
    normalised: dict[str, dict[str, Any]] = {}
    for raw_key, raw in questions.items():
        key = str(raw_key)
        if not isinstance(raw, dict):
            raise ValidationError(f"question {key!r} must be an object")
        kind = raw.get("type")
        if kind not in QUESTION_TYPES:
            raise ValidationError(
                f"question {key!r} has unsupported type {kind!r}; "
                f"expected one of {', '.join(QUESTION_TYPES)}",
                code="unsupported_question",
            )
        if "instructions" not in raw:
            raise ValidationError(f"question {key!r} requires instructions")
        item: dict[str, Any] = {"type": kind, "instructions": raw["instructions"]}
        if kind == "choice":
            criteria = raw.get("criteria")
            options = raw.get("options")
            if isinstance(criteria, dict) and criteria:
                if len(criteria) > 255:
                    raise ValidationError(f"question {key!r} has more than 255 options")
                item["criteria"] = {str(k): v for k, v in criteria.items()}
            elif isinstance(options, list) and options:
                if len(options) > 255:
                    raise ValidationError(f"question {key!r} has more than 255 options")
                item["criteria"] = {str(option): None for option in options}
            else:
                raise ValidationError(
                    f"question {key!r} is a choice and requires criteria or options"
                )
        elif kind == "score":
            criteria = raw.get("criteria")
            if criteria is not None:
                if not isinstance(criteria, list) or not 2 <= len(criteria) <= 10:
                    raise ValidationError(
                        f"question {key!r} score criteria must be a list of 2 to 10 levels"
                    )
                item["criteria"] = list(criteria)
        else:
            criteria = raw.get("criteria")
            if criteria is not None:
                item["criteria"] = criteria
        normalised[key] = item
    return normalised


def validate_state(state: Any) -> None:
    from keziah.errors import ValidationError

    if not isinstance(state, (str, dict, list)):
        raise ValidationError("state must be a string, object, or array")


@dataclass(slots=True)
class Job:
    job_id: str
    batch_id: str | None
    batch_ordinal: int | None
    idempotency_key: str | None
    idempotency_hash: str | None
    client_id: str
    created_at: str
    updated_at: str
    started_at: str | None
    finished_at: str | None
    not_before: str | None
    priority: int
    scheduling_class: str
    requested_model: str
    resolved_model: str
    model_version: str | None
    state: str
    payload: dict[str, Any]
    questions_hash: str
    attempt_count: int
    max_attempts: int
    lease_owner: str | None
    lease_expires_at: str | None
    execution_timeout_ms: int | None
    deadline_at: str | None
    result: dict[str, Any] | None
    error: dict[str, Any] | None
    queue_ms: int | None
    execution_ms: int | None
    total_ms: int | None
    cancel_requested: bool
    fallback: dict[str, Any] | None
    replayed: bool = False

    @property
    def schedule_model(self) -> str:
        return self.resolved_model


@dataclass(slots=True)
class Batch:
    batch_id: str
    client_id: str
    status: str
    requested_model: str | None
    job_count: int
    created_at: str
    activated_at: str | None
    idempotency_key: str | None
    idempotency_hash: str | None
    deadline_at: str | None
    counts: dict[str, int] = field(default_factory=dict)
    replayed: bool = False

    @property
    def completion(self) -> str:
        if self.status == BATCH_ABORTED:
            return "aborted"
        if self.status == BATCH_STAGING:
            return "staging"
        pending = sum(self.counts.get(name, 0) for name in INCOMPLETE_STATES)
        if self.job_count > 0 and pending == 0:
            return "complete"
        return "open"


@dataclass(frozen=True, slots=True)
class Candidate:
    """Lightweight scheduling view. The full payload stays in the backend."""

    job_id: str
    scheduling_class: str
    priority: int
    created_at: float
    client_id: str
    model: str
    questions_hash: str


@dataclass(slots=True)
class Resolution:
    requested: str
    resolved: str
    adapter: str
    fallback_used: bool
    fallback_reason: str | None
    chain: tuple[str, ...]


@dataclass(slots=True)
class SystemOneRequest:
    state: Any
    questions: dict[str, Any]
    model: str
    parameters: dict[str, Any] = field(default_factory=dict)
    job_id: str | None = None


@dataclass(slots=True)
class SystemOneResponse:
    answers: dict[str, Any]
    model_version: str | None = None
    usage: dict[str, Any] | None = None
    inference_seconds: float = 0.0
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class AdapterCapabilities:
    choice: bool = True
    score: bool = True
    noul: bool = True
    supports_native_batch: bool = False
    local: bool = True
    recommended_concurrency: int = 4


@dataclass(slots=True)
class ModelHealth:
    ok: bool
    permanent: bool = False
    detail: str = ""
    version: str | None = None


@dataclass(slots=True)
class ModelInfo:
    id: str
    adapter: str
    available: bool
    local: bool
    version: str | None
    capabilities: list[str]
    max_concurrency: int
    active: int = 0
    available_slots: int = 0
    execution: str = "async"
    health: str = "unknown"
    endpoint: str | None = None
    supports_native_batch: bool = False
    enabled: bool = True
