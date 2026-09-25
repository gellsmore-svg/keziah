"""Public job result. This is what callers compare across models."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(slots=True)
class Result:
    job_id: str
    status: str
    batch_id: str | None
    batch_ordinal: int | None
    requested_model: str
    resolved_model: str | None
    adapter: str | None
    model_version: str | None
    fallback_used: bool
    fallback_reason: str | None
    attempts: int
    timings: dict[str, Any]
    request: dict[str, Any]
    response: dict[str, Any] | None
    error: dict[str, Any] | None
    finished_at: str | None
    client_id: str = ""
    scheduling_class: str = "normal"
    priority: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class BatchReceipt:
    batch_id: str
    job_count: int
    status: str
    created_at: str
    completion: str
    idempotent_replay: bool = False
    counts: dict[str, int] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
