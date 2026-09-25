"""Shared fixtures. Core tests use the mock adapter only."""

from __future__ import annotations

from pathlib import Path

import pytest

from keziah.config import ModelSettings, QueueSettings, SchedulerSettings, Settings

QUESTIONS = {
    "department": {
        "type": "choice",
        "instructions": "Which team?",
        "options": ["billing", "technical", "sales"],
    },
    "needs_human": {"type": "noul", "instructions": "Does this need a human?"},
}


def make_settings(
    tmp_path: Path,
    mode: str = "memory",
    *,
    concurrency: int = 4,
    max_attempts: int = 3,
    max_queued: int = 1000,
    execution: str = "async",
    lease_seconds: float = 30.0,
    extra_models: dict | None = None,
    groups: dict | None = None,
    aliases: dict | None = None,
) -> Settings:
    models = {
        "mock": ModelSettings(
            adapter="mock",
            execution=execution,  # type: ignore[arg-type]
            max_concurrency=concurrency,
            local=True,
            supports_native_batch=False,
            max_native_batch_size=1,
        )
    }
    if extra_models:
        models.update(extra_models)
    return Settings(
        queue=QueueSettings(mode=mode, sqlite_path=str(tmp_path / "keziah.db")),  # type: ignore[arg-type]
        scheduler=SchedulerSettings(
            retry_jitter=0.0,
            retry_base_s=0.0,
            recovery_interval_s=0.05,
            default_max_attempts=max_attempts,
            max_queued_jobs=max_queued,
            lease_seconds=lease_seconds,
            max_batch_size=5000,
        ),
        models=models,
        model_groups=groups or {},
        aliases=aliases or {},
    )


@pytest.fixture
def questions() -> dict:
    return QUESTIONS
