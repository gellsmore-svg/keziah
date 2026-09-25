"""Configuration. Precedence is CLI overrides, then environment, then file, then defaults."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

from keziah.errors import ConfigError
from keziah.types import SCHEDULING_CLASSES

ExecutionKind = Literal["inline", "thread", "async"]
QueueMode = Literal["memory", "disk", "hybrid"]


class QueueSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: QueueMode = "hybrid"
    sqlite_path: str = "~/.keziah/keziah.db"
    busy_timeout_ms: int = 5000
    # Rebuild the hybrid ready-index from SQLite on this interval.
    reconcile_interval_s: float = 2.0


class ServerSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    host: str = "127.0.0.1"
    port: int = 8766
    api_key: str | None = None
    api_key_env: str = "KEZIAH_API_KEY"
    # Durable default: an accepted synchronous call keeps running if the client leaves.
    cancel_on_disconnect: bool = False
    log_level: str = "info"
    log_json: bool = True


class SchedulerSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_queued_jobs: int = 100_000
    max_batch_size: int = 200_000
    max_request_bytes: int = 1_000_000
    max_batch_bytes: int = 64_000_000
    max_active_jobs_per_client: int = 100_000
    max_questions: int = 64
    default_class: str = "normal"
    lease_seconds: float = 30.0
    heartbeat_fraction: float = 0.3
    recovery_interval_s: float = 0.25
    default_max_attempts: int = 3
    default_execution_timeout_ms: int = 30_000
    retry_base_s: float = 0.05
    retry_factor: float = 2.0
    retry_max_s: float = 30.0
    retry_jitter: float = 0.2
    # Within a class, one skip is worth this many priority points of deficit.
    client_skip_weight: float = 25.0
    priority_weight: float = 100.0
    age_weight: float = 1.0
    class_weights: dict[str, float] = Field(
        default_factory=lambda: {
            "interactive": 8.0,
            "normal": 4.0,
            "batch": 2.0,
            "bulk": 1.0,
        }
    )
    # Oldest job in a class increases that class's weight, capped at this multiple.
    starvation_boost_cap: float = 8.0
    starvation_seconds: float = 10.0
    # Disk mode reads this many candidates per scheduling class.
    disk_candidates_per_class: int = 32
    shutdown_grace_s: float = 5.0

    @field_validator("default_class")
    @classmethod
    def _class(cls, value: str) -> str:
        if value not in SCHEDULING_CLASSES:
            raise ValueError(f"default_class must be one of {SCHEDULING_CLASSES}")
        return value


class RetentionSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    success_seconds: float = 7 * 24 * 3600
    failure_seconds: float = 30 * 24 * 3600
    event_seconds: float = 7 * 24 * 3600
    auto_cleanup: bool = False
    cleanup_interval_s: float = 3600.0


class ModelSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    adapter: str
    enabled: bool = True
    execution: ExecutionKind = "async"
    max_concurrency: int = 4
    max_native_batch_size: int = 1
    max_native_batch_wait_ms: int = 0
    endpoint: str | None = None
    api_key_env: str | None = None
    model_name: str | None = None
    local: bool | None = None
    timeout_ms: int = 30_000
    device: str | None = None
    threads: int | None = None
    preload: bool = False
    checkpoint: str | None = None
    latency_ms: float = 0.0
    supports_native_batch: bool = False

    @field_validator("max_concurrency")
    @classmethod
    def _cap(cls, value: int) -> int:
        if value < 1:
            raise ValueError("max_concurrency must be >= 1")
        return value


class ModelGroupSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    primary: str
    fallbacks: list[str] = Field(default_factory=list)


class Settings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    queue: QueueSettings = Field(default_factory=QueueSettings)
    server: ServerSettings = Field(default_factory=ServerSettings)
    scheduler: SchedulerSettings = Field(default_factory=SchedulerSettings)
    retention: RetentionSettings = Field(default_factory=RetentionSettings)
    models: dict[str, ModelSettings] = Field(default_factory=dict)
    aliases: dict[str, str] = Field(default_factory=dict)
    model_groups: dict[str, ModelGroupSettings] = Field(default_factory=dict)

    def resolved_api_key(self) -> str | None:
        if self.server.api_key:
            return self.server.api_key
        env = os.environ.get(self.server.api_key_env, "").strip()
        return env or None


def default_settings() -> Settings:
    """Offline-runnable defaults. Laya and Jev are configured and report availability honestly."""
    return Settings(
        models={
            "mock": ModelSettings(
                adapter="mock",
                execution="async",
                max_concurrency=8,
                local=True,
                supports_native_batch=True,
                max_native_batch_size=8,
            ),
            "laya": ModelSettings(
                adapter="laya",
                execution="thread",
                max_concurrency=1,
                local=True,
                supports_native_batch=True,
                max_native_batch_size=8,
                max_native_batch_wait_ms=0,
            ),
            "jev": ModelSettings(
                adapter="jev",
                execution="async",
                max_concurrency=16,
                local=False,
                endpoint="https://api.typesafe.ai",
                api_key_env="TYPESAFE_API_KEY",
                model_name="jev-latest",
                supports_native_batch=False,
            ),
        },
        aliases={"default": "mock"},
    )


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _env_overlay() -> dict[str, Any]:
    """A small explicit map. Nested secrets stay in the config file's env pointers."""
    overlay: dict[str, Any] = {}
    queue: dict[str, Any] = {}
    server: dict[str, Any] = {}
    if os.environ.get("KEZIAH_MODE"):
        queue["mode"] = os.environ["KEZIAH_MODE"].strip()
    if os.environ.get("KEZIAH_DB"):
        queue["sqlite_path"] = os.environ["KEZIAH_DB"].strip()
    if os.environ.get("KEZIAH_HOST"):
        server["host"] = os.environ["KEZIAH_HOST"].strip()
    if os.environ.get("KEZIAH_PORT"):
        server["port"] = int(os.environ["KEZIAH_PORT"].strip())
    if os.environ.get("KEZIAH_LOG_LEVEL"):
        server["log_level"] = os.environ["KEZIAH_LOG_LEVEL"].strip()
    if os.environ.get("KEZIAH_API_KEY"):
        server["api_key"] = os.environ["KEZIAH_API_KEY"].strip()
    if queue:
        overlay["queue"] = queue
    if server:
        overlay["server"] = server
    return overlay


def load_settings(
    path: str | Path | None = None,
    *,
    overrides: dict[str, Any] | None = None,
    use_env: bool = True,
    use_defaults: bool = True,
) -> Settings:
    data: dict[str, Any] = {}
    if use_defaults:
        data = default_settings().model_dump()
    file_path = Path(path).expanduser() if path else None
    if file_path is None:
        env_path = os.environ.get("KEZIAH_CONFIG", "").strip()
        if env_path:
            file_path = Path(env_path).expanduser()
    if file_path is not None:
        if not file_path.is_file():
            raise ConfigError(f"config file not found: {file_path}")
        loaded = yaml.safe_load(file_path.read_text(encoding="utf-8")) or {}
        if not isinstance(loaded, dict):
            raise ConfigError("config file must be a mapping")
        # A file that sets ``models`` replaces the default catalogue rather than merging
        # half-configured entries with the built-in mock/laya/jev set.
        if "models" in loaded:
            data["models"] = {}
        if "aliases" in loaded:
            data["aliases"] = {}
        if "model_groups" in loaded:
            data["model_groups"] = {}
        data = _deep_merge(data, loaded)
    if use_env:
        data = _deep_merge(data, _env_overlay())
    if overrides:
        if "models" in overrides:
            data["models"] = {}
        data = _deep_merge(data, overrides)
    try:
        settings = Settings.model_validate(data)
    except Exception as exc:
        raise ConfigError(str(exc)) from exc
    _validate_graph(settings)
    return settings


def _validate_graph(settings: Settings) -> None:
    if not settings.models:
        raise ConfigError("at least one model must be configured")
    known = set(settings.models)
    for alias, target in settings.aliases.items():
        if alias in known:
            raise ConfigError(f"alias {alias!r} collides with a model id")
        if target in settings.aliases and target not in known and target not in settings.model_groups:
            # Chains are checked for cycles later; a target may be another alias.
            pass
    seen_aliases: set[str] = set()
    for alias in settings.aliases:
        cursor: str | None = alias
        trail: list[str] = []
        while cursor in settings.aliases:
            if cursor in trail:
                raise ConfigError(f"alias cycle: {' -> '.join(trail + [cursor])}")
            trail.append(cursor)
            cursor = settings.aliases[cursor]
        if cursor not in known and cursor not in settings.model_groups:
            raise ConfigError(f"alias {alias!r} resolves to unknown target {cursor!r}")
        seen_aliases.add(alias)
    for name, group in settings.model_groups.items():
        if name in known or name in settings.aliases:
            raise ConfigError(f"model group {name!r} collides with a model or alias")
        chain = [group.primary, *group.fallbacks]
        if not chain or not group.primary:
            raise ConfigError(f"model group {name!r} needs a primary")
        for item in chain:
            if item not in known:
                raise ConfigError(f"model group {name!r} references unknown model {item!r}")
    for model_id, spec in settings.models.items():
        if spec.adapter not in {"mock", "laya", "jev", "http", "systemone-http", "generic"}:
            raise ConfigError(f"model {model_id!r} has unknown adapter {spec.adapter!r}")
        if spec.adapter in {"http", "systemone-http", "generic"} and not spec.endpoint:
            raise ConfigError(f"model {model_id!r} http adapter requires endpoint")


def loopback_host(host: str) -> bool:
    return host.strip().lower() in {"127.0.0.1", "localhost", "::1"}
