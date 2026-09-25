"""Build the adapter named by a model configuration entry."""

from __future__ import annotations

from typing import Any

from keziah.config import ModelSettings
from keziah.errors import ConfigError


def build_adapter(model_id: str, spec: ModelSettings) -> Any:
    kind = spec.adapter
    if kind == "mock":
        from keziah.adapters.mock import MockAdapter

        return MockAdapter(
            model_id,
            latency_s=spec.latency_ms / 1000.0,
            supports_native_batch=spec.supports_native_batch or spec.max_native_batch_size > 1,
        )
    if kind == "laya":
        from keziah.adapters.laya import LayaAdapter

        return LayaAdapter(
            model_id,
            device=spec.device,
            threads=spec.threads,
            preload=spec.preload,
            checkpoint=spec.checkpoint,
            supports_native_batch=spec.supports_native_batch or spec.max_native_batch_size > 1,
        )
    if kind == "jev":
        from keziah.adapters.jev import JevAdapter

        return JevAdapter(
            model_id,
            endpoint=spec.endpoint or "https://api.typesafe.ai",
            api_key_env=spec.api_key_env or "TYPESAFE_API_KEY",
            model_name=spec.model_name or "jev-latest",
            timeout_s=spec.timeout_ms / 1000.0,
            health_ttl_s=spec.health_ttl_s,
        )
    if kind in {"http", "systemone-http", "generic"}:
        from keziah.adapters.http import GenericSystemOneHTTPAdapter

        if not spec.endpoint:
            raise ConfigError(f"model {model_id} requires endpoint")
        return GenericSystemOneHTTPAdapter(
            model_id,
            endpoint=spec.endpoint,
            api_key_env=spec.api_key_env,
            model_name=spec.model_name,
            timeout_s=spec.timeout_ms / 1000.0,
            supports_native_batch=spec.supports_native_batch,
            local=bool(spec.local),
        )
    raise ConfigError(f"unknown adapter {kind}")
