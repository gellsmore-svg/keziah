"""Configured models, aliases, and explicit fallback groups.

Aliases are a deterministic rename. Groups are the only path that may run a
job on a model other than the one the caller named, and only when the caller
named the group.
"""

from __future__ import annotations

from typing import Any

from keziah.adapters import build_adapter
from keziah.adapters.base import info_from
from keziah.config import Settings
from keziah.errors import ModelUnavailableError, ModelUnknownError
from keziah.types import ModelHealth, ModelInfo, Resolution


class Registry:
    def __init__(self, settings: Settings, adapters: dict[str, Any] | None = None) -> None:
        self.settings = settings
        self._adapters: dict[str, Any] = {}
        supplied = adapters or {}
        for model_id, spec in settings.models.items():
            self._adapters[model_id] = supplied.get(model_id) or build_adapter(model_id, spec)

    def adapter(self, model_id: str) -> Any:
        try:
            return self._adapters[model_id]
        except KeyError as exc:
            raise ModelUnknownError(f"unknown model {model_id!r}") from exc

    def health(self, model_id: str) -> ModelHealth:
        return self.adapter(model_id).health_sync()

    def follow(self, name: str) -> str:
        seen: list[str] = []
        cursor = name
        while cursor in self.settings.aliases:
            if cursor in seen:
                raise ModelUnknownError(f"alias cycle at {cursor!r}")
            seen.append(cursor)
            cursor = self.settings.aliases[cursor]
        return cursor

    def is_group(self, name: str) -> bool:
        return self.follow(name) in self.settings.model_groups

    def resolve(self, name: str) -> Resolution:
        expanded = self.follow(name)
        if expanded in self.settings.model_groups:
            return self._resolve_group(name, expanded)
        if expanded not in self.settings.models:
            raise ModelUnknownError(
                f"unknown model {name!r}. Configured models: {', '.join(sorted(self.settings.models))}"
            )
        spec = self.settings.models[expanded]
        if not spec.enabled:
            raise ModelUnavailableError(f"model {expanded!r} is disabled")
        return Resolution(
            requested=name,
            resolved=expanded,
            adapter=spec.adapter,
            fallback_used=False,
            fallback_reason=None,
            chain=(expanded,),
        )

    def describe(self, active: dict[str, int] | None = None) -> list[ModelInfo]:
        active = active or {}
        found: list[ModelInfo] = []
        for model_id, spec in self.settings.models.items():
            health = self.health(model_id)
            adapter = self.adapter(model_id)
            found.append(
                info_from(
                    model_id=model_id,
                    adapter=spec.adapter,
                    health=health,
                    caps=adapter.capabilities,
                    max_concurrency=spec.max_concurrency,
                    execution=spec.execution,
                    endpoint=spec.endpoint,
                    enabled=spec.enabled,
                    active=active.get(model_id, 0),
                )
            )
        return found

    def shutdown(self) -> None:
        for adapter in self._adapters.values():
            shutdown = getattr(adapter, "shutdown_sync", None)
            if shutdown is not None:
                shutdown()

    def _resolve_group(self, requested: str, group_name: str) -> Resolution:
        group = self.settings.model_groups[group_name]
        chain = (group.primary, *tuple(group.fallbacks))
        for index, model_id in enumerate(chain):
            spec = self.settings.models[model_id]
            if not spec.enabled:
                continue
            health = self.health(model_id)
            if health.ok:
                return Resolution(
                    requested=requested,
                    resolved=model_id,
                    adapter=spec.adapter,
                    fallback_used=index > 0,
                    fallback_reason=None if index == 0 else "primary_unavailable",
                    chain=chain,
                )
        primary = self.settings.models[chain[0]]
        if not primary.enabled:
            raise ModelUnavailableError(f"model group {group_name!r} has no enabled model")
        # Keep the job on the primary. Execution records a real unavailable error
        # or, on a later attempt, resolves the group again.
        return Resolution(
            requested=requested,
            resolved=chain[0],
            adapter=primary.adapter,
            fallback_used=False,
            fallback_reason=None,
            chain=chain,
        )
