"""System-1 adapter protocol.

Adapters answer a state plus bounded questions. They do not generate free text.
"""

from __future__ import annotations

from typing import Protocol

from keziah.types import AdapterCapabilities, ModelHealth, ModelInfo, SystemOneRequest, SystemOneResponse


class SystemOneAdapter(Protocol):
    model_id: str
    capabilities: AdapterCapabilities

    def health_sync(self) -> ModelHealth: ...

    async def health(self) -> ModelHealth: ...

    async def infer(self, request: SystemOneRequest) -> SystemOneResponse: ...

    def infer_blocking(self, request: SystemOneRequest) -> SystemOneResponse: ...

    async def shutdown(self) -> None: ...


def capability_names(caps: AdapterCapabilities) -> list[str]:
    names: list[str] = []
    if caps.choice:
        names.append("choice")
    if caps.score:
        names.append("score")
    if caps.noul:
        names.append("noul")
    return names


def info_from(
    *,
    model_id: str,
    adapter: str,
    health: ModelHealth,
    caps: AdapterCapabilities,
    max_concurrency: int,
    execution: str,
    endpoint: str | None,
    enabled: bool,
    active: int = 0,
) -> ModelInfo:
    return ModelInfo(
        id=model_id,
        adapter=adapter,
        available=bool(enabled and health.ok),
        local=caps.local,
        version=health.version,
        capabilities=capability_names(caps),
        max_concurrency=max_concurrency,
        active=active,
        available_slots=max(0, max_concurrency - active),
        execution=execution,
        health="ok" if health.ok else ("down" if health.permanent else "unavailable"),
        endpoint=endpoint,
        supports_native_batch=caps.supports_native_batch,
        enabled=enabled,
    )
