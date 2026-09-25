"""Generic System-1 HTTP adapter.

Any service that speaks ``POST /v1/systemone`` with ``state`` and ``questions``
can be configured here. This is not an LLM chat adapter.
"""

from __future__ import annotations

import time
from typing import Any

import httpx

from keziah.adapters.http_common import map_http_error, response_from_payload, secret_from_env
from keziah.errors import RetryableInferenceError
from keziah.types import AdapterCapabilities, ModelHealth, SystemOneRequest, SystemOneResponse


class GenericSystemOneHTTPAdapter:
    def __init__(
        self,
        model_id: str,
        *,
        endpoint: str,
        api_key_env: str | None = None,
        model_name: str | None = None,
        timeout_s: float = 30.0,
        supports_native_batch: bool = False,
        local: bool = False,
        path: str = "/v1/systemone",
    ) -> None:
        self.model_id = model_id
        self.endpoint = endpoint.rstrip("/")
        self.api_key_env = api_key_env
        self.model_name = model_name or model_id
        self.timeout_s = timeout_s
        self.path = path if path.startswith("/") else f"/{path}"
        self.capabilities = AdapterCapabilities(
            supports_native_batch=supports_native_batch,
            local=local,
            recommended_concurrency=8,
        )

    def health_sync(self) -> ModelHealth:
        if self.api_key_env and secret_from_env(self.api_key_env) is None:
            return ModelHealth(ok=False, permanent=True, detail=f"{self.api_key_env} is not set", version=self.model_name)
        return ModelHealth(ok=True, detail="configured", version=self.model_name)

    async def health(self) -> ModelHealth:
        return self.health_sync()

    def infer_blocking(self, request: SystemOneRequest) -> SystemOneResponse:
        started = time.perf_counter()
        try:
            response = httpx.post(
                f"{self.endpoint}{self.path}",
                json=self._body(request),
                headers=self._headers(),
                timeout=self.timeout_s,
            )
        except httpx.TimeoutException as exc:
            raise RetryableInferenceError("system-1 request timed out", code="timeout") from exc
        except httpx.TransportError as exc:
            raise RetryableInferenceError("system-1 connection failed", code="connection") from exc
        map_http_error(response)
        return self._parse(response, time.perf_counter() - started)

    async def infer(self, request: SystemOneRequest) -> SystemOneResponse:
        started = time.perf_counter()
        try:
            async with httpx.AsyncClient(timeout=self.timeout_s) as client:
                response = await client.post(
                    f"{self.endpoint}{self.path}",
                    json=self._body(request),
                    headers=self._headers(),
                )
        except httpx.TimeoutException as exc:
            raise RetryableInferenceError("system-1 request timed out", code="timeout") from exc
        except httpx.TransportError as exc:
            raise RetryableInferenceError("system-1 connection failed", code="connection") from exc
        map_http_error(response)
        return self._parse(response, time.perf_counter() - started)

    async def shutdown(self) -> None:
        return None

    def _body(self, request: SystemOneRequest) -> dict[str, Any]:
        return {
            "model": request.parameters.get("upstream_model") or self.model_name,
            "state": request.state,
            "questions": request.questions,
        }

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        key = secret_from_env(self.api_key_env)
        if key:
            headers["Authorization"] = f"Bearer {key}"
        return headers

    def _parse(self, response: httpx.Response, elapsed: float) -> SystemOneResponse:
        try:
            payload: dict[str, Any] = response.json()
        except ValueError as exc:
            raise RetryableInferenceError("system-1 endpoint returned non-JSON", code="server") from exc
        return response_from_payload(payload, elapsed=elapsed, fallback_version=self.model_name)
