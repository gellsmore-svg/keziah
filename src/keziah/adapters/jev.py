"""Jev adapter.

Jev is TypeSafe's hosted System-1 model. The wire protocol is
``POST {endpoint}/v1/systemone`` with a bearer token. The default endpoint is
``https://api.typesafe.ai`` and the default model name is ``jev-latest``.

The API key is read from the configured environment variable, then
``TYPESAFE_API_KEY``, then ``JEV_API_KEY``. It is never logged.
"""

from __future__ import annotations

import time
from typing import Any

import httpx

from keziah.adapters.http_common import map_http_error, response_from_payload, secret_from_env
from keziah.errors import PermanentInferenceError, RetryableInferenceError
from keziah.types import AdapterCapabilities, ModelHealth, SystemOneRequest, SystemOneResponse


class JevAdapter:
    def __init__(
        self,
        model_id: str = "jev",
        *,
        endpoint: str = "https://api.typesafe.ai",
        api_key_env: str | None = "TYPESAFE_API_KEY",
        model_name: str = "jev-latest",
        timeout_s: float = 30.0,
        health_ttl_s: float = 30.0,
    ) -> None:
        self.model_id = model_id
        self.endpoint = endpoint.rstrip("/")
        self.api_key_env = api_key_env
        self.model_name = model_name
        self.timeout_s = timeout_s
        self.health_ttl_s = health_ttl_s
        self.probe_count = 0
        self._health_cache: tuple[float, ModelHealth] | None = None
        self.capabilities = AdapterCapabilities(
            supports_native_batch=False,
            local=False,
            recommended_concurrency=16,
        )

    def health_sync(self) -> ModelHealth:
        """Live check against ``GET /v1/models``, reused for ``health_ttl_s``.

        Listing models is not an inference call. Job execution reads this cache,
        so a batch does not probe Jev once per decision.
        """
        now = time.monotonic()
        cached = self._health_cache
        if cached is not None and (now - cached[0]) < self.health_ttl_s:
            return cached[1]
        health = self._probe()
        self.probe_count += 1
        self._health_cache = (now, health)
        return health

    async def health(self) -> ModelHealth:
        return self.health_sync()

    def infer_blocking(self, request: SystemOneRequest) -> SystemOneResponse:
        key = self._require_key()
        body = self._body(request)
        started = time.perf_counter()
        try:
            response = httpx.post(
                f"{self.endpoint}/v1/systemone",
                json=body,
                headers=self._headers(key),
                timeout=self.timeout_s,
            )
        except httpx.TimeoutException as exc:
            raise RetryableInferenceError("jev request timed out", code="timeout") from exc
        except httpx.TransportError as exc:
            raise RetryableInferenceError("jev connection failed", code="connection") from exc
        map_http_error(response)
        try:
            payload: dict[str, Any] = response.json()
        except ValueError as exc:
            raise RetryableInferenceError("jev returned a non-JSON body", code="server") from exc
        return response_from_payload(payload, elapsed=time.perf_counter() - started, fallback_version=self.model_name)

    async def infer(self, request: SystemOneRequest) -> SystemOneResponse:
        key = self._require_key()
        body = self._body(request)
        started = time.perf_counter()
        try:
            async with httpx.AsyncClient(timeout=self.timeout_s) as client:
                response = await client.post(
                    f"{self.endpoint}/v1/systemone",
                    json=body,
                    headers=self._headers(key),
                )
        except httpx.TimeoutException as exc:
            raise RetryableInferenceError("jev request timed out", code="timeout") from exc
        except httpx.TransportError as exc:
            raise RetryableInferenceError("jev connection failed", code="connection") from exc
        map_http_error(response)
        try:
            payload: dict[str, Any] = response.json()
        except ValueError as exc:
            raise RetryableInferenceError("jev returned a non-JSON body", code="server") from exc
        return response_from_payload(payload, elapsed=time.perf_counter() - started, fallback_version=self.model_name)

    async def shutdown(self) -> None:
        return None

    def _body(self, request: SystemOneRequest) -> dict[str, Any]:
        model_name = request.parameters.get("upstream_model") or self.model_name
        return {"model": model_name, "state": request.state, "questions": request.questions}

    def _headers(self, key: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}

    def _probe(self) -> ModelHealth:
        key = self._key()
        env_name = self.api_key_env or "TYPESAFE_API_KEY"
        if key is None:
            return ModelHealth(
                ok=False,
                permanent=True,
                detail=f"{env_name} is not set",
                version=self.model_name,
            )
        try:
            response = httpx.get(
                f"{self.endpoint}/v1/models",
                headers={"Authorization": f"Bearer {key}"},
                timeout=min(self.timeout_s, 5.0),
            )
        except httpx.TimeoutException:
            return ModelHealth(
                ok=False,
                permanent=False,
                detail="jev health check timed out",
                version=self.model_name,
            )
        except httpx.TransportError:
            return ModelHealth(
                ok=False,
                permanent=False,
                detail="jev health check failed to connect",
                version=self.model_name,
            )
        status = response.status_code
        if status == 200:
            return ModelHealth(ok=True, detail="reachable", version=self._version_from(response))
        if status in {401, 403}:
            return ModelHealth(ok=False, permanent=True, detail="authentication failed", version=self.model_name)
        if status == 429 or status >= 500:
            return ModelHealth(
                ok=False,
                permanent=False,
                detail=f"jev health check returned HTTP {status}",
                version=self.model_name,
            )
        return ModelHealth(
            ok=False,
            permanent=True,
            detail=f"jev health check returned HTTP {status}",
            version=self.model_name,
        )

    def _version_from(self, response: httpx.Response) -> str:
        try:
            payload = response.json()
        except ValueError:
            return self.model_name
        models = payload.get("models") if isinstance(payload, dict) else None
        if not isinstance(models, list):
            return self.model_name
        names = [item.get("name") for item in models if isinstance(item, dict) and isinstance(item.get("name"), str)]
        if self.model_name in names:
            return self.model_name
        if names:
            return str(names[0])
        return self.model_name

    def _key(self) -> str | None:
        return secret_from_env(self.api_key_env, "TYPESAFE_API_KEY", "JEV_API_KEY")

    def _require_key(self) -> str:
        key = self._key()
        if not key:
            raise PermanentInferenceError("Jev API key is not configured", code="authentication")
        return key
