"""Laya adapter.

Laya is a local System-1 library. ``Router.predict(state, questions)`` answers
every question in one forward pass and already returns choice, score, and noul
probabilities. The router is created once and reused. Weights are not loaded
during health checks; ``preload`` / ``warm`` does that explicitly.

Inference is serialised with a lock. Laya's own server uses one worker because
a single CPU or GPU forward pass is the useful unit of concurrency.
"""

from __future__ import annotations

import threading
import time
from typing import Any

from keziah.errors import PermanentInferenceError, RetryableInferenceError
from keziah.types import AdapterCapabilities, ModelHealth, SystemOneRequest, SystemOneResponse

_PREDICT_KEYS = ("max_len", "head_max_len", "lang", "task")


class LayaAdapter:
    def __init__(
        self,
        model_id: str = "laya",
        *,
        device: str | None = None,
        threads: int | None = None,
        preload: bool = False,
        checkpoint: str | None = None,
        supports_native_batch: bool = True,
    ) -> None:
        self.model_id = model_id
        self.device = device
        self.threads = threads
        self.preload = preload
        self.checkpoint = checkpoint
        self._router: Any = None
        self._version: str | None = None
        self._lock = threading.Lock()
        self.capabilities = AdapterCapabilities(
            supports_native_batch=supports_native_batch,
            local=True,
            recommended_concurrency=1,
        )

    def health_sync(self) -> ModelHealth:
        try:
            import laya  # pyright: ignore[reportMissingImports]
        except ImportError:
            return ModelHealth(ok=False, permanent=True, detail="laya is not installed")
        version = getattr(laya, "__version__", None)
        self._version = version
        loaded = self._router is not None
        detail = "loaded" if loaded else "package importable; weights load on first use"
        return ModelHealth(ok=True, version=version, detail=detail)

    async def health(self) -> ModelHealth:
        return self.health_sync()

    def warm(self) -> None:
        self._router_blocking(preload=True)

    def infer_blocking(self, request: SystemOneRequest) -> SystemOneResponse:
        router = self._router_blocking(preload=self.preload)
        kwargs = _predict_kwargs(request)
        if self.checkpoint and "model" not in kwargs:
            kwargs["model"] = self.checkpoint
        started = time.perf_counter()
        try:
            with self._lock:
                raw = router.predict(request.state, request.questions, **kwargs)
        except ValueError as exc:
            raise PermanentInferenceError(str(exc), code="invalid_request") from exc
        except Exception as exc:
            raise RetryableInferenceError(f"laya inference failed: {type(exc).__name__}", code="transient") from exc
        return _response(raw, self._version, time.perf_counter() - started)

    async def infer(self, request: SystemOneRequest) -> SystemOneResponse:
        import asyncio

        return await asyncio.to_thread(self.infer_blocking, request)

    def infer_many_blocking(self, requests: list[SystemOneRequest]) -> list[SystemOneResponse]:
        if not requests:
            return []
        router = self._router_blocking(preload=self.preload)
        payload = [{"state": item.state, "questions": item.questions} for item in requests]
        if self.checkpoint:
            for item in payload:
                item["model"] = self.checkpoint
        started = time.perf_counter()
        try:
            with self._lock:
                raws = router.predict_batch(payload)
        except ValueError as exc:
            raise PermanentInferenceError(str(exc), code="invalid_request") from exc
        except Exception as exc:
            raise RetryableInferenceError(f"laya batch inference failed: {type(exc).__name__}", code="transient") from exc
        elapsed = time.perf_counter() - started
        share = elapsed / max(1, len(raws))
        return [_response(raw, self._version, share) for raw in raws]

    async def infer_many(self, requests: list[SystemOneRequest]) -> list[SystemOneResponse]:
        import asyncio

        return await asyncio.to_thread(self.infer_many_blocking, requests)

    async def shutdown(self) -> None:
        with self._lock:
            self._router = None

    def _router_blocking(self, *, preload: bool) -> Any:
        with self._lock:
            if self._router is not None:
                return self._router
            try:
                import laya  # pyright: ignore[reportMissingImports]
                from laya import Router  # pyright: ignore[reportMissingImports]
            except ImportError as exc:
                raise PermanentInferenceError("laya is not installed", code="model_unavailable") from exc
            self._version = getattr(laya, "__version__", None)
            if self.threads:
                try:
                    import torch  # pyright: ignore[reportMissingImports]

                    torch.set_num_threads(int(self.threads))
                except Exception:
                    pass
            try:
                router = Router(device=self.device, preload=preload)
            except Exception as exc:
                raise RetryableInferenceError(
                    f"laya failed to initialise: {type(exc).__name__}",
                    code="model_unavailable",
                ) from exc
            self._router = router
            return router


def _predict_kwargs(request: SystemOneRequest) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    for key in _PREDICT_KEYS:
        if key in request.parameters and request.parameters[key] is not None:
            kwargs[key] = request.parameters[key]
    model = request.parameters.get("checkpoint")
    if model:
        kwargs["model"] = model
    return kwargs


def _response(raw: dict[str, Any], package_version: str | None, elapsed: float) -> SystemOneResponse:
    routing = raw.get("routing") if isinstance(raw, dict) else None
    checkpoint = routing.get("model") if isinstance(routing, dict) else None
    version = None
    if package_version or checkpoint:
        version = "laya"
        if package_version:
            version += f"-{package_version}"
        if checkpoint:
            version += f"+{checkpoint}"
    answers = raw.get("answers") if isinstance(raw, dict) else None
    if not isinstance(answers, dict):
        raise PermanentInferenceError("laya returned no answers", code="invalid_response")
    return SystemOneResponse(
        answers=answers,
        model_version=version,
        usage=raw.get("usage") if isinstance(raw.get("usage"), dict) else None,
        inference_seconds=elapsed,
        raw={"model": raw.get("model"), "routing": routing, "answers": answers, "usage": raw.get("usage")},
    )
