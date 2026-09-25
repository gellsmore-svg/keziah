"""Deterministic offline System-1 adapter.

The same state, questions, and model id always produce the same probabilities.
No network, weights, or credentials.
"""

from __future__ import annotations

import hashlib
import threading
import time
from typing import Any

from keziah.errors import PermanentInferenceError, RetryableInferenceError
from keziah.jsonutil import canonical_json
from keziah.types import AdapterCapabilities, ModelHealth, SystemOneRequest, SystemOneResponse


class MockAdapter:
    def __init__(
        self,
        model_id: str = "mock",
        *,
        latency_s: float = 0.0,
        version: str = "mock-1",
        supports_native_batch: bool = True,
        script: list[BaseException | None] | None = None,
        gate: threading.Event | None = None,
    ) -> None:
        self.model_id = model_id
        self.latency_s = latency_s
        self.version = version
        self.script = list(script or [])
        self.gate = gate
        self.calls = 0
        self.entered = 0
        self.capabilities = AdapterCapabilities(
            supports_native_batch=supports_native_batch,
            local=True,
            recommended_concurrency=8,
        )

    def health_sync(self) -> ModelHealth:
        return ModelHealth(ok=True, version=self.version, detail="mock")

    async def health(self) -> ModelHealth:
        return self.health_sync()

    def infer_blocking(self, request: SystemOneRequest) -> SystemOneResponse:
        self._wait_gate()
        self._before()
        started = time.perf_counter()
        if self.latency_s:
            time.sleep(self.latency_s)
        response = self._answers(request)
        response.inference_seconds = time.perf_counter() - started
        return response

    async def infer(self, request: SystemOneRequest) -> SystemOneResponse:
        import asyncio

        if self.gate is not None:
            await asyncio.to_thread(self.gate.wait)
        self._before()
        started = time.perf_counter()
        if self.latency_s:
            await asyncio.sleep(self.latency_s)
        response = self._answers(request)
        response.inference_seconds = time.perf_counter() - started
        return response

    async def infer_many(self, requests: list[SystemOneRequest]) -> list[SystemOneResponse]:
        return [await self.infer(request) for request in requests]

    def infer_many_blocking(self, requests: list[SystemOneRequest]) -> list[SystemOneResponse]:
        return [self.infer_blocking(request) for request in requests]

    async def shutdown(self) -> None:
        return None

    def _wait_gate(self) -> None:
        self.entered += 1
        if self.gate is not None:
            self.gate.wait()

    def _before(self) -> None:
        self.calls += 1
        if not self.script:
            return
        item = self.script.pop(0)
        if item is not None:
            raise item

    def _answers(self, request: SystemOneRequest) -> SystemOneResponse:
        answers: dict[str, Any] = {}
        for key, question in request.questions.items():
            kind = question["type"]
            digest = hashlib.sha256(
                canonical_json(
                    {"model": self.model_id, "state": request.state, "question": key, "spec": question}
                ).encode("utf-8")
            ).digest()
            if kind == "choice":
                criteria = question.get("criteria")
                if isinstance(criteria, dict) and criteria:
                    options = list(criteria.keys())
                else:
                    options = [str(option) for option in question.get("options") or ["yes", "no"]]
                weights = [digest[index % len(digest)] + 1 for index in range(len(options))]
                total = float(sum(weights))
                probabilities = {option: weights[index] / total for index, option in enumerate(options)}
                choice = max(options, key=lambda option: (probabilities[option], option))
                top = probabilities[choice]
                confidence = max(0.0, (top - (1.0 / len(options))) / (1.0 - (1.0 / len(options)))) if len(options) > 1 else 1.0
                answers[key] = {
                    "type": "choice",
                    "choice": choice,
                    "probabilities": {name: round(value, 6) for name, value in probabilities.items()},
                    "confidence": round(confidence, 6),
                }
            elif kind == "noul":
                noul = digest[0] / 255
                answers[key] = {"type": "noul", "noul": round(noul, 6)}
            else:
                criteria = question.get("criteria") or ["low", "medium", "high"]
                levels = len(criteria)
                weights = [digest[index % len(digest)] + 1 for index in range(levels)]
                total = float(sum(weights))
                probabilities = {str(index): weights[index] / total for index in range(levels)}
                score = sum(index * probabilities[str(index)] for index in range(levels))
                legend = {str(index): criteria[index] for index in range(levels)}
                top = max(probabilities.values())
                confidence = max(0.0, (top - (1.0 / levels)) / (1.0 - (1.0 / levels))) if levels > 1 else 1.0
                answers[key] = {
                    "type": "score",
                    "score": round(score, 6),
                    "legend": legend,
                    "probabilities": {name: round(value, 6) for name, value in probabilities.items()},
                    "confidence": round(confidence, 6),
                }
        return SystemOneResponse(
            answers=answers,
            model_version=self.version,
            usage={"input_tokens": 0, "output_tokens": 0},
            raw={"model": self.model_id, "answers": answers},
        )


def scripted_failure(message: str, *, retryable: bool, code: str) -> BaseException:
    if retryable:
        return RetryableInferenceError(message, code=code)
    return PermanentInferenceError(message, code=code)
