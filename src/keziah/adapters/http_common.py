"""Shared HTTP error mapping for Jev and generic System-1 endpoints.

Credentials are never included in exceptions or logs.
"""

from __future__ import annotations

import os
from typing import Any

import httpx

from keziah.errors import PermanentInferenceError, RetryableInferenceError
from keziah.types import SystemOneResponse


def secret_from_env(*names: str | None) -> str | None:
    for name in names:
        if not name:
            continue
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return None


def map_http_error(response: httpx.Response) -> None:
    status = response.status_code
    if status < 400:
        return
    retry_after = _retry_after(response)
    if status in {401, 403}:
        raise PermanentInferenceError("authentication failed", code="authentication")
    if status == 404:
        raise PermanentInferenceError("system-1 endpoint not found", code="not_found")
    if status == 422 or status == 400:
        raise PermanentInferenceError("the model rejected the request", code="invalid_request")
    if status == 429 or status == 529:
        raise RetryableInferenceError(
            "the model is rate limiting",
            code="rate_limit",
            retry_after_s=retry_after,
        )
    if 500 <= status <= 599:
        raise RetryableInferenceError(f"the model returned HTTP {status}", code="server", retry_after_s=retry_after)
    raise PermanentInferenceError(f"the model returned HTTP {status}", code="http_status")


def response_from_payload(payload: dict[str, Any], *, elapsed: float, fallback_version: str | None) -> SystemOneResponse:
    answers = payload.get("answers")
    if not isinstance(answers, dict):
        raise PermanentInferenceError("system-1 response did not contain answers", code="invalid_response")
    version = payload.get("model")
    if not isinstance(version, str):
        version = fallback_version
    usage = payload.get("usage") if isinstance(payload.get("usage"), dict) else None
    # Drop nothing that is part of the decision, but do not keep transport headers.
    raw = {key: payload[key] for key in ("model", "answers", "usage") if key in payload}
    return SystemOneResponse(
        answers=answers,
        model_version=version,
        usage=usage,
        inference_seconds=elapsed,
        raw=raw,
    )


def _retry_after(response: httpx.Response) -> float | None:
    raw = response.headers.get("retry-after")
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        return None
