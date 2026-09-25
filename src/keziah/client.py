"""Small HTTP client for a running Keziah service."""

from __future__ import annotations

import time
from typing import Any

import httpx

from keziah.errors import (
    BackpressureError,
    ConflictError,
    KeziahError,
    ModelUnavailableError,
    ModelUnknownError,
    NotFoundError,
    Timeout,
    ValidationError,
)
from keziah.results import BatchReceipt, Result


class Client:
    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8766",
        *,
        api_key: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        headers = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        self._http = httpx.Client(base_url=self.base_url, headers=headers, timeout=timeout)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def models(self) -> list[dict[str, Any]]:
        return self._json("GET", "/v1/models")["models"]

    def model(self, model_id: str) -> dict[str, Any]:
        return self._json("GET", f"/v1/models/{model_id}")

    def systemone(self, *, model: str, state: Any, questions: dict[str, Any], timeout_s: float | None = None, **kwargs: Any) -> Result:
        body = {"model": model, "state": state, "questions": questions, **kwargs}
        if timeout_s is not None:
            body["timeout_s"] = timeout_s
        return _result(self._json("POST", "/v1/systemone", body))

    def submit(self, *, model: str, state: Any, questions: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        body = {"model": model, "state": state, "questions": questions, **kwargs}
        return self._json("POST", "/v1/jobs", body)

    def submit_batch(self, jobs: list[dict[str, Any]], *, model: str | None = None, **kwargs: Any) -> BatchReceipt:
        payload = self._json("POST", "/v1/batches", {"model": model, "jobs": jobs, **kwargs})
        return BatchReceipt(
            batch_id=payload["batch_id"],
            job_count=payload["job_count"],
            status=payload["status"],
            created_at=payload["created_at"],
            completion=payload.get("completion", "open"),
            idempotent_replay=payload.get("idempotent_replay", False),
            counts=payload.get("counts"),
        )

    def get_job(self, job_id: str) -> Result:
        return _result(self._json("GET", f"/v1/jobs/{job_id}"))

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        return self._json("GET", f"/v1/batches/{batch_id}")

    def get_results(self, batch_id: str, *, offset: int = 0, limit: int = 100) -> list[Result]:
        payload = self._json("GET", f"/v1/batches/{batch_id}/results", params={"offset": offset, "limit": limit})
        return [_result(item) for item in payload["results"]]

    def wait(self, job_id: str, timeout: float | None = None, poll: float = 0.05) -> Result:
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            result = self.get_job(job_id)
            if result.status in {"succeeded", "failed", "dead_letter", "cancelled"}:
                return result
            if deadline is not None and time.monotonic() >= deadline:
                raise Timeout(f"timed out waiting for {job_id}")
            time.sleep(poll)

    def wait_batch(self, batch_id: str, timeout: float | None = None, poll: float = 0.05) -> list[Result]:
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            batch = self.get_batch(batch_id)
            if batch.get("completion") in {"complete", "aborted"}:
                rows: list[Result] = []
                offset = 0
                while True:
                    page = self.get_results(batch_id, offset=offset, limit=1000)
                    if not page:
                        return rows
                    rows.extend(page)
                    if len(page) < 1000:
                        return rows
                    offset += 1000
            if deadline is not None and time.monotonic() >= deadline:
                raise Timeout(f"timed out waiting for batch {batch_id}")
            time.sleep(poll)

    def cancel(self, job_id: str) -> Result:
        return _result(self._json("DELETE", f"/v1/jobs/{job_id}"))

    def cancel_batch(self, batch_id: str) -> dict[str, Any]:
        return self._json("DELETE", f"/v1/batches/{batch_id}")

    def _json(self, method: str, path: str, body: dict[str, Any] | None = None, params: dict[str, Any] | None = None) -> Any:
        response = self._http.request(method, path, json=body, params=params)
        if response.status_code >= 400:
            _raise(response)
        if not response.content:
            return {}
        return response.json()


def _result(payload: dict[str, Any]) -> Result:
    return Result(
        job_id=payload["job_id"],
        status=payload["status"],
        batch_id=payload.get("batch_id"),
        batch_ordinal=payload.get("batch_ordinal"),
        requested_model=payload.get("requested_model", ""),
        resolved_model=payload.get("resolved_model"),
        adapter=payload.get("adapter"),
        model_version=payload.get("model_version"),
        fallback_used=bool(payload.get("fallback_used")),
        fallback_reason=payload.get("fallback_reason"),
        attempts=int(payload.get("attempts") or 0),
        timings=payload.get("timings") or {},
        request=payload.get("request") or {},
        response=payload.get("response"),
        error=payload.get("error"),
        finished_at=payload.get("finished_at"),
        client_id=payload.get("client_id") or "",
        scheduling_class=payload.get("scheduling_class") or "normal",
        priority=int(payload.get("priority") or 0),
    )


def _raise(response: httpx.Response) -> None:
    try:
        payload = response.json()
    except ValueError:
        payload = {}
    detail = payload.get("detail") if isinstance(payload, dict) else None
    if not isinstance(detail, dict):
        detail = {"code": "error", "message": response.text, "details": {}}
    message = str(detail.get("message") or response.reason_phrase)
    code = str(detail.get("code") or "error")
    details = detail.get("details") if isinstance(detail.get("details"), dict) else {}
    status = response.status_code
    if status == 404:
        raise NotFoundError(message, code=code, details=details)
    if status == 409:
        raise ConflictError(message, code=code, details=details)
    if status == 429:
        raise BackpressureError(message, code=code, details=details)
    if status == 422:
        raise ValidationError(message, code=code, details=details)
    if status == 503 and code == "unknown_model":
        raise ModelUnknownError(message, code=code, details=details)
    if status == 503:
        raise ModelUnavailableError(message, code=code, details=details)
    if status == 504:
        raise Timeout(message, code=code, details=details)
    raise KeziahError(message, code=code, details=details)
