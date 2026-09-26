"""HTTP API for Keziah.

OpenAPI here is the standard description of this service. It has nothing to do
with OpenAI. Synchronous ``/v1/systemone`` still takes a model slot; it does
not bypass the scheduler.
"""

from __future__ import annotations

import hmac
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from keziah import __version__
from keziah.config import Settings, loopback_host
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
from keziah.service import Keziah
from keziah.telemetry.logging import configure_logging, get_logger

log = get_logger("api")


class QuestionModel(BaseModel):
    type: str = Field(description="choice, score, or noul")
    instructions: Any = Field(description="What the model should decide")
    criteria: Any = None
    options: list[Any] | None = Field(default=None, description="Choice shorthand. Normalised to criteria.")


class SystemOneBody(BaseModel):
    model: str
    state: Any
    questions: dict[str, QuestionModel]
    parameters: dict[str, Any] | None = None
    client_id: str = ""
    priority: int = 0
    scheduling_class: str | None = None
    idempotency_key: str | None = None
    execution_timeout_ms: int | None = None
    deadline_at: str | None = None
    max_attempts: int | None = None
    timeout_s: float | None = Field(default=None, description="How long this HTTP call waits. Not the execution timeout.")


class JobBody(SystemOneBody):
    pass


class BatchJobBody(BaseModel):
    model: str | None = None
    state: Any
    questions: dict[str, QuestionModel]
    parameters: dict[str, Any] | None = None
    client_id: str | None = None
    priority: int | None = None
    scheduling_class: str | None = None
    idempotency_key: str | None = None
    execution_timeout_ms: int | None = None
    deadline_at: str | None = None
    max_attempts: int | None = None


class BatchBody(BaseModel):
    model: str | None = None
    jobs: list[BatchJobBody]
    client_id: str = ""
    priority: int = 0
    scheduling_class: str | None = None
    idempotency_key: str | None = None
    deadline_at: str | None = None
    max_attempts: int | None = None


class ErrorBody(BaseModel):
    code: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)


def create_app(service: Keziah | None = None, settings: Settings | None = None) -> FastAPI:
    holder: dict[str, Keziah] = {}

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if service is None:
            configure_logging((settings or Settings()).server.log_level)
            created = Keziah(settings=settings) if settings is not None else Keziah()
            holder["service"] = created
            owns = True
        else:
            holder["service"] = service
            service.start()
            owns = False
        active = holder["service"]
        _warn_exposure(active.settings)
        try:
            yield
        finally:
            if owns:
                active.shutdown()

    app = FastAPI(
        title="Keziah",
        version=__version__,
        summary="Queue and execution service for System-1 models.",
        description=(
            "Keziah accepts state-plus-questions decisions for System-1 models such as "
            "Laya and Jev, persists them when asked, and runs them under a shared "
            "concurrency limit. This OpenAPI document describes Keziah. It is not an "
            "OpenAI-compatible API."
        ),
        lifespan=lifespan,
    )

    def current() -> Keziah:
        try:
            return holder["service"]
        except KeyError as exc:
            raise HTTPException(status_code=503, detail={"code": "not_ready", "message": "starting"}) from exc

    def authorized(
        svc: Keziah = Depends(current),
        authorization: str | None = Header(default=None),
    ) -> Keziah:
        expected = svc.settings.resolved_api_key()
        if not expected:
            return svc
        supplied = authorization or ""
        ok = hmac.compare_digest(
            supplied.encode("utf-8", "surrogateescape"),
            f"Bearer {expected}".encode("utf-8", "surrogateescape"),
        )
        if not ok:
            raise HTTPException(status_code=401, detail={"code": "unauthorized", "message": "invalid bearer token"})
        return svc

    @app.get("/health/live", tags=["health"])
    def live() -> dict[str, str]:
        return {"status": "live"}

    @app.get("/health/ready", tags=["health"])
    def ready(svc: Keziah = Depends(current)) -> dict[str, Any]:
        body = svc.readiness()
        if body["status"] != "ready":
            raise HTTPException(status_code=503, detail=body)
        return body

    @app.get("/metrics", tags=["health"], response_class=PlainTextResponse)
    def metrics(svc: Keziah = Depends(authorized)) -> str:
        return svc.render_metrics()

    @app.get("/v1/models", tags=["models"])
    def list_models(svc: Keziah = Depends(authorized)) -> dict[str, Any]:
        return {"models": svc.models()}

    @app.get("/v1/models/{model_id}", tags=["models"])
    def model_detail(model_id: str, svc: Keziah = Depends(authorized)) -> dict[str, Any]:
        return _call(svc.show, model_id)

    @app.post("/v1/systemone", tags=["inference"])
    def systemone(body: SystemOneBody, request: Request, svc: Keziah = Depends(authorized)) -> dict[str, Any]:
        job_id = _call(
            svc.submit,
            model=body.model,
            state=body.state,
            questions=_questions(body.questions),
            client_id=body.client_id,
            priority=body.priority,
            scheduling_class=body.scheduling_class,
            idempotency_key=body.idempotency_key,
            parameters=body.parameters,
            execution_timeout_ms=body.execution_timeout_ms,
            deadline_at=body.deadline_at,
            max_attempts=body.max_attempts,
        )
        if svc.settings.server.cancel_on_disconnect and _disconnected(request):
            _call(svc.cancel, job_id)
            raise HTTPException(status_code=499, detail={"code": "cancelled", "message": "client disconnected"})
        try:
            result = svc.wait(job_id, timeout=body.timeout_s)
        except Timeout as exc:
            raise HTTPException(status_code=504, detail={"code": exc.code, "message": str(exc), "job_id": job_id}) from exc
        return result.to_dict()

    @app.post("/v1/jobs", tags=["jobs"], status_code=202)
    def create_job(body: JobBody, svc: Keziah = Depends(authorized)) -> dict[str, Any]:
        job_id = _call(
            svc.submit,
            model=body.model,
            state=body.state,
            questions=_questions(body.questions),
            client_id=body.client_id,
            priority=body.priority,
            scheduling_class=body.scheduling_class,
            idempotency_key=body.idempotency_key,
            parameters=body.parameters,
            execution_timeout_ms=body.execution_timeout_ms,
            deadline_at=body.deadline_at,
            max_attempts=body.max_attempts,
        )
        record = _call(svc.get_job_record, job_id)
        return {
            "job_id": job_id,
            "status": record["state"],
            "created_at": record["created_at"],
            "idempotent_replay": record["idempotent_replay"],
        }

    @app.get("/v1/jobs/{job_id}", tags=["jobs"])
    def read_job(job_id: str, svc: Keziah = Depends(authorized)) -> dict[str, Any]:
        return _call(svc.get, job_id).to_dict()

    @app.get("/v1/jobs/{job_id}/events", tags=["jobs"])
    def job_events(job_id: str, svc: Keziah = Depends(authorized)) -> dict[str, Any]:
        _call(svc.get, job_id)
        return {"events": svc.backend.get_events(job_id)}

    @app.delete("/v1/jobs/{job_id}", tags=["jobs"])
    def delete_job(job_id: str, svc: Keziah = Depends(authorized)) -> dict[str, Any]:
        return _call(svc.cancel, job_id).to_dict()

    @app.post("/v1/jobs/{job_id}/requeue", tags=["jobs"])
    def requeue_job(job_id: str, svc: Keziah = Depends(authorized)) -> dict[str, Any]:
        return _call(svc.requeue, job_id).to_dict()

    @app.post(
        "/v1/batches",
        tags=["batches"],
        status_code=202,
        openapi_extra={
            "requestBody": {
                "required": True,
                "content": {
                    "application/json": {
                        "schema": {"$ref": "#/components/schemas/BatchBody"},
                    },
                    "application/x-ndjson": {
                        "schema": {
                            "type": "string",
                            "description": "JSON Lines. Each line is one job. Optional header line keys are passed as query parameters model, client_id, and idempotency_key.",
                        }
                    },
                },
            }
        },
    )
    async def create_batch(request: Request, svc: Keziah = Depends(authorized)) -> dict[str, Any]:
        content_type = request.headers.get("content-type", "")
        if "ndjson" in content_type or "jsonl" in content_type:
            raw = await request.body()
            model = request.query_params.get("model")
            receipt = _call(
                svc.submit_batch,
                _iter_ndjson(raw),
                model=model,
                client_id=request.query_params.get("client_id") or "",
                idempotency_key=request.query_params.get("idempotency_key"),
                scheduling_class=request.query_params.get("scheduling_class"),
            )
        else:
            import json

            from pydantic import ValidationError as ModelValidationError

            try:
                payload = await request.json()
                body = BatchBody.model_validate(payload)
            except json.JSONDecodeError as exc:
                raise HTTPException(
                    status_code=422,
                    detail={"code": "invalid_request", "message": "invalid JSON", "details": {}},
                ) from exc
            except ModelValidationError as exc:
                raise HTTPException(
                    status_code=422,
                    detail={"code": "invalid_request", "message": "invalid request", "details": {}},
                ) from exc
            jobs = [_batch_job(item) for item in body.jobs]
            receipt = _call(
                svc.submit_batch,
                jobs,
                model=body.model,
                client_id=body.client_id,
                priority=body.priority,
                scheduling_class=body.scheduling_class,
                idempotency_key=body.idempotency_key,
                deadline_at=body.deadline_at,
                max_attempts=body.max_attempts,
            )
        response = receipt.to_dict()
        if receipt.job_count <= 1000 and not receipt.idempotent_replay:
            response["job_ids"] = [
                job.job_id for job in svc.backend.get_batch_results(receipt.batch_id, offset=0, limit=1000)
            ]
        else:
            response["job_ids"] = None
        response["results_url"] = f"/v1/batches/{receipt.batch_id}/results"
        return response

    @app.get("/v1/batches/{batch_id}", tags=["batches"])
    def read_batch(batch_id: str, svc: Keziah = Depends(authorized)) -> dict[str, Any]:
        return _call(svc.get_batch, batch_id)

    @app.get("/v1/batches/{batch_id}/results", tags=["batches"])
    def batch_results(
        batch_id: str,
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=100, ge=1, le=1000),
        svc: Keziah = Depends(authorized),
    ) -> dict[str, Any]:
        rows = _call(svc.get_results, batch_id, offset=offset, limit=limit)
        return {"batch_id": batch_id, "offset": offset, "results": [row.to_dict() for row in rows]}

    @app.delete("/v1/batches/{batch_id}", tags=["batches"])
    def delete_batch(batch_id: str, svc: Keziah = Depends(authorized)) -> dict[str, Any]:
        changed = _call(svc.cancel_batch, batch_id)
        return {"batch_id": batch_id, "affected": changed}

    @app.get("/v1/queue", tags=["queue"])
    def queue(state: str | None = None, limit: int = Query(default=50, ge=1, le=500), svc: Keziah = Depends(authorized)) -> dict[str, Any]:
        jobs = svc.backend.get_jobs(state=state, limit=limit)
        return {"stats": svc.stats(), "jobs": [svc._to_result(job).to_dict() for job in jobs]}

    @app.get("/v1/stats", tags=["queue"])
    def stats(svc: Keziah = Depends(authorized)) -> dict[str, Any]:
        return svc.stats()

    @app.post("/v1/queue/cleanup", tags=["queue"])
    def cleanup(svc: Keziah = Depends(authorized)) -> dict[str, int]:
        return svc.cleanup()

    return app


def _questions(questions: dict[str, QuestionModel]) -> dict[str, Any]:
    return {key: value.model_dump(exclude_none=True) for key, value in questions.items()}


def _iter_ndjson(raw: bytes):
    import json

    start = 0
    length = len(raw)
    while start < length:
        end = raw.find(b"\n", start)
        if end == -1:
            end = length
        line = raw[start:end].strip()
        start = end + 1
        if not line:
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValidationError(f"invalid JSONL line: {exc}") from exc
        if not isinstance(item, dict):
            raise ValidationError("each JSONL line must be an object")
        yield item


def _batch_job(item: BatchJobBody) -> dict[str, Any]:
    payload = item.model_dump(exclude_none=True)
    payload["questions"] = _questions(item.questions)
    return payload


def _call(fn: Any, *args: Any, **kwargs: Any) -> Any:
    try:
        return fn(*args, **kwargs)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=_detail(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=_detail(exc)) from exc
    except BackpressureError as exc:
        raise HTTPException(status_code=429, detail=_detail(exc)) from exc
    except (ValidationError, ModelUnknownError) as exc:
        raise HTTPException(status_code=422, detail=_detail(exc)) from exc
    except ModelUnavailableError as exc:
        raise HTTPException(status_code=503, detail=_detail(exc)) from exc
    except KeziahError as exc:
        raise HTTPException(status_code=400, detail=_detail(exc)) from exc


def _detail(exc: KeziahError) -> dict[str, Any]:
    return {"code": exc.code, "message": str(exc), "details": exc.details}


def _disconnected(request: Request) -> bool:
    # Starlette only knows after a receive. Best-effort: the call is still short.
    return bool(request.headers.get("x-keziah-disconnected") == "1")


def _warn_exposure(settings: Settings) -> None:
    if not loopback_host(settings.server.host) and not settings.resolved_api_key():
        log.warning(
            "Keziah is bound to %s without authentication. Set KEZIAH_API_KEY before exposing it.",
            settings.server.host,
        )
