"""Command line for Keziah.

``keziah serve`` runs the service. Other commands talk to that service over
HTTP unless ``--embedded`` is set, in which case the command owns a private
in-process queue. Embedded mode is how a large JSONL file is staged without
building one giant list in the client.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterator

import typer

from keziah import __version__
from keziah.client import Client
from keziah.config import load_settings
from keziah.errors import KeziahError
from keziah.service import Keziah
from keziah.telemetry.logging import configure_logging

app = typer.Typer(no_args_is_help=True, help="Queue and run System-1 decisions.")
batch_app = typer.Typer(no_args_is_help=True, help="Submit and inspect batches.")
queue_app = typer.Typer(no_args_is_help=True, help="Inspect and maintain the queue.")
app.add_typer(batch_app, name="batch")
app.add_typer(queue_app, name="queue")


class Ctx:
    def __init__(self, url: str, api_key: str | None, as_json: bool, config: str | None, embedded: bool, mode: str | None) -> None:
        self.url = url
        self.api_key = api_key
        self.as_json = as_json
        self.config = config
        self.embedded = embedded
        self.mode = mode


@app.callback()
def _root(
    ctx: typer.Context,
    url: str = typer.Option("http://127.0.0.1:8766", "--url", help="Keziah base URL."),
    api_key: str | None = typer.Option(None, "--api-key", envvar="KEZIAH_API_KEY", help="Bearer token."),
    as_json: bool = typer.Option(False, "--json", help="Print machine-readable JSON."),
    config: str | None = typer.Option(None, "--config", help="Configuration file."),
    embedded: bool = typer.Option(False, "--embedded", help="Run an in-process queue instead of calling HTTP."),
    mode: str | None = typer.Option(None, "--mode", help="memory, disk, or hybrid. Embedded and serve."),
) -> None:
    ctx.obj = Ctx(url, api_key, as_json, config, embedded, mode)


def _ctx(ctx: typer.Context) -> Ctx:
    return ctx.obj


def _emit(ctx: typer.Context, payload: Any, human: str | None = None) -> None:
    state = _ctx(ctx)
    if state.as_json or human is None:
        typer.echo(json.dumps(payload, indent=2, default=str))
    else:
        typer.echo(human)


def _open_embedded(state: Ctx) -> Keziah:
    settings = load_settings(state.config)
    if state.mode:
        settings.queue.mode = state.mode  # type: ignore[assignment]
    return Keziah(settings=settings)


def _client(state: Ctx) -> Client:
    return Client(state.url, api_key=state.api_key)


@app.command()
def serve(
    ctx: typer.Context,
    host: str | None = typer.Option(None, "--host"),
    port: int | None = typer.Option(None, "--port"),
    mode: str | None = typer.Option(None, "--mode", help="memory, disk, or hybrid."),
) -> None:
    """Run the network listener."""
    import uvicorn

    state = _ctx(ctx)
    settings = load_settings(state.config)
    chosen_mode = mode or state.mode
    if chosen_mode:
        settings.queue.mode = chosen_mode  # type: ignore[assignment]
    if host:
        settings.server.host = host
    if port:
        settings.server.port = port
    if state.api_key:
        settings.server.api_key = state.api_key
    configure_logging(settings.server.log_level, json_logs=settings.server.log_json)
    from keziah.api.app import create_app

    uvicorn.run(create_app(settings=settings), host=settings.server.host, port=settings.server.port, log_level="warning")


@app.command("models")
def models_cmd(ctx: typer.Context) -> None:
    """List configured System-1 models and whether they are available."""
    state = _ctx(ctx)
    if state.embedded:
        with _open_embedded(state) as service:
            rows = service.models()
    else:
        with _client(state) as client:
            rows = client.models()
    if state.as_json:
        _emit(ctx, {"models": rows})
        return
    for row in rows:
        flag = "up" if row["available"] else "down"
        typer.echo(f"{row['id']:16} {row['adapter']:12} {flag:4} concurrency={row['max_concurrency']}")


@app.command()
def show(ctx: typer.Context, model_id: str) -> None:
    """Show one configured model."""
    state = _ctx(ctx)
    try:
        if state.embedded:
            with _open_embedded(state) as service:
                payload = service.show(model_id)
        else:
            with _client(state) as client:
                payload = client.model(model_id)
    except KeziahError as exc:
        _fail(exc)
        return
    if state.as_json:
        _emit(ctx, payload)
        return
    for key in (
        "id",
        "adapter",
        "version",
        "health",
        "health_detail",
        "capabilities",
        "local",
        "execution",
        "max_concurrency",
        "active",
        "available_slots",
        "endpoint",
        "supports_native_batch",
    ):
        if key in payload and payload[key] not in (None, ""):
            typer.echo(f"{key}: {payload[key]}")


@app.command()
def submit(
    ctx: typer.Context,
    path: Path,
    model: str | None = typer.Option(None, "--model"),
    wait: bool = typer.Option(False, "--wait"),
    timeout: float = typer.Option(60.0, "--timeout"),
) -> None:
    """Submit one JSON request file."""
    body = json.loads(path.read_text(encoding="utf-8"))
    chosen = model or body.get("model")
    if not chosen:
        raise typer.BadParameter("pass --model or set model in the file")
    state = _ctx(ctx)
    try:
        if state.embedded:
            with _open_embedded(state) as service:
                job_id = service.submit(
                    model=chosen,
                    state=body.get("state"),
                    questions=body.get("questions") or {},
                    client_id=body.get("client_id", ""),
                    priority=int(body.get("priority", 0)),
                    scheduling_class=body.get("scheduling_class"),
                    idempotency_key=body.get("idempotency_key"),
                    parameters=body.get("parameters"),
                )
                payload: Any = service.wait(job_id, timeout=timeout).to_dict() if wait else {"job_id": job_id}
        else:
            with _client(state) as client:
                if wait:
                    payload = client.systemone(
                        model=chosen,
                        state=body.get("state"),
                        questions=body.get("questions") or {},
                        timeout_s=timeout,
                    ).to_dict()
                else:
                    payload = client.submit(
                        model=chosen,
                        state=body.get("state"),
                        questions=body.get("questions") or {},
                    )
    except KeziahError as exc:
        _fail(exc)
        return
    _emit(ctx, payload, human=payload.get("job_id") or payload.get("status"))


@batch_app.command("submit")
def batch_submit(
    ctx: typer.Context,
    path: Path,
    model: str | None = typer.Option(None, "--model"),
    wait: bool = typer.Option(False, "--wait"),
    timeout: float = typer.Option(120.0, "--timeout"),
) -> None:
    """Submit a JSON array file or a JSONL file. JSONL is streamed."""
    state = _ctx(ctx)
    try:
        if state.embedded:
            with _open_embedded(state) as service:
                receipt = service.submit_batch(_load_jobs(path), model=model)
                if wait:
                    rows = service.wait_batch(receipt.batch_id, timeout=timeout)
                    payload = {"batch": receipt.to_dict(), "results": [row.to_dict() for row in rows]}
                else:
                    payload = receipt.to_dict()
        else:
            with _client(state) as client:
                if path.suffix == ".jsonl":
                    payload = _post_jsonl(client, path, model)
                else:
                    receipt_http = client.submit_batch(list(_load_jobs(path)), model=model)
                    payload = receipt_http.to_dict()
                if wait:
                    rows = client.wait_batch(payload["batch_id"], timeout=timeout)
                    payload = {"batch": payload, "results": [row.to_dict() for row in rows]}
    except KeziahError as exc:
        _fail(exc)
        return
    _emit(ctx, payload, human=f"batch {payload.get('batch_id', payload.get('batch', {}).get('batch_id'))}")


@app.command("job")
def job_cmd(ctx: typer.Context, job_id: str) -> None:
    """Show one job."""
    _show_job(ctx, job_id)


@app.command("wait")
def wait_cmd(ctx: typer.Context, job_id: str, timeout: float = typer.Option(60.0, "--timeout")) -> None:
    """Wait until a job reaches a terminal state."""
    state = _ctx(ctx)
    try:
        if state.embedded:
            raise typer.BadParameter("wait needs a running service; omit --embedded")
        with _client(state) as client:
            payload = client.wait(job_id, timeout=timeout).to_dict()
    except KeziahError as exc:
        _fail(exc)
        return
    _emit(ctx, payload, human=f"{payload['job_id']} {payload['status']}")


@app.command("cancel")
def cancel_cmd(ctx: typer.Context, job_id: str) -> None:
    """Cancel a job. Running inference may finish the current attempt."""
    state = _ctx(ctx)
    try:
        if state.embedded:
            with _open_embedded(state) as service:
                payload = service.cancel(job_id).to_dict()
        else:
            with _client(state) as client:
                payload = client.cancel(job_id).to_dict()
    except KeziahError as exc:
        _fail(exc)
        return
    _emit(ctx, payload, human=f"{payload['job_id']} {payload['status']}")


@batch_app.command("status")
def batch_status(ctx: typer.Context, batch_id: str) -> None:
    """Show batch counts."""
    state = _ctx(ctx)
    try:
        if state.embedded:
            with _open_embedded(state) as service:
                payload = service.get_batch(batch_id)
        else:
            with _client(state) as client:
                payload = client.get_batch(batch_id)
    except KeziahError as exc:
        _fail(exc)
        return
    _emit(
        ctx,
        payload,
        human=(
            f"{payload['batch_id']} {payload['completion']} "
            f"succeeded={payload.get('succeeded', 0)} failed={payload.get('failed', 0)} "
            f"queued={payload.get('queued', 0)} running={payload.get('running', 0)}"
        ),
    )


@batch_app.command("wait")
def batch_wait(ctx: typer.Context, batch_id: str, timeout: float = typer.Option(120.0, "--timeout")) -> None:
    state = _ctx(ctx)
    try:
        if state.embedded:
            raise typer.BadParameter("batch wait needs a running service; omit --embedded")
        with _client(state) as client:
            rows = client.wait_batch(batch_id, timeout=timeout)
    except KeziahError as exc:
        _fail(exc)
        return
    _emit(ctx, [row.to_dict() for row in rows], human=f"{len(rows)} results")


@batch_app.command("cancel")
def batch_cancel(ctx: typer.Context, batch_id: str) -> None:
    state = _ctx(ctx)
    try:
        if state.embedded:
            with _open_embedded(state) as service:
                affected = service.cancel_batch(batch_id)
                payload = {"batch_id": batch_id, "affected": affected}
        else:
            with _client(state) as client:
                payload = client.cancel_batch(batch_id)
    except KeziahError as exc:
        _fail(exc)
        return
    _emit(ctx, payload, human=f"affected {payload['affected']}")


@queue_app.command("stats")
def queue_stats(ctx: typer.Context) -> None:
    state = _ctx(ctx)
    if state.embedded:
        with _open_embedded(state) as service:
            payload = service.stats()
    else:
        with _client(state) as client:
            payload = client._json("GET", "/v1/stats")
    counts = payload.get("jobs_by_state", {})
    _emit(ctx, payload, human=" ".join(f"{key}={value}" for key, value in counts.items() if value))


@queue_app.command("dead")
def queue_dead(ctx: typer.Context, limit: int = typer.Option(50, "--limit")) -> None:
    state = _ctx(ctx)
    if state.embedded:
        with _open_embedded(state) as service:
            jobs = service.backend.get_jobs(state="dead_letter", limit=limit)
            payload = [service._to_result(job).to_dict() for job in jobs]
    else:
        with _client(state) as client:
            payload = client._json("GET", "/v1/queue", params={"state": "dead_letter", "limit": limit})["jobs"]
    _emit(ctx, payload, human=f"{len(payload)} dead-letter jobs")


@queue_app.command("cleanup")
def queue_cleanup(ctx: typer.Context) -> None:
    state = _ctx(ctx)
    if state.embedded:
        with _open_embedded(state) as service:
            payload = service.cleanup()
    else:
        with _client(state) as client:
            payload = client._json("POST", "/v1/queue/cleanup")
    _emit(ctx, payload, human=f"removed jobs={payload.get('jobs', 0)} events={payload.get('events', 0)}")


@queue_app.command("requeue")
def queue_requeue(ctx: typer.Context, job_id: str) -> None:
    state = _ctx(ctx)
    try:
        if state.embedded:
            with _open_embedded(state) as service:
                payload = service.requeue(job_id).to_dict()
        else:
            with _client(state) as client:
                payload = client._json("POST", f"/v1/jobs/{job_id}/requeue")
    except KeziahError as exc:
        _fail(exc)
        return
    _emit(ctx, payload, human=f"{payload['job_id']} {payload['status']}")


@app.command()
def health(ctx: typer.Context) -> None:
    state = _ctx(ctx)
    if state.embedded:
        with _open_embedded(state) as service:
            payload = {"live": service.liveness(), "ready": service.readiness(), "models": service.models()}
    else:
        with _client(state) as client:
            live = client._json("GET", "/health/live")
            ready = client._http.get("/health/ready")
            payload = {"live": live, "ready_status": ready.status_code, "ready": _safe_json(ready)}
    _emit(ctx, payload, human=json.dumps(payload))


@app.command()
def benchmark(
    ctx: typer.Context,
    jobs: int = typer.Option(100, "--jobs"),
    model: str = typer.Option("mock", "--model"),
    output: Path | None = typer.Option(None, "--output"),
    mode: str | None = typer.Option(None, "--mode", help="memory, disk, or hybrid."),
) -> None:
    """Run the in-process benchmark and print JSON. Does not call a remote model."""
    from keziah.bench import run_benchmark

    state = _ctx(ctx)
    settings = load_settings(state.config, use_defaults=True)
    chosen_mode = mode or state.mode
    if chosen_mode:
        settings.queue.mode = chosen_mode  # type: ignore[assignment]
    # Benchmark defaults to the mock model even when the config also lists Laya and Jev.
    report = run_benchmark(jobs=jobs, model=model, mode=settings.queue.mode, settings=settings)
    text = json.dumps(report, indent=2)
    if output is not None:
        output.write_text(text + "\n", encoding="utf-8")
    typer.echo(text)


@app.command()
def version() -> None:
    typer.echo(__version__)


def main() -> None:
    app()


def _show_job(ctx: typer.Context, job_id: str) -> None:
    state = _ctx(ctx)
    try:
        if state.embedded:
            with _open_embedded(state) as service:
                payload = service.get(job_id).to_dict()
        else:
            with _client(state) as client:
                payload = client.get_job(job_id).to_dict()
    except KeziahError as exc:
        _fail(exc)
        return
    _emit(ctx, payload, human=f"{payload['job_id']} {payload['status']}")


def _load_jobs(path: Path) -> Iterator[dict[str, Any]]:
    if path.suffix == ".jsonl":
        with path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                text = line.strip()
                if not text:
                    continue
                try:
                    item = json.loads(text)
                except json.JSONDecodeError as exc:
                    raise typer.BadParameter(f"{path}:{line_number}: {exc}") from exc
                if not isinstance(item, dict):
                    raise typer.BadParameter(f"{path}:{line_number}: expected an object")
                yield item
        return
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "jobs" in payload:
        rows = payload["jobs"]
    else:
        rows = payload
    if not isinstance(rows, list):
        raise typer.BadParameter("JSON batch file must be a list or an object with jobs")
    for item in rows:
        yield item


def _post_jsonl(client: Client, path: Path, model: str | None) -> dict[str, Any]:
    with path.open("rb") as handle:
        response = client._http.post(
            "/v1/batches",
            content=handle,
            params={"model": model} if model else None,
            headers={"Content-Type": "application/x-ndjson"},
        )
    if response.status_code >= 400:
        from keziah.client import _raise

        _raise(response)
    return response.json()


def _safe_json(response: Any) -> Any:
    try:
        return response.json()
    except Exception:
        return {"status_code": response.status_code}


def _fail(exc: KeziahError) -> None:
    typer.echo(json.dumps({"code": exc.code, "message": str(exc), "details": exc.details}), err=True)
    raise typer.Exit(code=1)
