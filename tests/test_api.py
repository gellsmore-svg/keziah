"""HTTP surface, including OpenAPI and auth."""

from fastapi.testclient import TestClient

from keziah.api.app import create_app
from keziah.config import ServerSettings
from keziah.service import Keziah
from tests.conftest import make_settings


def test_http_flow(tmp_path, questions) -> None:
    settings = make_settings(tmp_path, "hybrid")
    service = Keziah(settings=settings)
    app = create_app(service)
    try:
        with TestClient(app) as client:
            assert client.get("/health/live").json()["status"] == "live"
            assert client.get("/health/ready").status_code == 200
            models = client.get("/v1/models").json()["models"]
            assert models[0]["id"] == "mock"
            detail = client.get("/v1/models/mock").json()
            assert detail["adapter"] == "mock"
            assert "api_key" not in detail
            sync = client.post(
                "/v1/systemone",
                json={"model": "mock", "state": {"message": "hi"}, "questions": questions},
            )
            assert sync.status_code == 200
            assert sync.json()["status"] == "succeeded"
            created = client.post(
                "/v1/jobs",
                json={"model": "mock", "state": "async", "questions": questions, "scheduling_class": "interactive"},
            )
            assert created.status_code == 202
            job_id = created.json()["job_id"]
            fetched = client.get(f"/v1/jobs/{job_id}")
            assert fetched.status_code == 200
            batch = client.post(
                "/v1/batches",
                json={
                    "model": "mock",
                    "jobs": [
                        {"state": "a", "questions": questions},
                        {"model": "mock", "state": "b", "questions": questions},
                    ],
                },
            )
            assert batch.status_code == 202
            body = batch.json()
            assert body["job_count"] == 2
            assert body["job_ids"] is not None
            deadline = 0
            while deadline < 50 and client.get(f"/v1/batches/{body['batch_id']}").json()["completion"] != "complete":
                deadline += 1
            results = client.get(f"/v1/batches/{body['batch_id']}/results").json()["results"]
            assert [row["batch_ordinal"] for row in results] == [0, 1]
            assert "keziah_jobs_submitted_total" in client.get("/metrics").text
            schema = client.get("/openapi.json").json()
            assert "/v1/systemone" in schema["paths"]
            assert "System-1" in schema["info"]["description"]
            missing = client.get("/v1/jobs/job_missing")
            assert missing.status_code == 404
    finally:
        service.shutdown()


def test_auth_required_when_configured(tmp_path, questions) -> None:
    settings = make_settings(tmp_path, "memory")
    settings.server = ServerSettings(api_key="test-token")
    service = Keziah(settings=settings)
    app = create_app(service)
    try:
        with TestClient(app) as client:
            assert client.get("/v1/models").status_code == 401
            ok = client.get("/v1/models", headers={"Authorization": "Bearer test-token"})
            assert ok.status_code == 200
            assert client.get("/health/live").status_code == 200
    finally:
        service.shutdown()


def test_ndjson_batch(tmp_path, questions) -> None:
    settings = make_settings(tmp_path, "memory")
    service = Keziah(settings=settings)
    app = create_app(service)
    try:
        import json

        lines = "\n".join(json.dumps({"state": {"n": index}, "questions": questions}) for index in range(3))
        with TestClient(app) as client:
            response = client.post(
                "/v1/batches?model=mock",
                content=lines,
                headers={"Content-Type": "application/x-ndjson"},
            )
            assert response.status_code == 202
            assert response.json()["job_count"] == 3
    finally:
        service.shutdown()
