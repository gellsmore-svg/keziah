"""Adapter error mapping. No network and no model weights."""

import httpx
import pytest

from keziah.adapters.http import GenericSystemOneHTTPAdapter
from keziah.adapters.http_common import map_http_error
from keziah.adapters.jev import JevAdapter
from keziah.adapters.laya import LayaAdapter
from keziah.errors import PermanentInferenceError, RetryableInferenceError
from keziah.types import SystemOneRequest


def test_http_status_mapping() -> None:
    map_http_error(httpx.Response(200))
    with pytest.raises(PermanentInferenceError) as auth:
        map_http_error(httpx.Response(401))
    assert auth.value.code == "authentication"
    with pytest.raises(PermanentInferenceError):
        map_http_error(httpx.Response(422))
    with pytest.raises(RetryableInferenceError) as limited:
        map_http_error(httpx.Response(429, headers={"retry-after": "2"}))
    assert limited.value.retry_after_s == 2
    with pytest.raises(RetryableInferenceError):
        map_http_error(httpx.Response(503))
    with pytest.raises(RetryableInferenceError):
        map_http_error(httpx.Response(529))


def test_jev_uses_token_without_leaking_it(monkeypatch) -> None:
    monkeypatch.setenv("TYPESAFE_API_KEY", "super-secret-token")
    seen = {}

    def fake_post(url, json, headers, timeout):
        seen["auth"] = headers["Authorization"]
        seen["body"] = json
        return httpx.Response(
            200,
            json={
                "model": "jev-1.13.0",
                "answers": {"needs_human": {"type": "noul", "noul": 0.25}},
                "usage": {"input_tokens": 4, "output_tokens": 1},
            },
        )

    monkeypatch.setattr(httpx, "post", fake_post)
    adapter = JevAdapter()
    response = adapter.infer_blocking(
        SystemOneRequest(state="hello", questions={"needs_human": {"type": "noul", "instructions": "Human?"}}, model="jev")
    )
    assert response.answers["needs_human"]["noul"] == 0.25
    assert seen["auth"] == "Bearer super-secret-token"
    assert "super-secret-token" not in str(seen["body"])
    assert "super-secret-token" not in str(response)


def test_jev_health_is_cached_and_not_an_inference(monkeypatch) -> None:
    monkeypatch.setenv("TYPESAFE_API_KEY", "super-secret-token")
    calls: list[str] = []

    def fake_get(url, headers, timeout):
        calls.append(url)
        assert headers["Authorization"] == "Bearer super-secret-token"
        assert "super-secret-token" not in url
        return httpx.Response(
            200,
            json={"models": [{"name": "jev-latest", "description": "flagship", "release_date": "2026-09-15"}]},
        )

    monkeypatch.setattr(httpx, "get", fake_get)
    adapter = JevAdapter(health_ttl_s=60)
    first = adapter.health_sync()
    second = adapter.health_sync()
    assert first.ok is True
    assert first.detail == "reachable"
    assert first.version == "jev-latest"
    assert second == first
    assert adapter.probe_count == 1
    assert calls == ["https://api.typesafe.ai/v1/models"]


def test_jev_health_auth_failure_is_cached(monkeypatch) -> None:
    monkeypatch.setenv("TYPESAFE_API_KEY", "super-secret-token")

    def fake_get(url, headers, timeout):
        return httpx.Response(401, json={"error": "nope"})

    monkeypatch.setattr(httpx, "get", fake_get)
    adapter = JevAdapter(health_ttl_s=60)
    health = adapter.health_sync()
    adapter.health_sync()
    assert health.ok is False
    assert health.permanent is True
    assert health.detail == "authentication failed"
    assert "super-secret-token" not in health.detail
    assert adapter.probe_count == 1


def test_jev_auth_failure(monkeypatch) -> None:
    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    monkeypatch.delenv("JEV_API_KEY", raising=False)
    adapter = JevAdapter()
    health = adapter.health_sync()
    assert health.ok is False
    assert health.permanent is True
    with pytest.raises(PermanentInferenceError):
        adapter.infer_blocking(SystemOneRequest(state="x", questions={"a": {"type": "noul", "instructions": "?"}}, model="jev"))


def test_generic_http_and_laya_missing(monkeypatch) -> None:
    def fake_post(url, json, headers, timeout):
        return httpx.Response(200, json={"model": "custom", "answers": {"a": {"type": "noul", "noul": 0.5}}})

    monkeypatch.setattr(httpx, "post", fake_post)
    adapter = GenericSystemOneHTTPAdapter("custom", endpoint="http://example.test")
    response = adapter.infer_blocking(
        SystemOneRequest(state="x", questions={"a": {"type": "noul", "instructions": "?"}}, model="custom")
    )
    assert response.model_version == "custom"
    laya = LayaAdapter()
    health = laya.health_sync()
    # The package may be absent in CI. Either state is honest.
    if not health.ok:
        assert health.permanent is True
        with pytest.raises(PermanentInferenceError):
            laya.infer_blocking(SystemOneRequest(state="x", questions={"a": {"type": "noul", "instructions": "?"}}, model="laya"))


def test_mock_is_deterministic() -> None:
    from keziah.adapters.mock import MockAdapter

    adapter = MockAdapter("mock")
    request = SystemOneRequest(
        state={"message": "same"},
        questions={"team": {"type": "choice", "instructions": "Team?", "options": ["a", "b"]}},
        model="mock",
    )
    first = adapter.infer_blocking(request)
    second = adapter.infer_blocking(request)
    assert first.answers == second.answers
    other = MockAdapter("other")
    assert other.infer_blocking(request).answers != first.answers
