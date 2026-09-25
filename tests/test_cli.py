"""CLI paths that do not need a running server."""

import json
from pathlib import Path

from typer.testing import CliRunner

from keziah.cli.main import app

runner = CliRunner()


def test_embedded_models_and_submit(tmp_path: Path) -> None:
    request = tmp_path / "request.json"
    request.write_text(
        json.dumps(
            {
                "state": {"message": "hello"},
                "questions": {"needs_human": {"type": "noul", "instructions": "Human?"}},
            }
        ),
        encoding="utf-8",
    )
    models = runner.invoke(app, ["--embedded", "--mode", "memory", "models"])
    assert models.exit_code == 0, models.stdout
    assert "mock" in models.stdout
    submitted = runner.invoke(
        app,
        ["--embedded", "--mode", "memory", "--json", "submit", str(request), "--model", "mock", "--wait"],
    )
    assert submitted.exit_code == 0, submitted.stdout
    payload = json.loads(submitted.stdout)
    assert payload["status"] == "succeeded"
    assert payload["response"]["answers"]["needs_human"]["type"] == "noul"


def test_embedded_jsonl_batch(tmp_path: Path) -> None:
    path = tmp_path / "requests.jsonl"
    question = {"needs_human": {"type": "noul", "instructions": "Human?"}}
    lines = [json.dumps({"state": {"n": index}, "questions": question}) for index in range(5)]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    result = runner.invoke(
        app,
        ["--embedded", "--mode", "memory", "--json", "batch", "submit", str(path), "--model", "mock", "--wait"],
    )
    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)
    assert len(payload["results"]) == 5
    assert [row["batch_ordinal"] for row in payload["results"]] == [0, 1, 2, 3, 4]


def test_version() -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert result.stdout.strip() == "0.1.0"
