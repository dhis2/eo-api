from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def test_get_processes_omits_internal_only_processes(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "open_climate_service.processing.routes.process_registry.list_processes",
        lambda: [
            {
                "id": "public_process",
                "title": "Public process",
                "description": "Visible",
                "execution": {"function": "mypackage.public.execute"},
                "expose": True,
                "jobControlOptions": ["sync-execute"],
            },
            {
                "id": "internal_process",
                "title": "Internal process",
                "description": "Hidden",
                "execution": {"function": "mypackage.internal.execute"},
                "expose": False,
                "jobControlOptions": ["sync-execute"],
            },
        ],
    )

    response = client.get("/processes")

    assert response.status_code == 200
    payload = response.json()
    ids = {item["id"] for item in payload["processes"]}
    # expose:False process must not be listed; expose:True process must be listed.
    # The list also includes all standard openEO processes, so we check membership.
    assert "internal_process" not in ids
    assert "public_process" in ids


def test_get_unknown_process_detail_returns_404(client: TestClient) -> None:
    response = client.get("/processes/unknown_process")

    assert response.status_code == 404


def test_get_internal_process_detail_returns_404(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "open_climate_service.processing.routes.process_registry.get_process",
        lambda process_id: {
            "id": process_id,
            "title": "Internal process",
            "execution": {"function": "mypackage.internal.execute"},
            "expose": False,
            "jobControlOptions": ["sync-execute"],
        },
    )

    response = client.get("/processes/internal_process")

    assert response.status_code == 404


def test_expose_false_yaml_fixture_is_hidden_from_catalog_and_routes(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    processes_subdir = tmp_path / "processes"
    processes_subdir.mkdir()
    (processes_subdir / "internal.yaml").write_text(
        """
- id: internal_process
  title: Internal process
  expose: false
  execution:
    function: mypackage.internal.execute
""",
        encoding="utf-8",
    )
    monkeypatch.setattr("open_climate_service.data_registry.services.processes.CONFIGS_DIR", processes_subdir)

    catalog_response = client.get("/processes")
    assert catalog_response.status_code == 200
    # The catalog includes standard openEO processes, so it won't be empty;
    # verify the internal_process is not exposed.
    ids = {p["id"] for p in catalog_response.json()["processes"]}
    assert "internal_process" not in ids

    detail_response = client.get("/processes/internal_process")
    assert detail_response.status_code == 404

    execution_response = client.post("/processes/internal_process/execution", json={})
    assert execution_response.status_code == 404


def test_post_process_execution_honors_case_insensitive_prefer_tokens(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "open_climate_service.processing.routes.process_registry.get_process",
        lambda process_id: {
            "id": process_id,
            "title": "Async process",
            "execution": {"function": "mypackage.async_process.execute"},
            "expose": True,
            "jobControlOptions": ["sync-execute", "async-execute"],
        },
    )
    monkeypatch.setattr(
        "open_climate_service.processing.routes.process_registry._get_dynamic_function",
        lambda path: lambda **kwargs: {"status": "completed"},
    )

    response = client.post(
        "/processes/async-process/execution",
        headers={"Prefer": "Respond-Async, wait=10"},
        json={},
    )

    assert response.status_code == 202
    payload = response.json()
    assert payload["status"] in {"accepted", "running", "successful"}
    assert response.headers["Location"].startswith("/internal/jobs/")


def test_post_process_execution_rejects_async_when_process_is_sync_only(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "open_climate_service.processing.routes.process_registry.get_process",
        lambda process_id: {
            "id": process_id,
            "title": "Sync only process",
            "execution": {"function": "mypackage.sync_only.execute"},
            "expose": True,
            "jobControlOptions": ["sync-execute"],
        },
    )

    response = client.post(
        "/processes/sync-only/execution",
        headers={"Prefer": "respond-async"},
        json={},
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "Process 'sync-only' does not support async execution"


def test_post_internal_process_execution_returns_404(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "open_climate_service.processing.routes.process_registry.get_process",
        lambda process_id: {
            "id": process_id,
            "title": "Internal process",
            "execution": {"function": "mypackage.internal.execute"},
            "expose": False,
            "jobControlOptions": ["sync-execute"],
        },
    )

    response = client.post("/processes/internal_process/execution", json={})

    assert response.status_code == 404


def test_post_unknown_process_id_returns_404(client: TestClient) -> None:
    response = client.post(
        "/processes/unknown_process/execution",
        json={"dataset_id": "chirps3_precipitation_daily", "start": "2026-01-05"},
    )
    assert response.status_code == 404


def test_post_process_execution_returns_500_for_invalid_execution_function(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "open_climate_service.processing.routes.process_registry.get_process",
        lambda process_id: {
            "id": process_id,
            "title": "Broken process",
            "execution": {"function": "mypackage.broken.execute"},
            "expose": True,
            "jobControlOptions": ["sync-execute"],
        },
    )

    def _raise_import_error(full_path: str) -> object:
        raise ModuleNotFoundError(f"No module named for {full_path}")

    monkeypatch.setattr(
        "open_climate_service.processing.routes.process_registry._get_dynamic_function", _raise_import_error
    )

    response = client.post("/processes/broken_process/execution", json={})

    assert response.status_code == 500
    assert "Failed to load execution.function" in response.json()["detail"]


def test_post_process_execution_returns_400_for_execution_value_error(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "open_climate_service.processing.routes.process_registry.get_process",
        lambda process_id: {
            "id": process_id,
            "title": "Broken process",
            "execution": {"function": "mypackage.broken.execute"},
            "expose": True,
            "jobControlOptions": ["sync-execute"],
        },
    )

    def _raise_value_error(full_path: str) -> object:
        def _func(**_: object) -> object:
            raise ValueError("Bad execution input")

        return _func

    monkeypatch.setattr(
        "open_climate_service.processing.routes.process_registry._get_dynamic_function", _raise_value_error
    )

    response = client.post("/processes/broken_process/execution", json={})

    assert response.status_code == 400
    assert response.json()["detail"] == "Bad execution input"
