from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from open_climate_service.ingestions.schemas import DatasetRecord
from open_climate_service.processing import services as processing_services
from tests.helpers import dataset_record


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


def test_get_processes_returns_openeo_catalog_while_execution_stays_native(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        processing_services,
        "run_resample_process",
        lambda **kwargs: ("artifact-123", dataset_record("chirps3_precipitation_daily_w_mon_sum")),
    )

    catalog_response = client.get("/processes")
    execution_response = client.post(
        "/processes/resample/execution",
        json={
            "source_dataset_id": "chirps3_precipitation_daily",
            "frequency": "W-MON",
            "method": "sum",
            "start": "2026-01-05",
        },
    )

    assert catalog_response.status_code == 200
    assert "processes" in catalog_response.json()
    assert execution_response.status_code == 200
    assert execution_response.json()["status"] == "completed"


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


def test_post_resample_execution_returns_completed_response(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        processing_services,
        "run_resample_process",
        lambda **kwargs: ("artifact-123", dataset_record("chirps3_precipitation_daily_w_mon_sum")),
    )

    response = client.post(
        "/processes/resample/execution",
        json={
            "source_dataset_id": "chirps3_precipitation_daily",
            "frequency": "W-MON",
            "method": "sum",
            "start": "2026-01-05",
            "end": "2026-01-12",
            "publish": True,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["artifact_id"] == "artifact-123"
    assert payload["status"] == "completed"
    assert payload["dataset"]["dataset_id"] == "chirps3_precipitation_daily_w_mon_sum"


def test_post_process_execution_honors_case_insensitive_prefer_tokens(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        processing_services,
        "run_resample_process",
        lambda **kwargs: ("artifact-123", dataset_record("chirps3_precipitation_daily_w_mon_sum")),
    )

    response = client.post(
        "/processes/resample/execution",
        headers={"Prefer": "Respond-Async, wait=10"},
        json={
            "source_dataset_id": "chirps3_precipitation_daily",
            "frequency": "W-MON",
            "method": "sum",
            "start": "2026-01-05",
            "end": "2026-01-12",
            "publish": True,
        },
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


def test_post_resample_execution_passes_params_to_service(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_run_resample_process(**kwargs: object) -> tuple[str, DatasetRecord]:
        captured.update(kwargs)
        return "artifact-456", dataset_record("chirps3_precipitation_daily_w_mon_sum")

    monkeypatch.setattr(processing_services, "run_resample_process", fake_run_resample_process)

    response = client.post(
        "/processes/resample/execution",
        json={
            "source_dataset_id": "chirps3_precipitation_daily",
            "frequency": "W-MON",
            "method": "sum",
            "start": "2026-01-05",
            "end": "2026-01-12",
            "overwrite": True,
            "publish": False,
        },
    )

    assert response.status_code == 200
    assert captured["source_dataset_id"] == "chirps3_precipitation_daily"
    assert captured["frequency"] == "W-MON"
    assert captured["method"] == "sum"
    assert captured["start"] == "2026-01-05"
    assert captured["end"] == "2026-01-12"
    assert captured["overwrite"] is True
    assert captured["publish"] is False


def test_post_resample_execution_returns_400_for_invalid_frequency(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = client.post(
        "/processes/resample/execution",
        json={
            "source_dataset_id": "chirps3_precipitation_daily",
            "frequency": "invalid",
            "method": "sum",
            "start": "2026-01-01",
        },
    )
    assert response.status_code == 400


def test_post_resample_execution_returns_400_for_null_frequency(client: TestClient) -> None:
    response = client.post(
        "/processes/resample/execution",
        json={
            "source_dataset_id": "chirps3_precipitation_daily",
            "frequency": None,
            "method": "sum",
            "start": "2026-01-01",
        },
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "frequency is required"


def test_post_resample_execution_returns_400_for_unsupported_method(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = client.post(
        "/processes/resample/execution",
        json={
            "source_dataset_id": "chirps3_precipitation_daily",
            "frequency": "W-MON",
            "method": "median",
            "start": "2026-01-05",
        },
    )
    assert response.status_code == 400


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
