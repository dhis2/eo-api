"""Tests for Prefer: respond-async on POST /ingestions and POST /sync/{dataset_id}."""

from typing import Any

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from open_climate_service.ingestions.schemas import IngestionResponse, SyncResponse
from tests.helpers import dataset_record


def test_post_ingestion_respond_async_returns_202_with_location(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "open_climate_service.ingestions.processes.registry_datasets.get_dataset",
        lambda dataset_id: {
            "id": dataset_id,
            "name": "CHIRPS3 precipitation",
            "variable": "precip",
            "period_type": "daily",
            "ingestion": {"plugin": "open_climate_service.streaming.plugins.chirps3.CHIRPS3DailyPlugin"},
        },
    )
    monkeypatch.setattr(
        "open_climate_service.ingestions.processes.get_extent_or_404",
        lambda: {"bbox": [1.0, 2.0, 3.0, 4.0], "country_code": "SLE"},
    )
    monkeypatch.setattr(
        "open_climate_service.ingestions.processes.services.create_artifact",
        lambda **kwargs: type("Artifact", (), {"artifact_id": "artifact-123"})(),
    )
    monkeypatch.setattr(
        "open_climate_service.ingestions.processes.services.get_ingestion_or_404",
        lambda artifact_id: IngestionResponse(
            ingestion_id=artifact_id,
            status="completed",
            dataset=dataset_record("chirps3_precipitation_daily"),
        ),
    )

    response = client.post(
        "/ingestions",
        headers={"Prefer": "respond-async"},
        json={
            "dataset_id": "chirps3_precipitation_daily",
            "start": "2026-01-01",
            "end": "2026-01-03",
            "publish": True,
        },
    )

    assert response.status_code == 202
    payload: dict[str, Any] = response.json()
    job_id = payload["ingestion_id"]
    assert response.headers["Location"] == f"/jobs/{job_id}"
    assert payload["dataset"] is None


def test_post_ingestion_without_prefer_header_returns_synchronous_response(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:

    monkeypatch.setattr(
        "open_climate_service.ingestions.routes._get_dataset_or_404",
        lambda dataset_id: {"id": dataset_id},
    )
    monkeypatch.setattr(
        "open_climate_service.ingestions.routes.get_extent_or_404",
        lambda: {"bbox": [1.0, 2.0, 3.0, 4.0]},
    )
    monkeypatch.setattr(
        "open_climate_service.ingestions.routes.services.create_artifact",
        lambda **kwargs: type("Artifact", (), {"artifact_id": "artifact-sync"})(),
    )
    monkeypatch.setattr(
        "open_climate_service.ingestions.routes.services.get_dataset_summary_for_artifact_or_404",
        lambda artifact_id: dataset_record("chirps3_precipitation_daily"),
    )

    response = client.post(
        "/ingestions",
        json={"dataset_id": "chirps3_precipitation_daily", "start": "2026-01-01"},
    )

    assert response.status_code == 200
    assert response.json()["ingestion_id"] == "artifact-sync"


def test_post_ingestion_respond_async_rejects_unknown_dataset_immediately(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "open_climate_service.ingestions.routes._get_dataset_or_404",
        lambda _dataset_id: (_ for _ in ()).throw(HTTPException(status_code=404, detail="Dataset not found")),
    )

    response = client.post(
        "/ingestions",
        headers={"Prefer": "respond-async"},
        json={"dataset_id": "unknown_dataset", "start": "2026-01-01"},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Dataset not found"


def test_post_sync_respond_async_returns_202_with_location(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "open_climate_service.ingestions.routes.services.plan_sync_dataset",
        lambda **_kwargs: {
            "source_dataset_id": "chirps3_precipitation_daily",
            "sync_kind": "temporal",
            "action": "append",
            "reason": "new_periods_available_for_append",
            "message": "Sync will append.",
            "current_start": "2026-01-01",
            "current_end": "2026-01-31",
            "target_end": "2026-02-10",
            "target_end_source": "request",
            "delta_start": "2026-02-01",
            "delta_end": "2026-02-10",
        },
    )
    monkeypatch.setattr(
        "open_climate_service.ingestions.processes.services.sync_dataset",
        lambda **kwargs: {
            "sync_id": "artifact-123",
            "status": "completed",
            "message": "Synced.",
            "dataset": dataset_record("chirps3_precipitation_daily"),
            "sync_detail": {
                "source_dataset_id": "chirps3_precipitation_daily",
                "sync_kind": "temporal",
                "action": "append",
                "reason": "new_periods_available_for_append",
                "message": "Sync will append.",
                "current_start": "2026-01-01",
                "current_end": "2026-01-31",
                "target_end": "2026-02-10",
                "target_end_source": "request",
                "delta_start": "2026-02-01",
                "delta_end": "2026-02-10",
            },
        },
    )

    response = client.post(
        "/sync/chirps3_precipitation_daily",
        headers={"Prefer": "respond-async"},
        json={"publish": True},
    )

    assert response.status_code == 202
    payload: dict[str, Any] = response.json()
    location = response.headers["Location"]
    assert location.startswith("/jobs/")
    assert payload["sync_id"] is None
    assert payload["dataset"] is None
    assert payload["sync_detail"] is None
    assert payload["message"] == "Sync queued"


def test_post_sync_respond_async_rejects_unplannable_request_immediately(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "open_climate_service.ingestions.routes.services.plan_sync_dataset",
        lambda **_kwargs: (_ for _ in ()).throw(HTTPException(status_code=400, detail="invalid end period")),
    )

    response = client.post(
        "/sync/chirps3_precipitation_daily",
        headers={"Prefer": "respond-async"},
        json={"end": "bad-period", "publish": True},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "invalid end period"


def test_post_sync_without_prefer_header_returns_synchronous_response(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "open_climate_service.ingestions.routes.services.sync_dataset",
        lambda **kwargs: SyncResponse(
            sync_id=None,
            status="up_to_date",
            message="Already current.",
            dataset=None,
            sync_detail=None,
        ),
    )

    response = client.post(
        "/sync/chirps3_precipitation_daily",
        json={"publish": True},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "up_to_date"
