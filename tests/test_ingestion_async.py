"""Tests for async ingestion and sync via Prefer: respond-async."""

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from open_climate_service.jobs.models import JobRecord, JobStatus


def _make_job(job_id: str = "job-123") -> JobRecord:
    from open_climate_service.shared.time import utc_now

    return JobRecord(
        job_id=job_id,
        process_id="ingestion",
        status=JobStatus.ACCEPTED,
        created_at=utc_now(),
        max_attempts=1,
        executor_kind="thread",
        request={},
        links=[],
    )


def test_post_ingestion_respond_async_returns_202_with_location(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _make_job("abc-123")
    mock_service = MagicMock()
    mock_service.submit_callable_job.return_value = job
    monkeypatch.setattr("open_climate_service.jobs.service.JobService.submit_callable_job", lambda self, **kw: job)
    monkeypatch.setattr(
        "open_climate_service.ingestions.routes._get_dataset_or_404",
        lambda _: {"id": "chirps3_precipitation_daily"},
    )
    monkeypatch.setattr("open_climate_service.ingestions.routes.get_extent_or_404", lambda: {"bbox": [0, 0, 1, 1]})

    response = client.post(
        "/ingestions",
        headers={"Prefer": "respond-async"},
        json={"dataset_id": "chirps3_precipitation_daily", "start": "2024-01-01"},
    )

    assert response.status_code == 202
    assert response.headers["Location"] == "/ingestions/jobs/abc-123"
    assert response.json()["ingestion_id"] == "abc-123"


def test_post_sync_respond_async_returns_202_with_location(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _make_job("sync-456")
    monkeypatch.setattr("open_climate_service.jobs.service.JobService.submit_callable_job", lambda self, **kw: job)
    monkeypatch.setattr(
        "open_climate_service.ingestions.routes.services.plan_sync_dataset",
        lambda dataset_id, end: MagicMock(),
    )

    response = client.post(
        "/sync/chirps3_precipitation_daily",
        headers={"Prefer": "respond-async"},
        json={},
    )

    assert response.status_code == 202
    assert response.headers["Location"] == "/ingestions/jobs/sync-456"


def test_get_ingestion_job_returns_job_with_correct_self_link(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = _make_job("job-789")
    monkeypatch.setattr("open_climate_service.jobs.store.get_job_record", lambda job_id: job)

    response = client.get("/ingestions/jobs/job-789")

    assert response.status_code == 200
    payload = response.json()
    assert payload["jobID"] == "job-789"
    links = {lnk["rel"]: lnk["href"] for lnk in payload["links"]}
    assert links["self"] == "/ingestions/jobs/job-789"


def test_get_ingestion_job_returns_404_for_unknown(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("open_climate_service.jobs.store.get_job_record", lambda job_id: None)

    response = client.get("/ingestions/jobs/does-not-exist")

    assert response.status_code == 404


def test_post_ingestion_without_prefer_does_not_return_202(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without Prefer: respond-async the response is never 202 Accepted."""
    monkeypatch.setattr(
        "open_climate_service.ingestions.routes._get_dataset_or_404",
        lambda _: {"id": "chirps3_precipitation_daily"},
    )
    from fastapi import HTTPException

    monkeypatch.setattr(
        "open_climate_service.ingestions.routes.get_extent_or_404",
        lambda: (_ for _ in ()).throw(HTTPException(status_code=422, detail="no extent")),
    )

    response = client.post(
        "/ingestions",
        json={"dataset_id": "chirps3_precipitation_daily", "start": "2024-01-01"},
    )

    assert response.status_code == 422
    assert "Location" not in response.headers
