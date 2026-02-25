from fastapi.testclient import TestClient

from eo_api.pipelines.schemas import (
    CHIRPS3PipelineInput,
    ERA5LandPipelineInput,
    PipelineResult,
)


def test_era5_land_missing_required_fields(client: TestClient) -> None:
    response = client.post("/pipelines/era5-land", json={})
    assert response.status_code == 422


def test_chirps3_missing_required_fields(client: TestClient) -> None:
    response = client.post("/pipelines/chirps3", json={})
    assert response.status_code == 422


def test_era5_land_invalid_bbox(client: TestClient) -> None:
    response = client.post(
        "/pipelines/era5-land",
        json={"start": "2024-01", "end": "2024-02", "bbox": [1.0, 2.0]},
    )
    assert response.status_code == 422


def test_chirps3_invalid_stage(client: TestClient) -> None:
    response = client.post(
        "/pipelines/chirps3",
        json={"start": "2024-01", "end": "2024-02", "bbox": [1, 2, 3, 4], "stage": "invalid"},
    )
    assert response.status_code == 422


def test_era5_land_input_defaults() -> None:
    inp = ERA5LandPipelineInput(start="2024-01", end="2024-02", bbox=[1.0, 2.0, 3.0, 4.0])
    assert inp.variables == ["2m_temperature", "total_precipitation"]


def test_chirps3_input_defaults() -> None:
    inp = CHIRPS3PipelineInput(start="2024-01", end="2024-02", bbox=[1.0, 2.0, 3.0, 4.0])
    assert inp.stage == "final"


def test_pipeline_result_defaults() -> None:
    result = PipelineResult(status="completed")
    assert result.files == []
    assert result.features is None
    assert result.message == ""


def test_pipeline_result_serialization() -> None:
    result = PipelineResult(
        status="completed",
        files=["/tmp/data/test.nc"],
        message="done",
    )
    data = result.model_dump()
    assert data["status"] == "completed"
    assert data["files"] == ["/tmp/data/test.nc"]
    assert data["message"] == "done"
