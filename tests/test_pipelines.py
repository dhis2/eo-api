from fastapi.testclient import TestClient

from eo_api.prefect_flows.schemas import (
    PipelineInput,
    PipelineResult,
)


def test_unknown_process_returns_404(client: TestClient) -> None:
    response = client.post("/pipelines/nonexistent", json={"inputs": {}})
    assert response.status_code == 404


def test_missing_body_returns_422(client: TestClient) -> None:
    response = client.post("/pipelines/era5-land-download")
    assert response.status_code == 422


def test_pipeline_input_model() -> None:
    inp = PipelineInput(process_id="era5-land-download", inputs={"start": "2024-01"})
    assert inp.process_id == "era5-land-download"
    assert inp.inputs == {"start": "2024-01"}


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
