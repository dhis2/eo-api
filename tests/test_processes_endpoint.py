from fastapi import FastAPI
from fastapi.testclient import TestClient

from eoapi.endpoints.processes import router as processes_router
from eoapi.processing.providers.base import RasterFetchResult


def create_client() -> TestClient:
    app = FastAPI()
    app.include_router(processes_router)
    return TestClient(app)


def test_processes_list_includes_architecture_processes() -> None:
    client = create_client()

    response = client.get("/processes")

    assert response.status_code == 200
    payload = response.json()
    process_ids = {process["id"] for process in payload["processes"]}
    assert process_ids == {"raster.zonal_stats", "raster.point_timeseries", "data.temporal_aggregate"}


def test_process_unknown_id() -> None:
    client = create_client()

    response = client.get("/processes/unknown")

    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "NotFound"


def test_process_definition_includes_implementation_output_schema() -> None:
    client = create_client()

    response = client.get("/processes/raster.zonal_stats")

    assert response.status_code == 200
    outputs = response.json()["outputs"]
    assert "implementation" in outputs
    assert outputs["implementation"]["type"] == "object"
    links = response.json()["links"]
    assert any(link["rel"] == "collection" and link["href"].endswith("/collections") for link in links)


def test_process_execution_and_job_status(monkeypatch) -> None:
    client = create_client()

    class FakeProvider:
        provider_id = "fake"

        def fetch(self, request):
            return RasterFetchResult(
                provider=self.provider_id,
                asset_paths=["/tmp/chirps_cached.nc"],
                from_cache=True,
            )

        def implementation_details(self):
            return {"adapter": "test.fake"}

    monkeypatch.setattr("eoapi.processing.service.build_provider", lambda dataset: FakeProvider())

    execute_response = client.post(
        "/processes/raster.zonal_stats/execution",
        json={
            "inputs": {
                "dataset_id": "chirps-daily",
                "params": ["precip"],
                "time": "2026-01-31",
                "aoi": [30.0, -10.0, 31.0, -9.0],
            }
        },
    )

    assert execute_response.status_code == 202
    execute_payload = execute_response.json()
    assert execute_payload["processId"] == "raster.zonal_stats"

    job_id = execute_payload["jobId"]
    job_response = client.get(f"/jobs/{job_id}")
    assert job_response.status_code == 200

    job_payload = job_response.json()
    assert job_payload["status"] == "succeeded"
    assert job_payload["outputs"]["rows"]
    assert job_payload["outputs"]["csv"]
    assert job_payload["outputs"]["dhis2"]["status"] == "stub"
    assert job_payload["outputs"]["implementation"]["provider"]["id"] == "fake"


def test_temporal_aggregate_process_execution(monkeypatch) -> None:
    client = create_client()

    class FakeProvider:
        provider_id = "fake"

        def fetch(self, request):
            return RasterFetchResult(
                provider=self.provider_id,
                asset_paths=["/tmp/chirps_cached.nc"],
                from_cache=True,
            )

    monkeypatch.setattr("eoapi.processing.service.build_provider", lambda dataset: FakeProvider())

    execute_response = client.post(
        "/processes/data.temporal_aggregate/execution",
        json={
            "inputs": {
                "dataset_id": "chirps-daily",
                "params": ["precip"],
                "time": "2026-01-31",
                "frequency": "P1M",
                "aggregation": "sum",
            }
        },
    )

    assert execute_response.status_code == 202
    job_id = execute_response.json()["jobId"]
    job_response = client.get(f"/jobs/{job_id}")
    assert job_response.status_code == 200
    outputs = job_response.json()["outputs"]
    assert outputs["options"]["frequency"] == "P1M"
    assert outputs["options"]["aggregation"] == "sum"
    assert outputs["rows"][0]["operation"] == "temporal_aggregate"
