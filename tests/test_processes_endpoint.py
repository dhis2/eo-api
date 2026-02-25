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
    assert process_ids == {
        "raster.zonal_stats",
        "raster.point_timeseries",
        "data.temporal_aggregate",
        "dhis2.pipeline",
    }


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

    assert execute_response.status_code == 200
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

    assert execute_response.status_code == 200
    job_id = execute_response.json()["jobId"]
    job_response = client.get(f"/jobs/{job_id}")
    assert job_response.status_code == 200
    outputs = job_response.json()["outputs"]
    assert outputs["options"]["frequency"] == "P1M"
    assert outputs["options"]["aggregation"] == "sum"
    assert outputs["rows"][0]["operation"] == "temporal_aggregate"


def test_dhis2_pipeline_execution_returns_data_value_set(monkeypatch) -> None:
    client = create_client()

    class FakeProvider:
        provider_id = "fake"

        def fetch(self, request):
            return RasterFetchResult(
                provider=self.provider_id,
                asset_paths=["/tmp/chirps_cached.nc"],
                from_cache=True,
            )

    def fake_zonal_stats_stub(**kwargs):
        return [
            {
                "dataset_id": "chirps-daily",
                "parameter": "precip",
                "operation": "zonal_stats",
                "time": "2026-01-31",
                "aoi_bbox": [30.0, -10.0, 31.0, -9.0],
                "asset_count": 1,
                "stat": "mean",
                "value": 7.81,
                "status": "computed",
                "note": None,
            }
        ]

    monkeypatch.setattr("eoapi.processing.pipeline.build_provider", lambda dataset: FakeProvider())
    monkeypatch.setattr("eoapi.processing.pipeline.zonal_stats_stub", fake_zonal_stats_stub)

    execute_response = client.post(
        "/processes/dhis2.pipeline/execution",
        json={
            "inputs": {
                "features": {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "id": "O6uvpzGd5pu",
                            "geometry": {
                                "type": "Polygon",
                                "coordinates": [[[30.0, -10.0], [31.0, -10.0], [31.0, -9.0], [30.0, -9.0], [30.0, -10.0]]],
                            },
                            "properties": {"name": "Bo"},
                        }
                    ],
                },
                "dataset_id": "chirps-daily",
                "params": ["precip"],
                "time": "2026-01-31",
                "aggregation": "mean",
                "data_element": "abc123def45",
            }
        },
    )

    assert execute_response.status_code == 200
    payload = execute_response.json()
    assert payload["processId"] == "dhis2.pipeline"
    assert payload["status"] == "succeeded"
    assert payload["outputs"]["summary"]["features"] == 1
    data_value_set = payload["outputs"]["dataValueSet"]
    assert data_value_set["period"] == "20260131"
    assert data_value_set["dataValues"][0]["dataElement"] == "abc123def45"
    assert data_value_set["dataValues"][0]["orgUnit"] == "O6uvpzGd5pu"
