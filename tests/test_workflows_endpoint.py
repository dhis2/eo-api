from fastapi import FastAPI
from fastapi.testclient import TestClient

from eoapi.endpoints.processes import router as processes_router
from eoapi.endpoints.workflows import router as workflows_router
from eoapi.processing.providers.base import RasterFetchResult


def create_client() -> TestClient:
    app = FastAPI()
    app.include_router(processes_router)
    app.include_router(workflows_router)
    return TestClient(app)


def test_workflow_crud_and_run(monkeypatch) -> None:
    client = create_client()

    class FakeProvider:
        provider_id = "fake"

        def fetch(self, request):
            return RasterFetchResult(
                provider=self.provider_id,
                asset_paths=[f"/tmp/{request.parameter}.nc"],
                from_cache=True,
            )

    monkeypatch.setattr("eoapi.processing.service.build_provider", lambda dataset: FakeProvider())

    create_response = client.post(
        "/workflows",
        json={
            "name": "climate-workflow",
            "steps": [
                {
                    "name": "zonal",
                    "processId": "raster.zonal_stats",
                    "payload": {
                        "inputs": {
                            "dataset_id": "chirps-daily",
                            "params": ["precip"],
                            "time": "2026-01-31",
                            "aoi": [30.0, -10.0, 31.0, -9.0],
                        }
                    },
                },
                {
                    "name": "timeseries",
                    "processId": "raster.point_timeseries",
                    "payload": {
                        "inputs": {
                            "dataset_id": "chirps-daily",
                            "params": ["precip"],
                            "time": "2026-01-31",
                            "aoi": {"bbox": [30.0, -10.0, 32.0, -8.0]},
                        }
                    },
                },
            ],
        },
    )

    assert create_response.status_code == 201
    workflow_id = create_response.json()["workflowId"]

    list_response = client.get("/workflows")
    assert list_response.status_code == 200
    assert any(item["workflowId"] == workflow_id for item in list_response.json()["workflows"])

    run_response = client.post(f"/workflows/{workflow_id}/run")
    assert run_response.status_code == 202
    run_payload = run_response.json()
    assert run_payload["workflowId"] == workflow_id
    assert len(run_payload["jobIds"]) == 2

    get_response = client.get(f"/workflows/{workflow_id}")
    assert get_response.status_code == 200
    get_payload = get_response.json()
    assert get_payload["lastRunJobIds"] == run_payload["jobIds"]

    delete_response = client.delete(f"/workflows/{workflow_id}")
    assert delete_response.status_code == 204

    missing_response = client.get(f"/workflows/{workflow_id}")
    assert missing_response.status_code == 404
