from fastapi import FastAPI
from fastapi.testclient import TestClient

from eoapi.endpoints.features import router as features_router
from eoapi.endpoints.processes import router as processes_router
from eoapi.endpoints.workflows import router as workflows_router


def create_client() -> TestClient:
    app = FastAPI()
    app.include_router(features_router)
    app.include_router(processes_router)
    app.include_router(workflows_router)
    return TestClient(app)


def test_workflow_crud_and_run() -> None:
    client = create_client()

    create_response = client.post(
        "/workflows",
        json={
            "name": "climate-workflow",
            "steps": [
                {
                    "name": "aggregate",
                    "processId": "eo-aggregate-import",
                    "payload": {
                        "inputs": {
                            "datasetId": "chirps-daily",
                            "parameters": ["precip"],
                            "datetime": "2026-01-31T00:00:00Z",
                            "orgUnitLevel": 2,
                            "aggregation": "mean",
                            "dhis2": {"dataElementId": "abc123", "dryRun": True},
                        }
                    },
                },
                {
                    "name": "cdd",
                    "processId": "xclim-cdd",
                    "payload": {
                        "inputs": {
                            "datasetId": "chirps-daily",
                            "parameter": "precip",
                            "start": "2026-01-01",
                            "end": "2026-01-31",
                            "orgUnitLevel": 2,
                            "threshold": {"value": 1.0, "unit": "mm/day"},
                            "dhis2": {"dataElementId": "abc123", "dryRun": True},
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
