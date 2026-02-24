from fastapi import FastAPI
from fastapi.testclient import TestClient

from eoapi.endpoints.processes import router as processes_router
from eoapi.endpoints.schedules import router as schedules_router
from eoapi.endpoints.workflows import router as workflows_router
from eoapi.jobs import create_pending_job
from eoapi.processing.providers.base import RasterFetchResult


def create_client() -> TestClient:
    app = FastAPI()
    app.include_router(processes_router)
    app.include_router(workflows_router)
    app.include_router(schedules_router)
    return TestClient(app)


def _patch_fake_provider(monkeypatch) -> None:
    class FakeProvider:
        provider_id = "fake"

        def fetch(self, request):
            return RasterFetchResult(
                provider=self.provider_id,
                asset_paths=[f"/tmp/{request.parameter}.nc"],
                from_cache=True,
            )

    monkeypatch.setattr("eoapi.processing.service.build_provider", lambda dataset: FakeProvider())


def _create_schedule(client: TestClient) -> str:
    response = client.post(
        "/schedules",
        json={
            "name": "nightly-zonal-stats",
            "cron": "0 0 * * *",
            "timezone": "UTC",
            "enabled": True,
            "processId": "raster.zonal_stats",
            "inputs": {
                "dataset_id": "chirps-daily",
                "params": ["precip"],
                "time": "2026-01-31",
                "aoi": [30.0, -10.0, 31.0, -9.0],
            },
        },
    )
    assert response.status_code == 201
    return response.json()["scheduleId"]


def test_schedule_crud_and_run(monkeypatch) -> None:
    _patch_fake_provider(monkeypatch)
    client = create_client()

    schedule_id = _create_schedule(client)

    list_response = client.get("/schedules")
    assert list_response.status_code == 200
    assert any(item["scheduleId"] == schedule_id for item in list_response.json()["schedules"])

    get_response = client.get(f"/schedules/{schedule_id}")
    assert get_response.status_code == 200
    assert get_response.json()["name"] == "nightly-zonal-stats"

    patch_response = client.patch(
        f"/schedules/{schedule_id}",
        json={"enabled": False},
    )
    assert patch_response.status_code == 200
    assert patch_response.json()["enabled"] is False

    run_response = client.post(f"/schedules/{schedule_id}/run")
    assert run_response.status_code == 202
    job_id = run_response.json()["jobId"]

    job_response = client.get(f"/jobs/{job_id}")
    assert job_response.status_code == 200
    assert job_response.json()["status"] == "succeeded"

    get_after_run = client.get(f"/schedules/{schedule_id}")
    assert get_after_run.status_code == 200
    assert get_after_run.json()["lastRunJobId"] == job_id

    delete_response = client.delete(f"/schedules/{schedule_id}")
    assert delete_response.status_code == 204

    not_found_response = client.get(f"/schedules/{schedule_id}")
    assert not_found_response.status_code == 404


def test_schedule_callback_runs_job(monkeypatch) -> None:
    _patch_fake_provider(monkeypatch)
    client = create_client()
    monkeypatch.setenv("EOAPI_SCHEDULER_TOKEN", "secret-token")

    schedule_id = _create_schedule(client)

    callback_response = client.post(
        f"/schedules/{schedule_id}/callback",
        headers={"X-Scheduler-Token": "secret-token"},
    )
    assert callback_response.status_code == 202
    callback_payload = callback_response.json()
    assert callback_payload["trigger"] == "scheduler-callback"

    job_id = callback_payload["jobId"]
    job_response = client.get(f"/jobs/{job_id}")
    assert job_response.status_code == 200
    assert job_response.json()["status"] == "succeeded"


def test_schedule_callback_invalid_token(monkeypatch) -> None:
    _patch_fake_provider(monkeypatch)
    client = create_client()
    monkeypatch.setenv("EOAPI_SCHEDULER_TOKEN", "secret-token")

    schedule_id = _create_schedule(client)
    response = client.post(
        f"/schedules/{schedule_id}/callback",
        headers={"X-Scheduler-Token": "wrong-token"},
    )

    assert response.status_code == 403
    assert response.json()["detail"]["code"] == "Forbidden"


def test_schedule_callback_missing_server_token(monkeypatch) -> None:
    _patch_fake_provider(monkeypatch)
    client = create_client()
    monkeypatch.delenv("EOAPI_SCHEDULER_TOKEN", raising=False)

    schedule_id = _create_schedule(client)
    response = client.post(
        f"/schedules/{schedule_id}/callback",
        headers={"X-Scheduler-Token": "any-value"},
    )

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "ServiceUnavailable"


def test_job_status_syncs_prefect_state(monkeypatch) -> None:
    client = create_client()
    monkeypatch.setenv("EOAPI_PREFECT_ENABLED", "true")

    job = create_pending_job(
        "raster.zonal_stats",
        inputs={},
        source="prefect",
        flow_run_id="flow-run-2",
    )

    def fake_get_flow_run(flow_run_id):
        assert flow_run_id == "flow-run-2"
        return {"state": {"type": "COMPLETED", "name": "Completed"}}

    monkeypatch.setattr("eoapi.endpoints.processes.get_flow_run", fake_get_flow_run)

    job_response = client.get(f"/jobs/{job['jobId']}")
    assert job_response.status_code == 200
    payload = job_response.json()
    assert payload["status"] == "succeeded"
    assert payload["progress"] == 100
    assert payload["execution"]["source"] == "prefect"


def test_schedule_run_for_workflow_target(monkeypatch) -> None:
    _patch_fake_provider(monkeypatch)
    client = create_client()

    workflow_response = client.post(
        "/workflows",
        json={
            "name": "scheduled-workflow",
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
                }
            ],
        },
    )
    assert workflow_response.status_code == 201
    workflow_id = workflow_response.json()["workflowId"]

    schedule_response = client.post(
        "/schedules",
        json={
            "name": "nightly-workflow",
            "cron": "0 0 * * *",
            "timezone": "UTC",
            "enabled": True,
            "workflowId": workflow_id,
        },
    )
    assert schedule_response.status_code == 201
    schedule_id = schedule_response.json()["scheduleId"]

    run_response = client.post(f"/schedules/{schedule_id}/run")
    assert run_response.status_code == 202
    run_payload = run_response.json()
    assert run_payload["workflowId"] == workflow_id
    assert run_payload["execution"]["source"] == "workflow"
    assert len(run_payload["jobIds"]) == 1
