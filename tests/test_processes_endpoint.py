from fastapi import FastAPI
from fastapi.testclient import TestClient
import pandas as pd
import xarray as xr

from eoapi.endpoints.features import router as features_router
from eoapi.endpoints.processes import router as processes_router
from eoapi.jobs import create_job


def create_client() -> TestClient:
    app = FastAPI()
    app.include_router(features_router)
    app.include_router(processes_router)
    return TestClient(app)


def test_process_execution_and_job_status() -> None:
    client = create_client()

    execute_response = client.post(
        "/processes/eo-aggregate-import/execution",
        json={
            "inputs": {
                "datasetId": "chirps-daily",
                "parameters": ["precip"],
                "datetime": "2026-01-31T00:00:00Z",
                "orgUnitLevel": 2,
                "aggregation": "mean",
                "dhis2": {
                    "dataElementId": "abc123",
                    "dryRun": True,
                },
            }
        },
    )

    assert execute_response.status_code == 202
    execute_payload = execute_response.json()
    assert execute_payload["processId"] == "eo-aggregate-import"

    job_id = execute_payload["jobId"]
    job_response = client.get(f"/jobs/{job_id}")
    assert job_response.status_code == 200

    job_payload = job_response.json()
    assert job_payload["status"] == "succeeded"
    assert job_payload["importSummary"]["dryRun"] is True

    features_response = client.get("/features/aggregated-results/items", params={"jobId": job_id})
    assert features_response.status_code == 200
    features_payload = features_response.json()
    assert features_payload["type"] == "FeatureCollection"
    assert features_payload["numberReturned"] >= 1


def test_process_unknown_id() -> None:
    client = create_client()

    response = client.get("/processes/unknown")

    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "NotFound"


def test_process_list_includes_xclim_processes() -> None:
    client = create_client()

    response = client.get("/processes")

    assert response.status_code == 200
    payload = response.json()
    process_ids = {process["id"] for process in payload["processes"]}
    assert "eo-aggregate-import" in process_ids
    assert "xclim-cdd" in process_ids
    assert "xclim-cwd" in process_ids
    assert "xclim-warm-days" in process_ids


def test_xclim_execution_dispatch(monkeypatch) -> None:
    client = create_client()

    def fake_run_xclim(process_id, inputs):
        outputs = {
            "importSummary": {
                "imported": 0,
                "updated": 0,
                "ignored": 1,
                "deleted": 0,
                "dryRun": True,
            },
            "features": [
                {
                    "type": "Feature",
                    "id": "org-demo",
                    "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
                    "properties": {"orgUnit": "org-demo", "value": 4.0},
                }
            ],
        }
        return create_job(process_id, inputs.model_dump(mode="json"), outputs)

    monkeypatch.setattr("eoapi.endpoints.processes._run_xclim", fake_run_xclim)

    execute_response = client.post(
        "/processes/xclim-cdd/execution",
        json={
            "inputs": {
                "datasetId": "chirps-daily",
                "parameter": "precip",
                "start": "2026-01-01",
                "end": "2026-01-31",
                "orgUnitLevel": 2,
                "threshold": {"value": 1.0, "unit": "mm/day"},
                "dhis2": {
                    "dataElementId": "abc123",
                    "dryRun": True,
                },
            }
        },
    )

    assert execute_response.status_code == 202
    payload = execute_response.json()
    assert payload["processId"] == "xclim-cdd"

    job_response = client.get(f"/jobs/{payload['jobId']}")
    assert job_response.status_code == 200
    assert job_response.json()["processId"] == "xclim-cdd"


def test_xclim_uses_dhis2eo_series_when_available(monkeypatch) -> None:
    client = create_client()

    def fake_extract_org_unit_series(**kwargs):
        times = pd.date_range("2026-01-01", "2026-01-31", freq="D")
        ids = [feature["id"] for feature in kwargs["org_units"]]
        return {
            org_unit_id: xr.DataArray(
                [0.4 + (index % 3) * 0.2 for index, _ in enumerate(times)],
                coords={"time": times},
                dims=["time"],
                attrs={"units": "mm/day"},
            )
            for org_unit_id in ids
        }

    monkeypatch.setattr("eoapi.endpoints.processes._extract_org_unit_series", fake_extract_org_unit_series)

    execute_response = client.post(
        "/processes/xclim-cdd/execution",
        json={
            "inputs": {
                "datasetId": "chirps-daily",
                "parameter": "precip",
                "start": "2026-01-01",
                "end": "2026-01-31",
                "orgUnitLevel": 2,
                "threshold": {"value": 1.0, "unit": "mm/day"},
                "dhis2": {
                    "dataElementId": "abc123",
                    "dryRun": True,
                },
            }
        },
    )

    assert execute_response.status_code == 202
    job_id = execute_response.json()["jobId"]
    features_response = client.get("/features/aggregated-results/items", params={"jobId": job_id})
    assert features_response.status_code == 200

    payload = features_response.json()
    assert payload["features"]
    assert payload["features"][0]["properties"]["source"] == "dhis2eo"


def test_xclim_falls_back_to_synthetic_on_extractor_error(monkeypatch) -> None:
    client = create_client()

    def fail_extract_org_unit_series(**kwargs):
        raise RuntimeError("network unavailable")

    monkeypatch.setattr("eoapi.endpoints.processes._extract_org_unit_series", fail_extract_org_unit_series)

    execute_response = client.post(
        "/processes/xclim-warm-days/execution",
        json={
            "inputs": {
                "datasetId": "era5-land-daily",
                "parameter": "2m_temperature",
                "start": "2026-01-01",
                "end": "2026-01-31",
                "orgUnitLevel": 2,
                "threshold": {"value": 35.0, "unit": "degC"},
                "dhis2": {
                    "dataElementId": "abc123",
                    "dryRun": True,
                },
            }
        },
    )

    assert execute_response.status_code == 202
    job_id = execute_response.json()["jobId"]
    features_response = client.get("/features/aggregated-results/items", params={"jobId": job_id})
    assert features_response.status_code == 200

    payload = features_response.json()
    assert payload["features"]
    assert payload["features"][0]["properties"]["source"] == "synthetic-fallback"


def test_aggregate_process_uses_dhis2_import_adapter(monkeypatch) -> None:
    client = create_client()

    def fake_import(data_values, dry_run):
        assert dry_run is False
        assert len(data_values) >= 1
        return {
            "imported": len(data_values),
            "updated": 0,
            "ignored": 0,
            "deleted": 0,
            "dryRun": False,
            "source": "dhis2",
        }

    monkeypatch.setattr("eoapi.endpoints.processes.import_data_values_to_dhis2", fake_import)

    execute_response = client.post(
        "/processes/eo-aggregate-import/execution",
        json={
            "inputs": {
                "datasetId": "chirps-daily",
                "parameters": ["precip"],
                "datetime": "2026-01-31T00:00:00Z",
                "orgUnitLevel": 2,
                "aggregation": "mean",
                "dhis2": {
                    "dataElementId": "abc123",
                    "dryRun": False,
                },
            }
        },
    )

    assert execute_response.status_code == 202
    job_id = execute_response.json()["jobId"]

    job_response = client.get(f"/jobs/{job_id}")
    assert job_response.status_code == 200
    payload = job_response.json()
    assert payload["importSummary"]["dryRun"] is False
    assert payload["importSummary"]["source"] == "dhis2"
