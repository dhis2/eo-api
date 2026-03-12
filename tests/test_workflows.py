from __future__ import annotations

from typing import Any, cast

import numpy as np
import pytest
import xarray as xr
from fastapi import HTTPException
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from eo_api.main import app
from eo_api.workflows.schemas import WorkflowExecuteRequest, WorkflowExecuteResponse, WorkflowRequest
from eo_api.workflows.services import engine
from eo_api.workflows.services.definitions import WorkflowDefinition, load_workflow_definition
from eo_api.workflows.services.simple_mapper import normalize_simple_request


def _valid_public_payload() -> dict[str, Any]:
    return {
        "request": {
            "workflow_id": "dhis2_datavalue_set_v1",
            "dataset_id": "chirps3_precipitation_daily",
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
            "org_unit_level": 3,
            "data_element": "abc123def45",
            "temporal_resolution": "monthly",
            "temporal_reducer": "sum",
            "spatial_reducer": "mean",
            "dry_run": True,
            "include_component_run_details": False,
        }
    }


def test_workflow_endpoint_exists_once() -> None:
    workflow_routes = {
        route.path
        for route in app.routes
        if isinstance(route, APIRoute) and route.path.startswith("/workflows") and "POST" in route.methods
    }
    assert workflow_routes == {"/workflows/dhis2-datavalue-set", "/workflows/execute", "/workflows/validate"}


def test_workflow_catalog_endpoint_returns_allowlisted_workflow(client: TestClient) -> None:
    response = client.get("/workflows")
    assert response.status_code == 200
    body = response.json()
    assert "workflows" in body
    assert len(body["workflows"]) >= 2
    by_id = {item["workflow_id"]: item for item in body["workflows"]}

    default = by_id["dhis2_datavalue_set_v1"]
    assert default["version"] == 1
    assert default["step_count"] == 5
    assert default["components"] == [
        "feature_source",
        "download_dataset",
        "temporal_aggregation",
        "spatial_aggregation",
        "build_datavalueset",
    ]

    fast = by_id["dhis2_datavalue_set_without_temporal_aggregation_v1"]
    assert fast["version"] == 1
    assert fast["step_count"] == 4
    assert fast["components"] == [
        "feature_source",
        "download_dataset",
        "spatial_aggregation",
        "build_datavalueset",
    ]


def test_components_catalog_endpoint_returns_five_components(client: TestClient) -> None:
    response = client.get("/components")
    assert response.status_code == 200
    items = response.json()["components"]
    names = {item["name"] for item in items}
    assert names == {
        "feature_source",
        "download_dataset",
        "temporal_aggregation",
        "spatial_aggregation",
        "build_datavalueset",
    }
    for item in items:
        assert item["version"] == "v1"
        assert isinstance(item["input_schema"], dict)
        assert "config_schema" not in item
        assert isinstance(item["output_schema"], dict)
        assert "EXECUTION_FAILED" in item["error_codes"]
        assert item["endpoint"]["method"] == "POST"
        assert item["endpoint"]["path"].startswith("/components/")


def test_components_catalog_include_internal_includes_config_schema(client: TestClient) -> None:
    response = client.get("/components?include_internal=true")
    assert response.status_code == 200
    items = response.json()["components"]
    assert len(items) >= 5
    for item in items:
        assert isinstance(item["config_schema"], dict)


def test_workflow_endpoint_returns_response_shape(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    stub = WorkflowExecuteResponse(
        status="completed",
        run_id="run-123",
        workflow_id="dhis2_datavalue_set_v1",
        workflow_version=1,
        dataset_id="chirps3_precipitation_daily",
        bbox=[-13.3, 6.9, -10.1, 10.0],
        feature_count=2,
        value_count=4,
        output_file="/tmp/data/chirps3_datavalueset.json",
        run_log_file="/tmp/data/workflow_runs/run-123.json",
        data_value_set={
            "dataValues": [
                {
                    "dataElement": "abc123def45",
                    "period": "202401",
                    "orgUnit": "OU_1",
                    "categoryOptionCombo": "HllvX50cXC0",
                    "attributeOptionCombo": "HllvX50cXC0",
                    "value": "12.3",
                }
            ]
        },
        component_runs=[],
    )

    def _execute_stub(
        payload: Any,
        workflow_id: str = "dhis2_datavalue_set_v1",
        request_params: dict[str, Any] | None = None,
        include_component_run_details: bool = False,
    ) -> WorkflowExecuteResponse:
        del payload, workflow_id, request_params, include_component_run_details
        return stub

    monkeypatch.setattr(
        "eo_api.workflows.routes.execute_workflow",
        _execute_stub,
    )

    response = client.post("/workflows/dhis2-datavalue-set", json=_valid_public_payload())
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "completed"
    assert body["run_id"] == "run-123"
    assert body["workflow_id"] == "dhis2_datavalue_set_v1"
    assert body["workflow_version"] == 1
    assert body["run_log_file"].endswith(".json")
    assert "dataValues" in body["data_value_set"]
    assert body["component_run_details_included"] is False
    assert body["component_run_details_available"] is True


def test_workflow_endpoint_validates_required_fields(client: TestClient) -> None:
    payload = _valid_public_payload()
    payload["request"].pop("org_unit_level")

    response = client.post("/workflows/dhis2-datavalue-set", json=payload)
    assert response.status_code == 422


def test_workflow_endpoint_accepts_simplified_payload(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    normalized = WorkflowExecuteRequest.model_validate(
        {
            "dataset_id": "chirps3_precipitation_daily",
            "start": "2024-01-01",
            "end": "2024-01-31",
            "feature_source": {"source_type": "dhis2_level", "dhis2_level": 3, "feature_id_property": "id"},
            "temporal_aggregation": {"target_period_type": "monthly", "method": "sum"},
            "spatial_aggregation": {"method": "mean"},
            "dhis2": {"data_element_uid": "abc123def45"},
        }
    )
    stub = WorkflowExecuteResponse(
        status="completed",
        run_id="run-123",
        workflow_id="dhis2_datavalue_set_v1",
        workflow_version=1,
        dataset_id="chirps3_precipitation_daily",
        bbox=[-13.3, 6.9, -10.1, 10.0],
        feature_count=2,
        value_count=4,
        output_file="/tmp/data/chirps3_datavalueset.json",
        run_log_file="/tmp/data/workflow_runs/run-123.json",
        data_value_set={"dataValues": []},
        component_runs=[],
    )

    def _execute_stub(
        payload: Any,
        workflow_id: str = "dhis2_datavalue_set_v1",
        request_params: dict[str, Any] | None = None,
        include_component_run_details: bool = False,
    ) -> WorkflowExecuteResponse:
        del payload, workflow_id, request_params, include_component_run_details
        return stub

    monkeypatch.setattr("eo_api.workflows.routes.normalize_simple_request", lambda payload: (normalized, []))
    monkeypatch.setattr(
        "eo_api.workflows.routes.execute_workflow",
        _execute_stub,
    )

    response = client.post("/workflows/dhis2-datavalue-set", json=_valid_public_payload())
    assert response.status_code == 200
    assert response.json()["status"] == "completed"


def test_inline_workflow_execute_endpoint_accepts_assembly(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    stub = WorkflowExecuteResponse(
        status="completed",
        run_id="run-assembly-123",
        workflow_id="adhoc_dhis2_v1",
        workflow_version=1,
        dataset_id="chirps3_precipitation_daily",
        bbox=[-13.3, 6.9, -10.1, 10.0],
        feature_count=2,
        value_count=4,
        output_file="/tmp/data/chirps3_datavalueset.json",
        run_log_file="/tmp/data/workflow_runs/run-assembly-123.json",
        data_value_set={"dataValues": []},
        component_runs=[],
    )

    def _execute_stub(
        payload: Any,
        workflow_id: str = "dhis2_datavalue_set_v1",
        workflow_definition: WorkflowDefinition | None = None,
        request_params: dict[str, Any] | None = None,
        include_component_run_details: bool = False,
    ) -> WorkflowExecuteResponse:
        del payload, request_params, include_component_run_details
        assert workflow_id == "adhoc_dhis2_v1"
        assert workflow_definition is not None
        assert workflow_definition.workflow_id == "adhoc_dhis2_v1"
        assert len(workflow_definition.steps) == 4
        return stub

    monkeypatch.setattr("eo_api.workflows.routes.execute_workflow", _execute_stub)

    response = client.post(
        "/workflows/execute",
        json={
            "workflow": {
                "workflow_id": "adhoc_dhis2_v1",
                "version": 1,
                "steps": [
                    {"component": "feature_source", "version": "v1", "config": {}},
                    {"component": "download_dataset", "version": "v1", "config": {}},
                    {"component": "spatial_aggregation", "version": "v1", "config": {}},
                    {"component": "build_datavalueset", "version": "v1", "config": {}},
                ],
            },
            "request": {
                "workflow_id": "adhoc_dhis2_v1",
                "dataset_id": "chirps3_precipitation_daily",
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "org_unit_level": 3,
                "data_element": "abc123def45",
                "temporal_resolution": "monthly",
                "temporal_reducer": "sum",
                "spatial_reducer": "mean",
                "include_component_run_details": False,
            },
        },
    )
    assert response.status_code == 200
    assert response.json()["workflow_id"] == "adhoc_dhis2_v1"


def test_inline_workflow_execute_endpoint_rejects_bad_component_chain(client: TestClient) -> None:
    response = client.post(
        "/workflows/execute",
        json={
            "workflow": {
                "workflow_id": "bad_adhoc_v1",
                "version": 1,
                "steps": [
                    {"component": "download_dataset", "version": "v1", "config": {}},
                    {"component": "build_datavalueset", "version": "v1", "config": {}},
                ],
            },
            "request": {
                "workflow_id": "bad_adhoc_v1",
                "dataset_id": "chirps3_precipitation_daily",
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "org_unit_level": 3,
                "data_element": "abc123def45",
            },
        },
    )
    assert response.status_code == 422


def test_workflow_validate_endpoint_accepts_valid_inline_workflow(client: TestClient) -> None:
    response = client.post(
        "/workflows/validate",
        json={
            "workflow": {
                "workflow_id": "adhoc_validate_v1",
                "version": 1,
                "steps": [
                    {"component": "feature_source", "version": "v1", "config": {}},
                    {"component": "download_dataset", "version": "v1", "config": {}},
                    {"component": "spatial_aggregation", "version": "v1", "config": {}},
                    {"component": "build_datavalueset", "version": "v1", "config": {}},
                ],
            },
            "request": {
                "workflow_id": "adhoc_validate_v1",
                "dataset_id": "chirps3_precipitation_daily",
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "org_unit_level": 3,
                "data_element": "abc123def45",
            },
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["valid"] is True
    assert body["workflow_id"] == "adhoc_validate_v1"
    assert body["step_count"] == 4
    assert len(body["resolved_steps"]) == 4
    assert body["errors"] == []


def test_workflow_validate_endpoint_rejects_runtime_knobs_in_step_config(client: TestClient) -> None:
    response = client.post(
        "/workflows/validate",
        json={
            "workflow": {
                "workflow_id": "adhoc_invalid_config_v1",
                "version": 1,
                "steps": [
                    {"component": "feature_source", "version": "v1", "config": {}},
                    {"component": "download_dataset", "version": "v1", "config": {"overwrite": True}},
                    {"component": "spatial_aggregation", "version": "v1", "config": {}},
                    {"component": "build_datavalueset", "version": "v1", "config": {}},
                ],
            },
            "request": {
                "workflow_id": "adhoc_invalid_config_v1",
                "dataset_id": "chirps3_precipitation_daily",
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "org_unit_level": 3,
                "data_element": "abc123def45",
            },
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["valid"] is False
    assert body["resolved_steps"] == []
    assert len(body["errors"]) == 1
    assert "validation failed" in body["errors"][0].lower()


def test_workflow_validate_endpoint_unknown_workflow_id(client: TestClient) -> None:
    response = client.post("/workflows/validate", json={"workflow_id": "does_not_exist"})
    assert response.status_code == 200
    body = response.json()
    assert body["valid"] is False
    assert body["step_count"] == 0
    assert len(body["errors"]) == 1
    assert "Unknown workflow_id" in body["errors"][0]


def test_component_spatial_aggregation_serializes_numpy_datetime64(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "eo_api.components.routes.services.require_dataset",
        lambda dataset_id: {"id": dataset_id, "variable": "precip"},
    )
    monkeypatch.setattr(
        "eo_api.components.routes.services.feature_source_component",
        lambda feature_source: (
            {"type": "FeatureCollection", "features": [{"id": "OU_1", "properties": {"id": "OU_1"}}]},
            [0.0, 0.0, 1.0, 1.0],
        ),
    )
    monkeypatch.setattr(
        "eo_api.components.routes.services.spatial_aggregation_component",
        lambda **kwargs: [{"org_unit": "OU_1", "time": np.datetime64("2024-01-01"), "value": 10.0}],
    )

    response = client.post(
        "/components/spatial-aggregation",
        json={
            "dataset_id": "chirps3_precipitation_daily",
            "start": "2024-01",
            "end": "2024-01",
            "feature_source": {"source_type": "dhis2_level", "dhis2_level": 2},
            "method": "mean",
            "include_records": True,
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["record_count"] == 1
    assert body["records"][0]["time"] == "2024-01-01T00:00:00"


def test_engine_orchestrates_components(monkeypatch: pytest.MonkeyPatch) -> None:
    request = {
        "dataset_id": "chirps3_precipitation_daily",
        "start": "2024-01-01",
        "end": "2024-01-31",
        "country_code": "SLE",
        "feature_source": {
            "source_type": "geojson_file",
            "geojson_path": "tests/data/sierra_leone_districts.geojson",
            "feature_id_property": "id",
        },
        "temporal_aggregation": {"target_period_type": "monthly", "method": "sum"},
        "spatial_aggregation": {"method": "mean"},
        "dhis2": {"data_element_uid": "abc123def45"},
    }

    dataset = {"id": "chirps3_precipitation_daily", "variable": "precip"}
    ds = xr.Dataset(
        {"precip": (("time", "lat", "lon"), [[[1.0]]])},
        coords={"time": ["2024-01-01"], "lat": [0], "lon": [0]},
    )

    monkeypatch.setattr(engine, "get_dataset", lambda dataset_id: dataset)

    called: dict[str, Any] = {"downloaded": False}

    def _download_dataset_component(**kwargs: Any) -> None:
        called["downloaded"] = True
        assert kwargs["bbox"] == [0.0, 0.0, 1.0, 1.0]
        assert kwargs["country_code"] == "SLE"

    monkeypatch.setattr(
        engine.component_services,
        "feature_source_component",
        lambda config: (
            {"type": "FeatureCollection", "features": [{"id": "OU_1", "properties": {"id": "OU_1"}}]},
            [0.0, 0.0, 1.0, 1.0],
        ),
    )
    monkeypatch.setattr(engine.component_services, "download_dataset_component", _download_dataset_component)
    monkeypatch.setattr(engine.component_services, "temporal_aggregation_component", lambda **kwargs: ds)
    monkeypatch.setattr(
        engine.component_services,
        "spatial_aggregation_component",
        lambda **kwargs: [{"org_unit": "OU_1", "time": "2024-01-01", "value": 10.0}],
    )
    monkeypatch.setattr(
        engine.component_services,
        "build_datavalueset_component",
        lambda **kwargs: ({"dataValues": [{"value": "10.0"}]}, "/tmp/data/out.json"),
    )
    monkeypatch.setattr(engine, "persist_run_log", lambda **kwargs: "/tmp/data/workflow_runs/run.json")

    response = engine.execute_workflow(
        engine.WorkflowExecuteRequest.model_validate(request),
        include_component_run_details=True,
    )
    assert response.status == "completed"
    assert response.run_id
    assert response.value_count == 1
    assert response.run_log_file.endswith(".json")
    assert len(response.component_runs) == 5
    assert [c.component for c in response.component_runs] == [
        "feature_source",
        "download_dataset",
        "temporal_aggregation",
        "spatial_aggregation",
        "build_datavalueset",
    ]
    assert response.component_run_details_included is True
    assert response.component_run_details_available is True
    assert called["downloaded"] is True


def test_engine_hides_component_details_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    request = WorkflowExecuteRequest.model_validate(
        {
            "dataset_id": "chirps3_precipitation_daily",
            "start": "2024-01-01",
            "end": "2024-01-31",
            "feature_source": {"source_type": "dhis2_level", "dhis2_level": 3},
            "temporal_aggregation": {"target_period_type": "monthly", "method": "sum"},
            "spatial_aggregation": {"method": "mean"},
            "dhis2": {"data_element_uid": "abc123def45"},
        }
    )
    ds = xr.Dataset(
        {"precip": (("time", "lat", "lon"), [[[1.0]]])},
        coords={"time": ["2024-01-01"], "lat": [0], "lon": [0]},
    )
    monkeypatch.setattr(
        engine,
        "get_dataset",
        lambda dataset_id: {"id": "chirps3_precipitation_daily", "variable": "precip"},
    )
    monkeypatch.setattr(
        engine.component_services,
        "feature_source_component",
        lambda config: (
            {"type": "FeatureCollection", "features": [{"id": "OU_1", "properties": {"id": "OU_1"}}]},
            [0, 0, 1, 1],
        ),
    )
    monkeypatch.setattr(engine.component_services, "download_dataset_component", lambda **kwargs: None)
    monkeypatch.setattr(engine.component_services, "temporal_aggregation_component", lambda **kwargs: ds)
    monkeypatch.setattr(
        engine.component_services,
        "spatial_aggregation_component",
        lambda **kwargs: [{"org_unit": "OU_1", "time": "2024-01-01", "value": 10.0}],
    )
    monkeypatch.setattr(
        engine.component_services,
        "build_datavalueset_component",
        lambda **kwargs: ({"dataValues": [{"value": "10.0"}]}, "/tmp/data/out.json"),
    )
    monkeypatch.setattr(engine, "persist_run_log", lambda **kwargs: "/tmp/data/workflow_runs/run.json")

    response = engine.execute_workflow(request)
    assert response.component_runs == []
    assert response.component_run_details_included is False
    assert response.component_run_details_available is True


def test_engine_returns_503_when_upstream_unreachable(monkeypatch: pytest.MonkeyPatch) -> None:
    request = WorkflowExecuteRequest.model_validate(
        {
            "dataset_id": "chirps3_precipitation_daily",
            "start": "2024-01-01",
            "end": "2024-01-31",
            "feature_source": {"source_type": "dhis2_level", "dhis2_level": 3},
            "temporal_aggregation": {"target_period_type": "monthly", "method": "sum"},
            "spatial_aggregation": {"method": "mean"},
            "dhis2": {"data_element_uid": "abc123def45"},
        }
    )
    monkeypatch.setattr(engine, "get_dataset", lambda dataset_id: {"id": "chirps3_precipitation_daily"})
    monkeypatch.setattr(
        engine.component_services,
        "feature_source_component",
        lambda config: (
            {"type": "FeatureCollection", "features": [{"id": "OU_1", "properties": {"id": "OU_1"}}]},
            [0, 0, 1, 1],
        ),
    )
    monkeypatch.setattr(
        engine.component_services,
        "download_dataset_component",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("Failed to connect to server")),
    )
    monkeypatch.setattr(engine, "persist_run_log", lambda **kwargs: "/tmp/data/workflow_runs/run.json")

    with pytest.raises(HTTPException) as exc_info:
        engine.execute_workflow(request)

    assert exc_info.value.status_code == 503
    detail = cast(dict[str, Any], exc_info.value.detail)
    assert detail["error"] == "upstream_unreachable"
    assert detail["error_code"] == "UPSTREAM_UNREACHABLE"
    assert detail["failed_component"] == "download_dataset"
    assert detail["failed_component_version"] == "v1"


def test_mapper_uses_year_format_for_yearly_dataset() -> None:
    normalized, _warnings = normalize_simple_request(
        WorkflowRequest.model_validate(
            {
                "dataset_id": "worldpop_population_yearly",
                "country_code": "SLE",
                "start_year": 2015,
                "end_year": 2026,
                "org_unit_level": 2,
                "data_element": "DE_UID",
                "temporal_resolution": "yearly",
            }
        )
    )
    assert normalized.start == "2015"
    assert normalized.end == "2026"


def test_mapper_uses_month_format_for_chirps_date_window() -> None:
    normalized, _warnings = normalize_simple_request(
        WorkflowRequest.model_validate(
            {
                "dataset_id": "chirps3_precipitation_daily",
                "start_date": "2024-01-01",
                "end_date": "2024-05-31",
                "org_unit_level": 2,
                "data_element": "DE_UID",
            }
        )
    )
    assert normalized.start == "2024-01"
    assert normalized.end == "2024-05"


def test_default_workflow_definition_has_expected_steps() -> None:
    workflow = load_workflow_definition()
    assert workflow.workflow_id == "dhis2_datavalue_set_v1"
    assert workflow.version == 1
    assert [step.component for step in workflow.steps] == [
        "feature_source",
        "download_dataset",
        "temporal_aggregation",
        "spatial_aggregation",
        "build_datavalueset",
    ]


def test_engine_follows_declarative_workflow_order(monkeypatch: pytest.MonkeyPatch) -> None:
    request = WorkflowExecuteRequest.model_validate(
        {
            "dataset_id": "chirps3_precipitation_daily",
            "start": "2024-01-01",
            "end": "2024-01-31",
            "feature_source": {"source_type": "dhis2_level", "dhis2_level": 3},
            "temporal_aggregation": {"target_period_type": "monthly", "method": "sum"},
            "spatial_aggregation": {"method": "mean"},
            "dhis2": {"data_element_uid": "abc123def45"},
        }
    )
    ds = xr.Dataset(
        {"precip": (("time", "lat", "lon"), [[[1.0]]])},
        coords={"time": ["2024-01-01"], "lat": [0], "lon": [0]},
    )
    monkeypatch.setattr(
        engine,
        "load_workflow_definition",
        lambda workflow_id: WorkflowDefinition.model_validate(
            {
                "workflow_id": workflow_id,
                "version": 1,
                "steps": [
                    {"component": "feature_source"},
                    {"component": "download_dataset"},
                    {"component": "spatial_aggregation"},
                    {"component": "build_datavalueset"},
                ],
            }
        ),
    )
    monkeypatch.setattr(
        engine,
        "get_dataset",
        lambda dataset_id: {"id": "chirps3_precipitation_daily", "variable": "precip"},
    )
    monkeypatch.setattr(
        engine.component_services,
        "feature_source_component",
        lambda config: (
            {"type": "FeatureCollection", "features": [{"id": "OU_1", "properties": {"id": "OU_1"}}]},
            [0, 0, 1, 1],
        ),
    )
    monkeypatch.setattr(engine.component_services, "download_dataset_component", lambda **kwargs: None)
    monkeypatch.setattr(engine.component_services, "temporal_aggregation_component", lambda **kwargs: ds)
    monkeypatch.setattr(
        engine.component_services,
        "spatial_aggregation_component",
        lambda **kwargs: [{"org_unit": "OU_1", "time": "2024-01-01", "value": 10.0}],
    )
    monkeypatch.setattr(
        engine.component_services,
        "build_datavalueset_component",
        lambda **kwargs: ({"dataValues": [{"value": "10.0"}]}, "/tmp/data/out.json"),
    )
    monkeypatch.setattr(engine, "persist_run_log", lambda **kwargs: "/tmp/data/workflow_runs/run.json")

    response = engine.execute_workflow(request, include_component_run_details=True)
    assert response.workflow_id == "dhis2_datavalue_set_v1"
    assert response.workflow_version == 1
    assert [c.component for c in response.component_runs] == [
        "feature_source",
        "download_dataset",
        "spatial_aggregation",
        "build_datavalueset",
    ]


def test_engine_rejects_unknown_workflow_id(monkeypatch: pytest.MonkeyPatch) -> None:
    request = WorkflowExecuteRequest.model_validate(
        {
            "dataset_id": "chirps3_precipitation_daily",
            "start": "2024-01-01",
            "end": "2024-01-31",
            "feature_source": {"source_type": "dhis2_level", "dhis2_level": 3},
            "temporal_aggregation": {"target_period_type": "monthly", "method": "sum"},
            "spatial_aggregation": {"method": "mean"},
            "dhis2": {"data_element_uid": "abc123def45"},
        }
    )
    monkeypatch.setattr(
        engine,
        "get_dataset",
        lambda dataset_id: {"id": "chirps3_precipitation_daily", "variable": "precip"},
    )

    with pytest.raises(HTTPException) as exc_info:
        engine.execute_workflow(request, workflow_id="not_allowlisted")

    assert exc_info.value.status_code == 422


def test_engine_resolves_step_config_from_request_params(monkeypatch: pytest.MonkeyPatch) -> None:
    request = WorkflowExecuteRequest.model_validate(
        {
            "dataset_id": "chirps3_precipitation_daily",
            "start": "2024-01-01",
            "end": "2024-01-31",
            "feature_source": {"source_type": "dhis2_level", "dhis2_level": 3},
            "temporal_aggregation": {"target_period_type": "monthly", "method": "sum"},
            "spatial_aggregation": {"method": "mean"},
            "dhis2": {"data_element_uid": "abc123def45"},
        }
    )
    ds = xr.Dataset(
        {"precip": (("time", "lat", "lon"), [[[1.0]]])},
        coords={"time": ["2024-01-01"], "lat": [0], "lon": [0]},
    )

    monkeypatch.setattr(
        engine,
        "load_workflow_definition",
        lambda workflow_id: WorkflowDefinition.model_validate(
            {
                "workflow_id": workflow_id,
                "version": 2,
                "steps": [
                    {"component": "feature_source"},
                    {
                        "component": "download_dataset",
                        "config": {"execution_mode": "$request.download_execution_mode"},
                    },
                    {
                        "component": "temporal_aggregation",
                        "config": {},
                    },
                    {"component": "spatial_aggregation"},
                    {"component": "build_datavalueset"},
                ],
            }
        ),
    )
    monkeypatch.setattr(
        engine,
        "get_dataset",
        lambda dataset_id: {"id": "chirps3_precipitation_daily", "variable": "precip"},
    )
    monkeypatch.setattr(
        engine.component_services,
        "feature_source_component",
        lambda config: (
            {"type": "FeatureCollection", "features": [{"id": "OU_1", "properties": {"id": "OU_1"}}]},
            [0, 0, 1, 1],
        ),
    )
    monkeypatch.setattr(engine.component_services, "download_dataset_component", lambda **kwargs: None)

    def _temporal_component(**kwargs: Any) -> xr.Dataset:
        assert kwargs["method"].value == "sum"
        assert kwargs["target_period_type"].value == "monthly"
        return ds

    monkeypatch.setattr(engine.component_services, "temporal_aggregation_component", _temporal_component)
    monkeypatch.setattr(
        engine.component_services,
        "spatial_aggregation_component",
        lambda **kwargs: [{"org_unit": "OU_1", "time": "2024-01-01", "value": 10.0}],
    )
    monkeypatch.setattr(
        engine.component_services,
        "build_datavalueset_component",
        lambda **kwargs: ({"dataValues": [{"value": "10.0"}]}, "/tmp/data/out.json"),
    )
    monkeypatch.setattr(engine, "persist_run_log", lambda **kwargs: "/tmp/data/workflow_runs/run.json")

    response = engine.execute_workflow(
        request,
        request_params={"download_execution_mode": "local"},
    )
    assert response.status == "completed"


def test_engine_rejects_invalid_step_config(monkeypatch: pytest.MonkeyPatch) -> None:
    request = WorkflowExecuteRequest.model_validate(
        {
            "dataset_id": "chirps3_precipitation_daily",
            "start": "2024-01-01",
            "end": "2024-01-31",
            "feature_source": {"source_type": "dhis2_level", "dhis2_level": 3},
            "temporal_aggregation": {"target_period_type": "monthly", "method": "sum"},
            "spatial_aggregation": {"method": "mean"},
            "dhis2": {"data_element_uid": "abc123def45"},
        }
    )
    monkeypatch.setattr(
        engine,
        "load_workflow_definition",
        lambda workflow_id: WorkflowDefinition.model_validate(
            {
                "workflow_id": workflow_id,
                "version": 2,
                "steps": [
                    {"component": "feature_source"},
                    {"component": "download_dataset"},
                    {"component": "temporal_aggregation", "config": {"invalid_key": 1}},
                    {"component": "spatial_aggregation"},
                    {"component": "build_datavalueset"},
                ],
            }
        ),
    )
    monkeypatch.setattr(
        engine,
        "get_dataset",
        lambda dataset_id: {"id": "chirps3_precipitation_daily", "variable": "precip"},
    )
    persisted: dict[str, Any] = {}

    def _persist_run_log(**kwargs: Any) -> str:
        persisted.update(kwargs)
        return "/tmp/data/workflow_runs/run.json"

    monkeypatch.setattr(engine, "persist_run_log", _persist_run_log)
    monkeypatch.setattr(
        engine.component_services,
        "feature_source_component",
        lambda config: (
            {"type": "FeatureCollection", "features": [{"id": "OU_1", "properties": {"id": "OU_1"}}]},
            [0, 0, 1, 1],
        ),
    )
    monkeypatch.setattr(engine.component_services, "download_dataset_component", lambda **kwargs: None)

    with pytest.raises(HTTPException) as exc_info:
        engine.execute_workflow(request)

    assert exc_info.value.status_code == 422
    detail = cast(dict[str, Any], exc_info.value.detail)
    assert detail["error"] == "workflow_execution_failed"
    assert detail["error_code"] == "CONFIG_VALIDATION_FAILED"
    assert detail["failed_component"] == "temporal_aggregation"
    assert detail["failed_component_version"] == "v1"
    assert persisted["error_code"] == "CONFIG_VALIDATION_FAILED"
    assert persisted["failed_component"] == "temporal_aggregation"
    assert persisted["failed_component_version"] == "v1"


def test_engine_download_dataset_remote_mode_uses_remote_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    request = WorkflowExecuteRequest.model_validate(
        {
            "dataset_id": "chirps3_precipitation_daily",
            "start": "2024-01-01",
            "end": "2024-01-31",
            "feature_source": {"source_type": "dhis2_level", "dhis2_level": 3},
            "temporal_aggregation": {"target_period_type": "monthly", "method": "sum"},
            "spatial_aggregation": {"method": "mean"},
            "dhis2": {"data_element_uid": "abc123def45"},
        }
    )
    monkeypatch.setattr(
        engine,
        "load_workflow_definition",
        lambda workflow_id: WorkflowDefinition.model_validate(
            {
                "workflow_id": workflow_id,
                "version": 1,
                "steps": [
                    {"component": "feature_source"},
                    {
                        "component": "download_dataset",
                        "config": {
                            "execution_mode": "remote",
                            "remote_url": "http://component-host/components/download-dataset",
                            "remote_retries": 2,
                            "remote_timeout_sec": 9,
                        },
                    },
                    {"component": "spatial_aggregation"},
                    {"component": "build_datavalueset"},
                ],
            }
        ),
    )
    monkeypatch.setattr(
        engine,
        "get_dataset",
        lambda dataset_id: {"id": "chirps3_precipitation_daily", "variable": "precip"},
    )
    monkeypatch.setattr(
        engine.component_services,
        "feature_source_component",
        lambda config: (
            {"type": "FeatureCollection", "features": [{"id": "OU_1", "properties": {"id": "OU_1"}}]},
            [0, 0, 1, 1],
        ),
    )
    remote_called: dict[str, Any] = {}

    def _remote_adapter(**kwargs: Any) -> None:
        remote_called.update(kwargs)

    monkeypatch.setattr(engine, "_invoke_remote_download_component", _remote_adapter)
    monkeypatch.setattr(
        engine.component_services,
        "spatial_aggregation_component",
        lambda **kwargs: [{"org_unit": "OU_1", "time": "2024-01-01", "value": 10.0}],
    )
    monkeypatch.setattr(
        engine.component_services,
        "build_datavalueset_component",
        lambda **kwargs: ({"dataValues": [{"value": "10.0"}]}, "/tmp/data/out.json"),
    )
    monkeypatch.setattr(engine, "persist_run_log", lambda **kwargs: "/tmp/data/workflow_runs/run.json")

    response = engine.execute_workflow(request)
    assert response.status == "completed"
    assert remote_called["remote_url"] == "http://component-host/components/download-dataset"
    assert remote_called["dataset_id"] == "chirps3_precipitation_daily"


def test_engine_rejects_remote_download_without_remote_url(monkeypatch: pytest.MonkeyPatch) -> None:
    request = WorkflowExecuteRequest.model_validate(
        {
            "dataset_id": "chirps3_precipitation_daily",
            "start": "2024-01-01",
            "end": "2024-01-31",
            "feature_source": {"source_type": "dhis2_level", "dhis2_level": 3},
            "temporal_aggregation": {"target_period_type": "monthly", "method": "sum"},
            "spatial_aggregation": {"method": "mean"},
            "dhis2": {"data_element_uid": "abc123def45"},
        }
    )
    monkeypatch.setattr(
        engine,
        "load_workflow_definition",
        lambda workflow_id: WorkflowDefinition.model_validate(
            {
                "workflow_id": workflow_id,
                "version": 1,
                "steps": [
                    {"component": "feature_source"},
                    {"component": "download_dataset", "config": {"execution_mode": "remote"}},
                    {"component": "spatial_aggregation"},
                    {"component": "build_datavalueset"},
                ],
            }
        ),
    )
    monkeypatch.setattr(
        engine,
        "get_dataset",
        lambda dataset_id: {"id": "chirps3_precipitation_daily", "variable": "precip"},
    )
    monkeypatch.setattr(
        engine.component_services,
        "feature_source_component",
        lambda config: (
            {"type": "FeatureCollection", "features": [{"id": "OU_1", "properties": {"id": "OU_1"}}]},
            [0, 0, 1, 1],
        ),
    )
    monkeypatch.setattr(engine, "persist_run_log", lambda **kwargs: "/tmp/data/workflow_runs/run.json")

    with pytest.raises(HTTPException) as exc_info:
        engine.execute_workflow(request)

    assert exc_info.value.status_code == 422
    detail = cast(dict[str, Any], exc_info.value.detail)
    assert detail["error_code"] == "CONFIG_VALIDATION_FAILED"
    assert detail["failed_component"] == "download_dataset"


def test_engine_rejects_remote_fields_in_local_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    request = WorkflowExecuteRequest.model_validate(
        {
            "dataset_id": "chirps3_precipitation_daily",
            "start": "2024-01-01",
            "end": "2024-01-31",
            "feature_source": {"source_type": "dhis2_level", "dhis2_level": 3},
            "temporal_aggregation": {"target_period_type": "monthly", "method": "sum"},
            "spatial_aggregation": {"method": "mean"},
            "dhis2": {"data_element_uid": "abc123def45"},
        }
    )
    monkeypatch.setattr(
        engine,
        "load_workflow_definition",
        lambda workflow_id: WorkflowDefinition.model_validate(
            {
                "workflow_id": workflow_id,
                "version": 1,
                "steps": [
                    {"component": "feature_source"},
                    {
                        "component": "download_dataset",
                        "config": {
                            "execution_mode": "local",
                            "remote_url": "http://should-not-be-here/components/download-dataset",
                        },
                    },
                    {"component": "spatial_aggregation"},
                    {"component": "build_datavalueset"},
                ],
            }
        ),
    )
    monkeypatch.setattr(
        engine,
        "get_dataset",
        lambda dataset_id: {"id": "chirps3_precipitation_daily", "variable": "precip"},
    )
    monkeypatch.setattr(
        engine.component_services,
        "feature_source_component",
        lambda config: (
            {"type": "FeatureCollection", "features": [{"id": "OU_1", "properties": {"id": "OU_1"}}]},
            [0, 0, 1, 1],
        ),
    )
    monkeypatch.setattr(engine, "persist_run_log", lambda **kwargs: "/tmp/data/workflow_runs/run.json")

    with pytest.raises(HTTPException) as exc_info:
        engine.execute_workflow(request)

    assert exc_info.value.status_code == 422
    detail = cast(dict[str, Any], exc_info.value.detail)
    assert detail["error_code"] == "CONFIG_VALIDATION_FAILED"
    assert detail["failed_component"] == "download_dataset"


def test_engine_supports_remote_mode_for_all_components(monkeypatch: pytest.MonkeyPatch) -> None:
    request = WorkflowExecuteRequest.model_validate(
        {
            "dataset_id": "chirps3_precipitation_daily",
            "start": "2024-01-01",
            "end": "2024-01-31",
            "feature_source": {"source_type": "dhis2_level", "dhis2_level": 3},
            "temporal_aggregation": {"target_period_type": "monthly", "method": "sum"},
            "spatial_aggregation": {"method": "mean"},
            "dhis2": {"data_element_uid": "abc123def45"},
        }
    )
    monkeypatch.setattr(
        engine,
        "load_workflow_definition",
        lambda workflow_id: WorkflowDefinition.model_validate(
            {
                "workflow_id": workflow_id,
                "version": 1,
                "steps": [
                    {
                        "component": "feature_source",
                        "config": {"execution_mode": "remote", "remote_url": "http://x/components/feature-source"},
                    },
                    {
                        "component": "download_dataset",
                        "config": {
                            "execution_mode": "remote",
                            "remote_url": "http://x/components/download-dataset",
                        },
                    },
                    {
                        "component": "temporal_aggregation",
                        "config": {
                            "execution_mode": "remote",
                            "remote_url": "http://x/components/temporal-aggregation",
                        },
                    },
                    {
                        "component": "spatial_aggregation",
                        "config": {
                            "execution_mode": "remote",
                            "remote_url": "http://x/components/spatial-aggregation",
                        },
                    },
                    {
                        "component": "build_datavalueset",
                        "config": {
                            "execution_mode": "remote",
                            "remote_url": "http://x/components/build-datavalue-set",
                        },
                    },
                ],
            }
        ),
    )
    monkeypatch.setattr(
        engine,
        "get_dataset",
        lambda dataset_id: {"id": "chirps3_precipitation_daily", "variable": "precip"},
    )

    called: dict[str, bool] = {
        "feature": False,
        "download": False,
        "temporal": False,
        "spatial": False,
        "build": False,
    }

    monkeypatch.setattr(
        engine,
        "_invoke_remote_feature_source_component",
        lambda **kwargs: (
            called.__setitem__("feature", True),
            {"type": "FeatureCollection", "features": [{"id": "OU_1", "properties": {"id": "OU_1"}}]},
            [0, 0, 1, 1],
        )[1:],
    )
    monkeypatch.setattr(
        engine,
        "_invoke_remote_download_component",
        lambda **kwargs: called.__setitem__("download", True),
    )
    monkeypatch.setattr(
        engine,
        "_invoke_remote_temporal_aggregation_component",
        lambda **kwargs: (called.__setitem__("temporal", True), {"sizes": {"time": 1}, "dims": ["time"]})[1],
    )
    monkeypatch.setattr(
        engine,
        "_invoke_remote_spatial_aggregation_component",
        lambda **kwargs: (
            called.__setitem__("spatial", True),
            [{"org_unit": "OU_1", "time": "2024-01-01", "value": 10.0}],
        )[1],
    )
    monkeypatch.setattr(
        engine,
        "_invoke_remote_build_datavalueset_component",
        lambda **kwargs: (
            called.__setitem__("build", True),
            ({"dataValues": [{"value": "10.0"}]}, "/tmp/data/out.json"),
        )[1],
    )
    monkeypatch.setattr(engine, "persist_run_log", lambda **kwargs: "/tmp/data/workflow_runs/run.json")

    response = engine.execute_workflow(request)
    assert response.status == "completed"
    assert all(called.values())
