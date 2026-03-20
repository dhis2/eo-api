from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest
import xarray as xr
from fastapi import HTTPException
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from eo_api.components import services as component_services
from eo_api.main import app
from eo_api.publications import pygeoapi as publication_pygeoapi
from eo_api.publications import services as publication_services
from eo_api.workflows.schemas import (
    AggregationMethod,
    PeriodType,
    WorkflowExecuteRequest,
    WorkflowExecuteResponse,
    WorkflowRequest,
)
from eo_api.workflows.services import engine, job_store, run_logs
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


def _standard_workflow_outputs(
    *,
    feature_step: str = "feature_source",
    spatial_step: str = "spatial_aggregation",
    build_step: str = "build_datavalueset",
) -> dict[str, dict[str, str]]:
    return {
        "bbox": {"from_step": feature_step, "output": "bbox"},
        "features": {"from_step": feature_step, "output": "features"},
        "records": {"from_step": spatial_step, "output": "records"},
        "data_value_set": {"from_step": build_step, "output": "data_value_set"},
        "output_file": {"from_step": build_step, "output": "output_file"},
    }


def _standard_publication_inputs(
    *,
    feature_step: str = "feature_source",
    spatial_step: str = "spatial_aggregation",
    build_step: str = "build_datavalueset",
) -> dict[str, dict[str, str]]:
    return {
        "features": {"from_step": feature_step, "output": "features"},
        "records": {"from_step": spatial_step, "output": "records"},
        "output_file": {"from_step": build_step, "output": "output_file"},
    }


def _patch_successful_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    ds = xr.Dataset(
        {"precip": (("time", "lat", "lon"), [[[1.0]]])},
        coords={"time": ["2024-01-01"], "lat": [0], "lon": [0]},
    )
    monkeypatch.setattr(
        engine,
        "get_dataset",
        lambda dataset_id: {"id": dataset_id, "variable": "precip"},
    )
    monkeypatch.setattr(
        engine.component_services,
        "feature_source_component",
        lambda config: (
            {"type": "FeatureCollection", "features": [{"id": "OU_1", "properties": {"id": "OU_1"}}]},
            [0.0, 0.0, 1.0, 1.0],
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


def _patch_successful_execution_multi_period(monkeypatch: pytest.MonkeyPatch) -> None:
    ds = xr.Dataset(
        {"precip": (("time", "lat", "lon"), [[[1.0]], [[2.0]]])},
        coords={"time": ["2024-01-01", "2024-02-01"], "lat": [0], "lon": [0]},
    )
    monkeypatch.setattr(
        engine,
        "get_dataset",
        lambda dataset_id: {"id": dataset_id, "variable": "precip"},
    )
    monkeypatch.setattr(
        engine.component_services,
        "feature_source_component",
        lambda config: (
            {"type": "FeatureCollection", "features": [{"id": "OU_1", "properties": {"id": "OU_1"}}]},
            [0.0, 0.0, 1.0, 1.0],
        ),
    )
    monkeypatch.setattr(engine.component_services, "download_dataset_component", lambda **kwargs: None)
    monkeypatch.setattr(engine.component_services, "temporal_aggregation_component", lambda **kwargs: ds)
    monkeypatch.setattr(
        engine.component_services,
        "spatial_aggregation_component",
        lambda **kwargs: [
            {"org_unit": "OU_1", "time": "2024-01-01", "value": 10.0},
            {"org_unit": "OU_1", "time": "2024-02-01", "value": 12.0},
        ],
    )
    monkeypatch.setattr(
        engine.component_services,
        "build_datavalueset_component",
        lambda **kwargs: ({"dataValues": [{"value": "10.0"}, {"value": "12.0"}]}, "/tmp/data/out.json"),
    )


def test_workflow_endpoint_exists_once() -> None:
    workflow_routes = {
        route.path
        for route in app.routes
        if isinstance(route, APIRoute) and route.path.startswith("/workflows") and "POST" in route.methods
    }
    assert workflow_routes == {
        "/workflows/dhis2-datavalue-set",
        "/workflows/execute",
        "/workflows/jobs/cleanup",
        "/workflows/schedules",
        "/workflows/schedules/{schedule_id}/trigger",
        "/workflows/validate",
    }


def test_ogc_process_routes_exist() -> None:
    ogc_routes = {
        route.path for route in app.routes if isinstance(route, APIRoute) and route.path.startswith("/ogcapi")
    }
    assert "/ogcapi" in ogc_routes
    assert "/ogcapi/processes" in ogc_routes
    assert "/ogcapi/processes/{process_id}" in ogc_routes
    assert "/ogcapi/processes/{process_id}/execution" in ogc_routes
    assert "/ogcapi/jobs" in ogc_routes
    assert "/ogcapi/jobs/{job_id}" in ogc_routes
    assert "/ogcapi/jobs/{job_id}/results" in ogc_routes


def test_publication_generated_pygeoapi_routes_exist() -> None:
    publication_routes = {
        route.path
        for route in app.routes
        if isinstance(route, APIRoute) and route.path.startswith("/publications/pygeoapi")
    }
    assert "/publications/pygeoapi/config" in publication_routes
    assert "/publications/pygeoapi/openapi" in publication_routes
    assert "/publications/pygeoapi/materialize" in publication_routes


def test_analytics_viewer_routes_exist() -> None:
    analytics_routes = {
        route.path for route in app.routes if isinstance(route, APIRoute) and route.path.startswith("/analytics")
    }
    assert "/analytics/publications/{resource_id}" in analytics_routes
    assert "/analytics/publications/{resource_id}/viewer" in analytics_routes


def test_pygeoapi_runtime_env_points_to_generated_documents() -> None:
    config_path = os.environ.get("PYGEOAPI_CONFIG")
    openapi_path = os.environ.get("PYGEOAPI_OPENAPI")
    assert config_path is not None
    assert openapi_path is not None
    assert config_path.endswith("pygeoapi-config.generated.yml")
    assert openapi_path.endswith("pygeoapi-openapi.generated.yml")
    assert Path(config_path).exists()
    assert Path(openapi_path).exists()


def test_pygeoapi_mount_serves_landing_page(client: TestClient) -> None:
    response = client.get("/ogcapi?f=json")
    assert response.status_code == 200
    body = response.json()
    assert body["title"] == "DHIS2 EO API"
    rels = {link["rel"] for link in body["links"]}
    assert {"self", "alternate", "data", "processes", "jobs"} <= rels


def test_publication_endpoint_missing_uses_typed_error_envelope(client: TestClient) -> None:
    response = client.get("/publications/does-not-exist")
    assert response.status_code == 404
    body = response.json()["detail"]
    assert body["error"] == "published_resource_not_found"
    assert body["error_code"] == "PUBLISHED_RESOURCE_NOT_FOUND"
    assert body["resource_id"] == "does-not-exist"


def test_analytics_endpoint_missing_uses_typed_error_envelope(client: TestClient) -> None:
    response = client.get("/analytics/publications/does-not-exist")
    assert response.status_code == 404
    body = response.json()["detail"]
    assert body["error"] == "published_resource_not_found"
    assert body["error_code"] == "PUBLISHED_RESOURCE_NOT_FOUND"
    assert body["resource_id"] == "does-not-exist"


def test_mapper_validation_uses_typed_error_envelope() -> None:
    payload = WorkflowRequest.model_construct(  # type: ignore[call-arg]
        workflow_id="dhis2_datavalue_set_v1",
        dataset_id="chirps3_precipitation_daily",
        org_unit_level=3,
        data_element="DE_UID",
        temporal_resolution=PeriodType.MONTHLY,
        temporal_reducer=AggregationMethod.SUM,
        spatial_reducer=AggregationMethod.MEAN,
        overwrite=False,
        dry_run=True,
        feature_id_property="id",
        include_component_run_details=False,
    )

    with pytest.raises(HTTPException) as exc_info:
        normalize_simple_request(payload)

    assert exc_info.value.status_code == 422
    detail = cast(dict[str, Any], exc_info.value.detail)
    assert detail["error"] == "workflow_request_invalid"
    assert detail["error_code"] == "REQUEST_VALIDATION_FAILED"


def test_workflow_catalog_endpoint_returns_allowlisted_workflow(client: TestClient) -> None:
    response = client.get("/workflows")
    assert response.status_code == 200
    body = response.json()
    assert "workflows" in body
    assert len(body["workflows"]) >= 2
    by_id = {item["workflow_id"]: item for item in body["workflows"]}

    default = by_id["dhis2_datavalue_set_v1"]
    assert default["version"] == 1
    assert default["publication_publishable"] is True
    assert default["publication_intent"] == "feature_collection"
    assert default["publication_exposure"] == "ogc"
    assert default["publication_asset_format"] is None
    assert default["publication_asset_binding"] is None
    assert default["publication_inputs"]["features"]["from_step"] == "get_features"
    assert default["serving_supported"] is True
    assert default["serving_asset_format"] == "geojson"
    assert default["serving_targets"] == ["pygeoapi", "analytics"]
    assert default["serving_error"] is None
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
    assert fast["publication_publishable"] is False
    assert fast["publication_intent"] is None
    assert fast["publication_exposure"] is None
    assert fast["publication_asset_format"] is None
    assert fast["publication_asset_binding"] is None
    assert fast["publication_inputs"] == {}
    assert fast["serving_supported"] is True
    assert fast["serving_asset_format"] == "geojson"
    assert fast["serving_targets"] == ["registry"]
    assert fast["serving_error"] is None
    assert fast["step_count"] == 4
    assert fast["components"] == [
        "feature_source",
        "download_dataset",
        "spatial_aggregation",
        "build_datavalueset",
    ]


def test_workflow_definition_allows_non_datavalueset_terminal_step_when_outputs_declared() -> None:
    definition = WorkflowDefinition.model_validate(
        {
            "workflow_id": "generic_records_v1",
            "version": 1,
            "steps": [
                {"id": "get_features", "component": "feature_source", "version": "v1"},
                {
                    "id": "spatial_agg",
                    "component": "spatial_aggregation",
                    "version": "v1",
                    "inputs": {
                        "bbox": {"from_step": "get_features", "output": "bbox"},
                        "features": {"from_step": "get_features", "output": "features"},
                    },
                },
            ],
            "outputs": {
                "features": {"from_step": "get_features", "output": "features"},
                "records": {"from_step": "spatial_agg", "output": "records"},
            },
        }
    )

    assert [step.component for step in definition.steps] == ["feature_source", "spatial_aggregation"]
    assert set(definition.outputs) == {"features", "records"}


def test_workflow_definition_requires_explicit_outputs() -> None:
    with pytest.raises(ValueError, match="declare at least one exported output"):
        WorkflowDefinition.model_validate(
            {
                "workflow_id": "missing_outputs_v1",
                "version": 1,
                "steps": [
                    {"component": "feature_source", "version": "v1"},
                    {"component": "download_dataset", "version": "v1"},
                    {"component": "temporal_aggregation", "version": "v1"},
                    {"component": "spatial_aggregation", "version": "v1"},
                    {"component": "build_datavalueset", "version": "v1"},
                ],
            }
        )


def test_publishable_workflow_can_declare_publication_asset_without_builder_inputs() -> None:
    definition = WorkflowDefinition.model_validate(
        {
            "workflow_id": "coverage_publish_v1",
            "version": 1,
            "publication": {
                "publishable": True,
                "intent": "coverage",
                "asset": {"from_step": "build", "output": "output_file"},
                "asset_format": "zarr",
            },
            "steps": [
                {"id": "feature_source", "component": "feature_source", "version": "v1"},
                {"id": "download_dataset", "component": "download_dataset", "version": "v1"},
                {
                    "id": "spatial_aggregation",
                    "component": "spatial_aggregation",
                    "version": "v1",
                    "inputs": {
                        "bbox": {"from_step": "feature_source", "output": "bbox"},
                        "features": {"from_step": "feature_source", "output": "features"},
                    },
                },
                {
                    "id": "build",
                    "component": "build_datavalueset",
                    "version": "v1",
                    "inputs": {"records": {"from_step": "spatial_aggregation", "output": "records"}},
                },
            ],
            "outputs": _standard_workflow_outputs(
                feature_step="feature_source",
                spatial_step="spatial_aggregation",
                build_step="build",
            ),
        }
    )

    assert definition.publication.asset is not None
    assert definition.publication.asset.from_step == "build"


def test_publishable_workflow_rejects_unsupported_serving_contract() -> None:
    with pytest.raises(ValueError, match="Unsupported publication serving contract"):
        WorkflowDefinition.model_validate(
            {
                "workflow_id": "tileset_publish_v1",
                "version": 1,
                "publication": {
                    "publishable": True,
                    "intent": "tileset",
                    "exposure": "ogc",
                    "asset": {"from_step": "build", "output": "output_file"},
                    "asset_format": "tiles",
                },
                "steps": [
                    {"id": "feature_source", "component": "feature_source", "version": "v1"},
                    {"id": "download_dataset", "component": "download_dataset", "version": "v1"},
                    {
                        "id": "spatial_aggregation",
                        "component": "spatial_aggregation",
                        "version": "v1",
                        "inputs": {
                            "bbox": {"from_step": "feature_source", "output": "bbox"},
                            "features": {"from_step": "feature_source", "output": "features"},
                        },
                    },
                    {
                        "id": "build",
                        "component": "build_datavalueset",
                        "version": "v1",
                        "inputs": {"records": {"from_step": "spatial_aggregation", "output": "records"}},
                    },
                ],
                "outputs": _standard_workflow_outputs(
                    feature_step="feature_source",
                    spatial_step="spatial_aggregation",
                    build_step="build",
                ),
            }
        )


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
        workflow_definition_source: str = "catalog",
    ) -> WorkflowExecuteResponse:
        del payload, workflow_id, request_params, include_component_run_details, workflow_definition_source
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


def test_workflow_job_result_missing_uses_typed_error_envelope(client: TestClient) -> None:
    response = client.get("/workflows/jobs/does-not-exist/result")
    assert response.status_code == 404
    body = response.json()["detail"]
    assert body["error"] == "job_not_found"
    assert body["error_code"] == "JOB_NOT_FOUND"
    assert body["job_id"] == "does-not-exist"


def test_pygeoapi_collection_missing_returns_not_found(client: TestClient) -> None:
    response = client.get("/pygeoapi/collections/does-not-exist", params={"f": "json"})
    assert response.status_code == 404


def test_ogc_job_results_unavailable_uses_typed_error_envelope(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("eo_api.ogc.routes.execute_workflow", lambda *args, **kwargs: None)

    response = client.post(
        "/ogcapi/processes/generic-dhis2-workflow/execution",
        headers={"Prefer": "respond-async"},
        json=_valid_public_payload(),
    )
    assert response.status_code == 202
    job_id = response.json()["jobID"]

    result_response = client.get(f"/ogcapi/jobs/{job_id}/results")
    assert result_response.status_code == 409
    body = result_response.json()["detail"]
    assert body["error"] == "job_result_unavailable"
    assert body["error_code"] == "JOB_RESULT_UNAVAILABLE"
    assert body["job_id"] == job_id


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
        workflow_definition_source: str = "catalog",
    ) -> WorkflowExecuteResponse:
        del payload, workflow_id, request_params, include_component_run_details, workflow_definition_source
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
        workflow_definition_source: str = "inline",
    ) -> WorkflowExecuteResponse:
        del payload, request_params, include_component_run_details
        assert workflow_id == "adhoc_dhis2_v1"
        assert workflow_definition is not None
        assert workflow_definition_source == "inline"
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
                "outputs": _standard_workflow_outputs(),
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
                "outputs": {
                    "data_value_set": {"from_step": "build_datavalueset", "output": "data_value_set"},
                    "output_file": {"from_step": "build_datavalueset", "output": "output_file"},
                },
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
                "outputs": _standard_workflow_outputs(),
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
    assert body["publication_publishable"] is False
    assert body["publication_intent"] is None
    assert body["publication_inputs"] == {}
    assert body["serving_supported"] is True
    assert body["serving_asset_format"] == "geojson"
    assert body["serving_targets"] == ["registry"]
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
                "outputs": _standard_workflow_outputs(),
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
    assert body["publication_publishable"] is False
    assert body["serving_supported"] is True
    assert body["resolved_steps"] == []
    assert len(body["errors"]) == 1
    assert "validation failed" in body["errors"][0].lower()


def test_workflow_validate_endpoint_unknown_workflow_id(client: TestClient) -> None:
    response = client.post("/workflows/validate", json={"workflow_id": "does_not_exist"})
    assert response.status_code == 200
    body = response.json()
    assert body["valid"] is False
    assert body["step_count"] == 0
    assert body["publication_publishable"] is False
    assert len(body["errors"]) == 1
    assert "Unknown workflow_id" in body["errors"][0]


def test_workflow_job_endpoints_return_persisted_result(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(run_logs, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(job_store, "DOWNLOAD_DIR", tmp_path)
    _patch_successful_execution(monkeypatch)

    response = client.post("/workflows/dhis2-datavalue-set", json=_valid_public_payload())
    assert response.status_code == 200
    run_id = response.json()["run_id"]

    job_response = client.get(f"/workflows/jobs/{run_id}")
    assert job_response.status_code == 200
    job_body = job_response.json()
    assert job_body["job_id"] == run_id
    assert job_body["status"] == "successful"
    assert job_body["process_id"] == "generic-dhis2-workflow"
    assert job_body["request"]["dataset_id"] == "chirps3_precipitation_daily"
    assert job_body["request"]["start_date"] == "2024-01-01"
    assert job_body["request"]["end_date"] == "2024-01-31"
    assert job_body["orchestration"]["definition_source"] == "catalog"
    assert job_body["orchestration"]["step_count"] == 5
    assert job_body["orchestration"]["components"] == [
        "feature_source",
        "download_dataset",
        "temporal_aggregation",
        "spatial_aggregation",
        "build_datavalueset",
    ]
    assert job_body["orchestration"]["steps"][0]["component"] == "feature_source"
    assert job_body["orchestration"]["steps"][0]["id"] == "get_features"
    assert job_body["orchestration"]["steps"][0]["version"] == "v1"
    assert job_body["orchestration"]["steps"][1]["inputs"]["bbox"] == {
        "from_step": "get_features",
        "output": "bbox",
    }
    links = {item["rel"]: item["href"] for item in job_body["links"]}
    assert links["self"].endswith(f"/workflows/jobs/{run_id}")
    assert links["result"].endswith(f"/workflows/jobs/{run_id}/result")
    assert links["trace"].endswith(f"/workflows/jobs/{run_id}/trace")
    assert links["collection"].endswith(f"/pygeoapi/collections/workflow-output-{run_id}")
    assert "analytics" not in links
    assert "result" not in job_body

    results_response = client.get(f"/workflows/jobs/{run_id}/result")
    assert results_response.status_code == 200
    assert results_response.json()["run_id"] == run_id

    trace_response = client.get(f"/workflows/jobs/{run_id}/trace")
    assert trace_response.status_code == 200
    trace_body = trace_response.json()
    assert trace_body["run_id"] == run_id
    assert trace_body["status"] == "completed"
    assert [item["component"] for item in trace_body["component_runs"]] == [
        "feature_source",
        "download_dataset",
        "temporal_aggregation",
        "spatial_aggregation",
        "build_datavalueset",
    ]


def test_delete_workflow_job_cascades_derived_artifacts(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_successful_execution(monkeypatch)
    output_path = job_store.DOWNLOAD_DIR / "cascade-test-datavalue-set.json"
    output_path.write_text('{"dataValues": [{"value": "10.0"}]}', encoding="utf-8")
    monkeypatch.setattr(
        engine.component_services,
        "build_datavalueset_component",
        lambda **kwargs: ({"dataValues": [{"value": "10.0"}]}, str(output_path)),
    )

    response = client.post("/workflows/dhis2-datavalue-set", json=_valid_public_payload())
    assert response.status_code == 200
    run_id = response.json()["run_id"]
    output_file = Path(response.json()["output_file"])
    run_log_file = Path(response.json()["run_log_file"])

    publications_response = client.get("/publications", params={"workflow_id": "dhis2_datavalue_set_v1"})
    assert publications_response.status_code == 200
    derived = next(
        item for item in publications_response.json()["resources"] if item["resource_id"] == f"workflow-output-{run_id}"
    )
    publication_file = publication_services.DOWNLOAD_DIR / "published_resources" / f"workflow-output-{run_id}.json"
    publication_asset = Path(derived["path"])
    job_file = job_store.DOWNLOAD_DIR / "workflow_jobs" / f"{run_id}.json"

    assert job_file.exists()
    assert run_log_file.exists()
    assert output_file.exists()
    assert publication_file.exists()
    assert publication_asset.exists()

    delete_response = client.delete(f"/workflows/jobs/{run_id}")
    assert delete_response.status_code == 200
    delete_body = delete_response.json()
    assert delete_body["job_id"] == run_id
    assert delete_body["deleted"] is True
    assert delete_body["deleted_publication"] == f"workflow-output-{run_id}"
    assert delete_body["pygeoapi_runtime_reload_required"] is False

    assert not job_file.exists()
    assert not run_log_file.exists()
    assert not output_file.exists()
    assert not publication_file.exists()
    assert not publication_asset.exists()

    job_response = client.get(f"/workflows/jobs/{run_id}")
    assert job_response.status_code == 404

    publication_response = client.get(f"/publications/workflow-output-{run_id}")
    assert publication_response.status_code == 404


def test_cleanup_workflow_jobs_dry_run_lists_terminal_candidates_without_deleting(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_successful_execution(monkeypatch)

    first = client.post("/workflows/dhis2-datavalue-set", json=_valid_public_payload())
    second = client.post("/workflows/dhis2-datavalue-set", json=_valid_public_payload())
    assert first.status_code == 200
    assert second.status_code == 200
    first_job_id = first.json()["run_id"]
    second_job_id = second.json()["run_id"]

    cleanup_response = client.post("/workflows/jobs/cleanup", params={"dry_run": "true", "keep_latest": 1})
    assert cleanup_response.status_code == 200
    body = cleanup_response.json()
    assert body["dry_run"] is True
    assert body["candidate_count"] == 1
    assert body["deleted_count"] == 0
    assert body["candidates"][0]["job_id"] == first_job_id
    assert body["deleted_job_ids"] == []

    assert client.get(f"/workflows/jobs/{first_job_id}").status_code == 200
    assert client.get(f"/workflows/jobs/{second_job_id}").status_code == 200


def test_cleanup_workflow_jobs_applies_retention_and_cascades_deletion(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_successful_execution(monkeypatch)

    first = client.post("/workflows/dhis2-datavalue-set", json=_valid_public_payload())
    second = client.post("/workflows/dhis2-datavalue-set", json=_valid_public_payload())
    assert first.status_code == 200
    assert second.status_code == 200
    first_job_id = first.json()["run_id"]
    second_job_id = second.json()["run_id"]

    apply_response = client.post("/workflows/jobs/cleanup", params={"dry_run": "false", "keep_latest": 1})
    assert apply_response.status_code == 200
    body = apply_response.json()
    assert body["dry_run"] is False
    assert body["deleted_count"] == 1
    assert body["deleted_job_ids"] == [first_job_id]

    assert client.get(f"/workflows/jobs/{first_job_id}").status_code == 404
    assert client.get(f"/publications/workflow-output-{first_job_id}").status_code == 404
    assert client.get(f"/workflows/jobs/{second_job_id}").status_code == 200


def test_ogc_async_execution_creates_job_and_results(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(run_logs, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(job_store, "DOWNLOAD_DIR", tmp_path)
    _patch_successful_execution(monkeypatch)

    response = client.post(
        "/ogcapi/processes/generic-dhis2-workflow/execution",
        headers={"Prefer": "respond-async"},
        json=_valid_public_payload(),
    )
    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "accepted"
    job_id = body["jobID"]

    job_response = client.get(f"/ogcapi/jobs/{job_id}")
    assert job_response.status_code == 200
    assert job_response.json()["status"] == "successful"

    results_response = client.get(f"/ogcapi/jobs/{job_id}/results")
    assert results_response.status_code == 200
    assert results_response.json()["run_id"] == job_id


def test_publications_endpoint_seeds_source_datasets(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(publication_services, "DOWNLOAD_DIR", tmp_path)

    response = client.get("/publications")
    assert response.status_code == 200
    body = response.json()
    resource_ids = {item["resource_id"] for item in body["resources"]}
    assert "dataset-chirps3_precipitation_daily" in resource_ids
    assert "dataset-worldpop_population_yearly" in resource_ids


def test_generated_pygeoapi_config_reflects_collection_registry(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    zarr_path = tmp_path / "chirps3_precipitation_daily.zarr"
    zarr_path.mkdir(parents=True)
    monkeypatch.setattr(publication_pygeoapi, "get_zarr_path", lambda dataset: zarr_path)

    response = client.get("/publications/pygeoapi/config")
    assert response.status_code == 200
    body = response.json()
    resources = body["resources"]
    assert len(resources) > 0
    assert "chirps3_precipitation_daily" in resources
    first = resources["chirps3_precipitation_daily"]
    assert first["type"] == "collection"
    assert "title" in first


def test_generated_pygeoapi_config_contains_collection_detail(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    zarr_path = tmp_path / "chirps3_precipitation_daily.zarr"
    zarr_path.mkdir(parents=True)
    monkeypatch.setattr(publication_pygeoapi, "get_zarr_path", lambda dataset: zarr_path)

    response = client.get("/publications/pygeoapi/config")
    assert response.status_code == 200
    collection = response.json()["resources"]["chirps3_precipitation_daily"]
    assert collection["type"] == "collection"
    assert collection["title"]["en"]
    assert collection["providers"][0]["type"] == "coverage"
    raster_link = next(link for link in collection["links"] if link["rel"] == "raster-capabilities")
    assert raster_link["href"].endswith("/raster/chirps3_precipitation_daily/capabilities")
    assert raster_link["title"] == "Raster Rendering Capabilities"


def test_generated_pygeoapi_config_uses_real_source_coverage_extent(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(publication_services, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(publication_pygeoapi, "DOWNLOAD_DIR", tmp_path)
    zarr_path = tmp_path / "chirps3_precipitation_daily.zarr"
    xr.Dataset(
        data_vars={
            "precip": (("time", "y", "x"), np.arange(8, dtype=float).reshape(2, 2, 2)),
        },
        coords={
            "time": np.array(["2024-01-01", "2024-01-02"], dtype="datetime64[ns]"),
            "y": xr.Variable(("y",), [9.5, 10.5], attrs={"units": "degrees_north"}),
            "x": xr.Variable(("x",), [39.5, 40.5], attrs={"units": "degrees_east"}),
        },
    ).rio.write_crs("EPSG:4326").to_zarr(zarr_path, mode="w")
    monkeypatch.setattr(publication_pygeoapi, "get_zarr_path", lambda dataset: zarr_path)
    monkeypatch.setattr(publication_services, "list_datasets", lambda: [
        {
            "id": "chirps3_precipitation_daily",
            "name": "Total precipitation (CHIRPS3)",
            "variable": "precip",
            "period_type": "daily",
            "source": "CHIRPS v3",
            "source_url": "https://example.test/chirps",
            "resolution": "5 km x 5 km",
            "units": "mm",
        }
    ])
    monkeypatch.setattr(
        publication_services,
        "get_data_coverage",
        lambda dataset: {
            "coverage": {
                "spatial": {"xmin": 39.5, "ymin": 9.5, "xmax": 40.5, "ymax": 10.5},
                "temporal": {"start": "2024-01-01", "end": "2024-01-02"},
            }
        },
    )

    response = client.get("/publications/pygeoapi/config")
    assert response.status_code == 200
    collection = response.json()["resources"]["chirps3_precipitation_daily"]
    assert collection["extents"]["spatial"]["bbox"] == [[39.5, 9.5, 40.5, 10.5]]
    assert collection["extents"]["temporal"]["begin"] == "2024-01-01"
    assert collection["extents"]["temporal"]["end"] == "2024-01-02"


def test_ogc_collection_html_for_coverage_includes_raster_controls(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    zarr_path = tmp_path / "chirps3_precipitation_daily.zarr"
    xr.Dataset(
        data_vars={
            "precip": (("time", "y", "x"), np.arange(8, dtype=float).reshape(2, 2, 2)),
        },
        coords={
            "time": np.array(["2024-01-01", "2024-01-02"], dtype="datetime64[ns]"),
            "y": xr.Variable(("y",), [9.5, 10.5], attrs={"units": "degrees_north"}),
            "x": xr.Variable(("x",), [39.5, 40.5], attrs={"units": "degrees_east"}),
        },
    ).rio.write_crs("EPSG:4326").to_zarr(zarr_path, mode="w")
    monkeypatch.setattr(publication_pygeoapi, "get_zarr_path", lambda dataset: zarr_path)

    response = client.get("/pygeoapi/collections/chirps3_precipitation_daily?f=html")
    assert response.status_code == 200
    assert "Update raster map" in response.text
    assert "Single-date preview example" in response.text
    assert "TileJSON example" in response.text


def test_workflow_success_registers_derived_publication(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(run_logs, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(job_store, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(publication_services, "DOWNLOAD_DIR", tmp_path)
    _patch_successful_execution(monkeypatch)

    response = client.post("/workflows/dhis2-datavalue-set", json=_valid_public_payload())
    assert response.status_code == 200
    run_id = response.json()["run_id"]

    publications_response = client.get("/publications", params={"workflow_id": "dhis2_datavalue_set_v1"})
    assert publications_response.status_code == 200
    resources = publications_response.json()["resources"]
    derived = next(item for item in resources if item["resource_id"] == f"workflow-output-{run_id}")
    assert derived["resource_class"] == "derived"
    assert derived["job_id"] == run_id
    assert derived["ogc_path"] == f"/pygeoapi/collections/workflow-output-{run_id}"
    assert derived["exposure"] == "ogc"
    assert derived["asset_format"] == "geojson"
    assert derived["path"].endswith(".geojson")
    assert derived["metadata"]["native_output_file"].endswith(".json")
    assert derived["metadata"]["period_count"] == 1
    assert derived["metadata"]["analytics_eligible"] is False
    assert not any(link["rel"] == "analytics" for link in derived["links"])
    geojson = Path(derived["path"]).read_text(encoding="utf-8")
    assert '"org_unit_name"' in geojson
    assert '"period": "2024-01"' in geojson
    assert '"period_type"' not in geojson
    assert '"dataset_id"' not in geojson


def test_dynamic_ogc_collection_routes_reflect_new_publication_without_restart(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_successful_execution(monkeypatch)

    response = client.post("/workflows/dhis2-datavalue-set", json=_valid_public_payload())
    assert response.status_code == 200
    run_id = response.json()["run_id"]
    collection_id = f"workflow-output-{run_id}"

    collections_response = client.get("/pygeoapi/collections", params={"f": "json"})
    assert collections_response.status_code == 200
    collections = collections_response.json()["collections"]
    derived = next(item for item in collections if item["id"] == collection_id)
    assert derived["itemType"] == "feature"

    detail_response = client.get(f"/pygeoapi/collections/{collection_id}", params={"f": "json"})
    assert detail_response.status_code == 200
    detail = detail_response.json()
    detail_links = {link["rel"]: link["href"] for link in detail["links"]}
    assert detail["id"] == collection_id
    assert "analytics" not in detail_links

    items_response = client.get(f"/pygeoapi/collections/{collection_id}/items", params={"f": "json", "limit": 5})
    assert items_response.status_code == 200
    items = items_response.json()
    assert items["type"] == "FeatureCollection"
    assert items["numberReturned"] == 1
    feature_props = items["features"][0]["properties"]
    assert set(feature_props) == {"org_unit", "org_unit_name", "period", "value"}


def test_dynamic_ogc_collection_routes_drop_deleted_publication_without_restart(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_successful_execution(monkeypatch)

    response = client.post("/workflows/dhis2-datavalue-set", json=_valid_public_payload())
    assert response.status_code == 200
    run_id = response.json()["run_id"]
    collection_id = f"workflow-output-{run_id}"

    before_delete = client.get(f"/pygeoapi/collections/{collection_id}", params={"f": "json"})
    assert before_delete.status_code == 200

    delete_response = client.delete(f"/workflows/jobs/{run_id}")
    assert delete_response.status_code == 200

    after_delete = client.get(f"/pygeoapi/collections/{collection_id}", params={"f": "json"})
    assert after_delete.status_code == 404


def test_analytics_viewer_config_and_html_for_publication(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_successful_execution(monkeypatch)

    response = client.post("/workflows/dhis2-datavalue-set", json=_valid_public_payload())
    assert response.status_code == 200
    run_id = response.json()["run_id"]
    resource_id = f"workflow-output-{run_id}"

    config_response = client.get(f"/analytics/publications/{resource_id}")
    assert config_response.status_code == 200
    config = config_response.json()
    assert config["resource_id"] == resource_id
    assert config["data_url"].startswith("/data/")
    assert config["links"]["collection"] == f"/pygeoapi/collections/{resource_id}"

    viewer_response = client.get(f"/analytics/publications/{resource_id}/viewer")
    assert viewer_response.status_code == 200
    assert "Time-aware choropleth view" in viewer_response.text
    assert resource_id in viewer_response.text


def test_multi_period_publication_adds_analytics_link(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(run_logs, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(job_store, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(publication_services, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(publication_pygeoapi, "DOWNLOAD_DIR", tmp_path)
    _patch_successful_execution_multi_period(monkeypatch)

    response = client.post("/workflows/dhis2-datavalue-set", json=_valid_public_payload())
    assert response.status_code == 200
    run_id = response.json()["run_id"]
    resource_id = f"workflow-output-{run_id}"

    publication_response = client.get(f"/publications/{resource_id}")
    assert publication_response.status_code == 200
    publication = publication_response.json()
    assert publication["metadata"]["period_count"] == 2
    assert publication["metadata"]["analytics_eligible"] is True
    analytics_link = next(link for link in publication["links"] if link["rel"] == "analytics")
    assert analytics_link["href"] == f"/analytics/publications/{resource_id}/viewer"

    job_response = client.get(f"/workflows/jobs/{run_id}")
    assert job_response.status_code == 200
    job_links = {item["rel"]: item["href"] for item in job_response.json()["links"]}
    assert job_links["analytics"].endswith(f"/analytics/publications/{resource_id}/viewer")

    config_response = client.get("/publications/pygeoapi/config")
    assert config_response.status_code == 200
    derived = config_response.json()["resources"][resource_id]
    analytics_link = next(link for link in derived["links"] if link["rel"] == "analytics")
    assert analytics_link["type"] == "text/html"
    assert analytics_link["title"] == "Analytics Viewer"
    assert analytics_link["href"].endswith(f"/analytics/publications/{resource_id}/viewer")


def test_workflow_with_publication_disabled_does_not_register_derived_publication(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(run_logs, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(job_store, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(publication_services, "DOWNLOAD_DIR", tmp_path)
    _patch_successful_execution(monkeypatch)

    payload = _valid_public_payload()
    payload["request"]["workflow_id"] = "dhis2_datavalue_set_without_temporal_aggregation_v1"

    response = client.post("/workflows/dhis2-datavalue-set", json=payload)
    assert response.status_code == 200
    run_id = response.json()["run_id"]

    publications_response = client.get(
        "/publications",
        params={"workflow_id": "dhis2_datavalue_set_without_temporal_aggregation_v1"},
    )
    assert publications_response.status_code == 200
    resources = publications_response.json()["resources"]
    assert all(item["resource_id"] != f"workflow-output-{run_id}" for item in resources)


def test_inline_workflow_publication_intent_is_blocked_by_server_guardrail(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(run_logs, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(job_store, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(publication_services, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.delenv("EO_API_ALLOW_INLINE_WORKFLOW_PUBLICATION", raising=False)
    _patch_successful_execution(monkeypatch)

    payload = {
        "workflow": {
            "workflow_id": "adhoc_chirps_mixed_exec_v1",
            "version": 1,
            "publication": {
                "publishable": True,
                "strategy": "on_success",
                "intent": "feature_collection",
                "inputs": _standard_publication_inputs(
                    feature_step="feature_source",
                    spatial_step="spatial_aggregation",
                    build_step="build_datavalueset",
                ),
            },
            "steps": [
                {"component": "feature_source", "version": "v1"},
                {"component": "download_dataset", "version": "v1"},
                {"component": "temporal_aggregation", "version": "v1"},
                {"component": "spatial_aggregation", "version": "v1"},
                {"component": "build_datavalueset", "version": "v1"},
            ],
            "outputs": _standard_workflow_outputs(
                feature_step="feature_source",
                spatial_step="spatial_aggregation",
                build_step="build_datavalueset",
            ),
        },
        "request": _valid_public_payload()["request"] | {"workflow_id": "adhoc_chirps_mixed_exec_v1"},
    }

    response = client.post("/workflows/execute", json=payload)
    assert response.status_code == 200
    run_id = response.json()["run_id"]

    publications_response = client.get("/publications", params={"workflow_id": "adhoc_chirps_mixed_exec_v1"})
    assert publications_response.status_code == 200
    resources = publications_response.json()["resources"]
    assert all(item["resource_id"] != f"workflow-output-{run_id}" for item in resources)


def test_inline_workflow_publication_intent_can_be_enabled_by_server_policy(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(run_logs, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(job_store, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(publication_services, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setenv("EO_API_ALLOW_INLINE_WORKFLOW_PUBLICATION", "true")
    _patch_successful_execution(monkeypatch)

    payload = {
        "workflow": {
            "workflow_id": "adhoc_chirps_mixed_exec_v1",
            "version": 1,
            "publication": {
                "publishable": True,
                "strategy": "on_success",
                "intent": "feature_collection",
                "inputs": _standard_publication_inputs(
                    feature_step="feature_source",
                    spatial_step="spatial_aggregation",
                    build_step="build_datavalueset",
                ),
            },
            "steps": [
                {"component": "feature_source", "version": "v1"},
                {"component": "download_dataset", "version": "v1"},
                {"component": "temporal_aggregation", "version": "v1"},
                {"component": "spatial_aggregation", "version": "v1"},
                {"component": "build_datavalueset", "version": "v1"},
            ],
            "outputs": _standard_workflow_outputs(
                feature_step="feature_source",
                spatial_step="spatial_aggregation",
                build_step="build_datavalueset",
            ),
        },
        "request": _valid_public_payload()["request"] | {"workflow_id": "adhoc_chirps_mixed_exec_v1"},
    }

    response = client.post("/workflows/execute", json=payload)
    assert response.status_code == 200
    run_id = response.json()["run_id"]

    publications_response = client.get("/publications", params={"workflow_id": "adhoc_chirps_mixed_exec_v1"})
    assert publications_response.status_code == 200
    resources = publications_response.json()["resources"]
    derived = next(item for item in resources if item["resource_id"] == f"workflow-output-{run_id}")
    assert derived["workflow_id"] == "adhoc_chirps_mixed_exec_v1"
    assert derived["exposure"] == "registry_only"


def test_ogc_process_sync_execution_links_to_collection(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(run_logs, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(job_store, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(publication_services, "DOWNLOAD_DIR", tmp_path)
    _patch_successful_execution(monkeypatch)

    response = client.post("/ogcapi/processes/generic-dhis2-workflow/execution", json=_valid_public_payload())
    assert response.status_code == 200
    body = response.json()
    collection_links = [item for item in body["links"] if item["rel"] == "collection"]
    assert len(collection_links) == 1
    assert "/pygeoapi/collections/workflow-output-" in collection_links[0]["href"]


def test_generated_pygeoapi_config_reflects_publication_registry(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(publication_services, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(publication_pygeoapi, "DOWNLOAD_DIR", tmp_path)
    zarr_path = tmp_path / "chirps3_precipitation_daily.zarr"
    zarr_path.mkdir(parents=True)
    monkeypatch.setattr(publication_pygeoapi, "get_zarr_path", lambda dataset: zarr_path)

    response = client.get("/publications/pygeoapi/config")
    assert response.status_code == 200
    body = response.json()
    resources = body["resources"]
    assert "chirps3_precipitation_daily" in resources
    chirps = resources["chirps3_precipitation_daily"]
    assert chirps["type"] == "collection"
    assert chirps["providers"][0]["type"] == "coverage"
    assert chirps["metadata"]["dataset_id"] == "chirps3_precipitation_daily"


def test_generated_pygeoapi_openapi_includes_derived_collection(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(run_logs, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(job_store, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(publication_services, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(publication_pygeoapi, "DOWNLOAD_DIR", tmp_path)
    zarr_path = tmp_path / "chirps3_precipitation_daily.zarr"
    zarr_path.mkdir(parents=True)
    monkeypatch.setattr(publication_pygeoapi, "get_zarr_path", lambda dataset: zarr_path)
    _patch_successful_execution(monkeypatch)

    workflow_response = client.post("/workflows/dhis2-datavalue-set", json=_valid_public_payload())
    assert workflow_response.status_code == 200
    run_id = workflow_response.json()["run_id"]

    response = client.get("/publications/pygeoapi/openapi")
    assert response.status_code == 200
    body = response.json()
    assert "/collections/chirps3_precipitation_daily" in body["paths"]
    assert "chirps3_precipitation_daily" in body["x-generated-resources"]
    assert f"/collections/workflow-output-{run_id}" in body["paths"]
    assert f"workflow-output-{run_id}" in body["x-generated-resources"]


def test_generated_pygeoapi_config_includes_geojson_derived_resource(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(run_logs, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(job_store, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(publication_services, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(publication_pygeoapi, "DOWNLOAD_DIR", tmp_path)
    zarr_path = tmp_path / "chirps3_precipitation_daily.zarr"
    zarr_path.mkdir(parents=True)
    monkeypatch.setattr(publication_pygeoapi, "get_zarr_path", lambda dataset: zarr_path)
    _patch_successful_execution(monkeypatch)

    workflow_response = client.post("/workflows/dhis2-datavalue-set", json=_valid_public_payload())
    assert workflow_response.status_code == 200
    run_id = workflow_response.json()["run_id"]

    response = client.get("/publications/pygeoapi/config")
    assert response.status_code == 200
    resources = response.json()["resources"]
    derived = resources[f"workflow-output-{run_id}"]
    assert derived["providers"][0]["name"] == "GeoJSON"
    assert derived["providers"][0]["type"] == "feature"
    assert derived["providers"][0]["data"].endswith(".geojson")
    assert not any(link["rel"] == "analytics" for link in derived["links"])


def test_generated_pygeoapi_config_includes_derived_coverage_resource(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(publication_services, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(publication_pygeoapi, "DOWNLOAD_DIR", tmp_path)
    zarr_path = tmp_path / "derived_coverage.zarr"
    zarr_path.mkdir(parents=True)

    response = WorkflowExecuteResponse(
        status="completed",
        run_id="coverage-run-1",
        workflow_id="coverage_publish_v1",
        workflow_version=1,
        dataset_id="chirps3_precipitation_daily",
        outputs={"output_file": str(zarr_path)},
        primary_output_name="output_file",
        output_file=str(zarr_path),
        run_log_file="/tmp/data/workflow_runs/coverage-run-1.json",
        component_runs=[],
    )
    publication_services.register_workflow_output_publication(
        response=response,
        kind=publication_services.PublishedResourceKind.COVERAGE,
        exposure=publication_services.PublishedResourceExposure.OGC,
        published_path=str(zarr_path),
        asset_format="zarr",
    )

    config_response = client.get("/publications/pygeoapi/config")
    assert config_response.status_code == 200
    resources = config_response.json()["resources"]
    derived = resources["workflow-output-coverage-run-1"]
    assert derived["providers"][0]["type"] == "coverage"
    assert derived["providers"][0]["data"] == str(zarr_path)
    link_rels = {link["rel"] for link in derived["links"]}
    assert "collection" in link_rels
    assert "raster-capabilities" in link_rels


def test_register_workflow_output_publication_rejects_unsupported_serving_contract(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(publication_services, "DOWNLOAD_DIR", tmp_path)
    response = WorkflowExecuteResponse(
        status="completed",
        run_id="tiles-run-1",
        workflow_id="tiles_publish_v1",
        workflow_version=1,
        dataset_id="chirps3_precipitation_daily",
        outputs={"output_file": "/tmp/tiles"},
        primary_output_name="output_file",
        output_file="/tmp/tiles",
        run_log_file="/tmp/data/workflow_runs/tiles-run-1.json",
        component_runs=[],
    )

    with pytest.raises(ValueError, match="Unsupported publication serving contract"):
        publication_services.register_workflow_output_publication(
            response=response,
            kind=publication_services.PublishedResourceKind.TILESET,
            exposure=publication_services.PublishedResourceExposure.OGC,
            published_path="/tmp/tiles",
            asset_format="tiles",
        )


def test_materialize_generated_pygeoapi_documents_writes_files(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(publication_services, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(publication_pygeoapi, "DOWNLOAD_DIR", tmp_path)
    zarr_path = tmp_path / "chirps3_precipitation_daily.zarr"
    zarr_path.mkdir(parents=True)
    monkeypatch.setattr(publication_pygeoapi, "get_zarr_path", lambda dataset: zarr_path)

    response = client.post("/publications/pygeoapi/materialize")
    assert response.status_code == 200
    body = response.json()
    config_path = Path(body["config_path"])
    openapi_path = Path(body["openapi_path"])
    assert config_path.exists()
    assert openapi_path.exists()
    config_text = config_path.read_text(encoding="utf-8")
    openapi_text = openapi_path.read_text(encoding="utf-8")
    assert "resources:" in config_text
    assert "http://127.0.0.1:8000/pygeoapi" in config_text
    assert "http://127.0.0.1:8000/pygeoapi" in openapi_text
    assert "http://127.0.0.1:8000/pygeoapi/collections/chirps3_precipitation_daily" in config_text


def test_get_published_resource_normalizes_legacy_pygeoapi_links(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(publication_services, "DOWNLOAD_DIR", tmp_path)
    resources_dir = tmp_path / "published_resources"
    resources_dir.mkdir(parents=True)
    legacy_resource = {
        "resource_id": "workflow-output-legacy",
        "resource_class": "derived",
        "kind": "feature_collection",
        "title": "Legacy workflow output",
        "description": "Legacy collection",
        "dataset_id": "chirps3_precipitation_daily",
        "workflow_id": "dhis2_datavalue_set_v1",
        "job_id": "legacy",
        "run_id": "legacy",
        "path": "data/downloads/legacy.geojson",
        "ogc_path": "/ogcapi/collections/workflow-output-legacy",
        "asset_format": "geojson",
        "exposure": "ogc",
        "created_at": "2026-03-20T00:00:00+00:00",
        "updated_at": "2026-03-20T00:00:00+00:00",
        "metadata": {},
        "links": [
            {"rel": "collection", "href": "/ogcapi/collections/workflow-output-legacy"},
            {"rel": "job", "href": "/workflows/jobs/legacy"},
        ],
    }
    (resources_dir / "workflow-output-legacy.json").write_text(json.dumps(legacy_resource), encoding="utf-8")

    resource = publication_services.get_published_resource("workflow-output-legacy")

    assert resource is not None
    assert resource.ogc_path == "/pygeoapi/collections/workflow-output-legacy"
    assert resource.links[0]["href"] == "/pygeoapi/collections/workflow-output-legacy"


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


def test_temporal_aggregation_component_passes_through_matching_period_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ds = xr.Dataset(
        {"precip": (("time", "lat", "lon"), [[[1.0]]])},
        coords={"time": ["2024-01-01"], "lat": [0], "lon": [0]},
    )
    aggregate_called = {"value": False}

    monkeypatch.setattr(component_services, "get_data", lambda **kwargs: ds)

    def _aggregate_temporal(**kwargs: Any) -> xr.Dataset:
        aggregate_called["value"] = True
        return ds

    monkeypatch.setattr(component_services, "aggregate_temporal", _aggregate_temporal)

    result = component_services.temporal_aggregation_component(
        dataset={"id": "chirps3_precipitation_daily", "variable": "precip", "period_type": "daily"},
        start="2024-01-01",
        end="2024-01-31",
        bbox=None,
        target_period_type=PeriodType.DAILY,
        method=AggregationMethod.SUM,
    )

    assert result is ds
    assert aggregate_called["value"] is False


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


def test_engine_spatial_aggregation_uses_temporally_aggregated_dataset(monkeypatch: pytest.MonkeyPatch) -> None:
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
    temporal_ds = xr.Dataset(
        {"precip": (("time", "lat", "lon"), [[[31.0]]])},
        coords={"time": ["2024-01"], "lat": [0], "lon": [0]},
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
    monkeypatch.setattr(engine.component_services, "temporal_aggregation_component", lambda **kwargs: temporal_ds)

    def _spatial_aggregation_component(**kwargs: Any) -> list[dict[str, Any]]:
        assert kwargs["aggregated_dataset"] is temporal_ds
        return [{"org_unit": "OU_1", "time": "2024-01", "value": 31.0}]

    monkeypatch.setattr(engine.component_services, "spatial_aggregation_component", _spatial_aggregation_component)
    monkeypatch.setattr(
        engine.component_services,
        "build_datavalueset_component",
        lambda **kwargs: ({"dataValues": [{"value": "31.0", "period": "202401"}]}, "/tmp/data/out.json"),
    )
    monkeypatch.setattr(engine, "persist_run_log", lambda **kwargs: "/tmp/data/workflow_runs/run.json")

    response = engine.execute_workflow(request, include_component_run_details=True)
    assert response.status == "completed"
    assert response.data_value_set["dataValues"][0]["period"] == "202401"


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


def test_engine_rejects_remote_spatial_after_temporal_aggregation(monkeypatch: pytest.MonkeyPatch) -> None:
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
    temporal_ds = xr.Dataset(
        {"precip": (("time", "lat", "lon"), [[[31.0]]])},
        coords={"time": ["2024-01"], "lat": [0], "lon": [0]},
    )
    workflow = WorkflowDefinition.model_validate(
        {
            "workflow_id": "dhis2_datavalue_set_v1",
            "version": 1,
            "steps": [
                {"component": "feature_source"},
                {"component": "download_dataset"},
                {"component": "temporal_aggregation"},
                {
                    "component": "spatial_aggregation",
                    "config": {
                        "execution_mode": "remote",
                        "remote_url": "http://localhost:8000/components/spatial-aggregation",
                    },
                },
                {"component": "build_datavalueset"},
            ],
            "outputs": _standard_workflow_outputs(
                feature_step="feature_source",
                spatial_step="spatial_aggregation",
                build_step="build_datavalueset",
            ),
        }
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
    monkeypatch.setattr(engine.component_services, "temporal_aggregation_component", lambda **kwargs: temporal_ds)
    monkeypatch.setattr(engine, "persist_run_log", lambda **kwargs: "/tmp/data/workflow_runs/run.json")

    with pytest.raises(HTTPException) as exc_info:
        engine.execute_workflow(request, workflow_definition=workflow)

    assert exc_info.value.status_code == 500
    detail = cast(dict[str, Any], exc_info.value.detail)
    assert detail["failed_component"] == "spatial_aggregation"
    assert "local spatial_aggregation" in detail["message"]


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
    assert [step.id for step in workflow.steps] == [
        "get_features",
        "download",
        "temporal_agg",
        "spatial_agg",
        "build_dhis2_payload",
    ]
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
                    {"id": "features", "component": "feature_source"},
                    {
                        "id": "download",
                        "component": "download_dataset",
                        "inputs": {"bbox": {"from_step": "features", "output": "bbox"}},
                    },
                    {
                        "id": "aggregate",
                        "component": "spatial_aggregation",
                        "inputs": {
                            "bbox": {"from_step": "features", "output": "bbox"},
                            "features": {"from_step": "features", "output": "features"},
                        },
                    },
                    {
                        "id": "build",
                        "component": "build_datavalueset",
                        "inputs": {"records": {"from_step": "aggregate", "output": "records"}},
                    },
                ],
                "outputs": {
                    "bbox": {"from_step": "features", "output": "bbox"},
                    "features": {"from_step": "features", "output": "features"},
                    "records": {"from_step": "aggregate", "output": "records"},
                    "data_value_set": {"from_step": "build", "output": "data_value_set"},
                    "output_file": {"from_step": "build", "output": "output_file"},
                },
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


def test_validate_workflow_reports_explicit_input_wiring(client: TestClient) -> None:
    response = client.post("/workflows/validate", json={"workflow_id": "dhis2_datavalue_set_v1"})
    assert response.status_code == 200
    body = response.json()
    assert body["valid"] is True
    assert body["publication_publishable"] is True
    assert body["publication_intent"] == "feature_collection"
    assert body["publication_exposure"] == "ogc"
    assert body["publication_inputs"]["records"]["from_step"] == "spatial_agg"
    assert body["serving_supported"] is True
    assert body["serving_asset_format"] == "geojson"
    assert body["serving_targets"] == ["pygeoapi", "analytics"]
    assert body["resolved_steps"][0]["id"] == "get_features"
    assert body["resolved_steps"][1]["resolved_inputs"]["bbox"] == {
        "from_step": "get_features",
        "output": "bbox",
    }
    assert body["resolved_steps"][3]["resolved_inputs"]["temporal_dataset"] == {
        "from_step": "temporal_agg",
        "output": "temporal_dataset",
    }


def test_schedule_trigger_reuses_existing_job(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_successful_execution(monkeypatch)

    create_response = client.post(
        "/workflows/schedules",
        json={
            "cron_expression": "0 2 * * *",
            "request": _valid_public_payload()["request"],
        },
    )
    assert create_response.status_code == 200
    schedule_id = create_response.json()["schedule_id"]

    trigger_payload = {"execution_time": "2026-03-19T02:00:00Z"}
    first_trigger = client.post(f"/workflows/schedules/{schedule_id}/trigger", json=trigger_payload)
    assert first_trigger.status_code == 200
    first_body = first_trigger.json()
    assert first_body["reused_existing_job"] is False
    assert first_body["status"] == "successful"

    second_trigger = client.post(f"/workflows/schedules/{schedule_id}/trigger", json=trigger_payload)
    assert second_trigger.status_code == 200
    second_body = second_trigger.json()
    assert second_body["reused_existing_job"] is True
    assert second_body["job_id"] == first_body["job_id"]

    job_response = client.get(f"/workflows/jobs/{first_body['job_id']}")
    assert job_response.status_code == 200
    job_body = job_response.json()
    assert job_body["trigger_type"] == "scheduled"
    assert job_body["schedule_id"] == schedule_id
    assert job_body["idempotency_key"] == first_body["idempotency_key"]


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
                "outputs": _standard_workflow_outputs(),
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
                "outputs": _standard_workflow_outputs(),
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
                "outputs": _standard_workflow_outputs(),
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

    def _remote_adapter(**kwargs: Any) -> dict[str, Any]:
        remote_called.update(kwargs)
        return {"status": "downloaded"}

    monkeypatch.setattr(component_services, "_invoke_registered_remote_component", _remote_adapter)
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
    assert remote_called["component_key"] == "download_dataset@v1"
    assert remote_called["remote_url"] == "http://component-host/components/download-dataset"
    assert remote_called["request"].dataset_id == "chirps3_precipitation_daily"


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
                "outputs": _standard_workflow_outputs(),
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
                "outputs": _standard_workflow_outputs(),
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


def test_engine_supports_remote_mode_for_remote_compatible_component_chain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
                "outputs": _standard_workflow_outputs(),
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
        "spatial": False,
        "build": False,
    }

    def _remote_adapter(**kwargs: Any) -> dict[str, Any]:
        component_key = kwargs["component_key"]
        if component_key == "feature_source@v1":
            called["feature"] = True
            return {
                "features": {"type": "FeatureCollection", "features": [{"id": "OU_1", "properties": {"id": "OU_1"}}]},
                "bbox": [0, 0, 1, 1],
            }
        if component_key == "download_dataset@v1":
            called["download"] = True
            return {"status": "downloaded"}
        if component_key == "spatial_aggregation@v1":
            called["spatial"] = True
            return {"records": [{"org_unit": "OU_1", "time": "2024-01-01", "value": 10.0}]}
        if component_key == "build_datavalueset@v1":
            called["build"] = True
            return {"data_value_set": {"dataValues": [{"value": "10.0"}]}, "output_file": "/tmp/data/out.json"}
        raise AssertionError(f"Unexpected remote component key: {component_key}")

    monkeypatch.setattr(component_services, "_invoke_registered_remote_component", _remote_adapter)
    monkeypatch.setattr(engine, "persist_run_log", lambda **kwargs: "/tmp/data/workflow_runs/run.json")

    response = engine.execute_workflow(request)
    assert response.status == "completed"
    assert all(called.values())
