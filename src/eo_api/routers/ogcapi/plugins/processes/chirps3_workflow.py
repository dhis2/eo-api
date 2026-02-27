"""CHIRPS3 to DHIS2 workflow process."""

from __future__ import annotations

import logging
from typing import Any

from pygeoapi.process.base import BaseProcessor

from eo_api.routers.ogcapi.plugins.processes.chirps3 import CHIRPS3Processor
from eo_api.routers.ogcapi.plugins.processes.data_aggregate import DataAggregateProcessor
from eo_api.routers.ogcapi.plugins.processes.datavalue_build import DataValueBuildProcessor
from eo_api.routers.ogcapi.plugins.processes.feature_fetch import FeatureFetchProcessor
from eo_api.routers.ogcapi.plugins.processes.schemas import ClimateDhis2WorkflowInput
from eo_api.routers.ogcapi.plugins.processes.workflow_runtime import run_process_with_trace

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    "version": "0.1.0",
    "id": "chirps3-dhis2-workflow",
    "title": "CHIRPS3 to DHIS2 Workflow",
    "description": "Orchestrate feature fetch, CHIRPS download, aggregation, and DHIS2 data value generation.",
    "jobControlOptions": ["sync-execute"],
    "keywords": ["climate", "CHIRPS3", "DHIS2", "workflow", "dataValueSet"],
    "inputs": {
        "start_date": {"title": "Start date", "schema": {"type": "string"}, "minOccurs": 1, "maxOccurs": 1},
        "end_date": {"title": "End date", "schema": {"type": "string"}, "minOccurs": 1, "maxOccurs": 1},
        "features_geojson": {
            "title": "Features GeoJSON",
            "schema": {"type": "object"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "org_unit_level": {
            "title": "Org unit level",
            "schema": {"type": "integer", "minimum": 1},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "parent_org_unit": {"title": "Parent org unit", "schema": {"type": "string"}, "minOccurs": 0, "maxOccurs": 1},
        "org_unit_ids": {
            "title": "Org unit IDs",
            "schema": {"type": "array", "items": {"type": "string"}},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "org_unit_id_property": {
            "title": "Org unit ID property",
            "schema": {"type": "string", "default": "id"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "data_element": {"title": "Data element UID", "schema": {"type": "string"}, "minOccurs": 1, "maxOccurs": 1},
        "category_option_combo": {
            "title": "Category option combo UID",
            "schema": {"type": "string"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "attribute_option_combo": {
            "title": "Attribute option combo UID",
            "schema": {"type": "string"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "data_set": {"title": "Dataset UID", "schema": {"type": "string"}, "minOccurs": 0, "maxOccurs": 1},
        "stage": {
            "title": "CHIRPS3 stage",
            "schema": {"type": "string", "enum": ["final", "prelim"], "default": "final"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "flavor": {
            "title": "CHIRPS3 flavor",
            "schema": {"type": "string", "enum": ["rnl", "sat"], "default": "rnl"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "spatial_reducer": {
            "title": "Spatial reducer",
            "schema": {"type": "string", "enum": ["mean", "sum"], "default": "mean"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "temporal_resolution": {
            "title": "Temporal resolution",
            "schema": {"type": "string", "enum": ["daily", "weekly", "monthly"], "default": "monthly"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "temporal_reducer": {
            "title": "Temporal reducer",
            "schema": {"type": "string", "enum": ["sum", "mean"], "default": "sum"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "value_rounding": {
            "title": "Value rounding",
            "schema": {"type": "integer", "minimum": 0, "maximum": 10, "default": 3},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "dry_run": {"title": "Dry run", "schema": {"type": "boolean", "default": True}, "minOccurs": 0, "maxOccurs": 1},
    },
    "outputs": {
        "result": {
            "title": "Workflow result",
            "schema": {"type": "object", "contentMediaType": "application/json"},
        }
    },
}


class Chirps3WorkflowProcessor(BaseProcessor):
    """Workflow orchestration process with decomposed internal services."""

    def __init__(self, processor_def: dict[str, Any]) -> None:
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data: dict[str, Any], outputs: Any = None) -> tuple[str, dict[str, Any]]:
        inputs = ClimateDhis2WorkflowInput.model_validate(data)
        workflow_trace: list[dict[str, Any]] = []
        LOGGER.info(
            "[chirps3-dhis2-workflow] start start_date=%s end_date=%s stage=%s flavor=%s dry_run=%s",
            inputs.start_date,
            inputs.end_date,
            inputs.stage,
            inputs.flavor,
            inputs.dry_run,
        )

        feature_result = run_process_with_trace(
            workflow_trace,
            step_name="feature_fetch",
            processor_cls=FeatureFetchProcessor,
            process_name="feature-fetch",
            data={
                "features_geojson": inputs.features_geojson,
                "org_unit_level": inputs.org_unit_level,
                "parent_org_unit": inputs.parent_org_unit,
                "org_unit_ids": inputs.org_unit_ids,
                "org_unit_id_property": inputs.org_unit_id_property,
                "bbox": inputs.bbox,
            },
        )
        download_result = run_process_with_trace(
            workflow_trace,
            step_name="chirps3_download",
            processor_cls=CHIRPS3Processor,
            process_name="chirps3-download",
            data={
                "start": inputs.start_date.strftime("%Y-%m"),
                "end": inputs.end_date.strftime("%Y-%m"),
                "bbox": list(feature_result["effective_bbox"]),
                "stage": inputs.stage,
                "flavor": inputs.flavor,
                "dry_run": True,
            },
        )
        files = [str(path) for path in download_result.get("files", [])]

        aggregate_result = run_process_with_trace(
            workflow_trace,
            step_name="aggregate",
            processor_cls=DataAggregateProcessor,
            process_name="data-aggregate",
            data={
                "start_date": str(inputs.start_date),
                "end_date": str(inputs.end_date),
                "files": files,
                "valid_features": feature_result["valid_features"],
                "spatial_reducer": inputs.spatial_reducer,
                "temporal_resolution": inputs.temporal_resolution,
                "temporal_reducer": inputs.temporal_reducer,
                "value_rounding": inputs.value_rounding,
            },
        )
        rows = aggregate_result["rows"]

        dv_result = run_process_with_trace(
            workflow_trace,
            step_name="build_datavalues",
            processor_cls=DataValueBuildProcessor,
            process_name="dhis2-datavalue-build",
            data={
                "rows": rows,
                "data_element": inputs.data_element,
                "category_option_combo": inputs.category_option_combo,
                "attribute_option_combo": inputs.attribute_option_combo,
                "data_set": inputs.data_set,
            },
        )

        return "application/json", {
            "status": "completed",
            "files": files,
            "summary": {
                "feature_count": len(feature_result["valid_features"]),
                "data_value_count": len(dv_result["dataValueSet"]["dataValues"]),
                "start_date": str(inputs.start_date),
                "end_date": str(inputs.end_date),
                "stage": inputs.stage,
                "flavor": inputs.flavor,
                "temporal_resolution": inputs.temporal_resolution,
                "spatial_reducer": inputs.spatial_reducer,
                "temporal_reducer": inputs.temporal_reducer,
                "imported": False,
            },
            "message": "Workflow completed (stopped at data value generation)",
            "dataValueSet": dv_result["dataValueSet"],
            "dataValueTable": dv_result["table"],
            "workflowTrace": workflow_trace,
            "importResponse": None,
        }

    def __repr__(self) -> str:
        return "<Chirps3WorkflowProcessor>"
