"""Generic dataset-to-DHIS2 workflow process."""

from __future__ import annotations

import os
from typing import Any

from pydantic import ValidationError
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from eo_api.integrations.orchestration.capabilities import build_collection_links_for_dataset, list_supported_datasets
from eo_api.integrations.orchestration.executor import execute_workflow_spec
from eo_api.integrations.orchestration.registry import build_default_component_registry
from eo_api.integrations.orchestration.templates import chirps3_dhis2_template, worldpop_dhis2_template
from eo_api.routers.ogcapi.plugins.processes.schemas import (
    GENERIC_DHIS2_WORKFLOW_INPUT_ADAPTER,
    GenericChirps3WorkflowInput,
    GenericWorldPopWorkflowInput,
)

DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/tmp/data")
SUPPORTED_DATASETS = list_supported_datasets()
CAPABILITIES_PATH = "/ogcapi/processes/generic-dhis2-workflow/capabilities"

PROCESS_METADATA = {
    "version": "0.1.0",
    "id": "generic-dhis2-workflow",
    "title": "Generic DHIS2 Workflow",
    "description": (
        "Generic orchestrator for dataset adapters and canonical workflow chain: "
        "features -> download -> temporal aggregation -> spatial aggregation -> DHIS2 payload builder."
    ),
    "jobControlOptions": ["sync-execute"],
    "keywords": ["workflow", "dhis2", "generic", "chirps3", "worldpop"],
    "inputs": {
        "dataset_type": {
            "title": "Dataset type",
            "schema": {"type": "string", "enum": SUPPORTED_DATASETS},
            "minOccurs": 1,
            "maxOccurs": 1,
        },
        "features_geojson": {"schema": {"type": "object"}, "minOccurs": 0, "maxOccurs": 1},
        "org_unit_level": {"schema": {"type": "integer", "minimum": 1}, "minOccurs": 0, "maxOccurs": 1},
        "parent_org_unit": {"schema": {"type": "string"}, "minOccurs": 0, "maxOccurs": 1},
        "org_unit_ids": {"schema": {"type": "array", "items": {"type": "string"}}, "minOccurs": 0, "maxOccurs": 1},
        "org_unit_id_property": {"schema": {"type": "string", "default": "id"}, "minOccurs": 0, "maxOccurs": 1},
        "bbox": {
            "schema": {"type": "array", "items": {"type": "number"}, "minItems": 4, "maxItems": 4},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "data_element": {"schema": {"type": "string"}, "minOccurs": 1, "maxOccurs": 1},
        "category_option_combo": {"schema": {"type": "string"}, "minOccurs": 0, "maxOccurs": 1},
        "attribute_option_combo": {"schema": {"type": "string"}, "minOccurs": 0, "maxOccurs": 1},
        "data_set": {"schema": {"type": "string"}, "minOccurs": 0, "maxOccurs": 1},
        "dry_run": {"schema": {"type": "boolean", "default": True}, "minOccurs": 0, "maxOccurs": 1},
        "start_date": {"schema": {"type": "string"}, "minOccurs": 0, "maxOccurs": 1},
        "end_date": {"schema": {"type": "string"}, "minOccurs": 0, "maxOccurs": 1},
        "stage": {"schema": {"type": "string", "enum": ["final", "prelim"]}, "minOccurs": 0, "maxOccurs": 1},
        "flavor": {"schema": {"type": "string", "enum": ["rnl", "sat"]}, "minOccurs": 0, "maxOccurs": 1},
        "spatial_reducer": {"schema": {"type": "string", "enum": ["mean", "sum"]}, "minOccurs": 0, "maxOccurs": 1},
        "temporal_resolution": {
            "schema": {"type": "string", "enum": ["daily", "weekly", "monthly", "yearly", "annual"]},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "temporal_reducer": {"schema": {"type": "string", "enum": ["sum", "mean"]}, "minOccurs": 0, "maxOccurs": 1},
        "value_rounding": {"schema": {"type": "integer", "minimum": 0, "maximum": 10}, "minOccurs": 0, "maxOccurs": 1},
        "country_code": {"schema": {"type": "string", "pattern": "^[A-Za-z]{2,3}$"}, "minOccurs": 0, "maxOccurs": 1},
        "start_year": {"schema": {"type": "integer", "minimum": 2015, "maximum": 2030}, "minOccurs": 0, "maxOccurs": 1},
        "end_year": {"schema": {"type": "integer", "minimum": 2015, "maximum": 2030}, "minOccurs": 0, "maxOccurs": 1},
        "output_format": {
            "schema": {"type": "string", "enum": ["netcdf", "geotiff", "zarr"]},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "raster_files": {"schema": {"type": "array", "items": {"type": "string"}}, "minOccurs": 0, "maxOccurs": 1},
        "reducer": {"schema": {"type": "string", "enum": ["sum", "mean"]}, "minOccurs": 0, "maxOccurs": 1},
    },
    "outputs": {
        "result": {
            "title": "Workflow output",
            "schema": {"type": "object", "contentMediaType": "application/json"},
        }
    },
    "links": [
        {
            "rel": "capabilities",
            "type": "application/json",
            "title": "Generic workflow dataset/service capabilities",
            "href": CAPABILITIES_PATH,
        }
    ],
}


def _build_chirps_input(inputs: GenericChirps3WorkflowInput) -> dict[str, Any]:
    return {
        "dataset": "chirps3",
        "features_geojson": inputs.features_geojson,
        "org_unit_level": inputs.org_unit_level,
        "parent_org_unit": inputs.parent_org_unit,
        "org_unit_ids": inputs.org_unit_ids,
        "org_unit_id_property": inputs.org_unit_id_property,
        "bbox": inputs.bbox,
        "data_element": inputs.data_element,
        "category_option_combo": inputs.category_option_combo,
        "attribute_option_combo": inputs.attribute_option_combo,
        "data_set": inputs.data_set,
        "dry_run": inputs.dry_run,
        "start_date": str(inputs.start_date),
        "end_date": str(inputs.end_date),
        "start_month": inputs.start_date.strftime("%Y-%m"),
        "end_month": inputs.end_date.strftime("%Y-%m"),
        "stage": inputs.stage,
        "flavor": inputs.flavor,
        "spatial_reducer": inputs.spatial_reducer,
        "temporal_resolution": inputs.temporal_resolution,
        "temporal_reducer": inputs.temporal_reducer,
        "value_rounding": inputs.value_rounding,
        "reducer": inputs.temporal_reducer,
    }


def _build_worldpop_input(inputs: GenericWorldPopWorkflowInput) -> dict[str, Any]:
    return {
        "dataset": "worldpop",
        "features_geojson": inputs.features_geojson,
        "org_unit_level": inputs.org_unit_level,
        "parent_org_unit": inputs.parent_org_unit,
        "org_unit_ids": inputs.org_unit_ids,
        "org_unit_id_property": inputs.org_unit_id_property,
        "bbox": inputs.bbox,
        "data_element": inputs.data_element,
        "category_option_combo": inputs.category_option_combo,
        "attribute_option_combo": inputs.attribute_option_combo,
        "data_set": inputs.data_set,
        "dry_run": inputs.dry_run,
        "country_code": inputs.country_code,
        "start_year": inputs.start_year,
        "end_year": inputs.end_year,
        "output_format": inputs.output_format,
        "raster_files": inputs.raster_files,
        "temporal_resolution": inputs.temporal_resolution,
        "reducer": inputs.reducer,
    }


class GenericDhis2WorkflowProcessor(BaseProcessor):
    """Generic process orchestrator for dataset adapters + canonical chain."""

    def __init__(self, processor_def: dict[str, Any]) -> None:
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data: dict[str, Any], outputs: Any = None) -> tuple[str, dict[str, Any]]:
        try:
            validated = GENERIC_DHIS2_WORKFLOW_INPUT_ADAPTER.validate_python(data)
        except ValidationError as exc:
            raise ProcessorExecuteError(str(exc)) from exc

        if isinstance(validated, GenericChirps3WorkflowInput):
            spec = chirps3_dhis2_template()
            workflow_input = _build_chirps_input(validated)
        else:
            spec = worldpop_dhis2_template()
            workflow_input = _build_worldpop_input(validated)

        run_result = execute_workflow_spec(
            spec=spec,
            registry=build_default_component_registry(),
            context={"download_dir": DOWNLOAD_DIR, "input": workflow_input},
        )

        outputs_map = run_result.get("outputs", {})
        files = outputs_map.get("download", {}).get("files", [])
        payload_step = outputs_map.get("dhis2_payload_builder", {})
        features_step = outputs_map.get("features", {})
        summary: dict[str, Any] = {
            "dataset_type": validated.dataset_type,
            "feature_count": len(features_step.get("valid_features", [])),
            "effective_bbox": features_step.get("effective_bbox"),
            "file_count": len(files),
            "workflow_status": run_result.get("status"),
        }
        if "summary" in payload_step:
            summary["payload"] = payload_step["summary"]
        if run_result.get("status") == "exited":
            summary["exit"] = run_result.get("exit", {})
        include_workflow_outputs = bool(payload_step.get("table")) or bool(payload_step.get("dataValueSet"))
        collection_links = build_collection_links_for_dataset(
            validated.dataset_type,
            include_workflow_outputs=include_workflow_outputs,
        )

        return "application/json", {
            "status": run_result.get("status"),
            "message": (
                "Generic workflow execution completed."
                if run_result.get("status") == "completed"
                else "Workflow exited early."
            ),
            "files": files,
            "summary": summary,
            "dataValueSet": payload_step.get("dataValueSet"),
            "dataValueTable": payload_step.get("table"),
            "links": collection_links,
            "workflowTrace": run_result.get("workflowTrace", []),
            "workflowOutputs": outputs_map,
            "exit": run_result.get("exit"),
        }

    def __repr__(self) -> str:
        return "<GenericDhis2WorkflowProcessor>"
