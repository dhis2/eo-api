"""WorldPop to DHIS2 workflow process.

This process is a thin orchestrator over reusable components:
- eo_api.integrations.worldpop_sync
- eo_api.integrations.worldpop_to_dhis2
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

from pydantic import ValidationError
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from eo_api.integrations.feature_fetch import resolve_features
from eo_api.integrations.workflow_runtime import run_component_with_trace
from eo_api.integrations.worldpop_sync import sync_worldpop
from eo_api.integrations.worldpop_to_dhis2 import build_worldpop_datavalueset
from eo_api.routers.ogcapi.plugins.processes.schemas import FeatureFetchInput, WorldPopDhis2WorkflowInput

DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/tmp/data")

PROCESS_METADATA = {
    "version": "0.1.0",
    "id": "worldpop-dhis2-workflow",
    "title": "WorldPop to DHIS2 workflow",
    "description": "Orchestrate WorldPop sync/request and build DHIS2 dataValueSet payload.",
    "jobControlOptions": ["sync-execute", "async-execute"],
    "keywords": ["worldpop", "dhis2", "datavalueset", "workflow"],
    "inputs": {
        "country_code": {"schema": {"type": "string", "pattern": "^[A-Za-z]{2,3}$"}, "minOccurs": 0, "maxOccurs": 1},
        "bbox": {
            "schema": {"type": "array", "items": {"type": "number"}, "minItems": 4, "maxItems": 4},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "start_year": {"schema": {"type": "integer", "minimum": 2015, "maximum": 2030, "default": 2015}},
        "end_year": {"schema": {"type": "integer", "minimum": 2015, "maximum": 2030, "default": 2030}},
        "output_format": {
            "schema": {"type": "string", "enum": ["netcdf", "geotiff", "zarr"], "default": "netcdf"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "raster_files": {"schema": {"type": "array", "items": {"type": "string"}}, "minOccurs": 0, "maxOccurs": 1},
        "dry_run": {"schema": {"type": "boolean", "default": False}, "minOccurs": 0, "maxOccurs": 1},
        "features_geojson": {"schema": {"type": "object"}, "minOccurs": 0, "maxOccurs": 1},
        "org_unit_level": {"schema": {"type": "integer", "minimum": 1}, "minOccurs": 0, "maxOccurs": 1},
        "parent_org_unit": {"schema": {"type": "string"}, "minOccurs": 0, "maxOccurs": 1},
        "org_unit_ids": {"schema": {"type": "array", "items": {"type": "string"}}, "minOccurs": 0, "maxOccurs": 1},
        "data_element": {"schema": {"type": "string"}, "minOccurs": 1, "maxOccurs": 1},
        "org_unit_id_property": {"schema": {"type": "string", "default": "id"}, "minOccurs": 0, "maxOccurs": 1},
        "reducer": {
            "schema": {"type": "string", "enum": ["sum", "mean"], "default": "sum"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "category_option_combo": {"schema": {"type": "string"}, "minOccurs": 0, "maxOccurs": 1},
        "attribute_option_combo": {"schema": {"type": "string"}, "minOccurs": 0, "maxOccurs": 1},
        "data_set": {"schema": {"type": "string"}, "minOccurs": 0, "maxOccurs": 1},
    },
    "outputs": {
        "result": {
            "title": "Workflow output",
            "schema": {"type": "object", "contentMediaType": "application/json"},
        }
    },
}

_YEAR_RE = re.compile(r"(19|20)\d{2}")


def _extract_year(path: str, fallback_year: int) -> int:
    match = _YEAR_RE.search(Path(path).name)
    if match:
        return int(match.group(0))
    return fallback_year


class WorldPopDhis2WorkflowProcessor(BaseProcessor):
    """Thin process orchestrator for WorldPop -> DHIS2 payload generation."""

    def __init__(self, processor_def: dict[str, Any]) -> None:
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data: dict[str, Any], outputs: Any = None) -> tuple[str, dict[str, Any]]:
        try:
            inputs = WorldPopDhis2WorkflowInput.model_validate(data)
        except ValidationError as exc:
            raise ProcessorExecuteError(str(exc)) from exc

        workflow_trace: list[dict[str, Any]] = []
        feature_result = run_component_with_trace(
            workflow_trace,
            step_name="feature_fetch",
            fn=resolve_features,
            inputs=FeatureFetchInput(
                bbox=inputs.bbox,
                features_geojson=inputs.features_geojson,
                org_unit_level=inputs.org_unit_level,
                parent_org_unit=inputs.parent_org_unit,
                org_unit_ids=inputs.org_unit_ids,
                org_unit_id_property=inputs.org_unit_id_property,
            ),
        )
        feature_collection = {
            "type": "FeatureCollection",
            "features": [
                {"type": "Feature", "id": item["orgUnit"], "geometry": item["geometry"], "properties": {}}
                for item in feature_result["valid_features"]
            ],
        }

        if inputs.raster_files:
            raster_files = list(inputs.raster_files)
            sync_summary = {
                "mode": "provided-raster-files",
                "file_count": len(raster_files),
                "implementation_status": "skipped",
            }
            workflow_trace.append(
                {
                    "step": "worldpop_sync",
                    "status": "skipped",
                    "durationMs": 0.0,
                    "reason": "raster_files provided",
                }
            )
        else:
            plan = run_component_with_trace(
                workflow_trace,
                step_name="worldpop_sync",
                fn=sync_worldpop,
                country_code=inputs.country_code,
                bbox=inputs.bbox,
                start_year=inputs.start_year,
                end_year=inputs.end_year,
                output_format=inputs.output_format,
                root_dir=Path(DOWNLOAD_DIR) / "worldpop_cache",
                dry_run=inputs.dry_run,
            )
            raster_files = [path for path in plan["planned_files"] if Path(path).exists()]
            sync_summary = {
                "mode": "worldpop-sync-component",
                "requested_file_count": len(plan["planned_files"]),
                "existing_file_count": len(raster_files),
                "missing_file_count": len(plan["planned_files"]) - len(raster_files),
                "implementation_status": plan.get("implementation_status", "unknown"),
            }

        if not raster_files:
            raise ProcessorExecuteError("No local raster files are available for payload generation")

        all_data_values: list[dict[str, Any]] = []
        table_columns: list[str] | None = None
        table_rows: list[dict[str, Any]] = []
        yearly_summaries: list[dict[str, Any]] = []
        default_year = inputs.start_year

        for path in raster_files:
            year = _extract_year(path, default_year)
            result = run_component_with_trace(
                workflow_trace,
                step_name=f"build_datavalues_{year}",
                fn=build_worldpop_datavalueset,
                features_geojson=feature_collection,
                raster_path=path,
                year=year,
                data_element=inputs.data_element,
                org_unit_id_property=inputs.org_unit_id_property,
                reducer=inputs.reducer,
                category_option_combo=inputs.category_option_combo,
                attribute_option_combo=inputs.attribute_option_combo,
                data_set=inputs.data_set,
            )
            all_data_values.extend(result["dataValueSet"]["dataValues"])
            table_columns = result["table"]["columns"]
            table_rows.extend(result["table"]["rows"])
            yearly_summaries.append(result["summary"])

        payload: dict[str, Any] = {"dataValues": all_data_values}
        if inputs.data_set:
            payload["dataSet"] = inputs.data_set

        return "application/json", {
            "status": "completed",
            "files": raster_files,
            "dataValueSet": payload,
            "table": {"columns": table_columns or [], "rows": table_rows},
            "summary": {
                "sync": sync_summary,
                "features": {
                    "feature_count": len(feature_result["valid_features"]),
                    "effective_bbox": feature_result["effective_bbox"],
                },
                "total_data_values": len(all_data_values),
                "years_processed": sorted({item["year"] for item in yearly_summaries}),
                "yearly": yearly_summaries,
            },
            "workflowTrace": workflow_trace,
        }

    def __repr__(self) -> str:
        return "<WorldPopDhis2WorkflowProcessor>"
