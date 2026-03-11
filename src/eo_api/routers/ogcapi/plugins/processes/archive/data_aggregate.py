"""Aggregate CHIRPS3 files over workflow features."""

from __future__ import annotations

import os
from typing import Any

from pydantic import ValidationError
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from eo_api.integrations.components.services.temporal_aggregate_service import aggregate_gridded_time_rows_by_features
from eo_api.routers.ogcapi.plugins.processes.schemas import DataAggregateInput

DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/tmp/data")

PROCESS_METADATA = {
    "version": "0.1.0",
    "id": "data-aggregate",
    "title": "Data aggregate",
    "description": "Aggregate downloaded raster files over normalized features.",
    "jobControlOptions": ["sync-execute"],
    "keywords": ["aggregation", "zonal", "timeseries"],
    "inputs": {
        "start_date": {"schema": {"type": "string"}, "minOccurs": 1, "maxOccurs": 1},
        "end_date": {"schema": {"type": "string"}, "minOccurs": 1, "maxOccurs": 1},
        "files": {"schema": {"type": "array", "items": {"type": "string"}}, "minOccurs": 1, "maxOccurs": 1},
        "valid_features": {"schema": {"type": "array", "items": {"type": "object"}}, "minOccurs": 1, "maxOccurs": 1},
        "stage": {"schema": {"type": "string", "enum": ["final", "prelim"], "default": "final"}},
        "flavor": {"schema": {"type": "string", "enum": ["rnl", "sat"], "default": "rnl"}},
        "spatial_reducer": {"schema": {"type": "string", "enum": ["mean", "sum"], "default": "mean"}},
        "temporal_resolution": {
            "schema": {"type": "string", "enum": ["daily", "weekly", "monthly"], "default": "monthly"}
        },
        "temporal_reducer": {"schema": {"type": "string", "enum": ["sum", "mean"], "default": "sum"}},
        "value_rounding": {"schema": {"type": "integer", "minimum": 0, "maximum": 10, "default": 3}},
    },
    "outputs": {
        "result": {
            "title": "Aggregated rows",
            "schema": {"type": "object", "contentMediaType": "application/json"},
        }
    },
}


class DataAggregateProcessor(BaseProcessor):
    """Process wrapper for workflow aggregation step."""

    def __init__(self, processor_def: dict[str, Any]) -> None:
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data: dict[str, Any], outputs: Any = None) -> tuple[str, dict[str, Any]]:
        try:
            inputs = DataAggregateInput.model_validate(data)
        except ValidationError as err:
            raise ProcessorExecuteError(str(err)) from err

        result = aggregate_gridded_time_rows_by_features(
            files=inputs.files,
            valid_features=inputs.valid_features,
            start_date=str(inputs.start_date),
            end_date=str(inputs.end_date),
            spatial_reducer=inputs.spatial_reducer,
            temporal_resolution=inputs.temporal_resolution,
            temporal_reducer=inputs.temporal_reducer,
            value_rounding=inputs.value_rounding,
            cache_root=DOWNLOAD_DIR,
            preferred_var="precip",
            stage=inputs.stage,
            flavor=inputs.flavor,
        )

        return "application/json", result

    def __repr__(self) -> str:
        return "<DataAggregateProcessor>"
