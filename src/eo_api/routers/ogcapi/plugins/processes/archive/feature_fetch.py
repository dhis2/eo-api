"""Fetch DHIS2/GeoJSON features for CHIRPS3 workflow."""

from __future__ import annotations

from typing import Any

from pydantic import ValidationError
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from eo_api.integrations.feature_fetch import resolve_features
from eo_api.routers.ogcapi.plugins.processes.schemas import FeatureFetchInput

PROCESS_METADATA = {
    "version": "0.1.0",
    "id": "feature-fetch",
    "title": "Feature fetch",
    "description": "Fetch and normalize org-unit features from inline GeoJSON or DHIS2 selectors.",
    "jobControlOptions": ["sync-execute"],
    "keywords": ["dhis2", "features", "geojson", "orgunits"],
    "inputs": {
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
    },
    "outputs": {
        "result": {
            "title": "Normalized features and effective bbox",
            "schema": {"type": "object", "contentMediaType": "application/json"},
        }
    },
}


class FeatureFetchProcessor(BaseProcessor):
    """Process wrapper for workflow feature-fetch step."""

    def __init__(self, processor_def: dict[str, Any]) -> None:
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data: dict[str, Any], outputs: Any = None) -> tuple[str, dict[str, Any]]:
        try:
            inputs = FeatureFetchInput.model_validate(data)
        except ValidationError as err:
            raise ProcessorExecuteError(str(err)) from err

        return "application/json", resolve_features(inputs)

    def __repr__(self) -> str:
        return "<FeatureFetchProcessor>"
