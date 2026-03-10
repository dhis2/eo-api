"""OGC Process stage wrapper for feature scope resolution."""

from __future__ import annotations

from typing import Any

from eo_api.integrations.components.services.feature_resolver_service import resolve_features
from eo_api.routers.ogcapi.plugins.processes.schemas import FeatureFetchInput


def _to_feature_collection(valid_features: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "id": item["orgUnit"], "geometry": item["geometry"], "properties": {}}
            for item in valid_features
        ],
    }


def run_feature_stage(params: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    """Resolve feature scope and emit both valid features and FeatureCollection."""
    del context
    result = resolve_features(FeatureFetchInput.model_validate(params))
    feature_collection = _to_feature_collection(result["valid_features"])
    return {
        "valid_features": result["valid_features"],
        "effective_bbox": result["effective_bbox"],
        "feature_collection": feature_collection,
    }
