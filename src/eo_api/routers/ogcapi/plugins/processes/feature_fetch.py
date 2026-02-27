"""Fetch DHIS2/GeoJSON features for CHIRPS3 workflow."""

from __future__ import annotations

from typing import Any

from pydantic import ValidationError
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from shapely.geometry import shape

from eo_api.integrations.dhis2_adapter import (
    create_client,
    get_org_unit_geojson,
    get_org_unit_subtree_geojson,
    get_org_units_geojson,
)
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


def _ensure_feature_collection(maybe_fc: dict[str, Any]) -> list[dict[str, Any]]:
    if maybe_fc.get("type") != "FeatureCollection":
        raise ProcessorExecuteError("features_geojson must be a GeoJSON FeatureCollection")
    features = maybe_fc.get("features", [])
    if not isinstance(features, list):
        raise ProcessorExecuteError("features_geojson.features must be an array")
    return features


def _feature_org_unit_id(feature: dict[str, Any], id_property: str) -> str | None:
    if feature.get("id"):
        return str(feature["id"])
    props = feature.get("properties") or {}
    if id_property in props and props[id_property] is not None:
        return str(props[id_property])
    return None


def _fetch_features_from_dhis2(inputs: FeatureFetchInput) -> list[dict[str, Any]]:
    client = create_client()
    try:
        if inputs.org_unit_ids:
            features: list[dict[str, Any]] = []
            for uid in inputs.org_unit_ids:
                geo = get_org_unit_geojson(client, uid)
                if geo.get("type") == "Feature":
                    features.append(geo)
                elif geo.get("type") == "FeatureCollection":
                    features.extend(geo.get("features", []))
            return features

        if inputs.parent_org_unit:
            if inputs.org_unit_level:
                fc = get_org_units_geojson(client, level=inputs.org_unit_level, parent=inputs.parent_org_unit)
                return _ensure_feature_collection(fc)
            fc = get_org_unit_subtree_geojson(client, inputs.parent_org_unit)
            return _ensure_feature_collection(fc)

        if inputs.org_unit_level:
            fc = get_org_units_geojson(client, level=inputs.org_unit_level)
            return _ensure_feature_collection(fc)

        raise ProcessorExecuteError("Provide one of: features_geojson, org_unit_ids, parent_org_unit, org_unit_level")
    finally:
        client.close()


def _features_union_bbox(features: list[dict[str, Any]]) -> tuple[float, float, float, float]:
    bounds: list[tuple[float, float, float, float]] = []
    for feature in features:
        geometry = feature.get("geometry")
        if not geometry:
            continue
        geom = shape(geometry)
        if geom.is_empty:
            continue
        bounds.append(geom.bounds)
    if not bounds:
        raise ProcessorExecuteError("No valid geometries found in input features")
    minx = min(b[0] for b in bounds)
    miny = min(b[1] for b in bounds)
    maxx = max(b[2] for b in bounds)
    maxy = max(b[3] for b in bounds)
    return (minx, miny, maxx, maxy)


class FeatureFetchProcessor(BaseProcessor):
    """Process wrapper for workflow feature-fetch step."""

    def __init__(self, processor_def: dict[str, Any]) -> None:
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data: dict[str, Any], outputs: Any = None) -> tuple[str, dict[str, Any]]:
        try:
            inputs = FeatureFetchInput.model_validate(data)
        except ValidationError as err:
            raise ProcessorExecuteError(str(err)) from err

        if inputs.features_geojson:
            features = _ensure_feature_collection(inputs.features_geojson)
        else:
            features = _fetch_features_from_dhis2(inputs)

        valid_features: list[dict[str, Any]] = []
        for feature in features:
            geometry = feature.get("geometry")
            if not geometry:
                continue
            org_unit_id = _feature_org_unit_id(feature, inputs.org_unit_id_property)
            if not org_unit_id:
                continue
            valid_features.append({"orgUnit": org_unit_id, "geometry": geometry})

        if not valid_features:
            raise ProcessorExecuteError("No valid features with geometry and org unit identifiers were found")

        union_bbox = _features_union_bbox([{"geometry": f["geometry"]} for f in valid_features])
        effective_bbox: tuple[float, float, float, float]
        if inputs.bbox:
            effective_bbox = (
                float(inputs.bbox[0]),
                float(inputs.bbox[1]),
                float(inputs.bbox[2]),
                float(inputs.bbox[3]),
            )
        else:
            effective_bbox = union_bbox

        return "application/json", {"valid_features": valid_features, "effective_bbox": effective_bbox}

    def __repr__(self) -> str:
        return "<FeatureFetchProcessor>"
