"""Reusable feature resolution for DHIS2 selectors or inline GeoJSON."""

from __future__ import annotations

from typing import Any

from pygeoapi.process.base import ProcessorExecuteError
from shapely.geometry import shape

from eo_api.integrations.dhis2_adapter import (
    create_client,
    get_org_unit_geojson,
    get_org_unit_subtree_geojson,
    get_org_units_geojson,
)
from eo_api.routers.ogcapi.plugins.processes.schemas import FeatureFetchInput


def ensure_feature_collection(maybe_fc: dict[str, Any]) -> list[dict[str, Any]]:
    """Validate and return GeoJSON FeatureCollection features."""
    if maybe_fc.get("type") != "FeatureCollection":
        raise ProcessorExecuteError("features_geojson must be a GeoJSON FeatureCollection")
    features = maybe_fc.get("features", [])
    if not isinstance(features, list):
        raise ProcessorExecuteError("features_geojson.features must be an array")
    return features


def feature_org_unit_id(feature: dict[str, Any], id_property: str) -> str | None:
    """Extract org unit id from feature.id or configured property key."""
    if feature.get("id"):
        return str(feature["id"])
    props = feature.get("properties") or {}
    if id_property in props and props[id_property] is not None:
        return str(props[id_property])
    return None


def fetch_features_from_dhis2(inputs: FeatureFetchInput) -> list[dict[str, Any]]:
    """Fetch org-unit features from DHIS2 based on selector inputs."""
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
                return ensure_feature_collection(fc)
            fc = get_org_unit_subtree_geojson(client, inputs.parent_org_unit)
            return ensure_feature_collection(fc)

        if inputs.org_unit_level:
            fc = get_org_units_geojson(client, level=inputs.org_unit_level)
            return ensure_feature_collection(fc)

        raise ProcessorExecuteError("Provide one of: features_geojson, org_unit_ids, parent_org_unit, org_unit_level")
    finally:
        client.close()


def features_union_bbox(features: list[dict[str, Any]]) -> tuple[float, float, float, float]:
    """Compute union bbox over feature geometries."""
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


def resolve_features(inputs: FeatureFetchInput) -> dict[str, Any]:
    """Resolve features from inline GeoJSON or DHIS2 selectors."""
    if inputs.features_geojson:
        features = ensure_feature_collection(inputs.features_geojson)
    else:
        features = fetch_features_from_dhis2(inputs)

    valid_features: list[dict[str, Any]] = []
    for feature in features:
        geometry = feature.get("geometry")
        if not geometry:
            continue
        org_unit_id = feature_org_unit_id(feature, inputs.org_unit_id_property)
        if not org_unit_id:
            continue
        valid_features.append({"orgUnit": org_unit_id, "geometry": geometry})

    if not valid_features:
        raise ProcessorExecuteError("No valid features with geometry and org unit identifiers were found")

    union_bbox = features_union_bbox([{"geometry": f["geometry"]} for f in valid_features])
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

    return {"valid_features": valid_features, "effective_bbox": effective_bbox}
