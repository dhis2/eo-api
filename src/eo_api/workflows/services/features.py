"""Feature source component for workflow execution."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import geopandas as gpd

from ...shared.dhis2_adapter import create_client, get_org_unit_geojson, get_org_units_geojson
from ..schemas import FeatureSourceConfig, FeatureSourceType


def resolve_features(config: FeatureSourceConfig) -> tuple[dict[str, Any], list[float]]:
    """Resolve features from a source and return FeatureCollection + bbox."""
    if config.source_type == FeatureSourceType.GEOJSON_FILE:
        collection = _read_geojson_file(config.geojson_path or "")
    elif config.source_type == FeatureSourceType.DHIS2_LEVEL:
        client = create_client()
        collection = get_org_units_geojson(client, level=config.dhis2_level, parent=config.dhis2_parent)
    else:
        client = create_client()
        collection = _collection_from_dhis2_ids(client, config.dhis2_ids or [])

    collection = _normalize_feature_collection(collection)
    bbox = _bbox_from_feature_collection(collection)
    return collection, bbox


def feature_id(feature: dict[str, Any], key: str) -> str:
    """Get feature identifier from properties, feature id, or UID fallbacks."""
    properties = feature.get("properties", {})
    value = properties.get(key) or feature.get("id") or properties.get("id") or properties.get("uid")
    if value is None:
        raise ValueError(f"Unable to find feature identifier using key '{key}'")
    return str(value)


def _read_geojson_file(path: str) -> dict[str, Any]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return _normalize_feature_collection(raw)


def _collection_from_dhis2_ids(client: Any, ou_ids: list[str]) -> dict[str, Any]:
    features: list[dict[str, Any]] = []
    for uid in ou_ids:
        unit_geojson = get_org_unit_geojson(client, uid)
        normalized = _normalize_feature_collection(unit_geojson)
        features.extend(normalized["features"])
    return {"type": "FeatureCollection", "features": features}


def _normalize_feature_collection(raw: dict[str, Any]) -> dict[str, Any]:
    raw_type = raw.get("type")
    if raw_type == "FeatureCollection":
        return raw
    if raw_type == "Feature":
        return {"type": "FeatureCollection", "features": [raw]}
    if "features" in raw and isinstance(raw["features"], list):
        return {"type": "FeatureCollection", "features": raw["features"]}
    raise ValueError("Input is not a valid GeoJSON feature or feature collection")


def _bbox_from_feature_collection(collection: dict[str, Any]) -> list[float]:
    if not collection.get("features"):
        raise ValueError("Feature collection is empty")
    bounds = gpd.read_file(json.dumps(collection)).total_bounds
    return [float(v) for v in bounds]
