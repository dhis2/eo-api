"""Build OGC-ready publication assets from workflow execution context."""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

import numpy as np

from ...data_manager.services.downloader import DOWNLOAD_DIR
from ..schemas import PeriodType
from .features import feature_id


def build_feature_collection_asset(
    *,
    dataset_id: str,
    features: dict[str, Any],
    records: list[dict[str, Any]],
    period_type: PeriodType,
    feature_id_property: str = "id",
) -> str:
    """Write a GeoJSON FeatureCollection derived from workflow records and features."""
    features_by_id = {feature_id(feature, feature_id_property): feature for feature in features.get("features", [])}
    output_features: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        org_unit = str(record["org_unit"])
        source_feature = features_by_id.get(org_unit)
        if source_feature is None:
            continue
        properties = source_feature.get("properties", {})
        output_features.append(
            {
                "type": "Feature",
                "id": f"{org_unit}-{record['time']}-{index}",
                "geometry": source_feature.get("geometry"),
                "properties": {
                    "org_unit": org_unit,
                    "org_unit_name": _org_unit_name(properties),
                    "period": _format_period(record["time"], period_type),
                    "value": record["value"],
                },
            }
        )

    collection = {"type": "FeatureCollection", "features": output_features}
    return _write_feature_collection(collection=collection, dataset_id=dataset_id)


def _write_feature_collection(*, collection: dict[str, Any], dataset_id: str) -> str:
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = DOWNLOAD_DIR / f"{dataset_id}_feature_collection_{now}.geojson"
    path.write_text(json.dumps(collection, indent=2), encoding="utf-8")
    return str(path)


def _format_period(time_value: Any, period_type: PeriodType) -> str:
    ts = np.datetime64(time_value)
    s = np.datetime_as_string(ts, unit="D")
    year, month, day = s.split("-")
    if period_type == PeriodType.DAILY:
        return f"{year}-{month}-{day}"
    if period_type == PeriodType.MONTHLY:
        return f"{year}-{month}"
    if period_type == PeriodType.YEARLY:
        return year
    if period_type == PeriodType.HOURLY:
        return np.datetime_as_string(ts, unit="h")
    return s


def _org_unit_name(properties: dict[str, Any]) -> str | None:
    for key in ("name", "displayName", "org_unit_name"):
        value = properties.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None
