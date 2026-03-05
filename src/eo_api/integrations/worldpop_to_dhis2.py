"""WorldPop zonal aggregation to DHIS2 dataValueSet component."""

from __future__ import annotations

from typing import Any

import numpy as np
import rasterio
from rasterio.mask import mask
from rasterio.warp import transform_geom

from eo_api.integrations.dhis2_datavalues import build_data_value_set


def _compute_stat(values: np.ndarray[Any, np.dtype[np.float64]], reducer: str) -> float | None:
    if values.size == 0:
        return None
    if reducer == "sum":
        return float(np.sum(values))
    if reducer == "mean":
        return float(np.mean(values))
    raise ValueError(f"Unsupported reducer '{reducer}'")


def build_worldpop_datavalueset(
    *,
    features_geojson: dict[str, Any],
    raster_path: str,
    year: int,
    data_element: str,
    org_unit_id_property: str = "id",
    reducer: str = "sum",
    category_option_combo: str | None = None,
    attribute_option_combo: str | None = None,
    data_set: str | None = None,
) -> dict[str, Any]:
    """Aggregate raster values by feature and format as DHIS2 dataValueSet."""
    if features_geojson.get("type") != "FeatureCollection":
        raise ValueError("features_geojson must be a GeoJSON FeatureCollection")
    features = features_geojson.get("features")
    if not isinstance(features, list):
        raise ValueError("features_geojson.features must be an array")
    if reducer not in {"sum", "mean"}:
        raise ValueError("reducer must be one of: sum, mean")

    period = f"{year:04d}"
    rows: list[dict[str, Any]] = []

    with rasterio.open(raster_path) as src:
        raster_crs = src.crs
        nodata = src.nodata

        for index, feature in enumerate(features):
            if not isinstance(feature, dict):
                continue
            geometry = feature.get("geometry")
            if not isinstance(geometry, dict):
                continue
            properties = feature.get("properties")
            props = properties if isinstance(properties, dict) else {}

            org_unit = feature.get("id")
            if org_unit is None:
                org_unit = props.get(org_unit_id_property)
            if org_unit is None:
                org_unit = props.get("id")
            if org_unit is None:
                continue

            projected_geometry = geometry
            if raster_crs:
                projected_geometry = transform_geom("EPSG:4326", raster_crs, geometry, precision=12)

            try:
                raster_data, _ = mask(
                    src,
                    [projected_geometry],
                    indexes=1,
                    crop=True,
                    all_touched=False,
                    filled=False,
                )
            except ValueError:
                raster_data = np.ma.array([], dtype=np.float64)

            if isinstance(raster_data, np.ma.MaskedArray):
                values = raster_data.compressed().astype(np.float64)
            else:
                values = np.array(raster_data, dtype=np.float64).ravel()

            if values.size and nodata is not None:
                values = values[~np.isclose(values, float(nodata), equal_nan=True)]

            stat_value = _compute_stat(values, reducer=reducer)
            if stat_value is None:
                continue
            rows.append({"orgUnit": str(org_unit), "period": period, "value": stat_value, "featureIndex": index})

    result = build_data_value_set(
        rows=rows,
        data_element=data_element,
        category_option_combo=category_option_combo,
        attribute_option_combo=attribute_option_combo,
        data_set=data_set,
    )
    result["summary"] = {
        "year": year,
        "reducer": reducer,
        "feature_count": len(features),
        "row_count": len(rows),
        "raster_path": raster_path,
    }
    return result
