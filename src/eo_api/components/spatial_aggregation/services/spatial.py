"""Spatial aggregation component for gridded datasets."""

from __future__ import annotations

from typing import Any

import numpy as np
import xarray as xr
from shapely import contains_xy
from shapely.geometry import shape

from ...data_manager.services.utils import get_lon_lat_dims, get_time_dim
from .features import feature_id


def aggregate_to_features(
    ds: xr.Dataset,
    *,
    variable: str,
    features: dict[str, Any],
    method: str,
    feature_id_property: str,
) -> list[dict[str, Any]]:
    """Aggregate one gridded variable into per-feature time series."""
    da = ds[variable]
    time_dim = get_time_dim(da)
    lon_dim, lat_dim = get_lon_lat_dims(da)
    lon_values = da[lon_dim].values
    lat_values = da[lat_dim].values
    lon_grid, lat_grid = np.meshgrid(lon_values, lat_values)

    output: list[dict[str, Any]] = []
    for feature in features.get("features", []):
        geom = shape(feature["geometry"])
        mask = contains_xy(geom, lon_grid, lat_grid)
        if not np.any(mask):
            continue

        mask_da = xr.DataArray(
            mask,
            dims=(lat_dim, lon_dim),
            coords={lat_dim: da[lat_dim], lon_dim: da[lon_dim]},
        )
        reduced = getattr(da.where(mask_da), method)(dim=[lat_dim, lon_dim], skipna=True)
        org_unit = feature_id(feature, feature_id_property)
        for t, value in zip(reduced[time_dim].values, reduced.values, strict=True):
            if np.isnan(value):
                continue
            output.append(
                {
                    "org_unit": org_unit,
                    "time": t,
                    "value": float(value),
                }
            )
    return output


def spatial_aggregation_component(
    *,
    dataset: dict[str, Any],
    start: str,
    end: str,
    bbox: list[float] | None,
    features: dict[str, Any],
    method: AggregationMethod,
    feature_id_property: str,
) -> list[dict[str, Any]]:
    """Load dataset and aggregate spatially to provided features."""
    ds = get_data(dataset=dataset, start=start, end=end, bbox=bbox)
    return aggregate_to_features(
        ds=ds,
        variable=dataset["variable"],
        features=features,
        method=method.value,
        feature_id_property=feature_id_property,
    )


# from workflows engine
def _run_spatial_aggregation(
    *,
    runtime: WorkflowRuntime,
    request: WorkflowExecuteRequest,
    dataset: dict[str, Any],
    context: dict[str, Any],
    step_config: dict[str, Any],
) -> dict[str, Any]:
    method = AggregationMethod(str(step_config.get("method", request.spatial_aggregation.method)))
    feature_id_property = str(step_config.get("feature_id_property", request.dhis2.org_unit_property))
    records = runtime.run(
        "spatial_aggregation",
        component_services.spatial_aggregation_component,
        dataset=dataset,
        start=request.start,
        end=request.end,
        bbox=_require_context(context, "bbox"),
        features=_require_context(context, "features"),
        method=method,
        feature_id_property=feature_id_property,
    )
    return {"records": records}
