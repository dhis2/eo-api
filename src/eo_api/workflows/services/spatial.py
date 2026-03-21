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
            # Keep component outputs JSON-safe for direct API exposure and remote execution.
            if isinstance(t, np.datetime64):
                time_value: Any = np.datetime_as_string(t, unit="s")
            elif isinstance(t, np.generic):
                time_value = t.item()
            else:
                time_value = t
            output.append(
                {
                    "org_unit": org_unit,
                    "time": time_value,
                    "value": float(value),
                }
            )
    return output
