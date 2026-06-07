"""aggregate_spatial — zonal statistics plugin process."""

from __future__ import annotations

from typing import Any, Callable

import numpy as np
import xarray as xr

from open_climate_service.process import process


def _parse_geometries(geometries: Any) -> list[Any]:
    """Extract a flat list of Shapely geometries from GeoJSON input."""
    from shapely.geometry import shape

    if isinstance(geometries, dict):
        gtype = geometries.get("type", "")
        if gtype == "FeatureCollection":
            return [shape(f["geometry"]) for f in geometries.get("features", [])]
        if gtype == "Feature":
            return [shape(geometries["geometry"])]
        return [shape(geometries)]
    return [shape(g) if isinstance(g, dict) else g for g in geometries]


def _find_dim(data: xr.Dataset | xr.DataArray, candidates: list[str]) -> str | None:
    dims = data.dims if isinstance(data, xr.DataArray) else set(data.dims)
    for c in candidates:
        if c in dims:
            return c
    return None


def _dataset_reduce_spatial(
    ds: xr.Dataset,
    mask: np.ndarray,
    reducer: Callable,
    y_dim: str,
    x_dim: str,
    t_dim: str | None,
) -> xr.Dataset:
    """Apply spatial mask and reducer to each variable in a Dataset.

    Iterates over time steps so the reducer receives a 1-D array of pixel values
    (matching the OpenEO reducer contract: array-in, scalar-out).
    """
    # Build mask DataArray aligned to y/x coords
    y_coords = ds[y_dim].values
    x_coords = ds[x_dim].values
    mask_da = xr.DataArray(mask, dims=[y_dim, x_dim], coords={y_dim: y_coords, x_dim: x_coords})
    masked = ds.where(mask_da)

    if t_dim is None or t_dim not in ds.dims:
        # No temporal dimension — reduce directly
        result_vars: dict[str, Any] = {}
        for var in ds.data_vars:
            vname = str(var)
            pixels = masked[vname].values[mask]
            pixels = pixels[~np.isnan(pixels)]
            val = reducer(data=pixels)
            result_vars[vname] = xr.DataArray(float(val))
        return xr.Dataset(result_vars)

    t_vals = ds[t_dim].values
    rows: list[dict[str, float]] = []
    for t_val in t_vals:
        row: dict[str, float] = {}
        for var in ds.data_vars:
            vname = str(var)
            slice_2d = masked[vname].sel({t_dim: t_val}).values
            pixels = slice_2d[mask]
            pixels = pixels[~np.isnan(pixels)]
            row[vname] = float(reducer(data=pixels)) if pixels.size else float("nan")
        rows.append(row)

    var_names = [str(v) for v in ds.data_vars]
    return xr.Dataset(
        {v: xr.DataArray([row[v] for row in rows], coords={t_dim: t_vals}, dims=[t_dim]) for v in var_names}
    )


@process(
    summary="Aggregate spatial data within geometries",
    parameters={
        "data": {"description": "A raster data cube."},
        "geometries": {"description": "GeoJSON FeatureCollection, Feature, or geometry."},
        "reducer": {"description": "A reducer to apply on the pixel values."},
        "target_dimension": {"description": "Name for the new geometry dimension (default: 'geometry')."},
        "context": {"description": "Optional context passed to the reducer."},
    },
)
def aggregate_spatial(
    data: Any,
    geometries: Any,
    reducer: Callable,
    target_dimension: str | None = None,
    context: Any = None,
) -> xr.Dataset:
    """Aggregate raster values within each polygon using the supplied reducer."""
    import rasterio.features
    from rasterio.transform import from_bounds
    from shapely.geometry import mapping

    geom_shapes = _parse_geometries(geometries)
    if not geom_shapes:
        raise ValueError("aggregate_spatial: geometries contains no shapes")

    # Promote DataArray to Dataset so we can handle both uniformly
    if isinstance(data, xr.DataArray):
        name = data.name or "data"
        data = data.to_dataset(name=name)

    x_dim = _find_dim(data, ["x", "longitude", "lon"])
    y_dim = _find_dim(data, ["y", "latitude", "lat"])
    t_dim = _find_dim(data, ["t", "time"])
    if x_dim is None or y_dim is None:
        raise ValueError(f"aggregate_spatial: cannot identify x/y dimensions in {list(data.dims)}")

    x_coords = data[x_dim].values.astype(float)
    y_coords = data[y_dim].values.astype(float)
    height = len(y_coords)
    width = len(x_coords)

    dx = float(abs(x_coords[1] - x_coords[0])) if width > 1 else 1.0
    dy = float(abs(y_coords[1] - y_coords[0])) if height > 1 else 1.0
    xmin = float(x_coords.min()) - dx / 2
    xmax = float(x_coords.max()) + dx / 2
    ymin = float(y_coords.min()) - dy / 2
    ymax = float(y_coords.max()) + dy / 2
    transform = from_bounds(xmin, ymin, xmax, ymax, width, height)

    geom_dim = target_dimension or "geometry"
    results: list[xr.Dataset] = []
    geom_labels: list[str] = []

    for geom in geom_shapes:
        mask = rasterio.features.geometry_mask(
            [mapping(geom)],
            out_shape=(height, width),
            transform=transform,
            invert=True,
        )
        # rasterio builds the mask top-row first (y descending); flip when y is ascending
        if height > 1 and float(y_coords[1]) > float(y_coords[0]):
            mask = mask[::-1]

        geom_ds = _dataset_reduce_spatial(data, mask, reducer, y_dim, x_dim, t_dim)
        results.append(geom_ds)
        geom_labels.append(str(mapping(geom)))

    combined = xr.concat(results, dim=geom_dim)
    combined[geom_dim] = geom_labels
    return combined
