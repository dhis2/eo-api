"""aggregate_spatial — zonal statistics plugin process."""

from __future__ import annotations

from typing import Any, Callable

import numpy as np
import xarray as xr

from open_climate_service.process import process


def _parse_geometries(geometries: Any) -> tuple[list[Any], list[str]]:
    """Extract Shapely geometries and stable string labels from GeoJSON input.

    Labels use the feature ``id`` when present, otherwise a sequential integer.
    """
    from shapely.geometry import shape

    if isinstance(geometries, dict):
        gtype = geometries.get("type", "")
        if gtype == "FeatureCollection":
            features = geometries.get("features", [])
            geoms = [shape(f["geometry"]) for f in features]
            labels = [str(f.get("id", i)) for i, f in enumerate(features)]
            return geoms, labels
        if gtype == "Feature":
            return [shape(geometries["geometry"])], [str(geometries.get("id", 0))]
        return [shape(geometries)], ["0"]
    items = [shape(g) if isinstance(g, dict) else g for g in geometries]
    return items, [str(i) for i in range(len(items))]


def _find_dim(data: xr.Dataset | xr.DataArray, candidates: list[str]) -> str | None:
    dims = data.dims if isinstance(data, xr.DataArray) else set(data.dims)
    for c in candidates:
        if c in dims:
            return c
    return None


def _make_reducer_caller(reducer: Callable, context: Any) -> Callable[[np.ndarray], float]:
    """Return a function that applies the reducer, forwarding ``context`` when supported.

    openEO reducers may or may not accept a ``context`` keyword; we inspect the
    signature once so context-aware reducers receive it without breaking the
    common array-only reducers (mean, median, ...).
    """
    import inspect

    pass_context = False
    if context is not None:
        try:
            params = inspect.signature(reducer).parameters
            pass_context = "context" in params or any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())
        except (TypeError, ValueError):
            pass_context = False

    def _call(pixels: np.ndarray) -> float:
        if not pixels.size:
            return float("nan")
        return float(reducer(data=pixels, context=context) if pass_context else reducer(data=pixels))

    return _call


def _dataset_reduce_spatial(
    ds: xr.Dataset,
    mask: np.ndarray,
    reducer: Callable,
    y_dim: str,
    x_dim: str,
    t_dim: str | None,
    context: Any = None,
) -> xr.Dataset:
    """Apply spatial mask and reducer to each variable in a Dataset.

    Iterates over time steps so the reducer receives a 1-D array of pixel values
    (matching the OpenEO reducer contract: array-in, scalar-out).

    Pixels are selected by boolean-indexing the raw arrays with ``mask`` directly
    rather than materialising ``ds.where(mask)`` — the latter allocates a full
    NaN-filled copy of the cube just to throw most of it away.
    """
    reduce = _make_reducer_caller(reducer, context)

    def _drop_nan(pixels: np.ndarray) -> np.ndarray:
        # Only floating arrays can carry NaN; np.isnan raises on integer dtypes.
        return pixels[~np.isnan(pixels)] if np.issubdtype(pixels.dtype, np.floating) else pixels

    if t_dim is None or t_dim not in ds.dims:
        # No temporal dimension — reduce directly
        result_vars: dict[str, Any] = {}
        for var in ds.data_vars:
            vname = str(var)
            pixels = _drop_nan(ds[vname].values[mask])
            result_vars[vname] = xr.DataArray(reduce(pixels))
        return xr.Dataset(result_vars)

    t_vals = ds[t_dim].values
    var_names = [str(v) for v in ds.data_vars]

    # Pre-extract numpy arrays with the time axis first so each time step is a
    # simple row index rather than an xarray label lookup (O(1) vs O(log N)).
    mask_flat = mask.ravel()
    var_arrs: dict[str, np.ndarray] = {}
    for vname in var_names:
        arr = ds[vname].values
        t_axis = list(ds[vname].dims).index(t_dim)
        if t_axis != 0:
            arr = np.moveaxis(arr, t_axis, 0)
        var_arrs[vname] = arr.reshape(len(t_vals), -1)

    rows: list[dict[str, float]] = []
    for t_idx in range(len(t_vals)):
        row: dict[str, float] = {}
        for vname in var_names:
            pixels = _drop_nan(var_arrs[vname][t_idx][mask_flat])
            row[vname] = reduce(pixels)
        rows.append(row)

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

    geom_shapes, geom_labels = _parse_geometries(geometries)
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

        geom_ds = _dataset_reduce_spatial(data, mask, reducer, y_dim, x_dim, t_dim, context)
        results.append(geom_ds)

    combined = xr.concat(results, dim=geom_dim)
    combined[geom_dim] = geom_labels
    return combined
