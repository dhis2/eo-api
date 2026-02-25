"""Raster operation stubs used by the skeleton process pipeline."""

from pathlib import Path
from typing import Any

X_DIM_CANDIDATES = ("x", "lon", "longitude")
Y_DIM_CANDIDATES = ("y", "lat", "latitude")


def _select_dataarray(dataset: Any, parameter: str) -> Any:
    if parameter in dataset.data_vars:
        return dataset[parameter]

    first_name = next(iter(dataset.data_vars.keys()), None)
    if first_name is None:
        raise RuntimeError("Dataset has no data variables")
    return dataset[first_name]


def _slice_dim(da: Any, dim: str, lower: float, upper: float) -> Any:
    values = da[dim].values
    if len(values) == 0:
        return da
    if values[0] <= values[-1]:
        return da.sel({dim: slice(lower, upper)})
    return da.sel({dim: slice(upper, lower)})


def _clip_to_bbox(da: Any, bbox: tuple[float, float, float, float]) -> Any:
    minx, miny, maxx, maxy = bbox
    x_dim = next((dim for dim in X_DIM_CANDIDATES if dim in da.dims), None)
    y_dim = next((dim for dim in Y_DIM_CANDIDATES if dim in da.dims), None)
    clipped = da

    if x_dim:
        clipped = _slice_dim(clipped, x_dim, minx, maxx)
    if y_dim:
        clipped = _slice_dim(clipped, y_dim, miny, maxy)
    return clipped


def _zonal_value(clipped: Any, aggregation: str) -> float | None:
    loaded = clipped.load()
    if aggregation == "sum":
        value = loaded.sum(skipna=True)
    elif aggregation == "min":
        value = loaded.min(skipna=True)
    elif aggregation == "max":
        value = loaded.max(skipna=True)
    else:
        value = loaded.mean(skipna=True)

    item = value.item() if hasattr(value, "item") else value
    if item is None:
        return None
    try:
        if item != item:  # NaN
            return None
    except Exception:
        pass
    return float(item)


def _apply_aggregation(values: list[float], aggregation: str) -> float | None:
    if not values:
        return None
    if aggregation == "sum":
        return float(sum(values))
    if aggregation == "min":
        return float(min(values))
    if aggregation == "max":
        return float(max(values))
    return float(sum(values) / len(values))


def _point_value(da: Any, lon: float, lat: float) -> float | None:
    x_dim = next((dim for dim in X_DIM_CANDIDATES if dim in da.dims), None)
    y_dim = next((dim for dim in Y_DIM_CANDIDATES if dim in da.dims), None)
    if x_dim is None or y_dim is None:
        return None

    sampled = da.sel({x_dim: lon, y_dim: lat}, method="nearest")
    loaded = sampled.load()
    item = loaded.item() if hasattr(loaded, "item") else loaded
    if item is None:
        return None
    try:
        if item != item:  # NaN
            return None
    except Exception:
        pass
    return float(item)


def zonal_stats_stub(
    *,
    dataset_id: str,
    time_value: str,
    bbox: tuple[float, float, float, float],
    assets: dict[str, list[str]],
    aggregation: str = "mean",
) -> list[dict[str, Any]]:
    """Compute zonal stat rows when possible, with safe fallbacks on read issues."""

    try:
        import xarray as xr
    except ImportError:
        xr = None

    rows: list[dict[str, Any]] = []
    for parameter, files in assets.items():
        existing_files = [str(path) for path in files if Path(path).exists()]
        value: float | None = None
        status = "computed"
        note = None

        if not existing_files:
            status = "missing_assets"
            note = "No readable raster assets found for requested parameter/time."
        elif xr is None:
            status = "missing_dependency"
            note = "xarray is required to compute zonal statistics."
        else:
            try:
                if len(existing_files) == 1:
                    dataset = xr.open_dataset(existing_files[0])
                else:
                    dataset = xr.open_mfdataset(existing_files, combine="by_coords")

                da = _select_dataarray(dataset, parameter)
                clipped = _clip_to_bbox(da, bbox)
                value = _zonal_value(clipped, aggregation)
                if value is None:
                    status = "no_data"
                    note = "No non-null values found in selected AOI/time slice."
            except Exception as exc:
                status = "read_error"
                note = f"Failed to read/compute raster values: {exc}"

        rows.append(
            {
                "dataset_id": dataset_id,
                "parameter": parameter,
                "operation": "zonal_stats",
                "time": time_value,
                "aoi_bbox": list(bbox),
                "asset_count": len(existing_files),
                "stat": aggregation,
                "value": value,
                "status": status,
                "note": note,
            }
        )
    return rows


def point_timeseries_stub(
    *,
    dataset_id: str,
    time_value: str,
    bbox: tuple[float, float, float, float],
    assets: dict[str, list[str]],
) -> list[dict[str, Any]]:
    """Compute point value at AOI centroid when possible, with safe fallbacks."""

    try:
        import xarray as xr
    except ImportError:
        xr = None

    rows: list[dict[str, Any]] = []
    lon = round((bbox[0] + bbox[2]) / 2.0, 6)
    lat = round((bbox[1] + bbox[3]) / 2.0, 6)

    for parameter, files in assets.items():
        existing_files = [str(path) for path in files if Path(path).exists()]
        value: float | None = None
        status = "computed"
        note = None

        if not existing_files:
            status = "missing_assets"
            note = "No readable raster assets found for requested parameter/time."
        elif xr is None:
            status = "missing_dependency"
            note = "xarray is required to compute point time-series."
        else:
            try:
                dataset = xr.open_dataset(existing_files[0])
                da = _select_dataarray(dataset, parameter)
                value = _point_value(da, lon, lat)
                if value is None:
                    status = "no_data"
                    note = "No non-null value found at sampled point."
            except Exception as exc:
                status = "read_error"
                note = f"Failed to read/compute point value: {exc}"

        rows.append(
            {
                "dataset_id": dataset_id,
                "parameter": parameter,
                "operation": "point_timeseries",
                "time": time_value,
                "point": [lon, lat],
                "asset_count": len(existing_files),
                "value": value,
                "status": status,
                "note": note,
            }
        )
    return rows


def temporal_aggregate_stub(
    *,
    dataset_id: str,
    time_value: str,
    bbox: tuple[float, float, float, float],
    assets: dict[str, list[str]],
    frequency: str,
    aggregation: str,
) -> list[dict[str, Any]]:
    """Compute simple temporal aggregation over per-file spatial means."""

    try:
        import xarray as xr
    except ImportError:
        xr = None

    rows: list[dict[str, Any]] = []
    for parameter, files in assets.items():
        existing_files = [str(path) for path in files if Path(path).exists()]
        sample_values: list[float] = []
        value: float | None = None
        status = "computed"
        note = None

        if not existing_files:
            status = "missing_assets"
            note = "No readable raster assets found for requested parameter/time window."
        elif xr is None:
            status = "missing_dependency"
            note = "xarray is required to compute temporal aggregation."
        else:
            try:
                for file_path in existing_files:
                    dataset = xr.open_dataset(file_path)
                    da = _select_dataarray(dataset, parameter)
                    clipped = _clip_to_bbox(da, bbox)
                    sample = _zonal_value(clipped, "mean")
                    if sample is not None:
                        sample_values.append(sample)
                value = _apply_aggregation(sample_values, aggregation)
                if value is None:
                    status = "no_data"
                    note = "No non-null samples found across files in temporal window."
            except Exception as exc:
                status = "read_error"
                note = f"Failed to read/compute temporal aggregation: {exc}"

        rows.append(
            {
                "dataset_id": dataset_id,
                "parameter": parameter,
                "operation": "temporal_aggregate",
                "source_time": time_value,
                "target_frequency": frequency,
                "aggregation": aggregation,
                "asset_count": len(existing_files),
                "sample_count": len(sample_values),
                "value": value,
                "status": status,
                "note": note,
            }
        )
    return rows
