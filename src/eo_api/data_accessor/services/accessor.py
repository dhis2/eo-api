"""Loading raster data from downloaded files into xarray."""

import logging
import os
import tempfile
from typing import Any

import numpy as np
import xarray as xr

from ...data_manager.services.downloader import get_cache_files, get_zarr_path
from ...data_manager.services.utils import get_lon_lat_dims, get_time_dim
from ...shared.time import numpy_datetime_to_period_string

logger = logging.getLogger(__name__)


def get_data(
    dataset: dict[str, Any],
    start: str | None = None,
    end: str | None = None,
    bbox: list[float] | None = None,
) -> xr.Dataset:
    """Load an xarray raster dataset for a given time range and bbox."""
    logger.info("Opening dataset")
    zarr_path = get_zarr_path(dataset)
    if zarr_path:
        logger.info(f"Using optimized zarr file: {zarr_path}")
        ds = xr.open_zarr(zarr_path, consolidated=True)
    else:
        logger.warning(
            f"Could not find optimized zarr file for dataset {dataset['id']}, using slower netcdf files instead."
        )
        files = get_cache_files(dataset)
        ds = xr.open_mfdataset(
            files,
            data_vars="minimal",
            coords="minimal",  # pyright: ignore[reportArgumentType]
            compat="override",
        )

    if start and end:
        logger.info(f"Subsetting time to {start} and {end}")
        time_dim = get_time_dim(ds)
        ds = ds.sel(**{time_dim: slice(start, end)})  # pyright: ignore[reportArgumentType]

    if bbox is not None:
        logger.info(f"Subsetting xy to {bbox}")
        xmin, ymin, xmax, ymax = list(map(float, bbox))
        lon_dim, lat_dim = get_lon_lat_dims(ds)
        # TODO: this assumes y axis increases towards north and is not very stable
        # ...and also does not consider partial pixels at the edges
        # ...should probably switch to rioxarray.clip instead
        ds = ds.sel(**{lon_dim: slice(xmin, xmax), lat_dim: slice(ymax, ymin)})  # pyright: ignore[reportArgumentType]

    return ds  # type: ignore[no-any-return]


def get_data_coverage(dataset: dict[str, Any]) -> dict[str, Any]:
    """Return temporal and spatial coverage metadata for downloaded data."""
    ds = get_data(dataset)
    try:
        if not ds:
            return {"temporal_coverage": None, "spatial_coverage": None}

        time_dim = get_time_dim(ds)
        lon_dim, lat_dim = get_lon_lat_dims(ds)

        start = numpy_datetime_to_period_string(ds[time_dim].min(), dataset["period_type"])  # type: ignore[arg-type]
        end = numpy_datetime_to_period_string(ds[time_dim].max(), dataset["period_type"])  # type: ignore[arg-type]

        xmin, xmax = ds[lon_dim].min().item(), ds[lon_dim].max().item()
        ymin, ymax = ds[lat_dim].min().item(), ds[lat_dim].max().item()

        return {
            "coverage": {
                "temporal": {"start": start, "end": end},
                "spatial": {"xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax},
            }
        }
    finally:
        ds.close()


def get_point_values(
    dataset: dict[str, Any],
    *,
    lon: float,
    lat: float,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, Any]:
    """Return dataset values at one point across the requested time range."""
    ds = get_data(dataset, start=start, end=end, bbox=None)
    try:
        if not ds.data_vars:
            raise ValueError(f"Dataset '{dataset['id']}' has no data variables available")

        lon_dim, lat_dim = get_lon_lat_dims(ds)
        time_dim = get_time_dim(ds)
        lon_values = ds[lon_dim]
        lat_values = ds[lat_dim]

        xmin, xmax = float(lon_values.min().item()), float(lon_values.max().item())
        ymin, ymax = float(lat_values.min().item()), float(lat_values.max().item())
        if lon < xmin or lon > xmax or lat < ymin or lat > ymax:
            raise ValueError(
                f"Requested point ({lon}, {lat}) is outside dataset coverage ([{xmin}, {ymin}] to [{xmax}, {ymax}])"
            )

        variable_name = str(dataset.get("variable") or str(next(iter(ds.data_vars))))
        if variable_name not in ds.data_vars:
            variable_name = str(next(iter(ds.data_vars)))
        data_array = ds[variable_name]
        point = data_array.sel({lon_dim: lon, lat_dim: lat}, method="nearest")

        actual_lon = float(point.coords[lon_dim].item())
        actual_lat = float(point.coords[lat_dim].item())
        series: list[dict[str, Any]] = []
        for raw_time, raw_value in zip(point[time_dim].values, point.values.tolist(), strict=False):
            value = _to_float(raw_value)
            series.append(
                {
                    "period": str(numpy_datetime_to_period_string(np.asarray(raw_time), dataset["period_type"])),
                    "value": value,
                }
            )

        if not series:
            raise ValueError(f"Dataset '{dataset['id']}' returned no values for the requested time range")

        return {
            "dataset_id": dataset["id"],
            "variable": variable_name,
            "requested": {"lon": lon, "lat": lat, "start": start, "end": end},
            "resolved_point": {"lon": actual_lon, "lat": actual_lat},
            "value_count": len(series),
            "values": series,
        }
    finally:
        ds.close()


def get_preview_summary(
    dataset: dict[str, Any],
    *,
    start: str | None = None,
    end: str | None = None,
    bbox: list[float] | None = None,
    max_cells: int = 25,
) -> dict[str, Any]:
    """Return summary statistics and a small sample for preview-oriented clients."""
    ds = get_data(dataset, start=start, end=end, bbox=bbox)
    try:
        if not ds.data_vars:
            raise ValueError(f"Dataset '{dataset['id']}' has no data variables available")

        variable_name = str(dataset.get("variable") or str(next(iter(ds.data_vars))))
        if variable_name not in ds.data_vars:
            variable_name = str(next(iter(ds.data_vars)))
        data_array = ds[variable_name]
        lon_dim, lat_dim = get_lon_lat_dims(data_array)
        time_dim = get_time_dim(data_array)

        valid = data_array.where(~xr.apply_ufunc(np.isnan, data_array))
        sample = _build_preview_sample(
            valid,
            dataset=dataset,
            lon_dim=lon_dim,
            lat_dim=lat_dim,
            time_dim=time_dim,
            max_cells=max_cells,
        )

        return {
            "dataset_id": dataset["id"],
            "variable": variable_name,
            "requested": {"start": start, "end": end, "bbox": bbox},
            "dims": {str(k): int(v) for k, v in valid.sizes.items()},
            "stats": {
                "min": _to_float(valid.min(skipna=True).item()),
                "max": _to_float(valid.max(skipna=True).item()),
                "mean": _to_float(valid.mean(skipna=True).item()),
                "value_count": int(valid.count().item()),
            },
            "sample": sample,
        }
    finally:
        ds.close()


def get_coverage_summary(
    dataset: dict[str, Any],
    *,
    start: str | None = None,
    end: str | None = None,
    bbox: list[float] | None = None,
    max_cells: int = 25,
) -> dict[str, Any]:
    """Return a lightweight coverage-style summary for a raster subset."""
    preview = get_preview_summary(
        dataset,
        start=start,
        end=end,
        bbox=bbox,
        max_cells=max_cells,
    )
    full_coverage = get_data_coverage(dataset).get("coverage", {})
    return {
        "dataset_id": preview["dataset_id"],
        "variable": preview["variable"],
        "requested": preview["requested"],
        "coverage": {
            "spatial": full_coverage.get("spatial"),
            "temporal": full_coverage.get("temporal"),
        },
        "subset": {
            "dims": preview["dims"],
            "stats": preview["stats"],
            "sample": preview["sample"],
        },
    }


def xarray_to_temporary_netcdf(ds: xr.Dataset) -> str:
    """Write a dataset to a temporary NetCDF file and return the path."""
    fd = tempfile.NamedTemporaryFile(suffix=".nc", delete=False)
    path = fd.name
    fd.close()
    ds.to_netcdf(path)
    return path


def cleanup_file(path: str) -> None:
    """Remove a file from disk."""
    os.remove(path)


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    scalar = np.asarray(value).item()
    if np.isnan(scalar):
        return None
    return float(scalar)


def _build_preview_sample(
    data_array: xr.DataArray,
    *,
    dataset: dict[str, Any],
    lon_dim: str,
    lat_dim: str,
    time_dim: str,
    max_cells: int,
) -> list[dict[str, Any]]:
    """Build a small JSON-safe sample from a raster subset."""
    max_cells = max(1, max_cells)
    sample_records: list[dict[str, Any]] = []

    time_values = data_array[time_dim].values
    lat_values = data_array[lat_dim].values
    lon_values = data_array[lon_dim].values

    time_step = max(1, int(np.ceil(len(time_values) / max_cells)))
    lat_step = max(1, int(np.ceil(len(lat_values) / max_cells)))
    lon_step = max(1, int(np.ceil(len(lon_values) / max_cells)))

    for time_index in range(0, len(time_values), time_step):
        for lat_index in range(0, len(lat_values), lat_step):
            for lon_index in range(0, len(lon_values), lon_step):
                value = data_array.isel({time_dim: time_index, lat_dim: lat_index, lon_dim: lon_index}).item()
                numeric_value = _to_float(value)
                if numeric_value is None:
                    continue
                sample_records.append(
                    {
                        "period": str(
                            numpy_datetime_to_period_string(
                                np.asarray(time_values[time_index]),
                                dataset["period_type"],
                            )
                        ),
                        "lat": float(lat_values[lat_index]),
                        "lon": float(lon_values[lon_index]),
                        "value": numeric_value,
                    }
                )
                if len(sample_records) >= max_cells:
                    return sample_records
    return sample_records
