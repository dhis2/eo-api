"""Loading raster data from downloaded files into xarray."""

import logging
import os
import tempfile
from typing import Any

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
