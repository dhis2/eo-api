"""Reprojection transform: reproject a dataset to the instance CRS."""

import logging
from typing import Any

import rioxarray  # noqa: F401  # pyright: ignore[reportUnusedImport]  # registers the .rio accessor
import xarray as xr

from open_climate_service import config as api_config

logger = logging.getLogger(__name__)


def reproject_to_instance_crs(
    ds: xr.Dataset,
    dataset: dict[str, Any],
    *,
    source_crs: str = "EPSG:4326",
) -> xr.Dataset:
    """Reproject dataset from source_crs to the instance CRS.

    No-op when the instance CRS matches source_crs (e.g. a WGS84 instance
    running a WGS84 source dataset). Override source_crs via the transform's
    ``params`` dict if your source uses a different input CRS.

    The dataset must already have ``x`` and ``y`` as its spatial dimension names.
    The streaming plugins and local Zarr materialization paths normalize those
    coordinates before transforms are run.
    """
    target_crs = api_config.get_crs()
    if target_crs == source_crs:
        return ds

    varname = dataset["variable"]
    logger.info("Reprojecting '%s' from %s to %s", varname, source_crs, target_crs)

    ds = ds.rio.set_spatial_dims(x_dim="x", y_dim="y")
    ds = ds.rio.write_crs(source_crs)
    reprojected: xr.Dataset = ds.rio.reproject(target_crs)
    return reprojected
