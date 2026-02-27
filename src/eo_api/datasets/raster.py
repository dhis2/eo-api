"""Raster data loading, temporal aggregation, and spatial feature extraction."""

import json
import logging
from typing import Any

import geopandas as gpd
import pandas as pd
import xarray as xr
from earthkit import transforms

from . import cache, preprocess
from .utils import get_time_dim

logger = logging.getLogger(__name__)


def get_data(dataset: dict[str, Any], start: str, end: str) -> xr.Dataset:
    """Load an xarray raster dataset for the given time range."""
    logger.info("Opening dataset")
    zarr_path = cache.get_zarr_path(dataset)
    if zarr_path:
        logger.info(f'Using optimized zarr file: {zarr_path}')
        ds = xr.open_zarr(zarr_path, consolidated=True)
    else:
        logger.warning(
            f"Could not find optimized zarr file for dataset {dataset['id']}, using slower netcdf files instead."
        )
        files = cache.get_cache_files(dataset)
        ds = xr.open_mfdataset(
            files,
            data_vars="minimal",
            coords="minimal",  # pyright: ignore[reportArgumentType]
            compat="override",
        )

    logger.info(f"Subsetting time to {start} and {end}")
    time_dim = get_time_dim(ds)
    ds = ds.sel(**{time_dim: slice(start, end)})  # pyright: ignore[reportArgumentType]

    for prep_name in dataset.get("preProcess", []):
        prep_func = getattr(preprocess, prep_name)
        ds = prep_func(ds)

    return ds  # type: ignore[no-any-return]


def to_timeperiod(
    ds: xr.Dataset,
    dataset: dict[str, Any],
    period_type: str,
    statistic: str,
    timezone_offset: int = 0,
) -> xr.Dataset:
    """Aggregate an xarray dataset to another period type."""
    valid_period_types = ["hourly", "daily", "monthly", "yearly"]
    if period_type not in valid_period_types:
        raise ValueError(f"Period type not supported: {period_type}")

    if dataset["periodType"] == period_type:
        return ds

    logger.info(f"Aggregating period type from {dataset['periodType']} to {period_type}")

    varname = dataset["variable"]
    arr = ds[varname]

    time_dim = get_time_dim(ds)
    valid = arr.isel({time_dim: 0}).notnull()

    if dataset["periodType"] == "hourly":
        if period_type == "daily":
            arr = transforms.temporal.daily_reduce(
                arr,
                how=statistic,
                time_shift={"hours": timezone_offset},
                remove_partial_periods=False,
            )
        elif period_type == "monthly":
            arr = transforms.temporal.monthly_reduce(
                arr,
                how=statistic,
                time_shift={"hours": timezone_offset},
                remove_partial_periods=False,
            )
        else:
            raise ValueError(f"Unsupported period aggregation from {dataset['periodType']} to {period_type}")

    elif dataset["periodType"] == "daily":
        if period_type == "monthly":
            arr = transforms.temporal.monthly_reduce(
                arr,
                how=statistic,
                remove_partial_periods=False,
            )
        else:
            raise ValueError(f"Unsupported period aggregation from {dataset['periodType']} to {period_type}")

    else:
        raise ValueError(f"Unsupported period aggregation from {dataset['periodType']} to {period_type}")

    arr = xr.where(valid, arr, None)
    arr = arr.compute()
    ds = arr.to_dataset()

    return ds


def to_features(
    ds: xr.Dataset,
    dataset: dict[str, Any],
    features: dict[str, Any],
    statistic: str,
) -> pd.DataFrame:
    """Aggregate an xarray dataset to GeoJSON features and return a DataFrame."""
    logger.info("Aggregating to org units")

    gdf = gpd.read_file(json.dumps(features))

    varname = dataset["variable"]
    ds_reduced = transforms.spatial.reduce(
        ds[varname],
        gdf,
        mask_dim="id",  # TODO: DONT HARDCODE
        how=statistic,
    )

    return ds_reduced.to_dataframe().reset_index()  # type: ignore[no-any-return]
