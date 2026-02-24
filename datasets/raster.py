import json
import logging

import xarray as xr
import geopandas as gpd
from earthkit import transforms

from . import preprocess
from . import cache

from .utils import get_time_dim


# logger
logger = logging.getLogger(__name__)


def get_data(dataset, start, end):
    '''Get xarray raster dataset for given time range'''
    # load xarray from cache
    logger.info('Accessing dataset')
    files = cache.get_cache_files(dataset)
    ds = xr.open_mfdataset(
        files,
        data_vars="minimal",
        coords="minimal",
        compat="override"
    )

    # subset time dim
    time_dim = get_time_dim(ds)
    ds = ds.sel(**{time_dim: slice(start, end)})

    # apply any preprocessing functions
    for prep_name in dataset.get('preProcess', []):
        prep_func = getattr(preprocess, prep_name)
        ds = prep_func(ds)

    # return
    return ds


def to_timeperiod(ds, dataset, period_type, statistic, timezone_offset=0):
    '''Aggregate given xarray dataset to another period type'''

    # NOTE: This function converts dataset with multiple variables to dataarray for single variable
    # ...so downstream functions have to consider that
    # TODO: Should probably change this.
    varname = dataset['variable']
    time_dim = get_time_dim(ds)

    # validate period types
    valid_period_types = ['hourly', 'daily', 'monthly', 'yearly']
    if period_type not in valid_period_types:
        raise ValueError(f'Period type not supported: {period_type}')
    
    # return early if no change
    if dataset['periodType'] == period_type:
        return ds[varname]

    # begin
    logger.info(f'Aggregating period type from {dataset["periodType"]} to {period_type}')

    # remember mask of valid pixels from original dataset (only one time point needed)
    valid = ds[varname].isel({time_dim: 0}).notnull()

    # hourly datasets
    if dataset['periodType'] == 'hourly':
        if period_type == 'daily':
            ds = transforms.temporal.daily_reduce(
                ds[varname],
                how=statistic,
                time_shift={"hours": timezone_offset},
                remove_partial_periods=False,
            )
        
        elif period_type == 'monthly':
            ds = transforms.temporal.monthly_reduce(
                ds[varname],
                how=statistic,
                time_shift={"hours": timezone_offset},
                remove_partial_periods=False,
            )
        
        else:
            raise Exception(f'Unsupported period aggregation from {dataset["periodType"]} to {period_type}')
    
    # daily datasets
    elif dataset['periodType'] == 'daily':
        if period_type == 'monthly':
            ds = transforms.temporal.monthly_reduce(
                ds[varname],
                how=statistic,
                remove_partial_periods=False,
            )
        
        else:
            raise Exception(f'Unsupported period aggregation from {dataset["periodType"]} to {period_type}')
        
    else:
        raise Exception(f'Unsupported period aggregation from {dataset["periodType"]} to {period_type}')
        
    # apply the original mask in case the aggregation turned nan values to 0s
    ds = xr.where(valid, ds, None)

    # IMPORTANT: compute to avoid slow dask graphs
    ds = ds.compute()

    # convert back to dataset
    ds = ds.to_dataset()

    # return
    return ds


def to_features(ds, dataset, features, statistic):
    '''Aggregate given xarray to geojson features and return pandas dataframe'''

    logger.info('Aggregating to org units')

    # load geojson as geopandas
    gdf = gpd.read_file(json.dumps(features))

    # aggregate
    varname = dataset['variable']
    ds = transforms.spatial.reduce(
        ds[varname],
        gdf,
        mask_dim="id", # TODO: DONT HARDCODE
        how=statistic,
    )

    # convert to df
    df = ds.to_dataframe().reset_index()

    # return
    return df

