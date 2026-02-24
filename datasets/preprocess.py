import logging

import xarray as xr


# logger
logger = logging.getLogger(__name__)


def deaccumulate_era5(ds_cumul):
    '''Convert ERA5 cumulative hourly data to incremental hourly data'''

    logger.info('Deaccumulating ERA5 dataset')
    # NOTE: this is hardcoded to era5 specific cumulative patterns and varnames
    
    # shift all values to previous hour, so the values don't spill over to the next day
    ds_cumul = ds_cumul.shift(valid_time=-1)
    
    # convert cumulative to diffs
    ds_diffs = ds_cumul.diff(dim='valid_time')
    ds_diffs = ds_diffs.reindex(valid_time=ds_cumul.valid_time)

    # use cumul values where accumulation resets (00:00) and diff everywhere else
    is_reset = ds_cumul['valid_time'].dt.hour == 0
    ds_hourly = xr.where(is_reset, ds_cumul, ds_diffs)

    # return
    return ds_hourly
