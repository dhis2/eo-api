
import xarray as xr
from earthkit import transforms

from datasets.cache import get_time_dim

def aggregate(ds, dataset, period_type, start, end, timezone_offset=0):
    varname = dataset['variable']
    agg_method = dataset['aggregation']['temporal']
    time_dim = get_time_dim(ds)

    # remember mask of valid pixels from original dataset (only one time point needed)
    valid = ds[varname].isel({time_dim: 0}).notnull()

    # hourly datasets
    if dataset['periodType'] == 'hourly':
        if period_type == 'daily':
            ds = transforms.temporal.daily_reduce(
                ds[varname],
                how=agg_method,
                time_shift={"hours": timezone_offset},
                remove_partial_periods=False,
            )
        
        elif period_type == 'monthly':
            ds = transforms.temporal.monthly_reduce(
                ds[varname],
                how=agg_method,
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
                how=agg_method,
                remove_partial_periods=False,
            )
        
        else:
            raise Exception(f'Unsupported period aggregation from {dataset["periodType"]} to {period_type}')
        
    # apply the original mask in case the aggregation turned nan values to 0s
    ds = xr.where(valid, ds, None)

    # return
    return ds