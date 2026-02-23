import atexit
import importlib
import inspect
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import xarray as xr
import numpy as np

from . import registry

# paths
SCRIPT_DIR = Path(__file__).parent.resolve()
CACHE_DIR = SCRIPT_DIR / 'cache'

# TEMPORARY QUEUED BACKGROUND JOB EXECUTOR UNTIL WE GET A PROPER ONE
CACHE_WORKER = ThreadPoolExecutor(max_workers=1)
atexit.register(CACHE_WORKER.shutdown, wait=True, cancel_futures=True)

# get constants for org units bbox and country code (hacky hardcoded for now)
# TODO: these should be defined centrally somewhere or retrieved from DHIS2 connection
BBOX = [-13.25, 6.79, -10.23, 10.05]
COUNTRY_CODE = 'SLE'

def build_dataset_cache(dataset_id, start, end, overwrite):
    # get dataset
    dataset = registry.get_dataset(dataset_id)

    # get download function
    cache_info = dataset['cacheInfo']
    eo_download_func_path = cache_info['eoFunction']
    eo_download_func = get_dynamic_function(eo_download_func_path)
    print(eo_download_func_path, eo_download_func)

    # construct standard params
    params = cache_info['defaultParams']
    params.update({
        'start': start,
        'end': end,
        'dirname': CACHE_DIR,
        'prefix': get_cache_prefix(dataset),
        'overwrite': overwrite,
    })

    # add in varying spatial args
    sig = inspect.signature(eo_download_func)
    if 'bbox' in sig.parameters.keys():
        params['bbox'] = BBOX
    elif 'country_code' in sig.parameters.keys():
        params['country_code'] = COUNTRY_CODE

    # execute the download
    print(params)
    CACHE_WORKER.submit(eo_download_func, **params)

def get_cache_info(dataset):
    # find all files with cache prefix
    # TODO: this is not bulletproof, eg 2m_temperature would also get 2m_temperature_max which is a different dataset
    # ...probably need a delimeter to specify end of dataset name... 
    prefix = get_cache_prefix(dataset)
    files = list(CACHE_DIR.glob(f'{prefix}*'))
    if not files:
        cache_info = dict(
            temporal_coverage = None,
            spatial_coverage = None,
        )
        return cache_info

    # open first of sorted filenames, should be sufficient to get earliest time period
    ds = xr.open_dataset(sorted(files)[0])

    # get dim names
    time_dim = get_time_dim(ds)
    lon_dim, lat_dim = get_lon_lat_dims(ds)

    # get start time
    start = numpy_period_string(ds[time_dim].min().values, dataset['periodType'])

    # get space scope
    xmin,xmax = ds[lon_dim].min().item(), ds[lon_dim].max().item()
    ymin,ymax = ds[lat_dim].min().item(), ds[lat_dim].max().item()

    # open last of sorted filenames, should be sufficient to get latest time period
    ds = xr.open_dataset(sorted(files)[-1])

    # get end time
    end = numpy_period_string(ds[time_dim].max().values, dataset['periodType'])

    # cache info
    cache_info = dict(
        temporal_coverage = {'start': start, 'end': end},
        spatial_coverage = {'xmin': xmin, 'ymin': ymin, 'xmax': xmax, 'ymax': ymax},
    )
    return cache_info

def get_cache_prefix(dataset):
    prefix = dataset['id']
    return prefix

def get_dynamic_function(full_path):
    # Split the path into: 'dhis2eo.data.cds.era5_land.hourly' and 'function'
    parts = full_path.split('.')
    module_path = ".".join(parts[:-1])
    function_name = parts[-1]

    # This handles all the intermediate sub-package imports automatically
    module = importlib.import_module(module_path)
    
    return getattr(module, function_name)


##########
# Helpers

# TODO: these should go in a separate utils module

def get_time_dim(ds):
    # get first available time dim
    time_dim = None
    for time_name in ['valid_time', 'time']:
        if hasattr(ds, time_name):
            time_dim = time_name
            break
    if time_dim is None:
        raise Exception(f'Unable to find time dimension: {ds.coordinates}')
    
    return time_dim
    
def get_lon_lat_dims(ds):
    # get first available spatial dim
    lat_dim = None
    lon_dim = None
    for lon_name,lat_name in [('lon','lat'), ('longitude','latitude'), ('x','y')]:
        if hasattr(ds, lat_name):
            lat_dim = lat_name
            lon_dim = lon_name
            break
    if lat_dim is None:
        raise Exception(f'Unable to find space dimension: {ds.coordinates}')

    return lon_dim, lat_dim

def numpy_period_string(t: np.datetime64, period_type: str) -> str:
    # convert numpy dateime to period string
    s = np.datetime_as_string(t, unit="s")

    if period_type == "hourly":
        return s[:13]        # YYYY-MM-DDTHH

    if period_type == "daily":
        return s[:10]        # YYYY-MM-DD

    if period_type == "monthly":
        return s[:7]         # YYYY-MM

    if period_type == "yearly":
        return s[:4]         # YYYY

    raise ValueError(f"Unknown periodType: {period_type}")
