import atexit
import importlib
import inspect
import logging
import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import xarray as xr
import numpy as np

from . import registry
from .utils import get_time_dim, get_lon_lat_dims, numpy_period_string
from constants import BBOX, COUNTRY_CODE

# logger
logger = logging.getLogger(__name__)

# paths
SCRIPT_DIR = Path(__file__).parent.resolve()
CACHE_DIR = SCRIPT_DIR / 'cache'

def build_dataset_cache(dataset, start, end, overwrite, background_tasks):
    # get download function
    cache_info = dataset['cacheInfo']
    eo_download_func_path = cache_info['eoFunction']
    eo_download_func = get_dynamic_function(eo_download_func_path)
    #logger.info(eo_download_func_path, eo_download_func)

    # construct standard params
    params = cache_info['defaultParams']
    params.update({
        'start': start,
        'end': end or datetime.date.today().isoformat(),  # todays date if empty
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
    #logger.info(params)
    #CACHE_WORKER.submit(eo_download_func, **params)
    background_tasks.add_task(eo_download_func, **params)

def get_cache_info(dataset):
    # find all files with cache prefix
    # TODO: this is not bulletproof, eg 2m_temperature would also get 2m_temperature_max which is a different dataset
    # ...probably need a delimeter to specify end of dataset name... 
    files = get_cache_files(dataset)
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
        coverage=dict(
            temporal = {'start': start, 'end': end},
            spatial = {'xmin': xmin, 'ymin': ymin, 'xmax': xmax, 'ymax': ymax},
        )
    )
    return cache_info

def get_cache_prefix(dataset):
    prefix = dataset['id']
    return prefix

def get_cache_files(dataset):
    prefix = get_cache_prefix(dataset)
    files = list(CACHE_DIR.glob(f'{prefix}*'))
    return files

def get_dynamic_function(full_path):
    # Split the path into: 'dhis2eo.data.cds.era5_land.hourly' and 'function'
    parts = full_path.split('.')
    module_path = ".".join(parts[:-1])
    function_name = parts[-1]

    # This handles all the intermediate sub-package imports automatically
    module = importlib.import_module(module_path)
    
    return getattr(module, function_name)
