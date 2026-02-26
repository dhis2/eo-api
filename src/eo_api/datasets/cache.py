import importlib
import inspect
import logging
import datetime
from pathlib import Path

import xarray as xr
import numpy as np

from . import registry
from .utils import get_time_dim, get_lon_lat_dims, numpy_period_string
from .constants import BBOX, COUNTRY_CODE, CACHE_OVERRIDE

# logger
logger = logging.getLogger(__name__)

# paths
SCRIPT_DIR = Path(__file__).parent.resolve()
CACHE_DIR = SCRIPT_DIR / 'cache'
if CACHE_OVERRIDE: 
    CACHE_DIR = Path(CACHE_OVERRIDE)

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
    background_tasks.add_task(eo_download_func, **params)

def optimize_dataset_cache(dataset):
    logger.info(f'Optimizing cache for dataset {dataset["id"]}')

    # open all cache files as xarray
    files = get_cache_files(dataset)
    logger.info(f'Opening {len(files)} files from cache')
    # for fil in files:
    #     d = xr.open_dataset(fil)
    #     print(d)
    # fdsfs
    ds = xr.open_mfdataset(files)

    # trim to only minimal vars and coords
    logger.info('Trimming unnecessary variables and coordinates')
    varname = dataset['variable']
    ds = ds[[varname]]
    keep_coords = [get_time_dim(ds)] + list(get_lon_lat_dims(ds))
    drop_coords = [
        c for c in ds.coords
        if c not in keep_coords
    ]
    ds = ds.drop_vars(drop_coords)

    # determine optimal chunk sizes
    logger.info(f'Determining optimal chunk size for zarr archive')
    ds_autochunk = ds.chunk('auto').unify_chunks()
    # extract the first chunk size for each dimension to force uniformity
    uniform_chunks = {dim: ds_autochunk.chunks[dim][0] for dim in ds_autochunk.dims}
    # override with time space chunks
    time_space_chunks = compute_time_space_chunks(ds, dataset)
    uniform_chunks.update( time_space_chunks )
    logging.info(f'--> {uniform_chunks}')

    # save as zarr
    logger.info(f'Saving to optimized zarr file')
    zarr_path = CACHE_DIR / f'{get_cache_prefix(dataset)}.zarr'
    ds_chunked = ds.chunk(uniform_chunks)
    ds_chunked.to_zarr(zarr_path, mode='w')
    ds_chunked.close()

    logger.info('Finished cache optimization')

def compute_time_space_chunks(ds, dataset, max_spatial_chunk=256):
    chunks = {}

    # time
    # set to common access patterns depending on original dataset period 
    # TODO: could potentially allow this to be customized in the dataset yaml file
    dim = get_time_dim(ds)
    period_type = dataset['periodType']
    if period_type == 'hourly':
        chunks[dim] = 24 * 7
    elif period_type == 'daily':
        chunks[dim] = 30
    elif period_type == 'monthly':
        chunks[dim] = 12
    elif period_type == 'yearly':
        chunks[dim] = 1

    # space
    lon_dim,lat_dim = get_lon_lat_dims(ds)
    chunks[lon_dim] = min(ds.sizes[lon_dim], max_spatial_chunk)
    chunks[lat_dim] = min(ds.sizes[lat_dim], max_spatial_chunk)

    return chunks

def get_cache_info(dataset):
    # find all files with cache prefix
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
    # TODO: this is not bulletproof, eg 2m_temperature might also get another dataset named 2m_temperature_modified
    # ...probably need a delimeter to specify end of dataset name... 
    prefix = get_cache_prefix(dataset)
    files = list(CACHE_DIR.glob(f'{prefix}*.nc'))
    return files

def get_zarr_path(dataset):
    prefix = get_cache_prefix(dataset)
    optimized = CACHE_DIR / f'{prefix}.zarr'
    if optimized.exists():
        return optimized

def get_dynamic_function(full_path):
    # Split the path into: 'dhis2eo.data.cds.era5_land.hourly' and 'function'
    parts = full_path.split('.')
    module_path = ".".join(parts[:-1])
    function_name = parts[-1]

    # This handles all the intermediate sub-package imports automatically
    module = importlib.import_module(module_path)
    
    return getattr(module, function_name)
