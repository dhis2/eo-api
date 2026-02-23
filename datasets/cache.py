from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from dhis2eo.data.cds import era5_land

from . import registry

SCRIPT_DIR = Path(__file__).parent.resolve()
CACHE_DIR = SCRIPT_DIR / 'cache'

CACHE_WORKER = ThreadPoolExecutor(max_workers=1)

BBOX = [-13.25, 6.79, -10.23, 10.05]

def cache_dataset(dataset_id, start, end, variables):
    # get dataset
    dataset = registry.get_dataset(dataset_id)

    # get download function
    eo_download_name = dataset['eoDownloader']
    eo_download_func = era5_land.hourly.download #getattr(dhis2eo.data, eo_download_name)

    # construct params
    params = {
        'start': start,
        'end': end,
        'bbox': BBOX,
        'variables': variables.split(','),
        'dirname': CACHE_DIR,
        'prefix': dataset['id'],
    }

    # execute the download (blocking)
    CACHE_WORKER.submit(eo_download_func, **params)
