
from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from starlette.background import BackgroundTask

import constants
from . import registry
from . import cache
from . import raster
from . import units
from . import serialize

router = APIRouter()

@router.get("/")
def list_datasets():
    """
    Returned list of available datasets from registry.
    """
    datasets = registry.list_datasets()
    return datasets

def get_dataset_or_404(dataset_id: str):
    dataset = registry.get_dataset(dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    return dataset

@router.get("/{dataset_id}", response_model=dict)
def get_dataset(dataset_id: str):
    """
    Get a single dataset by ID.
    """
    dataset = get_dataset_or_404(dataset_id)
    cache_info = cache.get_cache_info(dataset)
    dataset.update(cache_info)
    return dataset

@router.get("/{dataset_id}/build_cache", response_model=dict)
def build_dataset_cache(dataset_id: str, start: str, end: str | None = None, overwrite: bool = False, background_tasks: BackgroundTasks = None):
    """
    Download and cache dataset.
    """
    dataset = get_dataset_or_404(dataset_id)
    cache.build_dataset_cache(dataset, start=start, end=end, overwrite=overwrite, background_tasks=background_tasks)
    return {'status': 'Dataset caching request submitted for processing'}

@router.get("/{dataset_id}/{period_type}/orgunits", response_model=list)
def get_dataset_period_type_org_units(dataset_id: str, period_type: str, start: str, end: str, temporal_aggregation: str, spatial_aggregation: str):
    """
    Get a dataset dynamically aggregated to a given period type and org units and return json values.
    """
    # get dataset metadata
    dataset = get_dataset_or_404(dataset_id)
    
    # get raster data
    ds = raster.get_data(dataset, start, end)

    # aggregate to period type
    ds = raster.to_timeperiod(ds, dataset, period_type, statistic=temporal_aggregation)

    # aggregate to geojson features
    df = raster.to_features(ds, dataset, features=constants.ORG_UNITS_GEOJSON, statistic=spatial_aggregation)

    # convert units if needed (inplace)
    # NOTE: here we do it after agggregation to dataframe to speedup computation
    units.convert_pandas_units(df, dataset)

    # serialize to json
    data = serialize.dataframe_to_json_data(df, dataset, period_type)
    return data

@router.get("/{dataset_id}/{period_type}/raster")
def get_dataset_period_type_raster(dataset_id: str, period_type: str, start: str, end: str, temporal_aggregation: str):
    """
    Get a dataset dynamically aggregated to a given period type and return as downloadable raster file.
    """
    # get dataset metadata
    dataset = get_dataset_or_404(dataset_id)
    
    # get raster data
    ds = raster.get_data(dataset, start, end)

    # aggregate to period type
    ds = raster.to_timeperiod(ds, dataset, period_type, statistic=temporal_aggregation)

    # convert units if needed (inplace)
    units.convert_xarray_units(ds, dataset)

    # serialize to temporary netcdf
    file_path = serialize.xarray_to_temporary_netcdf(ds)

    # return as streaming file and delete after completion
    return FileResponse(
        file_path,
        media_type="application/x-netcdf",
        filename='eo-api-raster-download.nc',
        background=BackgroundTask(serialize.cleanup_file, file_path)
    )

@router.get("/{dataset_id}/{period_type}/tiles")
def get_dataset_period_type_tiles(dataset_id: str, period_type: str, start: str, end: str, temporal_aggregation: str):
    pass
