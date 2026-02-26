"""FastAPI router exposing dataset endpoints."""

from typing import Any

import xarray as xr
from fastapi import APIRouter, BackgroundTasks, HTTPException, Response
from fastapi.responses import FileResponse
from starlette.background import BackgroundTask

from . import cache, constants, raster, registry, serialize, units

router = APIRouter()


@router.get("/")
def list_datasets() -> list[dict[str, Any]]:
    """Return list of available datasets from registry."""
    return registry.list_datasets()


def _get_dataset_or_404(dataset_id: str) -> dict[str, Any]:
    """Look up a dataset by ID or raise 404."""
    dataset = registry.get_dataset(dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    return dataset


@router.get("/{dataset_id}", response_model=dict)
def get_dataset(dataset_id: str) -> dict[str, Any]:
    """Get a single dataset by ID."""
    dataset = _get_dataset_or_404(dataset_id)
    cache_info = cache.get_cache_info(dataset)
    dataset.update(cache_info)
    return dataset


@router.get("/{dataset_id}/build_cache", response_model=dict)
def build_dataset_cache(
    dataset_id: str,
    start: str,
    end: str | None = None,
    overwrite: bool = False,
    background_tasks: BackgroundTasks | None = None,
) -> dict[str, str]:
    """Download and cache dataset as local netcdf files direct from the source."""
    dataset = _get_dataset_or_404(dataset_id)
    cache.build_dataset_cache(dataset, start=start, end=end, overwrite=overwrite, background_tasks=background_tasks)
    return {"status": "Dataset caching request submitted for processing"}


@router.get("/{dataset_id}/optimize_cache", response_model=dict)
def optimize_dataset_cache(
    dataset_id: str,
    background_tasks: BackgroundTasks | None = None,
) -> dict[str, str]:
    """Optimize dataset cache by collecting all cache files to a single zarr archive."""
    dataset = _get_dataset_or_404(dataset_id)
    if background_tasks is not None:
        background_tasks.add_task(cache.optimize_dataset_cache, dataset)
    return {"status": "Dataset cache optimization submitted for processing"}


def _get_dataset_period_type(
    dataset: dict[str, Any],
    period_type: str,
    start: str,
    end: str,
    temporal_aggregation: str,
) -> xr.Dataset:
    """Load and temporally aggregate a dataset."""
    # TODO: maybe move this and similar somewhere better like a pipelines.py file?
    ds = raster.get_data(dataset, start, end)
    ds = raster.to_timeperiod(ds, dataset, period_type, statistic=temporal_aggregation)
    return ds


@router.get("/{dataset_id}/{period_type}/orgunits", response_model=list)
def get_dataset_period_type_org_units(
    dataset_id: str,
    period_type: str,
    start: str,
    end: str,
    temporal_aggregation: str,
    spatial_aggregation: str,
) -> list[dict[str, Any]]:
    """Get a dataset aggregated to a given period type and org units as JSON values."""
    dataset = _get_dataset_or_404(dataset_id)
    ds = _get_dataset_period_type(dataset, period_type, start, end, temporal_aggregation)

    df = raster.to_features(ds, dataset, features=constants.ORG_UNITS_GEOJSON, statistic=spatial_aggregation)

    # convert units if needed (inplace)
    # NOTE: here we do it after aggregation to dataframe to speedup computation
    units.convert_pandas_units(df, dataset)

    return serialize.dataframe_to_json_data(df, dataset, period_type)


@router.get("/{dataset_id}/{period_type}/orgunits/preview", response_model=list)
def get_dataset_period_type_org_units_preview(
    dataset_id: str,
    period_type: str,
    period: str,
    temporal_aggregation: str,
    spatial_aggregation: str,
) -> Response:
    """Preview a PNG map image of a dataset aggregated to a given period and org units."""
    dataset = _get_dataset_or_404(dataset_id)

    start = end = period
    ds = _get_dataset_period_type(dataset, period_type, start, end, temporal_aggregation)

    df = raster.to_features(ds, dataset, features=constants.ORG_UNITS_GEOJSON, statistic=spatial_aggregation)

    # convert units if needed (inplace)
    # NOTE: here we do it after aggregation to dataframe to speedup computation
    units.convert_pandas_units(df, dataset)

    image_data = serialize.dataframe_to_preview(df, dataset, period_type)
    return Response(content=image_data, media_type="image/png")


@router.get("/{dataset_id}/{period_type}/raster")
def get_dataset_period_type_raster(
    dataset_id: str,
    period_type: str,
    start: str,
    end: str,
    temporal_aggregation: str,
) -> FileResponse:
    """Get a dataset aggregated to a given period type as a downloadable raster file."""
    dataset = _get_dataset_or_404(dataset_id)
    ds = _get_dataset_period_type(dataset, period_type, start, end, temporal_aggregation)

    units.convert_xarray_units(ds, dataset)

    file_path = serialize.xarray_to_temporary_netcdf(ds)
    return FileResponse(
        file_path,
        media_type="application/x-netcdf",
        filename="eo-api-raster-download.nc",
        background=BackgroundTask(serialize.cleanup_file, file_path),
    )


@router.get("/{dataset_id}/{period_type}/raster/preview")
def get_dataset_period_type_raster_preview(
    dataset_id: str,
    period_type: str,
    period: str,
    temporal_aggregation: str,
) -> Response:
    """Preview a PNG map image of a dataset aggregated to a given period."""
    dataset = _get_dataset_or_404(dataset_id)

    start = end = period
    ds = _get_dataset_period_type(dataset, period_type, start, end, temporal_aggregation)

    units.convert_xarray_units(ds, dataset)

    image_data = serialize.xarray_to_preview(ds, dataset, period_type)
    return Response(content=image_data, media_type="image/png")


@router.get("/{dataset_id}/{period_type}/tiles")
def get_dataset_period_type_tiles(
    dataset_id: str,
    period_type: str,
    start: str,
    end: str,
    temporal_aggregation: str,
) -> None:
    """Placeholder for future tile-based dataset access."""
