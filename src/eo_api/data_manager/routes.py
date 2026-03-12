"""FastAPI router exposing dataset endpoints."""

from typing import Any

import xarray as xr
from fastapi import APIRouter, BackgroundTasks, HTTPException, Response
from fastapi.responses import FileResponse
from starlette.background import BackgroundTask

from .services import constants, downloader
from ..data_registry.routes import _get_dataset_or_404

router = APIRouter()


@router.get("/{dataset_id}/download", response_model=dict)
def download_dataset(
    dataset_id: str,
    start: str,
    end: str | None = None,
    overwrite: bool = False,
    background_tasks: BackgroundTasks = None,
) -> dict[str, str]:
    """Download dataset as local netcdf files direct from the source."""
    dataset = _get_dataset_or_404(dataset_id)
    downloader.download_dataset(dataset, start=start, end=end, overwrite=overwrite, background_tasks=background_tasks)
    return {"status": "Downloading data for dataset"}


@router.get("/{dataset_id}/build_zarr", response_model=dict)
def build_dataset_zarr(
    dataset_id: str,
    background_tasks: BackgroundTasks = None,
) -> dict[str, str]:
    """Optimize dataset downloads by collecting all files to a single zarr archive."""
    dataset = _get_dataset_or_404(dataset_id)
    if background_tasks is not None:
        background_tasks.add_task(downloader.build_dataset_zarr, dataset)
    return {"status": "Building zarr file from dataset downloads"}
