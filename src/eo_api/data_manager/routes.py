"""FastAPI router exposing dataset endpoints."""

from fastapi import APIRouter, BackgroundTasks

from ..data_registry.routes import _get_dataset_or_404
from .services import downloader

router = APIRouter()


@router.get(
    "/{dataset_id}/download",
    response_model=dict,
    summary="Internal dataset download",
)
def download_dataset(
    dataset_id: str,
    start: str,
    background_tasks: BackgroundTasks,
    end: str | None = None,
    overwrite: bool = False,
) -> dict[str, str]:
    """Internal low-level cache download route kept for compatibility."""
    dataset = _get_dataset_or_404(dataset_id)
    downloader.download_dataset(
        dataset,
        start=start,
        end=end,
        bbox=None,
        country_code=None,
        overwrite=overwrite,
        background_tasks=background_tasks,
    )
    return {"status": "Downloading data for dataset"}


@router.get(
    "/{dataset_id}/build_zarr",
    response_model=dict,
    summary="Internal dataset Zarr build",
)
def build_dataset_zarr(
    dataset_id: str,
    background_tasks: BackgroundTasks,
) -> dict[str, str]:
    """Internal low-level cache optimization route kept for compatibility."""
    dataset = _get_dataset_or_404(dataset_id)
    background_tasks.add_task(downloader.build_dataset_zarr, dataset)
    return {"status": "Building zarr file from dataset downloads"}
