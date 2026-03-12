"""FastAPI router exposing dataset endpoints."""

from fastapi import APIRouter, BackgroundTasks

from ..data_registry.routes import require_dataset
from .services.download import download_dataset_component
from .schemas.fastapi import DownloadDatasetRunRequest, DownloadDatasetRunResponse

router = APIRouter()


# @router.get("/{dataset_id}/download", response_model=dict)
# def download_dataset(
#     dataset_id: str,
#     start: str,
#     background_tasks: BackgroundTasks,
#     end: str | None = None,
#     overwrite: bool = False,
# ) -> dict[str, str]:
#     """Download dataset as local netcdf files direct from the source."""
#     dataset = _get_dataset_or_404(dataset_id)
#     downloader.download_dataset(dataset, start=start, end=end, overwrite=overwrite, background_tasks=background_tasks)
#     return {"status": "Downloading data for dataset"}


# @router.get("/{dataset_id}/build_zarr", response_model=dict)
# def build_dataset_zarr(
#     dataset_id: str,
#     background_tasks: BackgroundTasks,
# ) -> dict[str, str]:
#     """Optimize dataset downloads by collecting all files to a single zarr archive."""
#     dataset = _get_dataset_or_404(dataset_id)
#     background_tasks.add_task(downloader.build_dataset_zarr, dataset)
#     return {"status": "Building zarr file from dataset downloads"}


@router.post("/run", response_model=DownloadDatasetRunResponse)
def run_download_dataset(payload: DownloadDatasetRunRequest) -> DownloadDatasetRunResponse:
    """Download dataset files for the selected period/scope."""
    dataset = require_dataset(payload.dataset_id)
    bbox = payload.bbox or BBOX
    download_dataset_component(
        dataset=dataset,
        start=payload.start,
        end=payload.end,
        overwrite=payload.overwrite,
        country_code=payload.country_code,
        bbox=bbox,
    )
    return DownloadDatasetRunResponse(
        status="completed",
        dataset_id=payload.dataset_id,
        start=payload.start,
        end=payload.end,
    )
