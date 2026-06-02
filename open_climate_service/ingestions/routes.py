"""Routes for EO ingestion, datasets, and sync operations."""

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from starlette.responses import Response

from open_climate_service.data_registry.routes import _get_dataset_or_404
from open_climate_service.extents.services import get_extent_or_404
from open_climate_service.ingestions import services
from open_climate_service.ingestions.schemas import (
    CreateIngestionRequest,
    DatasetDetailRecord,
    DatasetListResponse,
    IngestionListResponse,
    IngestionResponse,
    SyncDatasetRequest,
    SyncDetail,
    SyncResponse,
)

ingestions_router = APIRouter()
datasets_router = APIRouter()
zarr_router = APIRouter()
sync_router = APIRouter()


@ingestions_router.post("")
def create_ingestion(
    request: CreateIngestionRequest,
) -> IngestionResponse:
    """Create or update a managed dataset from a dataset template and configured extent."""
    dataset = _get_dataset_or_404(request.dataset_id)
    extent = get_extent_or_404()
    resolved_bbox = list(extent["bbox"])
    resolved_country_code = extent.get("country_code")
    artifact = services.create_artifact(
        dataset=dataset,
        start=request.start,
        end=request.end,
        bbox=resolved_bbox,
        country_code=resolved_country_code,
        overwrite=request.overwrite,
        publish=request.publish,
    )
    return IngestionResponse(
        ingestion_id=artifact.artifact_id,
        status="completed",
        dataset=services.get_dataset_summary_for_artifact_or_404(artifact.artifact_id),
    )


@ingestions_router.get("", response_model=IngestionListResponse)
def list_ingestions() -> IngestionListResponse:
    """List ingestion run records for operational and admin use."""
    return services.list_ingestions()


@ingestions_router.get("/{ingestion_id}", response_model=IngestionResponse)
def get_ingestion(ingestion_id: str) -> IngestionResponse:
    """Return the managed dataset view created for a given ingestion."""
    return services.get_ingestion_or_404(ingestion_id)


@datasets_router.get("", response_model=DatasetListResponse)
def list_datasets() -> DatasetListResponse:
    """List managed datasets."""
    return services.list_datasets()


@datasets_router.get("/{dataset_id}", response_model=DatasetDetailRecord)
def get_dataset(dataset_id: str) -> DatasetDetailRecord:
    """Get managed dataset metadata and available versions."""
    return services.get_dataset_or_404(dataset_id)


@datasets_router.get("/{dataset_id}/download")
def download_artifact_file(dataset_id: str) -> FileResponse:
    """Download the primary saved file for a dataset when available."""
    artifact = services.get_latest_artifact_for_dataset_or_404(dataset_id)
    if artifact.path is None or artifact.format.value == "zarr":
        raise HTTPException(
            status_code=409,
            detail="Dataset is not a single downloadable file; use metadata and dataset assets instead",
        )

    media_type = "application/x-netcdf"
    filename = f"{dataset_id}.nc"
    return FileResponse(artifact.path, media_type=media_type, filename=filename)


@zarr_router.api_route("/{dataset_id}", methods=["GET", "HEAD"])
def get_canonical_zarr_store_info(dataset_id: str) -> dict[str, object]:
    """Return canonical Zarr store listing for a managed dataset."""
    return services.get_dataset_zarr_store_info_or_404(dataset_id)


@zarr_router.api_route("/{dataset_id}/{relative_path:path}", methods=["GET", "HEAD"], response_model=None)
def get_canonical_zarr_store_file(dataset_id: str, relative_path: str) -> FileResponse | Response | dict[str, object]:
    """Serve canonical Zarr store content for a managed dataset."""
    return services.get_dataset_zarr_store_file_or_404(dataset_id, relative_path)


@sync_router.post("/{dataset_id}")
def sync_dataset(
    dataset_id: str,
    request: SyncDatasetRequest,
) -> SyncResponse:
    """Sync a managed dataset forward from its latest available time step."""
    return services.sync_dataset(
        dataset_id=dataset_id,
        end=request.end,
        publish=request.publish,
    )


@sync_router.get("/{dataset_id}/plan", response_model=SyncDetail)
def plan_sync_dataset(dataset_id: str, end: str | None = None) -> SyncDetail:
    """Return the sync plan for a managed dataset without starting a download."""
    return services.plan_sync_dataset(dataset_id=dataset_id, end=end)
