"""Routes for EO artifact ingestion and artifact access."""

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from starlette.responses import Response

from eo_api.artifacts import services
from eo_api.artifacts.schemas import (
    ArtifactListResponse,
    ArtifactRecord,
    CreateIngestionRequest,
    IngestionResponse,
)
from eo_api.data_registry.routes import _get_dataset_or_404

ingestions_router = APIRouter()
router = APIRouter()


@ingestions_router.post("", response_model=IngestionResponse)
def create_ingestion(request: CreateIngestionRequest) -> IngestionResponse:
    """Create a managed artifact by ingesting a dataset request."""
    dataset = _get_dataset_or_404(request.dataset_id)
    artifact = services.create_artifact(
        dataset=dataset,
        start=request.start,
        end=request.end,
        bbox=list(request.bbox) if request.bbox is not None else None,
        country_code=request.country_code,
        overwrite=request.overwrite,
        prefer_zarr=request.prefer_zarr,
        publish=request.publish,
    )
    return IngestionResponse(ingestion_id=artifact.artifact_id, status="completed", artifact=artifact)


@ingestions_router.get("/{ingestion_id}", response_model=ArtifactRecord)
def get_ingestion(ingestion_id: str) -> ArtifactRecord:
    """Return the artifact record created for a given ingestion."""
    return services.get_artifact_or_404(ingestion_id)


@router.get("", response_model=ArtifactListResponse)
def list_artifacts() -> ArtifactListResponse:
    """List stored artifacts."""
    return services.list_artifacts()


@router.get("/{artifact_id}", response_model=ArtifactRecord)
def get_artifact(artifact_id: str) -> ArtifactRecord:
    """Get stored artifact metadata."""
    return services.get_artifact_or_404(artifact_id)


@router.get("/{artifact_id}/download")
def download_artifact_file(artifact_id: str) -> FileResponse:
    """Download the primary saved file for an artifact when available."""
    artifact = services.get_artifact_or_404(artifact_id)
    if artifact.path is None or artifact.format.value == "zarr":
        raise HTTPException(
            status_code=409,
            detail="Artifact is not a single downloadable file; use metadata and asset_paths instead",
        )

    media_type = "application/x-netcdf"
    filename = f"{artifact.dataset_id}.nc"
    return FileResponse(artifact.path, media_type=media_type, filename=filename)


@router.get("/{artifact_id}/zarr")
def get_zarr_store_info(artifact_id: str) -> dict[str, object]:
    """Return Zarr store metadata and top-level entries for a Zarr artifact."""
    return services.get_zarr_store_info_or_404(artifact_id)


@router.get("/{artifact_id}/zarr/{relative_path:path}", response_model=None)
def get_zarr_store_file(artifact_id: str, relative_path: str) -> FileResponse | Response | dict[str, object]:
    """Serve a file, metadata document, or directory listing from within a Zarr store."""
    return services.get_zarr_store_file_or_404(artifact_id, relative_path)
