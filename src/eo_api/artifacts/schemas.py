"""Pydantic schemas for artifact and ingestion APIs."""

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class ArtifactFormat(StrEnum):
    """Supported stored artifact formats."""

    ZARR = "zarr"
    NETCDF = "netcdf"


class PublicationStatus(StrEnum):
    """Publication lifecycle states."""

    UNPUBLISHED = "unpublished"
    PUBLISHED = "published"


class CoverageSpatial(BaseModel):
    """Spatial extent summary."""

    xmin: float
    ymin: float
    xmax: float
    ymax: float


class CoverageTemporal(BaseModel):
    """Temporal extent summary."""

    start: str
    end: str


class ArtifactCoverage(BaseModel):
    """Artifact coverage metadata."""

    spatial: CoverageSpatial
    temporal: CoverageTemporal


class ArtifactRequestScope(BaseModel):
    """Original request parameters used to create an artifact."""

    start: str
    end: str | None = None
    bbox: tuple[float, float, float, float] | None = None
    country_code: str | None = None


class ArtifactPublication(BaseModel):
    """Publication metadata for an artifact."""

    status: PublicationStatus = PublicationStatus.UNPUBLISHED
    collection_id: str | None = None
    published_at: datetime | None = None
    pygeoapi_path: str | None = None


class ArtifactRecord(BaseModel):
    """Stored artifact metadata."""

    artifact_id: str
    dataset_id: str
    dataset_name: str
    variable: str
    format: ArtifactFormat
    path: str | None = None
    asset_paths: list[str] = Field(default_factory=list)
    variables: list[str] = Field(default_factory=list)
    request_scope: ArtifactRequestScope
    coverage: ArtifactCoverage
    created_at: datetime
    publication: ArtifactPublication = Field(default_factory=ArtifactPublication)


class CreateIngestionRequest(BaseModel):
    """Request payload for ingesting remote EO data into a managed artifact."""

    dataset_id: str
    start: str
    end: str | None = None
    bbox: tuple[float, float, float, float] | None = None
    country_code: str | None = None
    overwrite: bool = False
    prefer_zarr: bool = True
    publish: bool = True


class IngestionResponse(BaseModel):
    """Response returned after creating a new artifact via ingestion."""

    ingestion_id: str
    status: str
    artifact: ArtifactRecord


class ArtifactListResponse(BaseModel):
    """Collection response for artifacts."""

    items: list[ArtifactRecord]


class CollectionRecord(BaseModel):
    """Native FastAPI view of a published collection."""

    collection_id: str
    dataset_id: str
    dataset_name: str
    variable: str
    latest_artifact_id: str
    artifact_count: int
    coverage: ArtifactCoverage
    latest_created_at: datetime
    pygeoapi_path: str


class CollectionArtifactRecord(BaseModel):
    """Artifact summary as exposed from a collection detail view."""

    artifact_id: str
    created_at: datetime
    format: ArtifactFormat
    request_scope: ArtifactRequestScope
    coverage: ArtifactCoverage
    artifact_path: str | None = None
    artifact_api_path: str


class CollectionDetailRecord(CollectionRecord):
    """Detailed native FastAPI view of a published collection."""

    artifacts: list[CollectionArtifactRecord]


class CollectionListResponse(BaseModel):
    """Collection response for published collections."""

    items: list[CollectionRecord]
