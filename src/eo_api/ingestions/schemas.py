"""Pydantic schemas for ingestion, dataset, and sync APIs."""

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

    xmin: float = Field(description="Minimum longitude of the covered spatial extent.")
    ymin: float = Field(description="Minimum latitude of the covered spatial extent.")
    xmax: float = Field(description="Maximum longitude of the covered spatial extent.")
    ymax: float = Field(description="Maximum latitude of the covered spatial extent.")


class CoverageTemporal(BaseModel):
    """Temporal extent summary."""

    start: str = Field(description="First covered time period in dataset-native string form.")
    end: str = Field(description="Last covered time period in dataset-native string form.")


class ArtifactCoverage(BaseModel):
    """Artifact coverage metadata."""

    spatial: CoverageSpatial = Field(description="Covered spatial extent of the managed dataset.")
    temporal: CoverageTemporal = Field(description="Covered temporal extent of the managed dataset.")


class ArtifactRequestScope(BaseModel):
    """Original request parameters used to create an artifact."""

    start: str = Field(description="Requested start period for the ingestion or sync operation.")
    end: str | None = Field(default=None, description="Requested end period for the ingestion or sync operation.")
    extent_id: str | None = Field(
        default=None,
        description="Configured EO API extent identifier used to resolve spatial scope for this request.",
    )
    bbox: tuple[float, float, float, float] | None = Field(
        default=None,
        description="Requested bounding box when the artifact was created from an explicit bbox.",
    )


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
    """Request payload for creating or updating a managed dataset."""

    dataset_id: str = Field(description="Source dataset template id from the EO API registry.")
    start: str = Field(description="Start period to ingest.")
    end: str | None = Field(default=None, description="Optional end period to ingest.")
    extent_id: str | None = Field(
        default=None,
        description="Configured EO API extent identifier used to resolve spatial scope for this ingestion.",
    )
    overwrite: bool = Field(
        default=False,
        description="Whether to force regeneration of an existing matching artifact.",
    )
    prefer_zarr: bool = Field(
        default=True,
        description="Whether to prefer GeoZarr materialization when available.",
    )
    publish: bool = Field(
        default=True,
        description="Whether to publish the resulting dataset through pygeoapi.",
    )


class ArtifactListResponse(BaseModel):
    """Envelope response for internal artifact records."""

    kind: str = Field(
        default="ArtifactList",
        description="Self-describing envelope type for this collection response.",
        examples=["ArtifactList"],
    )
    items: list[ArtifactRecord] = Field(
        default_factory=list,
        description="Internal artifact records managed by this EO API instance.",
    )


class DatasetAccessLink(BaseModel):
    """Access link for a managed dataset."""

    href: str = Field(description="Relative API path for this dataset access mode.")
    rel: str = Field(description="Relationship type of the link.")
    title: str = Field(description="Human-readable label for the link target.")


class DatasetPublication(BaseModel):
    """Public publication summary for a managed dataset."""

    status: PublicationStatus = Field(description="Publication state of the dataset in the OGC-facing layer.")
    published_at: datetime | None = Field(default=None, description="Timestamp when the dataset was last published.")


class DatasetRecord(BaseModel):
    """Native FastAPI view of a managed dataset."""

    dataset_id: str = Field(description="Stable public identifier for the managed dataset.")
    source_dataset_id: str = Field(description="Dataset template id from which this managed dataset was created.")
    dataset_name: str = Field(description="Full display name of the dataset.")
    short_name: str | None = Field(default=None, description="Short display name of the dataset.")
    variable: str = Field(description="Primary raster variable stored in the dataset.")
    period_type: str = Field(description="Temporal period type of the dataset, for example daily or yearly.")
    units: str | None = Field(default=None, description="Units of the primary variable.")
    resolution: str | None = Field(default=None, description="Native spatial resolution summary.")
    source: str | None = Field(default=None, description="Upstream source name.")
    source_url: str | None = Field(default=None, description="Upstream source documentation URL.")
    extent: ArtifactCoverage = Field(description="Current covered spatial and temporal extent of the dataset.")
    last_updated: datetime = Field(description="Timestamp when EO API last materialized or updated the dataset.")
    links: list[DatasetAccessLink] = Field(
        default_factory=list,
        description="Available API access links for this managed dataset.",
    )
    publication: DatasetPublication = Field(description="Publication summary for this managed dataset.")


class DatasetVersionRecord(BaseModel):
    """Version summary as exposed from a dataset detail view."""

    created_at: datetime = Field(description="Timestamp when this dataset version was created.")
    format: ArtifactFormat = Field(description="Stored format of this dataset version.")
    coverage: ArtifactCoverage = Field(description="Covered spatial and temporal extent for this dataset version.")
    request_scope: ArtifactRequestScope | None = Field(
        default=None,
        description="Original request scope that produced this version, when available.",
    )


class DatasetDetailRecord(DatasetRecord):
    """Detailed native FastAPI view of a managed dataset."""

    versions: list[DatasetVersionRecord] = Field(
        description="Slim version history derived from internal artifact records."
    )


class IngestionResponse(BaseModel):
    """Response returned after creating or looking up a managed dataset via ingestion."""

    ingestion_id: str = Field(description="Identifier of the ingestion event.")
    status: str = Field(description="Execution status of the ingestion request.")
    dataset: DatasetRecord = Field(description="Managed dataset summary produced or resolved by the ingestion.")


class DatasetListResponse(BaseModel):
    """Envelope response for managed datasets."""

    kind: str = Field(
        default="DatasetList",
        description="Self-describing envelope type for this collection response.",
        examples=["DatasetList"],
    )
    items: list[DatasetRecord] = Field(
        default_factory=list,
        description="Managed datasets available in this EO API instance.",
        examples=[
            [
                {
                    "dataset_id": "chirps3_precipitation_daily_sle",
                    "source_dataset_id": "chirps3_precipitation_daily",
                    "dataset_name": "Total precipitation (CHIRPS3)",
                    "short_name": "Total precipitation",
                    "variable": "precip",
                    "period_type": "daily",
                    "units": "mm",
                    "resolution": "5 km x 5 km",
                    "source": "CHIRPS v3",
                    "source_url": "https://www.chc.ucsb.edu/data/chirps3",
                    "extent": {
                        "spatial": {"xmin": -13.5, "ymin": 6.9, "xmax": -10.1, "ymax": 10.0},
                        "temporal": {"start": "2024-01-01", "end": "2024-01-31"},
                    },
                    "last_updated": "2026-03-27T08:40:24.344473Z",
                    "links": [
                        {
                            "href": "/datasets/chirps3_precipitation_daily_sle",
                            "rel": "self",
                            "title": "Dataset detail",
                        },
                        {
                            "href": "/zarr/chirps3_precipitation_daily_sle",
                            "rel": "zarr",
                            "title": "Zarr store",
                        },
                    ],
                    "publication": {"status": "published", "published_at": "2026-03-27T08:40:24.346357Z"},
                }
            ]
        ],
    )


class SyncDatasetRequest(BaseModel):
    """Request payload for syncing a managed dataset forward."""

    end: str | None = Field(default=None, description="Optional end period to sync through.")
    prefer_zarr: bool = Field(default=True, description="Whether to prefer GeoZarr materialization when syncing.")
    publish: bool = Field(default=True, description="Whether to publish the resulting dataset version.")


class SyncResponse(BaseModel):
    """Response returned after syncing or checking a managed dataset."""

    sync_id: str | None = Field(
        default=None,
        description="Identifier of the sync-created version when a new version was written.",
    )
    status: str = Field(description="Execution status, for example completed or up_to_date.")
    dataset: DatasetDetailRecord = Field(description="Current dataset detail after the sync operation.")
