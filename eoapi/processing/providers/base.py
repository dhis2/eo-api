"""Provider contracts for raster-source fetching."""

from datetime import date
from typing import Protocol

from pydantic import BaseModel, Field

BBox = tuple[float, float, float, float]


class RasterFetchRequest(BaseModel):
    """Normalized fetch request passed to all providers."""

    dataset_id: str = Field(min_length=1)
    parameter: str = Field(min_length=1)
    start: date
    end: date
    bbox: BBox = (-180.0, -90.0, 180.0, 90.0)


class RasterFetchResult(BaseModel):
    """Provider fetch result with cache provenance."""

    provider: str
    asset_paths: list[str]
    from_cache: bool


class RasterProvider(Protocol):
    """Protocol implemented by all dataset-specific raster providers."""

    provider_id: str

    def fetch(self, request: RasterFetchRequest) -> RasterFetchResult: ...

    def implementation_details(self) -> dict[str, str]:
        """Return provider/backend metadata for execution traceability."""
        ...
