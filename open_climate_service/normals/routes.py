"""Routes for the /normals endpoint."""

from __future__ import annotations

from fastapi import APIRouter

from open_climate_service.extents.services import get_extent_or_404
from open_climate_service.normals import services
from open_climate_service.normals.schemas import NormalsRequest, NormalsResponse

router = APIRouter()


@router.post("", response_model=NormalsResponse)
def compute_normals(request: NormalsRequest) -> NormalsResponse:
    """Compute day-of-year climate normals from a managed dataset.

    For ERA5-Land daily datasets backed by the DestinE Earth Data Hub, the
    reference-period data is loaded directly from the EDH Zarr store without
    requiring a prior 30-year ingestion — typical runtime is under 5 minutes
    for national extents.

    The resulting normals artifact is registered in the dataset registry with
    a ``dayofyear`` dimension (1–366) and published to the STAC catalog.
    """
    extent = get_extent_or_404()
    bbox = extent["bbox"]  # [xmin, ymin, xmax, ymax]
    return services.compute_normals(request, bbox)
