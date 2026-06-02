"""Routes for the STAC catalogue."""

from fastapi import APIRouter, Request

from open_climate_service.stac import services as stac_services
from open_climate_service.stac.schemas import StacCatalogResponse

router = APIRouter()


@router.get("", response_model=StacCatalogResponse)
def get_stac_landing(request: Request) -> dict[str, object]:
    """Return the STAC catalog landing page."""
    return stac_services.build_catalog(request)


@router.get("/catalog.json", response_model=StacCatalogResponse)
def get_stac_catalog_json(request: Request) -> dict[str, object]:
    """Return the STAC catalog landing page (catalog.json alias)."""
    return stac_services.build_catalog(request)


@router.get("/collections/{dataset_id}")
def get_stac_collection(dataset_id: str, request: Request) -> dict[str, object]:
    """Return one STAC collection document."""
    return stac_services.build_collection(dataset_id, request)
