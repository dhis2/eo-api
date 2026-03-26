"""Routes for the native collection registry API."""

from fastapi import APIRouter

from eo_api.collections import services
from eo_api.collections.schemas import CollectionDetailRecord, CollectionListResponse

router = APIRouter()


@router.get("", response_model=CollectionListResponse)
def list_collections() -> CollectionListResponse:
    """List published collections from the native FastAPI registry view."""
    return services.list_collections()


@router.get("/{collection_id}", response_model=CollectionDetailRecord)
def get_collection(collection_id: str) -> CollectionDetailRecord:
    """Get a published collection from the native FastAPI registry view."""
    return services.get_collection_or_404(collection_id)
