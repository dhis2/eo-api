"""Services for the native collection registry API."""

from eo_api.artifacts import services as artifact_services
from eo_api.collections.schemas import CollectionDetailRecord, CollectionListResponse


def list_collections() -> CollectionListResponse:
    """Return published collections as a native FastAPI registry view."""
    return artifact_services.list_collections()


def get_collection_or_404(collection_id: str) -> CollectionDetailRecord:
    """Return a single native collection view or raise 404."""
    return artifact_services.get_collection_or_404(collection_id)
