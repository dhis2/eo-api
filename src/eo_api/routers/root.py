"""Root API endpoints."""

from fastapi import APIRouter

from eo_api.schemas import MessageResponse

router = APIRouter()


@router.get("/")
def read_index() -> MessageResponse:
    """Return a welcome message for the root endpoint."""
    return MessageResponse(message="Welcome to DHIS2 EO API")
