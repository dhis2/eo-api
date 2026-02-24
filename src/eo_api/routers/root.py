"""Root API endpoints."""

from fastapi import APIRouter

from eo_api.schemas import StatusMessage

router = APIRouter()


@router.get("/")
def read_index() -> StatusMessage:
    """Return a welcome message for the root endpoint."""
    return StatusMessage(message="Welcome to DHIS2 EO API")
