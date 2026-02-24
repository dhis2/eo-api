"""Root API endpoints."""

from fastapi import APIRouter

from eo_api.schemas import HealthStatus, Status, StatusMessage

router = APIRouter()


@router.get("/")
def read_index() -> StatusMessage:
    """Return a welcome message for the root endpoint."""
    return StatusMessage(message="Welcome to DHIS2 EO API")


@router.get("/health")
def health() -> HealthStatus:
    """Return health status for container health checks."""
    return HealthStatus(status=Status.HEALTHY)
