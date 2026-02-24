"""Root API endpoints."""

import sys
from importlib.metadata import version

from fastapi import APIRouter

from eo_api.schemas import AppInfo, HealthStatus, Status, StatusMessage

router = APIRouter(tags=["System"])


@router.get("/")
def read_index() -> StatusMessage:
    """Return a welcome message for the root endpoint."""
    return StatusMessage(message="Welcome to DHIS2 EO API")


@router.get("/health")
def health() -> HealthStatus:
    """Return health status for container health checks."""
    return HealthStatus(status=Status.HEALTHY)


@router.get("/info")
def info() -> AppInfo:
    """Return application version and environment info."""
    return AppInfo(
        app_version=version("eo-api"),
        python_version=sys.version,
        titiler_version=version("titiler.core"),
        pygeoapi_version=version("pygeoapi"),
        uvicorn_version=version("uvicorn"),
    )
