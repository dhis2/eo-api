"""Root API endpoints."""

import sys
from importlib.metadata import version

from fastapi import APIRouter, Request

from .schemas import AppInfo, HealthStatus, Link, RootResponse, Status

router = APIRouter()


@router.get("/")
def read_index(request: Request) -> RootResponse:
    """Return a welcome message with navigation links."""
    base = str(request.base_url).rstrip("/")
    return RootResponse(
        message="Welcome to DHIS2 EO API",
        links=[
            Link(href=f"{base}/ogcapi/", rel="ogcapi", title="OGC API"),
            Link(href=f"{base}/extents", rel="extents", title="Extents"),
            Link(href=f"{base}/datasets", rel="datasets", title="Datasets"),
            Link(href=f"{base}/prefect/", rel="prefect", title="Prefect"),
            Link(href=f"{base}/docs", rel="docs", title="API Docs"),
        ],
    )


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


@router.get("/prefect/")
def prefect_placeholder() -> dict[str, str]:
    """Reserve the future Prefect surface with an explicit placeholder."""
    return {
        "status": "placeholder",
        "message": (
            "Prefect is not mounted on this branch yet. This path is reserved for future pipeline orchestration."
        ),
    }
