"""Pydantic response models."""

from enum import StrEnum

from pydantic import BaseModel


class StatusMessage(BaseModel):
    """Simple status message response."""

    message: str


class Status(StrEnum):
    """Health check status values."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


class HealthStatus(BaseModel):
    """Health check response."""

    status: Status


class AppInfo(BaseModel):
    """Application version and environment info."""

    app_version: str
    python_version: str
    titiler_version: str
    pygeoapi_version: str
    uvicorn_version: str
