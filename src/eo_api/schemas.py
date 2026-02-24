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
