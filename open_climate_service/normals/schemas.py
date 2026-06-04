"""Schemas for the /normals endpoint."""

from __future__ import annotations

from pydantic import BaseModel, Field


class NormalsRequest(BaseModel):
    """Request body for POST /normals."""

    source_dataset_id: str = Field(
        description="Dataset ID to compute normals from (e.g. 'era5land_temperature_daily')."
    )
    period: tuple[int, int] = Field(
        default=(1991, 2020),
        description="Reference period as [start_year, end_year] inclusive.",
    )
    smoothing_window: int = Field(
        default=31,
        ge=0,
        description=("Days for WMO-recommended circular rolling mean. Set to 0 to disable smoothing."),
    )
    publish: bool = Field(default=True)


class NormalsResponse(BaseModel):
    """Response body for POST /normals."""

    normals_id: str = Field(description="Dataset ID of the computed normals artifact.")
    source_dataset_id: str
    period: tuple[int, int]
    smoothing_window: int
    status: str
    dataset: dict | None = None
