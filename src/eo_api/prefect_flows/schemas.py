"""Pydantic models for pipeline inputs and outputs."""

from typing import Any

from pydantic import BaseModel, Field


class PipelineInput(BaseModel):
    """Generic input for any pipeline -- just a process ID and its parameters."""

    process_id: str = Field(..., description="OGC process identifier")
    inputs: dict[str, Any] = Field(..., description="Process input parameters")


class PipelineResult(BaseModel):
    """Output from a pipeline run."""

    status: str
    files: list[str] = Field(default_factory=list, description="Paths to downloaded files")
    features: dict[str, Any] | None = Field(default=None, description="Aggregated GeoJSON FeatureCollection")
    message: str = ""
