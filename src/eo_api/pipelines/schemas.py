"""Pydantic models for pipeline inputs and outputs."""

from typing import Any, Literal

from pydantic import BaseModel, Field


class ERA5LandPipelineInput(BaseModel):
    """Input for the ERA5-Land download pipeline."""

    start: str = Field(..., description="Start date (YYYY-MM)")
    end: str = Field(..., description="End date (YYYY-MM)")
    bbox: list[float] = Field(..., min_length=4, max_length=4, description="Bounding box [west, south, east, north]")
    variables: list[str] = Field(
        default=["2m_temperature", "total_precipitation"],
        min_length=1,
        description="ERA5-Land variable names",
    )


class CHIRPS3PipelineInput(BaseModel):
    """Input for the CHIRPS3 download pipeline."""

    start: str = Field(..., description="Start date (YYYY-MM)")
    end: str = Field(..., description="End date (YYYY-MM)")
    bbox: list[float] = Field(..., min_length=4, max_length=4, description="Bounding box [west, south, east, north]")
    stage: Literal["final", "prelim"] = Field(default="final", description="CHIRPS3 product stage")


class PipelineResult(BaseModel):
    """Output from a pipeline run."""

    status: str
    files: list[str] = Field(default_factory=list, description="Paths to downloaded files")
    features: dict[str, Any] | None = Field(default=None, description="Aggregated GeoJSON FeatureCollection")
    message: str = ""
