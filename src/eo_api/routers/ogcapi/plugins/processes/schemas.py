"""Pydantic models for OGC API process inputs and outputs."""

from pydantic import BaseModel, Field


class ClimateProcessInput(BaseModel):
    """Common inputs for climate data processes."""

    start: str = Field(..., description="Start date (YYYY-MM)")
    end: str = Field(..., description="End date (YYYY-MM)")
    bbox: list[float] = Field(..., min_length=4, max_length=4, description="Bounding box [west, south, east, north]")
    dry_run: bool = Field(default=True, description="If true, return data without pushing to DHIS2")


class ERA5LandInput(ClimateProcessInput):
    """ERA5-Land specific inputs."""

    variables: list[str] = Field(
        default=["2m_temperature", "total_precipitation"],
        min_length=1,
        description="ERA5-Land variable names",
    )


class CHIRPS3Input(ClimateProcessInput):
    """CHIRPS3 specific inputs."""

    stage: str = Field(default="final", pattern=r"^(final|prelim)$", description="Product stage")


class ProcessOutput(BaseModel):
    """Standard process output."""

    status: str
    files: list[str] = Field(default_factory=list, description="Paths to downloaded files")
    summary: dict = Field(default_factory=dict, description="Summary statistics")
    message: str = ""
