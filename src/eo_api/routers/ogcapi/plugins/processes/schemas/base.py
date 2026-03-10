"""Base process input schemas for climate download processes."""

from __future__ import annotations

from pydantic import BaseModel, Field, model_validator


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
    flavor: str = Field(default="rnl", pattern=r"^(rnl|sat)$", description="Product flavor")

    @model_validator(mode="after")
    def validate_stage_flavor(self) -> "CHIRPS3Input":
        if self.stage == "prelim" and self.flavor != "sat":
            raise ValueError("For stage='prelim', flavor must be 'sat'")
        return self
