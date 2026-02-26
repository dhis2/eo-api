"""Pydantic models for OGC API process inputs and outputs."""

from datetime import date
from typing import Any

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


class ZonalStatisticsInput(BaseModel):
    """Inputs for raster zonal statistics process."""

    geojson: dict[str, Any] | str = Field(
        ...,
        description="GeoJSON FeatureCollection object or URI/path to GeoJSON file",
    )
    raster: str = Field(..., description="Raster path or URI")
    band: int = Field(default=1, ge=1, description="1-based raster band index")
    stats: list[str] = Field(
        default_factory=lambda: ["mean"],
        min_length=1,
        description="Statistics to compute",
    )
    feature_id_property: str = Field(default="id", description="Fallback property key for feature ID")
    output_property: str = Field(default="zonal_statistics", description="Property key where zonal stats are attached")
    all_touched: bool = Field(default=False)
    include_nodata: bool = Field(default=False)
    nodata: float | None = Field(default=None, description="Optional nodata override")

    @model_validator(mode="after")
    def validate_stats(self) -> "ZonalStatisticsInput":
        """Ensure all requested statistics are supported."""
        supported = {"count", "sum", "mean", "min", "max", "median", "std"}
        invalid = [stat for stat in self.stats if stat not in supported]
        if invalid:
            raise ValueError(f"Unsupported stats requested: {invalid}. Allowed stats: {sorted(supported)}")
        return self


class CHIRPS3DHIS2PipelineInput(BaseModel):
    """Inputs for CHIRPS3 -> DHIS2 data value pipeline."""

    start_date: date = Field(..., description="Inclusive start date (YYYY-MM-DD)")
    end_date: date = Field(..., description="Inclusive end date (YYYY-MM-DD)")
    dry_run: bool = Field(default=True)
    bbox: list[float] | None = Field(
        default=None,
        min_length=4,
        max_length=4,
        description="Optional [west, south, east, north]. Uses feature union bbox if omitted.",
    )
    features_geojson: dict[str, Any] | None = Field(default=None, description="Optional GeoJSON FeatureCollection")
    org_unit_level: int | None = Field(
        default=None,
        ge=1,
        description="DHIS2 org unit level",
    )
    parent_org_unit: str | None = Field(default=None, description="Optional parent org unit UID")
    org_unit_ids: list[str] | None = Field(default=None, description="Optional explicit org unit UIDs")
    org_unit_id_property: str = Field(default="id", description="Property name for org unit UID")
    data_element: str = Field(..., description="DHIS2 data element UID")
    category_option_combo: str | None = Field(default=None)
    attribute_option_combo: str | None = Field(default=None)
    data_set: str | None = Field(default=None)
    stage: str = Field(default="final", pattern=r"^(final|prelim)$")
    spatial_reducer: str = Field(default="mean", pattern=r"^(mean|sum)$")
    temporal_resolution: str = Field(
        default="monthly",
        pattern=r"^(daily|weekly|monthly)$",
        description="Aggregation period",
    )
    temporal_reducer: str = Field(
        default="sum",
        pattern=r"^(sum|mean)$",
        description="Reducer for weekly/monthly",
    )
    value_rounding: int = Field(default=3, ge=0, le=10)
    auto_import: bool = Field(default=False)
    import_strategy: str = Field(default="CREATE_AND_UPDATE")

    @model_validator(mode="after")
    def validate_date_window(self) -> "CHIRPS3DHIS2PipelineInput":
        """Ensure date window is valid."""
        if self.end_date < self.start_date:
            raise ValueError("end_date must be greater than or equal to start_date")
        return self


class ProcessOutput(BaseModel):
    """Standard process output."""

    status: str
    files: list[str] = Field(default_factory=list, description="Paths to downloaded files")
    summary: dict = Field(default_factory=dict, description="Summary statistics")
    message: str = ""
