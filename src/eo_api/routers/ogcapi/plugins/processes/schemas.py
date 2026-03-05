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
    flavor: str = Field(default="rnl", pattern=r"^(rnl|sat)$", description="Product flavor")

    @model_validator(mode="after")
    def validate_stage_flavor(self) -> "CHIRPS3Input":
        if self.stage == "prelim" and self.flavor != "sat":
            raise ValueError("For stage='prelim', flavor must be 'sat'")
        return self


class WorldPopSyncInput(BaseModel):
    """Inputs for WorldPop data sync planning."""

    country_code: str | None = Field(
        default=None,
        pattern=r"^[A-Za-z]{2,3}$",
        description="ISO country code (2 or 3 letters). Provide this or bbox.",
    )
    bbox: list[float] | None = Field(
        default=None,
        min_length=4,
        max_length=4,
        description="Bounding box [west, south, east, north]. Provide this or country_code.",
    )
    start_year: int = Field(default=2015, ge=2015, le=2030, description="Inclusive start year")
    end_year: int = Field(default=2030, ge=2015, le=2030, description="Inclusive end year")
    output_format: str = Field(default="netcdf", pattern=r"^(netcdf|geotiff|zarr)$")
    dry_run: bool = Field(default=True)

    @model_validator(mode="after")
    def validate_scope(self) -> "WorldPopSyncInput":
        has_country_code = self.country_code is not None
        has_bbox = self.bbox is not None
        if has_country_code == has_bbox:
            raise ValueError("Provide exactly one of country_code or bbox")
        if self.end_year < self.start_year:
            raise ValueError("end_year must be greater than or equal to start_year")
        return self


class WorldPopDhis2WorkflowInput(BaseModel):
    """Inputs for WorldPop -> DHIS2 payload workflow."""

    country_code: str | None = Field(
        default=None,
        pattern=r"^[A-Za-z]{2,3}$",
        description="ISO country code (2 or 3 letters). Required unless raster_files is provided.",
    )
    bbox: list[float] | None = Field(
        default=None,
        min_length=4,
        max_length=4,
        description="Bounding box [west, south, east, north]. Required unless raster_files is provided.",
    )
    start_year: int = Field(default=2015, ge=2015, le=2030, description="Inclusive start year")
    end_year: int = Field(default=2030, ge=2015, le=2030, description="Inclusive end year")
    output_format: str = Field(default="netcdf", pattern=r"^(netcdf|geotiff|zarr)$")
    raster_files: list[str] | None = Field(
        default=None,
        description="Optional local raster file list. If provided, sync step is skipped.",
    )
    dry_run: bool = Field(default=False, description="If true, sync is planning-only.")
    features_geojson: dict[str, Any] | None = Field(
        default=None,
        description="Optional GeoJSON FeatureCollection for aggregation.",
    )
    org_unit_level: int | None = Field(default=None, ge=1, description="DHIS2 org unit level selector")
    parent_org_unit: str | None = Field(default=None, description="Optional parent org unit UID")
    org_unit_ids: list[str] | None = Field(default=None, description="Optional explicit org unit UIDs")
    data_element: str = Field(..., description="DHIS2 data element UID.")
    org_unit_id_property: str = Field(default="id", description="Fallback feature property for orgUnit UID.")
    reducer: str = Field(default="sum", pattern=r"^(sum|mean)$")
    category_option_combo: str | None = Field(default=None)
    attribute_option_combo: str | None = Field(default=None)
    data_set: str | None = Field(default=None)

    @model_validator(mode="after")
    def validate_scope(self) -> "WorldPopDhis2WorkflowInput":
        if self.end_year < self.start_year:
            raise ValueError("end_year must be greater than or equal to start_year")
        if not self.raster_files:
            has_country_code = self.country_code is not None
            has_bbox = self.bbox is not None
            if has_country_code == has_bbox:
                raise ValueError("Provide exactly one of country_code or bbox unless raster_files is provided")

        has_feature_selectors = (
            bool(self.org_unit_ids) or self.org_unit_level is not None or self.parent_org_unit is not None
        )
        if self.features_geojson is None and not has_feature_selectors:
            raise ValueError("Provide features_geojson or one of: org_unit_ids, parent_org_unit, org_unit_level")
        return self


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


class ClimateDhis2WorkflowInput(BaseModel):
    """Inputs for climate -> DHIS2 workflow."""

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
    flavor: str = Field(default="rnl", pattern=r"^(rnl|sat)$")
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
    def validate_date_window(self) -> "ClimateDhis2WorkflowInput":
        """Ensure date window is valid."""
        if self.end_date < self.start_date:
            raise ValueError("end_date must be greater than or equal to start_date")
        if self.stage == "prelim" and self.flavor != "sat":
            raise ValueError("For stage='prelim', flavor must be 'sat'")
        return self


class FeatureFetchInput(BaseModel):
    """Inputs for feature fetching step (DHIS2 or inline GeoJSON)."""

    bbox: list[float] | None = Field(default=None, min_length=4, max_length=4)
    features_geojson: dict[str, Any] | None = Field(default=None)
    org_unit_level: int | None = Field(default=None, ge=1)
    parent_org_unit: str | None = Field(default=None)
    org_unit_ids: list[str] | None = Field(default=None)
    org_unit_id_property: str = Field(default="id")


class DataValueBuildInput(BaseModel):
    """Inputs for dataValueSet builder step."""

    rows: list[dict[str, Any]] = Field(default_factory=list)
    data_element: str = Field(..., description="DHIS2 data element UID")
    category_option_combo: str | None = Field(default=None)
    attribute_option_combo: str | None = Field(default=None)
    data_set: str | None = Field(default=None)


class DataAggregateInput(BaseModel):
    """Inputs for data aggregation step."""

    start_date: date = Field(..., description="Inclusive start date (YYYY-MM-DD)")
    end_date: date = Field(..., description="Inclusive end date (YYYY-MM-DD)")
    files: list[str] = Field(default_factory=list, description="Downloaded CHIRPS3 file paths")
    valid_features: list[dict[str, Any]] = Field(default_factory=list, description="Feature rows with orgUnit/geometry")
    stage: str = Field(default="final", pattern=r"^(final|prelim)$")
    flavor: str = Field(default="rnl", pattern=r"^(rnl|sat)$")
    spatial_reducer: str = Field(default="mean", pattern=r"^(mean|sum)$")
    temporal_resolution: str = Field(default="monthly", pattern=r"^(daily|weekly|monthly)$")
    temporal_reducer: str = Field(default="sum", pattern=r"^(sum|mean)$")
    value_rounding: int = Field(default=3, ge=0, le=10)

    @model_validator(mode="after")
    def validate_date_window(self) -> "DataAggregateInput":
        if self.end_date < self.start_date:
            raise ValueError("end_date must be greater than or equal to start_date")
        if self.stage == "prelim" and self.flavor != "sat":
            raise ValueError("For stage='prelim', flavor must be 'sat'")
        return self


class ProcessOutput(BaseModel):
    """Standard process output."""

    status: str
    files: list[str] = Field(default_factory=list, description="Paths to downloaded files")
    summary: dict = Field(default_factory=dict, description="Summary statistics")
    message: str = ""
