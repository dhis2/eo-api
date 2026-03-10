"""WorldPop-specific process schemas."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, model_validator


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
