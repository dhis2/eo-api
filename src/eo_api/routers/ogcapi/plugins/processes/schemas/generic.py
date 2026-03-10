"""Schemas for generic-dhis2-workflow input variants."""

from __future__ import annotations

from datetime import date
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, TypeAdapter, model_validator


class GenericWorkflowBaseInput(BaseModel):
    """Common selectors and DHIS2 mapping fields for generic workflow runs."""

    features_geojson: dict[str, Any] | None = Field(default=None, description="Optional GeoJSON FeatureCollection")
    org_unit_level: int | None = Field(default=None, ge=1, description="DHIS2 org unit level selector")
    parent_org_unit: str | None = Field(default=None, description="Optional parent org unit UID")
    org_unit_ids: list[str] | None = Field(default=None, description="Optional explicit org unit UIDs")
    org_unit_id_property: str = Field(default="id", description="Fallback feature property for orgUnit UID.")
    bbox: list[float] | None = Field(
        default=None,
        min_length=4,
        max_length=4,
        description="Optional [west, south, east, north] input bbox.",
    )
    data_element: str = Field(..., description="DHIS2 data element UID.")
    category_option_combo: str | None = Field(default=None)
    attribute_option_combo: str | None = Field(default=None)
    data_set: str | None = Field(default=None)
    dry_run: bool = Field(default=True)

    @model_validator(mode="after")
    def validate_feature_scope(self) -> "GenericWorkflowBaseInput":
        has_feature_selectors = (
            bool(self.org_unit_ids) or self.org_unit_level is not None or self.parent_org_unit is not None
        )
        if self.features_geojson is None and not has_feature_selectors:
            raise ValueError("Provide features_geojson or one of: org_unit_ids, parent_org_unit, org_unit_level")
        return self


class GenericChirps3WorkflowInput(GenericWorkflowBaseInput):
    """Generic workflow branch for CHIRPS3 data source."""

    dataset_type: Literal["chirps3"] = "chirps3"
    start_date: date = Field(..., description="Inclusive start date (YYYY-MM-DD)")
    end_date: date = Field(..., description="Inclusive end date (YYYY-MM-DD)")
    stage: str = Field(default="final", pattern=r"^(final|prelim)$")
    flavor: str = Field(default="rnl", pattern=r"^(rnl|sat)$")
    spatial_reducer: str = Field(default="mean", pattern=r"^(mean|sum)$")
    temporal_resolution: str = Field(default="monthly", pattern=r"^(daily|weekly|monthly)$")
    temporal_reducer: str = Field(default="sum", pattern=r"^(sum|mean)$")
    value_rounding: int = Field(default=3, ge=0, le=10)

    @model_validator(mode="after")
    def validate_temporal_window(self) -> "GenericChirps3WorkflowInput":
        if self.end_date < self.start_date:
            raise ValueError("end_date must be greater than or equal to start_date")
        if self.stage == "prelim" and self.flavor != "sat":
            raise ValueError("For stage='prelim', flavor must be 'sat'")
        return self


class GenericWorldPopWorkflowInput(GenericWorkflowBaseInput):
    """Generic workflow branch for WorldPop data source."""

    dataset_type: Literal["worldpop"] = "worldpop"
    country_code: str | None = Field(
        default=None,
        pattern=r"^[A-Za-z]{2,3}$",
        description="ISO country code (2 or 3 letters). Required unless raster_files is provided.",
    )
    start_year: int = Field(default=2015, ge=2015, le=2030, description="Inclusive start year")
    end_year: int = Field(default=2030, ge=2015, le=2030, description="Inclusive end year")
    output_format: str = Field(default="netcdf", pattern=r"^(netcdf|geotiff|zarr)$")
    raster_files: list[str] | None = Field(
        default=None,
        description="Optional local raster file list. If provided, sync step is skipped.",
    )
    temporal_resolution: str = Field(default="yearly", pattern=r"^(yearly|annual|monthly|weekly|daily)$")
    reducer: str = Field(default="sum", pattern=r"^(sum|mean)$")

    @model_validator(mode="after")
    def validate_scope(self) -> "GenericWorldPopWorkflowInput":
        if self.end_year < self.start_year:
            raise ValueError("end_year must be greater than or equal to start_year")
        if not self.raster_files:
            has_country_code = self.country_code is not None
            has_bbox = self.bbox is not None
            if has_country_code == has_bbox:
                raise ValueError("Provide exactly one of country_code or bbox unless raster_files is provided")
        return self


GenericDhis2WorkflowInput = Annotated[
    GenericChirps3WorkflowInput | GenericWorldPopWorkflowInput,
    Field(discriminator="dataset_type"),
]

GENERIC_DHIS2_WORKFLOW_INPUT_ADAPTER: TypeAdapter[GenericDhis2WorkflowInput] = TypeAdapter(GenericDhis2WorkflowInput)
