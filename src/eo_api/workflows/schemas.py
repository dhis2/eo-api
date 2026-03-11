"""Schemas for generic DHIS2 workflow execution."""

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, model_validator


class FeatureSourceType(StrEnum):
    """Supported feature source backends."""

    GEOJSON_FILE = "geojson_file"
    DHIS2_LEVEL = "dhis2_level"
    DHIS2_IDS = "dhis2_ids"


class AggregationMethod(StrEnum):
    """Supported numeric aggregation methods."""

    MEAN = "mean"
    SUM = "sum"
    MIN = "min"
    MAX = "max"


class PeriodType(StrEnum):
    """Supported temporal period types."""

    HOURLY = "hourly"
    DAILY = "daily"
    MONTHLY = "monthly"
    YEARLY = "yearly"


class FeatureSourceConfig(BaseModel):
    """How to fetch features for spatial aggregation."""

    source_type: FeatureSourceType
    geojson_path: str | None = None
    dhis2_level: int | None = None
    dhis2_ids: list[str] | None = None
    dhis2_parent: str | None = None
    feature_id_property: str = "id"

    @model_validator(mode="after")
    def validate_by_source(self) -> "FeatureSourceConfig":
        """Enforce required fields per source backend."""
        if self.source_type == FeatureSourceType.GEOJSON_FILE and not self.geojson_path:
            raise ValueError("geojson_path is required when source_type='geojson_file'")
        if self.source_type == FeatureSourceType.DHIS2_LEVEL and self.dhis2_level is None:
            raise ValueError("dhis2_level is required when source_type='dhis2_level'")
        if self.source_type == FeatureSourceType.DHIS2_IDS and not self.dhis2_ids:
            raise ValueError("dhis2_ids is required when source_type='dhis2_ids'")
        return self


class TemporalAggregationConfig(BaseModel):
    """Temporal rollup config."""

    target_period_type: PeriodType
    method: AggregationMethod = AggregationMethod.SUM


class SpatialAggregationConfig(BaseModel):
    """Spatial aggregation config."""

    method: AggregationMethod = AggregationMethod.MEAN


class Dhis2DataValueSetConfig(BaseModel):
    """Mapping from aggregate outputs to DHIS2 DataValueSet fields."""

    data_element_uid: str
    category_option_combo_uid: str = "HllvX50cXC0"
    attribute_option_combo_uid: str = "HllvX50cXC0"
    data_set_uid: str | None = None
    org_unit_property: str = "id"
    stored_by: str | None = None


class WorkflowExecuteRequest(BaseModel):
    """End-to-end workflow request."""

    dataset_id: str
    start: str
    end: str
    overwrite: bool = False
    country_code: str | None = None
    feature_source: FeatureSourceConfig
    temporal_aggregation: TemporalAggregationConfig
    spatial_aggregation: SpatialAggregationConfig = Field(default_factory=SpatialAggregationConfig)
    dhis2: Dhis2DataValueSetConfig


class ComponentRun(BaseModel):
    """Execution metadata for one workflow component."""

    component: str
    status: str
    started_at: str
    ended_at: str
    duration_ms: int
    inputs: dict[str, Any]
    outputs: dict[str, Any] | None = None
    error: str | None = None


class WorkflowExecuteResponse(BaseModel):
    """Workflow execution response."""

    status: str
    run_id: str
    workflow_id: str
    workflow_version: int
    dataset_id: str
    bbox: list[float]
    feature_count: int
    value_count: int
    output_file: str
    run_log_file: str
    data_value_set: dict[str, Any]
    component_runs: list[ComponentRun]
    component_run_details_included: bool = False
    component_run_details_available: bool = True


class WorkflowCatalogItem(BaseModel):
    """Discoverable workflow definition summary."""

    workflow_id: str
    version: int
    step_count: int
    components: list[str]


class WorkflowCatalogResponse(BaseModel):
    """List of allowlisted workflow definitions."""

    workflows: list[WorkflowCatalogItem]


class WorkflowRequest(BaseModel):
    """Public flat workflow request payload."""

    workflow_id: str = "dhis2_datavalue_set_v1"
    dataset_id: str
    start_date: str | None = None
    end_date: str | None = None
    start_year: int | None = None
    end_year: int | None = None
    org_unit_level: int | None = None
    org_unit_ids: list[str] | None = None
    data_element: str
    temporal_resolution: PeriodType = PeriodType.MONTHLY
    temporal_reducer: AggregationMethod = AggregationMethod.SUM
    spatial_reducer: AggregationMethod = AggregationMethod.MEAN
    overwrite: bool = False
    dry_run: bool = True
    feature_id_property: str = "id"
    stage: str | None = None
    flavor: str | None = None
    country_code: str | None = None
    output_format: str | None = None
    reducer: str | None = None
    include_component_run_details: bool = False

    @model_validator(mode="after")
    def validate_time_window(self) -> "WorkflowRequest":
        """Require either date range or year range."""
        has_dates = bool(self.start_date and self.end_date)
        has_years = self.start_year is not None and self.end_year is not None
        if not has_dates and not has_years:
            raise ValueError("Provide either start_date/end_date or start_year/end_year")
        if self.org_unit_level is None and not self.org_unit_ids:
            raise ValueError("Provide org_unit_level or org_unit_ids")
        return self
