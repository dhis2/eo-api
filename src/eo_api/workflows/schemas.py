"""Schemas for generic DHIS2 workflow execution."""

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, model_validator


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


# TODO: Below should not be hardcoded like this, but rather defined by each component 

# ComponentName = Literal[
#     "feature_source",
#     "download_dataset",
#     "temporal_aggregation",
#     "spatial_aggregation",
#     "build_datavalueset",
# ]

# SUPPORTED_COMPONENTS: Final[set[str]] = set(ComponentName.__args__)  # type: ignore[attr-defined]
# SUPPORTED_COMPONENT_VERSIONS: Final[dict[str, set[str]]] = {component: {"v1"} for component in SUPPORTED_COMPONENTS}

# COMPONENT_INPUTS: Final[dict[str, set[str]]] = {
#     "feature_source": set(),
#     "download_dataset": {"bbox"},
#     "temporal_aggregation": {"bbox"},
#     "spatial_aggregation": {"bbox", "features"},
#     "build_datavalueset": {"records"},
# }

# COMPONENT_OUTPUTS: Final[dict[str, set[str]]] = {
#     "feature_source": {"features", "bbox"},
#     "download_dataset": set(),
#     "temporal_aggregation": {"temporal_dataset"},
#     "spatial_aggregation": {"records"},
#     "build_datavalueset": {"data_value_set", "output_file"},
# }


class WorkflowStep(BaseModel):
    """One component step in a declarative workflow definition."""

    component: ComponentName
    version: str = "v1"
    config: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_component_version(self) -> "WorkflowStep":
        """Ensure component@version exists in the registered component catalog."""
        supported_versions = SUPPORTED_COMPONENT_VERSIONS.get(self.component, set())
        if self.version not in supported_versions:
            known = ", ".join(sorted(supported_versions)) or "<none>"
            raise ValueError(
                f"Unsupported component version '{self.component}@{self.version}'. Supported versions: {known}"
            )
        return self


class WorkflowDefinition(BaseModel):
    """Declarative workflow definition."""

    workflow_id: str
    version: int = 1
    steps: list[WorkflowStep]

    @model_validator(mode="after")
    def validate_steps(self) -> "WorkflowDefinition":
        """Require terminal DataValueSet step and validate component compatibility."""
        if not self.steps:
            raise ValueError("Workflow steps cannot be empty")
        if self.steps[-1].component != "build_datavalueset":
            raise ValueError("The last workflow step must be 'build_datavalueset'")
        available_context: set[str] = set()
        for step in self.steps:
            required_inputs = COMPONENT_INPUTS[step.component]
            missing_inputs = required_inputs - available_context
            if missing_inputs:
                missing = ", ".join(sorted(missing_inputs))
                raise ValueError(f"Component '{step.component}' is missing required upstream outputs: {missing}")
            available_context.update(COMPONENT_OUTPUTS[step.component])
        return self
