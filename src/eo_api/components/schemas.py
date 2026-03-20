"""Schemas for component discovery, manifests, and execution endpoints."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from ..workflows.schemas import (
    AggregationMethod,
    Dhis2DataValueSetConfig,
    FeatureSourceConfig,
    PeriodType,
)


class ComponentEndpoint(BaseModel):
    """HTTP endpoint metadata for a component."""

    path: str
    method: str


class ComponentDefinition(BaseModel):
    """Component metadata for discovery."""

    name: str
    version: str = "v1"
    description: str
    inputs: list[str]
    outputs: list[str]
    workflow_inputs_required: list[str] = Field(default_factory=list)
    workflow_inputs_optional: list[str] = Field(default_factory=list)
    input_schema: dict[str, Any] = Field(default_factory=dict)
    config_schema: dict[str, Any] | None = None
    output_schema: dict[str, Any] = Field(default_factory=dict)
    error_codes: list[str] = Field(default_factory=list)
    endpoint: ComponentEndpoint


class ComponentRuntimeManifest(BaseModel):
    """Runtime metadata for one registered component."""

    type: Literal["python"] = "python"
    supported_execution_modes: list[str] = Field(default_factory=lambda: ["local"])
    local_handler: str | None = None
    remote_handler: str | None = None
    remote_request_bindings: dict[str, Any] = Field(default_factory=dict)
    remote_response_bindings: dict[str, str] = Field(default_factory=dict)


class ComponentManifest(BaseModel):
    """Internal manifest used to register a component."""

    name: str
    version: str = "v1"
    description: str
    inputs: list[str]
    outputs: list[str]
    workflow_inputs_required: list[str] = Field(default_factory=list)
    workflow_inputs_optional: list[str] = Field(default_factory=list)
    input_schema: dict[str, Any] = Field(default_factory=dict)
    config_schema: dict[str, Any] | None = None
    output_schema: dict[str, Any] = Field(default_factory=dict)
    error_codes: list[str] = Field(default_factory=list)
    endpoint: ComponentEndpoint
    runtime: ComponentRuntimeManifest

    def to_definition(self) -> ComponentDefinition:
        """Project internal manifest to public discovery metadata."""
        return ComponentDefinition(
            name=self.name,
            version=self.version,
            description=self.description,
            inputs=self.inputs,
            outputs=self.outputs,
            workflow_inputs_required=self.workflow_inputs_required,
            workflow_inputs_optional=self.workflow_inputs_optional,
            input_schema=self.input_schema,
            config_schema=self.config_schema,
            output_schema=self.output_schema,
            error_codes=self.error_codes,
            endpoint=self.endpoint,
        )


class ComponentCatalogResponse(BaseModel):
    """List of discoverable components."""

    components: list[ComponentDefinition]


class FeatureSourceRunRequest(BaseModel):
    """Execute feature source component."""

    feature_source: FeatureSourceConfig
    include_features: bool = False


class FeatureSourceRunResponse(BaseModel):
    """Feature source component result."""

    bbox: list[float]
    feature_count: int
    features: dict[str, Any] | None = None


class DownloadDatasetRunRequest(BaseModel):
    """Execute dataset download component."""

    dataset_id: str
    start: str
    end: str
    overwrite: bool = False
    country_code: str | None = None
    bbox: list[float] | None = None


class DownloadDatasetRunResponse(BaseModel):
    """Download component result."""

    status: str
    dataset_id: str
    start: str
    end: str


class TemporalAggregationRunRequest(BaseModel):
    """Execute temporal aggregation component from cached dataset."""

    dataset_id: str
    start: str
    end: str
    target_period_type: PeriodType
    method: AggregationMethod = AggregationMethod.SUM
    bbox: list[float] | None = None


class TemporalAggregationRunResponse(BaseModel):
    """Temporal aggregation result summary."""

    dataset_id: str
    sizes: dict[str, int]
    dims: list[str]


class SpatialAggregationRunRequest(BaseModel):
    """Execute spatial aggregation component from cached dataset."""

    dataset_id: str
    start: str
    end: str
    feature_source: FeatureSourceConfig
    method: AggregationMethod = AggregationMethod.MEAN
    bbox: list[float] | None = None
    feature_id_property: str = "id"
    include_records: bool = False
    max_preview_rows: int = 20


class SpatialAggregationRunResponse(BaseModel):
    """Spatial aggregation result with sample rows."""

    dataset_id: str
    record_count: int
    preview: list[dict[str, Any]]
    records: list[dict[str, Any]] | None = None


class BuildDataValueSetRunRequest(BaseModel):
    """Execute build_datavalueset component directly from records."""

    dataset_id: str
    period_type: PeriodType
    records: list[dict[str, Any]] = Field(default_factory=list)
    dhis2: Dhis2DataValueSetConfig


class BuildDataValueSetRunResponse(BaseModel):
    """Build_datavalueset component output."""

    value_count: int
    output_file: str
    data_value_set: dict[str, Any]
