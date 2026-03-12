"""Component service implementations and discovery metadata."""

from __future__ import annotations

from typing import Any, Final

import xarray as xr
from fastapi import HTTPException

from ..data_accessor.services.accessor import get_data
from ..data_manager.services import downloader
from ..data_registry.services.datasets import get_dataset
from ..workflows.schemas import (
    AggregationMethod,
    Dhis2DataValueSetConfig,
    FeatureSourceConfig,
    PeriodType,
)
from ..workflows.services.datavalueset import build_data_value_set
from ..workflows.services.features import resolve_features
from ..workflows.services.preflight import check_upstream_connectivity
from ..workflows.services.spatial import aggregate_to_features
from ..workflows.services.temporal import aggregate_temporal
from .schemas import ComponentDefinition, ComponentEndpoint

_ERROR_CODES_V1: Final[list[str]] = [
    "INPUT_VALIDATION_FAILED",
    "CONFIG_VALIDATION_FAILED",
    "OUTPUT_VALIDATION_FAILED",
    "UPSTREAM_UNREACHABLE",
    "EXECUTION_FAILED",
]

_COMPONENT_REGISTRY: Final[dict[str, ComponentDefinition]] = {
    "feature_source@v1": ComponentDefinition(
        name="feature_source",
        version="v1",
        description="Resolve feature source and compute bbox.",
        inputs=["feature_source"],
        outputs=["features", "bbox"],
        input_schema={
            "type": "object",
            "properties": {"feature_source": {"type": "object"}},
            "required": ["feature_source"],
        },
        config_schema={
            "type": "object",
            "properties": {
                "execution_mode": {"type": "string", "enum": ["local", "remote"]},
                "remote_url": {"type": ["string", "null"]},
                "remote_timeout_sec": {"type": "number"},
                "remote_retries": {"type": "integer"},
                "remote_retry_delay_sec": {"type": "number"},
            },
            "additionalProperties": False,
        },
        output_schema={
            "type": "object",
            "properties": {
                "features": {"type": "object"},
                "bbox": {"type": "array", "items": {"type": "number"}, "minItems": 4, "maxItems": 4},
            },
            "required": ["features", "bbox"],
        },
        error_codes=_ERROR_CODES_V1,
        endpoint=ComponentEndpoint(path="/components/feature-source", method="POST"),
    ),
    "download_dataset@v1": ComponentDefinition(
        name="download_dataset",
        version="v1",
        description="Download dataset files for period and bbox.",
        inputs=["dataset_id", "start", "end", "overwrite", "country_code", "bbox"],
        outputs=["status"],
        input_schema={
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string"},
                "start": {"type": "string"},
                "end": {"type": "string"},
                "overwrite": {"type": "boolean"},
                "country_code": {"type": ["string", "null"]},
                "bbox": {"type": "array", "items": {"type": "number"}, "minItems": 4, "maxItems": 4},
            },
            "required": ["dataset_id", "start", "end", "overwrite", "bbox"],
        },
        config_schema={
            "type": "object",
            "properties": {
                "execution_mode": {"type": "string", "enum": ["local", "remote"]},
                "remote_url": {"type": ["string", "null"]},
                "remote_timeout_sec": {"type": "number"},
                "remote_retries": {"type": "integer"},
                "remote_retry_delay_sec": {"type": "number"},
            },
            "additionalProperties": False,
        },
        output_schema={"type": "object", "properties": {"status": {"type": "string"}}},
        error_codes=_ERROR_CODES_V1,
        endpoint=ComponentEndpoint(path="/components/download-dataset", method="POST"),
    ),
    "temporal_aggregation@v1": ComponentDefinition(
        name="temporal_aggregation",
        version="v1",
        description="Aggregate dataset over time dimension.",
        inputs=["dataset_id", "start", "end", "target_period_type", "method", "bbox"],
        outputs=["dataset"],
        input_schema={
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string"},
                "start": {"type": "string"},
                "end": {"type": "string"},
                "target_period_type": {"type": "string"},
                "method": {"type": "string"},
                "bbox": {"type": ["array", "null"], "items": {"type": "number"}},
            },
            "required": ["dataset_id", "start", "end", "target_period_type", "method"],
        },
        config_schema={
            "type": "object",
            "properties": {
                "execution_mode": {"type": "string", "enum": ["local", "remote"]},
                "remote_url": {"type": ["string", "null"]},
                "remote_timeout_sec": {"type": "number"},
                "remote_retries": {"type": "integer"},
                "remote_retry_delay_sec": {"type": "number"},
            },
            "additionalProperties": False,
        },
        output_schema={"type": "object", "properties": {"dataset": {"type": "object"}}},
        error_codes=_ERROR_CODES_V1,
        endpoint=ComponentEndpoint(path="/components/temporal-aggregation", method="POST"),
    ),
    "spatial_aggregation@v1": ComponentDefinition(
        name="spatial_aggregation",
        version="v1",
        description="Aggregate gridded dataset to features.",
        inputs=["dataset_id", "start", "end", "feature_source", "method"],
        outputs=["records"],
        input_schema={
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string"},
                "start": {"type": "string"},
                "end": {"type": "string"},
                "feature_source": {"type": "object"},
                "method": {"type": "string"},
            },
            "required": ["dataset_id", "start", "end", "feature_source", "method"],
        },
        config_schema={
            "type": "object",
            "properties": {
                "execution_mode": {"type": "string", "enum": ["local", "remote"]},
                "remote_url": {"type": ["string", "null"]},
                "remote_timeout_sec": {"type": "number"},
                "remote_retries": {"type": "integer"},
                "remote_retry_delay_sec": {"type": "number"},
            },
            "additionalProperties": False,
        },
        output_schema={"type": "object", "properties": {"records": {"type": "array"}}},
        error_codes=_ERROR_CODES_V1,
        endpoint=ComponentEndpoint(path="/components/spatial-aggregation", method="POST"),
    ),
    "build_datavalueset@v1": ComponentDefinition(
        name="build_datavalueset",
        version="v1",
        description="Build and serialize DHIS2 DataValueSet JSON.",
        inputs=["dataset_id", "period_type", "records", "dhis2"],
        outputs=["data_value_set", "output_file"],
        input_schema={
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string"},
                "period_type": {"type": "string"},
                "records": {"type": "array"},
                "dhis2": {"type": "object"},
            },
            "required": ["dataset_id", "period_type", "records", "dhis2"],
        },
        config_schema={
            "type": "object",
            "properties": {
                "execution_mode": {"type": "string", "enum": ["local", "remote"]},
                "remote_url": {"type": ["string", "null"]},
                "remote_timeout_sec": {"type": "number"},
                "remote_retries": {"type": "integer"},
                "remote_retry_delay_sec": {"type": "number"},
            },
            "additionalProperties": False,
        },
        output_schema={
            "type": "object",
            "properties": {"data_value_set": {"type": "object"}, "output_file": {"type": "string"}},
            "required": ["data_value_set", "output_file"],
        },
        error_codes=_ERROR_CODES_V1,
        endpoint=ComponentEndpoint(path="/components/build-datavalue-set", method="POST"),
    ),
}


def component_catalog(*, include_internal: bool = False) -> list[ComponentDefinition]:
    """Return discoverable component definitions.

    By default, internal orchestration-only metadata (config_schema) is hidden.
    """
    components = list(_COMPONENT_REGISTRY.values())
    if include_internal:
        return components
    return [component.model_copy(update={"config_schema": None}) for component in components]


def component_registry() -> dict[str, ComponentDefinition]:
    """Return registry entries keyed by component@version."""
    return dict(_COMPONENT_REGISTRY)


def feature_source_component(config: FeatureSourceConfig) -> tuple[dict[str, Any], list[float]]:
    """Run feature source component."""
    return resolve_features(config)


def download_dataset_component(
    *,
    dataset: dict[str, Any],
    start: str,
    end: str,
    overwrite: bool,
    country_code: str | None,
    bbox: list[float],
) -> None:
    """Run connectivity preflight and download dataset files."""
    check_upstream_connectivity(dataset)
    downloader.download_dataset(
        dataset=dataset,
        start=start,
        end=end,
        overwrite=overwrite,
        background_tasks=None,
        country_code=country_code,
        bbox=bbox,
    )


def temporal_aggregation_component(
    *,
    dataset: dict[str, Any],
    start: str,
    end: str,
    bbox: list[float] | None,
    target_period_type: PeriodType,
    method: AggregationMethod,
) -> xr.Dataset:
    """Load dataset and aggregate over time."""
    ds = get_data(dataset=dataset, start=start, end=end, bbox=bbox)
    return aggregate_temporal(ds=ds, period_type=target_period_type, method=method)


def spatial_aggregation_component(
    *,
    dataset: dict[str, Any],
    start: str,
    end: str,
    bbox: list[float] | None,
    features: dict[str, Any],
    method: AggregationMethod,
    feature_id_property: str,
) -> list[dict[str, Any]]:
    """Load dataset and aggregate spatially to provided features."""
    ds = get_data(dataset=dataset, start=start, end=end, bbox=bbox)
    return aggregate_to_features(
        ds=ds,
        variable=dataset["variable"],
        features=features,
        method=method.value,
        feature_id_property=feature_id_property,
    )


def build_datavalueset_component(
    *,
    dataset_id: str,
    period_type: PeriodType,
    records: list[dict[str, Any]],
    dhis2: Dhis2DataValueSetConfig,
) -> tuple[dict[str, Any], str]:
    """Build and serialize DHIS2 DataValueSet from records."""
    return build_data_value_set(records=records, dataset_id=dataset_id, period_type=period_type, config=dhis2)


def require_dataset(dataset_id: str) -> dict[str, Any]:
    """Resolve dataset or raise 404."""
    dataset = get_dataset(dataset_id)
    if dataset is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    return dataset
