"""Component service implementations and discovery metadata."""

from __future__ import annotations

from typing import Any

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
from .schemas import ComponentDefinition


def component_catalog() -> list[ComponentDefinition]:
    """Return all discoverable component definitions."""
    return [
        ComponentDefinition(
            name="feature_source",
            description="Resolve feature source and compute bbox.",
            inputs=["feature_source"],
            outputs=["features", "bbox"],
        ),
        ComponentDefinition(
            name="download_dataset",
            description="Download dataset files for period and bbox.",
            inputs=["dataset_id", "start", "end", "overwrite", "country_code", "bbox"],
            outputs=["status"],
        ),
        ComponentDefinition(
            name="temporal_aggregation",
            description="Aggregate dataset over time dimension.",
            inputs=["dataset_id", "start", "end", "target_period_type", "method", "bbox"],
            outputs=["dataset"],
        ),
        ComponentDefinition(
            name="spatial_aggregation",
            description="Aggregate gridded dataset to features.",
            inputs=["dataset_id", "start", "end", "feature_source", "method"],
            outputs=["records"],
        ),
        ComponentDefinition(
            name="build_datavalueset",
            description="Build and serialize DHIS2 DataValueSet JSON.",
            inputs=["dataset_id", "period_type", "records", "dhis2"],
            outputs=["data_value_set", "output_file"],
        ),
    ]


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
