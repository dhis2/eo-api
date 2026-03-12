"""Component discovery and execution endpoints."""

from __future__ import annotations

from typing import Any

import numpy as np
from fastapi import APIRouter, Query

from ..data_manager.services.constants import BBOX
from . import services
from .schemas import (
    BuildDataValueSetRunRequest,
    BuildDataValueSetRunResponse,
    ComponentCatalogResponse,
    DownloadDatasetRunRequest,
    DownloadDatasetRunResponse,
    FeatureSourceRunRequest,
    FeatureSourceRunResponse,
    SpatialAggregationRunRequest,
    SpatialAggregationRunResponse,
    TemporalAggregationRunRequest,
    TemporalAggregationRunResponse,
)

router = APIRouter()


def _to_jsonable_scalar(value: Any) -> Any:
    """Convert numpy scalars/datetimes to JSON-safe native values."""
    if isinstance(value, np.datetime64):
        return np.datetime_as_string(value, unit="s")
    if isinstance(value, np.generic):
        return value.item()
    return value


def _json_safe_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Ensure record rows are JSON-serializable."""
    return [{key: _to_jsonable_scalar(value) for key, value in record.items()} for record in records]


@router.get("/components", response_model=ComponentCatalogResponse, response_model_exclude_none=True)
def list_components(include_internal: bool = Query(default=False)) -> ComponentCatalogResponse:
    """List all discoverable reusable components."""
    return ComponentCatalogResponse(components=services.component_catalog(include_internal=include_internal))


@router.post("/components/feature-source", response_model=FeatureSourceRunResponse)
def run_feature_source(payload: FeatureSourceRunRequest) -> FeatureSourceRunResponse:
    """Resolve feature source to features and bbox."""
    features, bbox = services.feature_source_component(payload.feature_source)
    return FeatureSourceRunResponse(
        bbox=bbox,
        feature_count=len(features["features"]),
        features=features if payload.include_features else None,
    )


@router.post("/components/download-dataset", response_model=DownloadDatasetRunResponse)
def run_download_dataset(payload: DownloadDatasetRunRequest) -> DownloadDatasetRunResponse:
    """Download dataset files for the selected period/scope."""
    dataset = services.require_dataset(payload.dataset_id)
    bbox = payload.bbox or BBOX
    services.download_dataset_component(
        dataset=dataset,
        start=payload.start,
        end=payload.end,
        overwrite=payload.overwrite,
        country_code=payload.country_code,
        bbox=bbox,
    )
    return DownloadDatasetRunResponse(
        status="completed",
        dataset_id=payload.dataset_id,
        start=payload.start,
        end=payload.end,
    )


@router.post("/components/temporal-aggregation", response_model=TemporalAggregationRunResponse)
def run_temporal_aggregation(payload: TemporalAggregationRunRequest) -> TemporalAggregationRunResponse:
    """Aggregate a dataset temporally."""
    dataset = services.require_dataset(payload.dataset_id)
    ds = services.temporal_aggregation_component(
        dataset=dataset,
        start=payload.start,
        end=payload.end,
        bbox=payload.bbox,
        target_period_type=payload.target_period_type,
        method=payload.method,
    )
    return TemporalAggregationRunResponse(
        dataset_id=payload.dataset_id,
        sizes={str(k): int(v) for k, v in ds.sizes.items()},
        dims=[str(d) for d in ds.dims],
    )


@router.post("/components/spatial-aggregation", response_model=SpatialAggregationRunResponse)
def run_spatial_aggregation(payload: SpatialAggregationRunRequest) -> SpatialAggregationRunResponse:
    """Aggregate a dataset spatially to features."""
    dataset = services.require_dataset(payload.dataset_id)
    features, bbox = services.feature_source_component(payload.feature_source)
    records = services.spatial_aggregation_component(
        dataset=dataset,
        start=payload.start,
        end=payload.end,
        bbox=payload.bbox or bbox,
        features=features,
        method=payload.method,
        feature_id_property=payload.feature_id_property,
    )
    json_records = _json_safe_records(records)
    return SpatialAggregationRunResponse(
        dataset_id=payload.dataset_id,
        record_count=len(json_records),
        preview=json_records[: payload.max_preview_rows],
        records=json_records if payload.include_records else None,
    )


@router.post("/components/build-datavalue-set", response_model=BuildDataValueSetRunResponse)
def run_build_datavalueset(payload: BuildDataValueSetRunRequest) -> BuildDataValueSetRunResponse:
    """Build and serialize a DHIS2 DataValueSet from records."""
    data_value_set, output_file = services.build_datavalueset_component(
        dataset_id=payload.dataset_id,
        period_type=payload.period_type,
        records=payload.records,
        dhis2=payload.dhis2,
    )
    return BuildDataValueSetRunResponse(
        value_count=len(data_value_set.get("dataValues", [])),
        output_file=output_file,
        data_value_set=data_value_set,
    )
