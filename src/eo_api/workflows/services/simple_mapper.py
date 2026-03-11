"""Mapper from simplified workflow inputs to internal workflow request."""

from __future__ import annotations

from fastapi import HTTPException

from ...data_registry.services.datasets import get_dataset
from ..schemas import (
    AggregationMethod,
    Dhis2DataValueSetConfig,
    FeatureSourceConfig,
    FeatureSourceType,
    SpatialAggregationConfig,
    TemporalAggregationConfig,
    WorkflowExecuteRequest,
    WorkflowRequest,
)

_IGNORED_FIELDS = ["dry_run", "stage", "flavor", "output_format"]


def normalize_simple_request(payload: WorkflowRequest) -> tuple[WorkflowExecuteRequest, list[str]]:
    """Translate public workflow request format to internal workflow request."""
    inputs = payload
    dataset_id = inputs.dataset_id
    dataset = get_dataset(dataset_id)

    period_type = str(dataset.get("period_type", "")).lower() if dataset else ""

    if inputs.start_date and inputs.end_date:
        if period_type == "yearly":
            start = inputs.start_date[:4]
            end = inputs.end_date[:4]
        elif period_type in {"hourly", "daily", "monthly"}:
            # dhis2eo downloaders expect month windows for these dataset types.
            start = inputs.start_date[:7]
            end = inputs.end_date[:7]
        else:
            start = inputs.start_date
            end = inputs.end_date
    elif inputs.start_year is not None and inputs.end_year is not None:
        if period_type == "yearly":
            start = str(inputs.start_year)
            end = str(inputs.end_year)
        else:
            start = f"{inputs.start_year}-01-01"
            end = f"{inputs.end_year}-12-31"
    else:
        raise HTTPException(status_code=422, detail="Provide either start_date/end_date or start_year/end_year")

    if inputs.org_unit_level is not None:
        feature_source = FeatureSourceConfig(
            source_type=FeatureSourceType.DHIS2_LEVEL,
            dhis2_level=inputs.org_unit_level,
            feature_id_property=inputs.feature_id_property,
        )
    elif inputs.org_unit_ids:
        feature_source = FeatureSourceConfig(
            source_type=FeatureSourceType.DHIS2_IDS,
            dhis2_ids=inputs.org_unit_ids,
            feature_id_property=inputs.feature_id_property,
        )
    else:
        raise HTTPException(status_code=422, detail="Provide org_unit_level or org_unit_ids")

    reducer_alias = AggregationMethod(inputs.reducer.lower()) if inputs.reducer else None
    spatial_method = reducer_alias or inputs.spatial_reducer
    temporal_method = reducer_alias or inputs.temporal_reducer

    normalized = WorkflowExecuteRequest(
        dataset_id=dataset_id,
        start=start,
        end=end,
        overwrite=inputs.overwrite,
        country_code=inputs.country_code,
        feature_source=feature_source,
        temporal_aggregation=TemporalAggregationConfig(
            target_period_type=inputs.temporal_resolution,
            method=temporal_method,
        ),
        spatial_aggregation=SpatialAggregationConfig(method=spatial_method),
        dhis2=Dhis2DataValueSetConfig(data_element_uid=inputs.data_element),
    )

    warnings = [f"Input field '{field}' is currently accepted but not used in execution" for field in _IGNORED_FIELDS]
    return normalized, warnings
