"""Temporal aggregation component."""

from __future__ import annotations

from typing import cast

import xarray as xr

from ...data_manager.services.utils import get_time_dim
from ..schemas import AggregationMethod, PeriodType

_PERIOD_TO_FREQ: dict[PeriodType, str] = {
    PeriodType.HOURLY: "1h",
    PeriodType.DAILY: "1D",
    PeriodType.MONTHLY: "MS",
    PeriodType.YEARLY: "YS",
}


def aggregate_temporal(ds: xr.Dataset, *, period_type: PeriodType, method: AggregationMethod) -> xr.Dataset:
    """Resample a dataset over the time dimension to the target period."""
    time_dim = get_time_dim(ds)
    freq = _PERIOD_TO_FREQ[period_type]
    resampled = ds.resample({time_dim: freq})
    return cast(xr.Dataset, getattr(resampled, method.value)(keep_attrs=True))


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


# from workflows engine
def _run_temporal_aggregation(
    *,
    runtime: WorkflowRuntime,
    request: WorkflowExecuteRequest,
    dataset: dict[str, Any],
    context: dict[str, Any],
    step_config: dict[str, Any],
) -> dict[str, Any]:
    target_period_type = PeriodType(
        str(step_config.get("target_period_type", request.temporal_aggregation.target_period_type))
    )
    method = AggregationMethod(str(step_config.get("method", request.temporal_aggregation.method)))
    temporal_ds = runtime.run(
        "temporal_aggregation",
        component_services.temporal_aggregation_component,
        dataset=dataset,
        start=request.start,
        end=request.end,
        bbox=_require_context(context, "bbox"),
        target_period_type=target_period_type,
        method=method,
    )
    return {"temporal_dataset": temporal_ds}
