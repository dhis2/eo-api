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
