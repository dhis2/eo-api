"""Forecast cubes: two temporal axes instead of one.

A forecast is not a time series. The same date has as many values as there are runs that
predicted it, each better than the last, so a store that keeps only one of them cannot say
which — and a store keyed on the date being predicted overwrites yesterday's forecast every
time it refreshes. Forecast cubes therefore carry two axes:

    (reference_time, lead_time, y, x)

``reference_time`` is when the forecast was issued and ``lead_time`` how far ahead it reaches,
so the date a value describes is ``reference_time + lead_time`` — published as the
``forecast_valid_time`` auxiliary coordinate.

**Lead time rather than valid time** as the second axis, because keyed on valid time the array
is triangular: each run fills a different diagonal and a rolling archive is mostly fill value.
Keyed on lead time it is dense, every run having exactly the same number of steps.

**Neither axis is named ``t``.** ``get_time_dim`` resolves ``t``/``valid_time``/``time`` and a
dozen modules act on the result, all taking it to mean *the period this value describes*.
Neither forecast axis means that: keyed on the issue date, coverage would report when forecasts
were made rather than what they cover, and the map slider would scrub the day the forecast was
produced. So ``get_time_dim`` raises on a forecast cube and callers have to opt in — a loud
failure rather than a plausible wrong answer. Same reasoning as ``Cadence.IRREGULAR`` versus
returning ``None``.

For the same reason the valid-time coordinate is **not** called ``time`` or ``valid_time``:
``get_time_dim`` tests ``hasattr``, so either name would be returned as the time *dimension*
and callers would fail on ``ds.sizes[...]`` with a confusing error.

Consumers that only want "the current forecast" do not need to know any of this — see
:func:`latest_reference_view`, which collapses a cube to an ordinary ``(t, y, x)`` dataset.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import xarray as xr

REFERENCE_DIM = "reference_time"
"""Issue time: when the forecast was produced. CF ``forecast_reference_time``."""

LEAD_DIM = "lead_time"
"""How far ahead each step reaches, in days. CF ``forecast_period``."""

VALID_COORD = "forecast_valid_time"
"""The date each value describes, as a ``(reference_time, lead_time)`` auxiliary coordinate."""

CF_STANDARD_NAMES = {
    REFERENCE_DIM: "forecast_reference_time",
    LEAD_DIM: "forecast_period",
}


def is_forecast_cube(ds: Any) -> bool:
    """True when a dataset carries both forecast axes as dimensions.

    Both, deliberately: a cube with only ``lead_time`` is a single run that has lost its issue
    time, and one with only ``reference_time`` is a time series whose axis has been misnamed.
    Neither should be treated as a forecast archive.
    """
    sizes = getattr(ds, "sizes", None)
    if sizes is None:
        return False
    return REFERENCE_DIM in sizes and LEAD_DIM in sizes


def lead_days(ds: xr.Dataset | xr.DataArray) -> np.ndarray:
    """``lead_time`` as whole days, whichever way the store spelled it.

    A lead is written as an integer count of days with ``units: days``, which is the CF encoding
    for ``forecast_period`` — and which xarray then decodes back to ``timedelta64`` on read. So
    the same axis is integer days in a plugin and nanoseconds in a reader, and anything taking
    the raw values as a day count publishes ``lead_time: 86400000000000``. Both spellings are
    legitimate, so every consumer that wants a number of days comes through here.
    """
    values = np.asarray(ds.coords[LEAD_DIM].values)
    if np.issubdtype(values.dtype, np.timedelta64):
        return (values / np.timedelta64(1, "D")).astype("int64")
    return values.astype("int64")


def lead_offsets(ds: xr.Dataset | xr.DataArray) -> np.ndarray:
    """``lead_time`` as ``timedelta64[ns]`` offsets, for adding to an issue time."""
    return lead_days(ds).astype("timedelta64[D]").astype("timedelta64[ns]")


def valid_time(ds: xr.Dataset | xr.DataArray) -> xr.DataArray:
    """Return the valid-time coordinate, computing it if the store did not publish one.

    Recomputed rather than required, so a cube written by a plugin that omitted the auxiliary
    coordinate is still describable. ``lead_time`` is taken as whole days.
    """
    if VALID_COORD in getattr(ds, "coords", {}):
        return ds.coords[VALID_COORD]
    reference = ds.coords[REFERENCE_DIM]
    offsets = xr.DataArray(lead_offsets(ds), dims=(LEAD_DIM,), coords={LEAD_DIM: ds.coords[LEAD_DIM]})
    return (reference + offsets).rename(VALID_COORD)


def valid_time_bounds(ds: xr.Dataset | xr.DataArray) -> tuple[np.datetime64, np.datetime64]:
    """Earliest and latest date the cube says anything about.

    This — not the span of issue times — is a forecast store's temporal coverage. A run issued
    today covering ten days is not a store that ends today.
    """
    values = np.asarray(valid_time(ds).values, dtype="datetime64[ns]")
    return values.min(), values.max()


def latest_reference(ds: xr.Dataset | xr.DataArray) -> np.datetime64:
    """The most recent issue time in the cube — the current forecast."""
    latest = np.asarray(ds.coords[REFERENCE_DIM].values, dtype="datetime64[ns]").max()
    return np.datetime64(latest, "ns")


def latest_reference_view(ds: xr.Dataset, *, reference: Any = None, time_dim: str = "t") -> xr.Dataset:
    """Collapse a forecast cube to an ordinary ``(t, y, x)`` dataset for one issue time.

    The archive keeps every run; almost every consumer wants one. Selecting a single
    ``reference_time`` leaves valid time one-dimensional along ``lead_time``, so it can become
    the ``t`` axis — after which the DHIS2 and CHAP exports, the aggregation processes and the
    anomaly process work unchanged, with no forecast awareness at all.

    ``reference`` defaults to the latest run. The chosen issue time is kept as a scalar
    ``reference_time`` coordinate, so the result still records which forecast it came from.
    """
    if not is_forecast_cube(ds):
        return ds
    chosen = latest_reference(ds) if reference is None else np.datetime64(reference, "ns")
    one = ds.sel({REFERENCE_DIM: chosen})
    stamps = np.asarray(valid_time(ds).sel({REFERENCE_DIM: chosen}).values, dtype="datetime64[ns]")
    one = one.assign_coords({time_dim: (LEAD_DIM, stamps)}).swap_dims({LEAD_DIM: time_dim})
    # `lead_time` survives as a coordinate along `t`, which keeps the lead of each step
    # recoverable — a consumer can still tell a one-day forecast from a ten-day one.
    return one.drop_vars(VALID_COORD, errors="ignore")


def period_bounds_as_strings(ds: xr.Dataset, period_type: str) -> tuple[str, str]:
    """Valid-time bounds as dataset-native period id strings."""
    from open_climate_service.shared.time import datetime_to_period_string

    first, last = valid_time_bounds(ds)
    return (
        datetime_to_period_string(pd.Timestamp(first).to_pydatetime(), period_type),
        datetime_to_period_string(pd.Timestamp(last).to_pydatetime(), period_type),
    )
