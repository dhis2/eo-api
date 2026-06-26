"""Built-in openEO process: climate anomaly (observed − climatological normal).

Delegates the day-of-year/month alignment and subtraction to
``earthkit.transforms.climatology.anomaly``, which indexes the normal's ordinal axis
(``dayofyear`` 1..366 or ``month`` 1..12, auto-detected from the climatology) onto the
observed cube's datetime axis and combines them — so anomalies can be computed from an
already-published observed dataset and a published normal (see the ``climate_anomaly``
workflow). ``relative=True`` yields percent-of-normal.
"""

from __future__ import annotations

from typing import Any, cast

import numpy as np
import xarray as xr
from earthkit.transforms import climatology as ek_climatology

from open_climate_service.process import process

_ORDINALS = ("dayofyear", "month")
_METHODS = ("absolute", "relative")

# Day count separating a sub-daily/daily observed cube from a monthly one: a day-of-year
# normal expects the former, a month normal the latter.
_MONTHLY_STEP_DAYS = 20

# Units / standard names that mark an interval-scale temperature, for which a *relative*
# (percent-of-normal) anomaly is meaningless: dividing by the normal flips sign for a
# negative normal and diverges as the normal approaches 0.
_TEMPERATURE_UNITS = {
    "degc",
    "°c",
    "c",
    "celsius",
    "degree_celsius",
    "degrees_celsius",
    "k",
    "kelvin",
    "degree_kelvin",
    "degrees_kelvin",
}


def _is_temperature_like(*arrays: xr.DataArray) -> bool:
    """True if any cube's units/standard_name marks it as a temperature (interval scale)."""
    for da in arrays:
        attrs: dict[str, Any] = getattr(da, "attrs", {}) or {}
        units = str(attrs.get("units", "")).strip().lower()
        standard_name = str(attrs.get("standard_name", "")).strip().lower()
        if units in _TEMPERATURE_UNITS or "temperature" in standard_name:
            return True
    return False


def _observed_step_days(observed: xr.DataArray, t_dim: str) -> float | None:
    """Median spacing of the observed time axis in days (None for a single timestep)."""
    times = observed[t_dim].values
    if times.size < 2:
        return None
    diffs = np.abs(np.diff(times).astype("timedelta64[h]").astype("float64")) / 24.0
    return float(np.median(diffs))


def _match_spatial_grid(normal: xr.DataArray, observed: xr.DataArray, t_dim: str) -> xr.DataArray:
    """Snap the normal onto the observed cube's spatial grid.

    Observed and normal cubes can be produced independently (e.g. a CDS-derived observed
    vs an EDH normal whose longitudes were remapped from [0, 360)), so equal nominal grids
    may still differ by floating-point noise (~1e-11). earthkit's anomaly subtracts the
    climatology with xarray, which aligns coordinates by exact equality — that would
    intersect such grids to nothing and yield an empty anomaly. Reindex the normal onto the
    observed coordinates by nearest neighbour within half a grid step, so float noise snaps
    cleanly while genuinely unmatched cells become NaN rather than collapsing the grid.

    The step is taken from the first spacing of each axis, i.e. a *regular* grid is assumed
    (true for the lat/lon and UTM grids this serves); an irregular axis would mis-size the
    tolerance.
    """
    spatial_dims = [d for d in observed.dims if d != t_dim and d in normal.dims and d in observed.coords]
    if not spatial_dims:
        return normal
    steps = [abs(float(np.diff(observed[d].values)[0])) for d in spatial_dims if observed.sizes[d] > 1]
    tolerance = 0.5 * min(steps) if steps else None
    return normal.reindex({d: observed[d] for d in spatial_dims}, method="nearest", tolerance=tolerance)


@process(
    summary="Climate anomaly (observed − climatological normal)",
    parameters={
        "observed": {"description": "Observed cube with a datetime time axis (e.g. era5land_temperature_daily)."},
        "normal": {"description": "Climatological normal with a `dayofyear` or `month` ordinal axis."},
        "method": {
            "description": (
                "'absolute' (observed − normal, default) or 'relative' (percent: "
                "100·(observed − normal)/normal). 'relative' is only meaningful for a "
                "ratio-scale variable such as precipitation, not temperature. "
                "'standardised' (z-score) needs a standard-deviation normal — not yet "
                "supported (see issue #223)."
            )
        },
    },
)
def compute_anomaly(observed: xr.DataArray, normal: xr.DataArray, method: str = "absolute") -> xr.DataArray:
    """Compute observed − climatological normal, aligning the normal by day-of-year/month.

    earthkit indexes the normal's ordinal axis (``dayofyear`` or ``month``) by each observed
    timestep's calendar value and combines per ``method``; the result keeps the observed
    time axis and stays lazy/dask-backed. The observed temporal resolution must match the
    normal's ordinal axis (daily observed ↔ ``dayofyear`` normal, monthly observed ↔
    ``month`` normal); a mismatch is rejected rather than silently resampled.
    """
    if method not in _METHODS:
        if method == "standardised":
            raise ValueError("method 'standardised' (z-score) needs a standard-deviation normal — see issue #223")
        raise ValueError(f"method must be one of {_METHODS}, got {method!r}")

    t_dim = next(
        (d for d in observed.dims if d in observed.coords and np.issubdtype(observed[d].dtype, np.datetime64)),
        None,
    )
    if t_dim is None:
        raise ValueError("compute_anomaly requires a datetime temporal dimension on the observed cube")
    t_dim = str(t_dim)

    ordinal = next((d for d in _ORDINALS if d in normal.dims), None)
    if ordinal is None:
        raise ValueError(f"normal must have one of {_ORDINALS} as a dimension, got dims {tuple(normal.dims)}")

    if method == "relative" and _is_temperature_like(observed, normal):
        raise ValueError(
            "method 'relative' is not meaningful for temperature (an interval scale): dividing by the "
            "normal flips sign for a negative normal and diverges near 0 °C. Use 'absolute', or 'relative' "
            "only for ratio-scale variables such as precipitation."
        )

    # Reject an observed/normal resolution mismatch: earthkit would otherwise silently
    # resample the observed to the normal's frequency (e.g. daily → monthly means).
    step = _observed_step_days(observed, t_dim)
    if step is not None:
        if ordinal == "month" and step < _MONTHLY_STEP_DAYS:
            raise ValueError(
                f"a 'month' normal expects a monthly observed dataset, but the observed steps ~{step:.0f} "
                "day(s); pair it with a monthly observed, or use a 'dayofyear' normal"
            )
        if ordinal == "dayofyear" and step >= _MONTHLY_STEP_DAYS:
            raise ValueError(
                f"a 'dayofyear' normal expects a daily observed dataset, but the observed steps ~{step:.0f} "
                "day(s); pair it with a daily observed, or use a 'month' normal"
            )

    # Guard the float-noise grid mismatch before earthkit's xarray subtraction (see helper).
    normal = _match_spatial_grid(normal, observed, t_dim)
    return cast(
        xr.DataArray,
        ek_climatology.anomaly(observed, climatology=normal, time_dim=t_dim, relative=method == "relative"),
    )
