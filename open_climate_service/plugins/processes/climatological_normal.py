"""Built-in openEO process: climatological normal (day-of-year or month-of-year).

Reduces a cube's temporal dimension to a WMO-style climatology, so normals can be
computed from an already-ingested managed GeoZarr collection (load the reference period
with ``load_collection``'s ``temporal_extent``, then ``save_result``/publish). The output
replaces the temporal axis with a non-temporal ordinal dimension — ``dayofyear`` (1..366)
or ``month`` (1..12) depending on ``frequency`` — which the STAC layer declares and the
map viewer's generic slider/dropdown renders.
"""

from __future__ import annotations

import numpy as np
import xarray as xr

from open_climate_service.process import process

_FREQUENCIES = ("dayofyear", "month")


def _circular_rolling_mean(da: xr.DataArray, window: int, dim: str) -> xr.DataArray:
    """Circular rolling mean over the (leading) ``dim`` axis (wraps end→start)."""
    vals = np.concatenate([da.values, da.values, da.values], axis=0)
    result = np.empty_like(da.values)
    half = window // 2
    n = da.sizes[dim]
    for i in range(n):
        centre = n + i
        result[i] = vals[centre - half : centre + half + 1].mean(axis=0)
    return da.copy(data=result)


@process(
    summary="Climatological normal (day-of-year or month-of-year)",
    parameters={
        "frequency": {"description": "Climatology resolution: 'dayofyear' (1..366, default) or 'month' (1..12)."},
        "smoothing_window": {
            "description": (
                "Circular rolling-mean window in days for WMO day-of-year smoothing "
                "(0 disables, default 31). Ignored when frequency='month'."
            )
        },
    },
)
def climatological_normal(data: xr.DataArray, frequency: str = "dayofyear", smoothing_window: int = 31) -> xr.DataArray:
    """Compute a climatology from a cube's temporal dimension.

    The mean per ``frequency`` bin over the loaded time range; the temporal dimension is
    reduced to an ordinal ``dayofyear`` (1..366) or ``month`` (1..12) dimension. For
    ``dayofyear`` the result is optionally circular-smoothed (WMO 31-day window); smoothing
    is not applied to ``month`` (a 12-value axis). Select the reference period via
    ``load_collection``'s ``temporal_extent`` (e.g. ``["1991-01-01", "2020-12-31"]``).
    """
    if frequency not in _FREQUENCIES:
        raise ValueError(f"frequency must be one of {_FREQUENCIES}, got {frequency!r}")
    t_dim = next(
        (d for d in data.dims if d in data.coords and np.issubdtype(data[d].dtype, np.datetime64)),
        "t" if "t" in data.dims else None,
    )
    if t_dim is None:
        raise ValueError("climatological_normal requires a temporal dimension on the input cube")
    t_dim = str(t_dim)

    clim = data.groupby(f"{t_dim}.{frequency}").mean(t_dim)
    clim = clim.transpose(frequency, ...)  # ensure the ordinal axis leads (for smoothing)
    # Circular smoothing is only meaningful for the dense day-of-year axis, not 12 months.
    if frequency == "dayofyear" and smoothing_window and int(smoothing_window) > 0:
        clim = _circular_rolling_mean(clim, int(smoothing_window), frequency)
    return clim
