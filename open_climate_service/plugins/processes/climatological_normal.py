"""Built-in openEO process: day-of-year climatological normal.

Reduces a cube's temporal dimension to a WMO-style day-of-year climatology, so normals
can be computed from an already-ingested managed GeoZarr collection (load the reference
period with ``load_collection``'s ``temporal_extent``, then ``save_result``/publish). The
output replaces the temporal axis with a non-temporal ``dayofyear`` dimension (1..366),
which the STAC layer declares and the map viewer's generic slider renders.
"""

from __future__ import annotations

import numpy as np
import xarray as xr

from open_climate_service.process import process


def _circular_rolling_mean(da: xr.DataArray, window: int) -> xr.DataArray:
    """Circular rolling mean over the (leading) dayofyear axis (wraps Dec→Jan)."""
    vals = np.concatenate([da.values, da.values, da.values], axis=0)
    result = np.empty_like(da.values)
    half = window // 2
    n = da.sizes["dayofyear"]
    for i in range(n):
        centre = n + i
        result[i] = vals[centre - half : centre + half + 1].mean(axis=0)
    return da.copy(data=result)


@process(
    summary="Day-of-year climatological normal",
    parameters={
        "smoothing_window": {
            "description": "Circular rolling-mean window in days for WMO smoothing (0 disables). Default 31."
        },
    },
)
def climatological_normal(data: xr.DataArray, smoothing_window: int = 31) -> xr.DataArray:
    """Compute a day-of-year climatology from a cube's temporal dimension.

    The mean per day-of-year over the loaded time range, optionally circular-smoothed
    (WMO 31-day); the temporal dimension is reduced to a ``dayofyear`` dimension (1..366).
    Select the reference period via ``load_collection``'s ``temporal_extent`` (e.g.
    ``["1991-01-01", "2020-12-31"]``).
    """
    t_dim = next(
        (d for d in data.dims if d in data.coords and np.issubdtype(data[d].dtype, np.datetime64)),
        "t" if "t" in data.dims else None,
    )
    if t_dim is None:
        raise ValueError("climatological_normal requires a temporal dimension on the input cube")
    t_dim = str(t_dim)

    clim = data.groupby(f"{t_dim}.dayofyear").mean(t_dim)
    clim = clim.transpose("dayofyear", ...)  # ensure dayofyear is the leading axis for smoothing
    if smoothing_window and int(smoothing_window) > 0:
        clim = _circular_rolling_mean(clim, int(smoothing_window))
    return clim
