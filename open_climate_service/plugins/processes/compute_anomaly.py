"""Built-in openEO process: climate anomaly (observed − climatological normal).

Aligns a normal's ordinal axis (``dayofyear`` 1..366 or ``month`` 1..12) onto an observed
cube's datetime ``t`` axis and combines them, so anomalies can be computed from an already
-published observed dataset and a published normal (see the ``climate_anomaly`` workflow).
The standard openEO ``subtract`` can't do this — it would require the normal to already
carry a matching ``t`` axis.
"""

from __future__ import annotations

import numpy as np
import xarray as xr

from open_climate_service.process import process

_ORDINALS = ("dayofyear", "month")
_METHODS = ("absolute", "relative")


def _match_spatial_grid(aligned: xr.DataArray, observed: xr.DataArray, t_dim: str) -> xr.DataArray:
    """Snap the aligned normal onto the observed cube's spatial grid.

    Observed and normal cubes can be produced independently (e.g. a CDS-derived observed
    vs an EDH-derived normal whose longitudes were remapped from [0, 360)), so equal
    nominal grids may still differ by floating-point noise (~1e-11). xarray aligns
    coordinates by exact equality, which would intersect such grids to nothing — and the
    whole anomaly would come out empty. Reindex the normal onto the observed coordinates by
    nearest neighbour within half a grid step, so float noise snaps cleanly while genuinely
    unmatched cells become NaN rather than silently collapsing the grid.
    """
    spatial_dims = [d for d in observed.dims if d != t_dim and d in aligned.dims and d in observed.coords]
    if not spatial_dims:
        return aligned
    steps = [abs(float(np.diff(observed[d].values)[0])) for d in spatial_dims if observed.sizes[d] > 1]
    tolerance = 0.5 * min(steps) if steps else None
    return aligned.reindex({d: observed[d] for d in spatial_dims}, method="nearest", tolerance=tolerance)


@process(
    summary="Climate anomaly (observed − climatological normal)",
    parameters={
        "observed": {"description": "Observed cube with a datetime time axis (e.g. era5land_temperature_daily)."},
        "normal": {"description": "Climatological normal with a `dayofyear` or `month` ordinal axis."},
        "method": {
            "description": (
                "'absolute' (observed − normal, default) or 'relative' (percent: "
                "100·(observed − normal)/normal). 'standardised' (z-score) needs a "
                "standard-deviation normal — not yet supported (see issue #223)."
            )
        },
    },
)
def compute_anomaly(observed: xr.DataArray, normal: xr.DataArray, method: str = "absolute") -> xr.DataArray:
    """Compute observed − climatological normal, aligning the normal by day-of-year/month.

    The normal's ordinal axis (``dayofyear`` or ``month``) is indexed by each observed
    timestep's corresponding calendar value and broadcast onto the observed ``t`` axis,
    then combined per ``method``. The result keeps the observed ``(t, y, x)`` shape. The
    selection is vectorised (lazy/dask-preserving), so large cubes aren't materialised.
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

    ordinal = next((d for d in _ORDINALS if d in normal.dims), None)
    if ordinal is None:
        raise ValueError(f"normal must have one of {_ORDINALS} as a dimension, got dims {tuple(normal.dims)}")

    # Per-timestep calendar value (e.g. day-of-year) along t, used to pick the matching
    # normal slice for every observed time step (vectorised → stays lazy).
    calendar_index = getattr(observed[t_dim].dt, ordinal)
    aligned = normal.sel({ordinal: calendar_index}).drop_vars(ordinal, errors="ignore")
    aligned = _match_spatial_grid(aligned, observed, str(t_dim))

    if method == "absolute":
        return observed - aligned
    return 100.0 * (observed - aligned) / aligned  # relative (%)
