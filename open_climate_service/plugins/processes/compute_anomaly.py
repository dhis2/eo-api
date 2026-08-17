"""Built-in openEO process: climate anomaly (observed − climatological normal).

Delegates the day-of-year/month alignment and subtraction to
``earthkit.transforms.climatology.anomaly``, which indexes the normal's ordinal axis
(``dayofyear`` 1..366 or ``month`` 1..12, auto-detected from the climatology) onto the
observed cube's datetime axis and combines them — so anomalies can be computed from an
already-published observed dataset and a published normal (see the ``climate_anomaly``
workflow). ``relative=True`` yields percent-of-normal.

The subtraction, the calendar indexing and the ``relative`` percent form are all earthkit's.
What this module adds is deliberately limited to two kinds of thing, kept separate so the
first shrinks over time and the second does not (see CLIM-859):

**Tracked upstream gaps** — delete once earthkit covers them:

- ``_match_spatial_grid`` works around exact-equality coordinate alignment.

**OCS policy** — ours to own, because earthkit is a general library and should not be
deciding our domain rules:

- refusing ``relative`` for temperature, an interval scale where percent-of-normal is
  meaningless;
- refusing an observed/normal temporal-resolution mismatch, which earthkit would otherwise
  silently resample.
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


_COORD_NOISE_RTOL = 1e-6
"""How far a normal's coordinate may sit from the observed one, as a fraction of the grid step.

Sized for floating-point noise and nothing else. The noise this absorbs is ~1e-11 absolute on
coordinates of order 1–360, so a millionth of a cell is already five orders of magnitude of
headroom, while still being five orders below any offset that could represent a different cell.
"""


def _match_spatial_grid(normal: xr.DataArray, observed: xr.DataArray, t_dim: str) -> xr.DataArray:
    """Relabel the normal's spatial coordinates with the observed cube's, or raise.

    Observed and normal cubes can be produced independently (e.g. a CDS-derived observed vs
    an EDH normal whose longitudes were remapped from [0, 360)), so nominally equal grids
    may still differ by floating-point noise (~1e-11). earthkit's anomaly subtracts the
    climatology with plain xarray arithmetic, which aligns coordinates by **exact
    equality**, and it does not warn when that fails.

    Measured against earthkit-transforms 1.0.0: with 1e-11 of noise on a single row, the
    result keeps its full shape — a closing ``broadcast_like`` restores the dimensions — but
    every cell on the mismatched row comes back NaN. So the output *looks* structurally
    correct while silently losing data, and when the whole axis is uniformly offset (the
    realistic remapped-longitude case) that is every cell in the cube.

    This is therefore **coordinate normalisation, not resampling**: the axes must already
    describe the same cells (same length, same spacing, same positions to within
    :data:`_COORD_NOISE_RTOL` of a step), and then the observed coordinates are copied over
    so the arithmetic aligns. Anything larger raises and asks for an explicit regridding step.

    An earlier version reindexed by nearest neighbour within *half a grid step*, which is
    nine orders of magnitude more slack than float noise needs, and paired different physical
    cells without saying so: on a 0.1° grid, a normal offset by a full 0.1° came back shifted
    one cell with only a single NaN, so the anomaly was computed against the neighbouring
    cell for 9 of 10 cells while looking clean. A cell-centre vs cell-edge convention
    mismatch between two products is exactly that case.

    Upstream gap, not a preference: earthkit could either accept a tolerance or — cheaper and
    arguably better — detect the failed alignment and raise instead of returning a quietly
    NaN cube. To be filed against ecmwf/earthkit-transforms; delete this helper once it lands.

    A *regular* grid is assumed (true for the lat/lon and UTM grids this serves); the spacing
    check below is what makes that assumption explicit rather than silent.
    """
    spatial_dims = [str(d) for d in observed.dims if d != t_dim and d in normal.dims and d in observed.coords]
    if not spatial_dims:
        return normal

    replacements: dict[str, Any] = {}
    for dim in spatial_dims:
        obs_coord = observed[dim].values
        nrm_coord = normal[dim].values

        if obs_coord.shape != nrm_coord.shape:
            raise ValueError(
                f"observed and normal disagree on the size of '{dim}' "
                f"({obs_coord.size} vs {nrm_coord.size}); regrid the normal onto the observed "
                "grid before computing an anomaly"
            )
        if obs_coord.size < 2:
            # A single cell has no spacing to compare; fall through to the position check,
            # which needs a step — use the coordinate magnitude to scale the tolerance.
            step = abs(float(obs_coord[0])) or 1.0
        else:
            obs_steps = np.diff(obs_coord.astype(float))
            nrm_steps = np.diff(nrm_coord.astype(float))
            step = abs(float(obs_steps[0]))
            if not np.allclose(nrm_steps, obs_steps, rtol=_COORD_NOISE_RTOL, atol=0.0):
                raise ValueError(
                    f"observed and normal have different '{dim}' spacing "
                    f"({obs_steps[0]:g} vs {nrm_steps[0]:g}); regrid the normal onto the "
                    "observed grid before computing an anomaly"
                )

        offset = np.abs(nrm_coord.astype(float) - obs_coord.astype(float))
        worst = float(offset.max())
        if worst > _COORD_NOISE_RTOL * abs(step):
            raise ValueError(
                f"observed and normal '{dim}' coordinates differ by up to {worst:g} "
                f"({worst / abs(step):.3g} of the {abs(step):g} grid step), which is more than "
                "floating-point noise — they describe different cells. Regrid the normal onto "
                "the observed grid before computing an anomaly."
            )
        replacements[dim] = observed[dim]

    # assign_coords, not reindex: a pure relabel cannot reorder, drop or pair cells.
    return normal.assign_coords(replacements)


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
                "supported (see CLIM-887)."
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
            raise ValueError("method 'standardised' (z-score) needs a standard-deviation normal — see CLIM-887")
        raise ValueError(f"method must be one of {_METHODS}, got {method!r}")

    # earthkit 1.0 auto-detects the time dimension, so passing time_dim below is belt and
    # braces. The detection is kept for the *error*: given a cube with no datetime axis,
    # earthkit fails with "Invalid frequency 'month' - see xarray documentation", which
    # names neither the real problem nor the cube. Checking here says what is actually wrong.
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

    # OCS policy, not an earthkit gap: earthkit offers `relative` for any variable, which is
    # correct for a general library. Deciding it is invalid for our temperature datasets is
    # our call, so this guard stays regardless of what upstream does.
    if method == "relative" and _is_temperature_like(observed, normal):
        raise ValueError(
            "method 'relative' is not meaningful for temperature (an interval scale): dividing by the "
            "normal flips sign for a negative normal and diverges near 0 °C. Use 'absolute', or 'relative' "
            "only for ratio-scale variables such as precipitation."
        )

    # OCS policy, as above. earthkit would silently resample the observed cube to the
    # normal's frequency (daily observed against a month normal → monthly means), returning
    # a plausible result for a pairing the caller almost certainly did not intend. Upstream
    # could reasonably warn; refusing outright is our decision, so this guard also stays.
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
