"""Shared helpers for streaming dataset plugins.

`normalize_period` turns a freshly read raster / dataset into the canonical
``(t, y, x)`` single-variable shape the store expects: drop curvilinear 2-D
lon/lat helpers, rename dims (incl. projected ``X``/``Y``), reproject-clip to the
requested bbox, drop the band axis, mask the nodata sentinel, and stamp the period
onto a time dimension.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    import xarray as xr

# Source dim/coord names mapped onto the canonical x / y axes. Includes geographic
# (lon/lat) and projected (X/Y) spellings so projected-grid plugins need no bespoke
# rename. First match wins.
_X_NAMES = ("lon", "longitude", "x", "X")
_Y_NAMES = ("lat", "latitude", "y", "Y")
_TIME_NAMES = ("time", "valid_time")


def normalize_period(
    obj: "xr.DataArray | xr.Dataset",
    *,
    variable: str,
    source_variable: str | None = None,
    period: str | None = None,
    nodata: float | None = None,
    bbox: list[float] | None = None,
    bbox_crs: int | str = "EPSG:4326",
    squeeze_band: bool = True,
    time_dim: str = "t",
    x_dim: str = "x",
    y_dim: str = "y",
) -> "xr.Dataset":
    """Normalize a fetched raster/dataset to the canonical single-period shape.

    Steps (each applied only when relevant):
    1. drop curvilinear 2-D ``lon``/``lat`` helper coordinates (they are not the
       store's spatial dims and confuse both the rename below and rioxarray)
    2. rename the source x axis (``lon``/``longitude``/``X``) → `x_dim`, the y axis
       (``lat``/``latitude``/``Y``) → `y_dim`, and ``time``/``valid_time`` → `time_dim`
    3. clip to `bbox`, reprojecting the bbox from `bbox_crs` to the source CRS — so
       a projected-grid source (e.g. UTM) clips correctly against a WGS84 bbox
       without any bespoke coordinate transform (requires the source to carry a CRS)
    4. drop a singleton ``band`` dimension
    5. wrap a `DataArray` into a `Dataset` named `variable` (or rename
       `source_variable` → `variable` for a `Dataset`)
    6. mask the `nodata` sentinel to NaN
    7. stamp `period` onto `time_dim` when the data has no time dimension yet
    """
    import xarray as xr

    aux_2d = [c for c in getattr(obj, "coords", {}) if obj[c].ndim > 1 and c in (_X_NAMES + _Y_NAMES)]
    if aux_2d:
        obj = obj.drop_vars(aux_2d)

    names = set(getattr(obj, "dims", ())) | set(getattr(obj, "coords", {}))
    rename: dict[str, str] = {}
    for src in _X_NAMES:
        if src in names and x_dim not in names:
            rename[src] = x_dim
            break
    for src in _Y_NAMES:
        if src in names and y_dim not in names:
            rename[src] = y_dim
            break
    for src in _TIME_NAMES:
        if src in names and time_dim not in names:
            rename[src] = time_dim
            break
    if rename:
        obj = obj.rename(rename)

    if bbox is not None:
        import rioxarray  # noqa: F401  # pyright: ignore[reportUnusedImport]  # activates the .rio accessor

        xmin, ymin, xmax, ymax = map(float, bbox)
        obj = obj.rio.clip_box(minx=xmin, miny=ymin, maxx=xmax, maxy=ymax, crs=bbox_crs)

    if squeeze_band and "band" in getattr(obj, "dims", ()):
        obj = obj.squeeze("band", drop=True)

    if isinstance(obj, xr.DataArray):
        ds = obj.to_dataset(name=variable)
    else:
        ds = obj
        if source_variable and source_variable != variable and source_variable in ds:
            ds = ds.rename({source_variable: variable})
        if variable in ds:
            ds = ds[[variable]]

    if nodata is not None and variable in ds:
        ds[variable] = ds[variable].where(ds[variable] != nodata)

    if period is not None and time_dim not in ds.dims:
        ds = ds.expand_dims({time_dim: [np.datetime64(period)]})

    return ds
