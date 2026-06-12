"""Climate normals (day-of-year climatology) as a streaming dataset plugin.

Computes a WMO-style day-of-year climatology (default reference period 1991–2020)
by reading the reference period **directly from the Earth Data Hub** daily ERA5-Land
Zarr store — so no prior 30-year ingestion is required.

The result is stored with a real ``dayofyear`` dimension (1..366) — a non-temporal
stepping axis declared in STAC ``cube:dimensions`` and rendered by the map viewer's
generic dimension slider (see the ordinal-dimension support). It flows through the
standard ingestion → registry → STAC → publish pipeline; no bespoke endpoint.

Computing normals from an already-ingested managed dataset is handled the openEO way (a
process graph over a loaded collection + ``save_result``), not by this plugin.

The heavy climatology is computed once per ingestion (cached) and then served one
day-of-year at a time to fit the per-period streaming contract.
"""

from __future__ import annotations

import os
from threading import Lock
from typing import Any
from urllib.parse import urlparse, urlunparse

import numpy as np
import xarray as xr

from open_climate_service.streaming.protocol import GridSpec
from open_climate_service.transforms.unit_conversion import kelvin_to_celsius, metres_to_mm

_DEFAULT_PERIOD = (1991, 2020)
_DEFAULT_SMOOTHING = 31
_DAYOFYEAR_DIM = "dayofyear"

_EDH_DAILY_URL = "https://api.earthdatahub.destine.eu/era5/era5-land-daily-utc-v1.zarr"
_EDH_API_KEY_ENV = "EDH_API_KEY"


def _circular_rolling_mean(da: xr.DataArray, window: int) -> xr.DataArray:
    """Circular rolling mean over the dayofyear axis (wraps Dec→Jan)."""
    vals = np.concatenate([da.values, da.values, da.values], axis=0)
    result = np.empty_like(da.values)
    half = window // 2
    n = da.sizes["dayofyear"]
    for i in range(n):
        centre = n + i
        result[i] = vals[centre - half : centre + half + 1].mean(axis=0)
    return da.copy(data=result)


def _apply_unit_transform(ds: xr.Dataset, variable: str, transform: str | None) -> xr.Dataset:
    if transform in (None, "", "none"):
        return ds
    if transform == "kelvin_to_celsius":
        return kelvin_to_celsius(ds, {"variable": variable})
    if transform == "metres_to_mm":
        return metres_to_mm(ds, {"variable": variable})
    raise ValueError(f"Unknown unit_transform {transform!r}")


class ClimateNormalsPlugin:
    """Streaming plugin that materialises a day-of-year climatology from Earth Data Hub.

    Args (from ``ingestion.params``):
        variable: output variable name written into the store.
        edh_variable: EDH variable name to read (e.g. ``t2m``, ``tp``).
        period: ``[start_year, end_year]`` reference period (default 1991–2020).
        smoothing_window: circular rolling-mean window in days (default 31; 0 disables).
        unit_transform: ``kelvin_to_celsius`` | ``metres_to_mm`` | ``none``.
    """

    max_concurrency = 1  # single heavy compute; serialise access to the cache
    commit_batch_size = 30

    def __init__(
        self,
        *,
        variable: str,
        edh_variable: str,
        period: list[int] | tuple[int, int] = _DEFAULT_PERIOD,
        smoothing_window: int = _DEFAULT_SMOOTHING,
        unit_transform: str | None = None,
        **_: Any,
    ) -> None:
        if not edh_variable:
            raise ValueError("ClimateNormalsPlugin requires 'edh_variable' in params")
        self.variable = variable
        self.edh_variable = edh_variable
        self.period = (int(period[0]), int(period[1]))
        self.smoothing_window = int(smoothing_window)
        self.unit_transform = unit_transform
        self._lock = Lock()
        self._climatology: xr.Dataset | None = None

    # -- streaming contract ---------------------------------------------------

    async def probe(self, bbox: list[float], **_: Any) -> GridSpec:
        import asyncio

        clim = await asyncio.to_thread(self._ensure_climatology, bbox)
        return GridSpec(
            shape=(int(clim.sizes["y"]), int(clim.sizes["x"])),
            crs=4326,
            dtype=np.dtype(clim[self.variable].dtype),
            nodata=None,
            time_dim=_DAYOFYEAR_DIM,  # the stepping axis is day-of-year, not datetime
        )

    async def periods(self, start: str, end: str) -> list[str]:
        # One entry per day-of-year present in the climatology (1..366), independent
        # of the requested range. probe() runs first and populates the cache.
        _ = start, end
        if self._climatology is not None:
            return [str(int(d)) for d in self._climatology[_DAYOFYEAR_DIM].values]
        return [str(d) for d in range(1, 367)]

    async def fetch_period(self, period_id: str, bbox: list[float], **_: Any) -> xr.Dataset:
        import asyncio

        return await asyncio.to_thread(self._fetch_sync, period_id, bbox)

    # -- internals ------------------------------------------------------------

    def _fetch_sync(self, period_id: str, bbox: list[float]) -> xr.Dataset:
        clim = self._ensure_climatology(bbox)
        # Keep dayofyear as a length-1 dimension so the orchestrator appends along it.
        return clim.sel({_DAYOFYEAR_DIM: [int(period_id)]})

    def _ensure_climatology(self, bbox: list[float]) -> xr.Dataset:
        with self._lock:
            if self._climatology is None:
                self._climatology = self._compute_climatology(bbox)
            return self._climatology

    def _compute_climatology(self, bbox: list[float]) -> xr.Dataset:
        region = self._load_reference(bbox)
        normals = region.groupby("valid_time.dayofyear").mean("valid_time")
        if self.smoothing_window > 0:
            normals[self.variable] = _circular_rolling_mean(normals[self.variable], self.smoothing_window)
        normals = _apply_unit_transform(normals, self.variable, self.unit_transform)
        return normals.load()

    def _load_reference(self, bbox: list[float]) -> xr.Dataset:
        """Load the reference-period data from EDH as a (valid_time, y, x) dataset."""
        start_year, end_year = self.period
        eps = 0.05
        xmin, ymin, xmax, ymax = map(float, bbox)
        lon_min = (xmin % 360) - eps
        lon_max = (xmax % 360) + eps
        ds = _edh_open_daily()
        region = (
            ds[[self.edh_variable]]
            .sel(
                latitude=slice(ymax + eps, ymin - eps),
                longitude=slice(lon_min, lon_max),
                valid_time=slice(f"{start_year}-01-01", f"{end_year}-12-31"),
            )
            .load()
        )
        ds.close()
        # Normalise longitude 0–360 → –180/180 and standardise dim/var names.
        region = region.assign_coords(longitude=((region.longitude + 180) % 360) - 180)
        region = region.rename({"longitude": "x", "latitude": "y"})
        if self.edh_variable != self.variable:
            region = region.rename({self.edh_variable: self.variable})
        return region


def _edh_open_daily() -> xr.Dataset:
    """Open the Earth Data Hub daily ERA5-Land Zarr store (Basic-auth via EDH_API_KEY)."""
    url = _EDH_DAILY_URL
    token = os.environ.get(_EDH_API_KEY_ENV, "")
    if token:
        parts = urlparse(url)
        url = urlunparse(parts._replace(netloc=f"edh:{token}@{parts.netloc}"))
    return xr.open_zarr(  # type: ignore[no-any-return]
        url, consolidated=True, storage_options={"client_kwargs": {"trust_env": True}}
    )
