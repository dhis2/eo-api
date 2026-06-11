"""Climate normals (day-of-year climatology) as a streaming dataset plugin.

Computes a WMO-style day-of-year climatology (default reference period 1991–2020)
and exposes it as an ordinary daily dataset on a *nominal* leap year (2000), so it
flows through the standard ingestion → registry → STAC → publish pipeline and
renders in the map viewer like any other daily dataset — no bespoke endpoint.

Two reference-data sources are supported via ``ingestion.params``:

- ``reference: edh`` — read the reference period directly from the Earth Data Hub
  daily ERA5-Land Zarr store. This avoids ingesting 30 years of data first.
- ``reference: dataset`` — read it from an already-ingested managed GeoZarr store
  (``source_dataset_id``) that itself covers the reference period.

The heavy climatology is computed once per ingestion (cached) and then served one
day at a time to fit the per-period streaming contract.
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from threading import Lock
from typing import Any
from urllib.parse import urlparse, urlunparse

import numpy as np
import xarray as xr

from open_climate_service.streaming.protocol import GridSpec
from open_climate_service.transforms.unit_conversion import kelvin_to_celsius, metres_to_mm

# Nominal (leap) year the day-of-year axis is mapped onto, so dayofyear 1..366
# becomes 2000-01-01 .. 2000-12-31 — a normal daily time axis.
_NOMINAL_YEAR = 2000
_DEFAULT_PERIOD = (1991, 2020)
_DEFAULT_SMOOTHING = 31

_EDH_DAILY_URL = "https://api.earthdatahub.destine.eu/era5/era5-land-daily-utc-v1.zarr"
_EDH_API_KEY_ENV = "EDH_API_KEY"
_EDH_GRID_DEG = 0.1


def _nominal_dates() -> list[str]:
    """ISO dates for day-of-year 1..366 on the nominal leap year."""
    base = date(_NOMINAL_YEAR, 1, 1)
    return [(base + timedelta(days=i)).isoformat() for i in range(366)]


def _dayofyear_for(period_id: str) -> int:
    """Day-of-year (1..366) for a nominal-year ISO date."""
    d = date.fromisoformat(period_id[:10])
    return (d - date(_NOMINAL_YEAR, 1, 1)).days + 1


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
    """Streaming plugin that materialises a day-of-year climatology.

    Args (from ``ingestion.params``):
        reference: ``edh`` (read the Earth Data Hub daily store directly) or
            ``dataset`` (read an ingested managed GeoZarr store).
        variable: output variable name written into the store.
        source_dataset_id: managed dataset id to read when ``reference: dataset``.
        edh_variable: EDH variable name (e.g. ``t2m``, ``tp``) when ``reference: edh``.
        period: ``[start_year, end_year]`` reference period (default 1991–2020).
        smoothing_window: circular rolling-mean window in days (default 31; 0 disables).
        unit_transform: ``kelvin_to_celsius`` | ``metres_to_mm`` | ``none``. EDH
            sources usually need one; managed-dataset sources are already converted.
    """

    max_concurrency = 1  # single heavy compute; serialise access to the cache
    commit_batch_size = 30

    def __init__(
        self,
        *,
        reference: str = "edh",
        variable: str,
        source_dataset_id: str | None = None,
        edh_variable: str | None = None,
        period: list[int] | tuple[int, int] = _DEFAULT_PERIOD,
        smoothing_window: int = _DEFAULT_SMOOTHING,
        unit_transform: str | None = None,
        **_: Any,
    ) -> None:
        if reference not in {"edh", "dataset"}:
            raise ValueError(f"reference must be 'edh' or 'dataset', got {reference!r}")
        if reference == "edh" and not edh_variable:
            raise ValueError("reference='edh' requires 'edh_variable' in params")
        if reference == "dataset" and not source_dataset_id:
            raise ValueError("reference='dataset' requires 'source_dataset_id' in params")
        self.reference = reference
        self.variable = variable
        self.source_dataset_id = source_dataset_id
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
        )

    async def periods(self, start: str, end: str) -> list[str]:
        # Day-of-year climatology spans a full (nominal leap) year regardless of
        # the requested range — one entry per day-of-year.
        _ = start, end
        return _nominal_dates()

    async def fetch_period(self, period_id: str, bbox: list[float], **_: Any) -> xr.Dataset:
        import asyncio

        return await asyncio.to_thread(self._fetch_sync, period_id, bbox)

    # -- internals ------------------------------------------------------------

    def _fetch_sync(self, period_id: str, bbox: list[float]) -> xr.Dataset:
        clim = self._ensure_climatology(bbox)
        doy = _dayofyear_for(period_id)
        day = clim.sel(dayofyear=doy, drop=True)
        return day.expand_dims(t=[np.datetime64(period_id[:10], "D")])

    def _ensure_climatology(self, bbox: list[float]) -> xr.Dataset:
        with self._lock:
            if self._climatology is None:
                self._climatology = self._compute_climatology(bbox)
            return self._climatology

    def _compute_climatology(self, bbox: list[float]) -> xr.Dataset:
        region = self._load_reference(bbox)
        time_dim = "valid_time" if "valid_time" in region.dims else "t"
        normals = region.groupby(f"{time_dim}.dayofyear").mean(time_dim)
        if self.smoothing_window > 0:
            normals[self.variable] = _circular_rolling_mean(normals[self.variable], self.smoothing_window)
        normals = _apply_unit_transform(normals, self.variable, self.unit_transform)
        return normals.load()

    def _load_reference(self, bbox: list[float]) -> xr.Dataset:
        """Load the reference-period data as a (time, y, x) dataset named ``self.variable``."""
        start_year, end_year = self.period
        if self.reference == "edh":
            return self._load_reference_edh(bbox, start_year, end_year)
        return self._load_reference_dataset(start_year, end_year)

    def _load_reference_edh(self, bbox: list[float], start_year: int, end_year: int) -> xr.Dataset:
        assert self.edh_variable is not None
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

    def _load_reference_dataset(self, start_year: int, end_year: int) -> xr.Dataset:
        from open_climate_service.data_accessor.services.accessor import open_icechunk_dataset
        from open_climate_service.data_manager.services.downloader import get_icechunk_path

        assert self.source_dataset_id is not None
        store_path = get_icechunk_path({"id": self.source_dataset_id})
        if not store_path.exists():
            raise ValueError(
                f"Source dataset '{self.source_dataset_id}' has no managed store at {store_path}. "
                f"Ingest it (covering {start_year}–{end_year}) before computing normals from it."
            )
        ds = open_icechunk_dataset(str(store_path))
        if self.variable not in ds:
            raise ValueError(f"Variable '{self.variable}' not found in source dataset '{self.source_dataset_id}'")
        time_dim = "t" if "t" in ds.dims else ("valid_time" if "valid_time" in ds.dims else "time")
        region = ds[[self.variable]].sel({time_dim: slice(f"{start_year}-01-01", f"{end_year}-12-31")}).load()
        ds.close()
        if time_dim != "valid_time" and time_dim != "t":
            region = region.rename({time_dim: "t"})
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
