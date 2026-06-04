"""ERA5-Land streaming plugins."""

from __future__ import annotations

import asyncio
import math
import tempfile
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from importlib import import_module
from pathlib import Path
from threading import Lock
from typing import Any, cast

import numpy as np
import xarray as xr

from open_climate_service.shared.time import datetime_to_period_string, parse_period_string_to_datetime
from open_climate_service.streaming.protocol import GridSpec
from open_climate_service.transforms.unit_conversion import kelvin_to_celsius, metres_to_mm

_ERA5_LAND_RES_DEG = 0.1

# CDS API long-name variables for reanalysis-era5-land-monthly-means
_CDS_VARIABLE_NAMES: dict[str, str] = {
    "t2m": "2m_temperature",
    "tp": "total_precipitation",
}


class ERA5LandHourlySingleBandPlugin:
    """Streaming plugin for direct single-band ERA5-Land hourly variables."""

    max_concurrency = 2
    commit_batch_size = 24

    def __init__(self, variable: str) -> None:
        if not variable:
            raise ValueError("ERA5LandHourlySingleBandPlugin requires a non-empty variable")
        self.variable = variable
        self._cache_lock = Lock()
        self._cached_bbox: tuple[float, float, float, float] | None = None
        self._cached_region: xr.Dataset | None = None

    async def probe(self, bbox: list[float], **params: Any) -> GridSpec:
        _ = params
        region = await asyncio.to_thread(self._region_for_bbox, bbox)
        data = region[self.variable]
        return GridSpec(
            shape=(int(data.sizes["latitude"]), int(data.sizes["longitude"])),
            crs=4326,
            dtype=np.dtype(data.dtype),
            nodata=None,
            time_dim="t",
            x_dim="x",
            y_dim="y",
        )

    async def periods(self, start: str, end: str) -> list[str]:
        current = parse_period_string_to_datetime(start)
        limit = parse_period_string_to_datetime(end)
        if current > limit:
            return []
        periods: list[str] = []
        while current <= limit:
            periods.append(datetime_to_period_string(current, "hourly"))
            current += timedelta(hours=1)
        return periods

    async def fetch_period(self, period_id: str, bbox: list[float], **params: Any) -> xr.Dataset:
        _ = params
        return await asyncio.to_thread(self._fetch_sync, period_id, bbox)

    def _fetch_sync(self, period_id: str, bbox: list[float]) -> xr.Dataset:
        region = self._region_for_bbox(bbox)
        timestamp = parse_period_string_to_datetime(period_id).replace(tzinfo=None)
        selected = region.sel(valid_time=slice(timestamp, timestamp))
        ds = selected[[self.variable]].load()
        rename_map = {"longitude": "x", "latitude": "y", "valid_time": "t"}
        return ds.rename(rename_map) if rename_map else ds

    def _region_for_bbox(self, bbox: list[float]) -> xr.Dataset:
        bbox_tuple = cast(tuple[float, float, float, float], tuple(map(float, bbox)))
        with self._cache_lock:
            if self._cached_region is not None and self._cached_bbox == bbox_tuple:
                return self._cached_region
            self._close_cached_region_locked()
            region = _open_era5_land_region(self.variable, bbox_tuple)
            self._cached_bbox = bbox_tuple
            self._cached_region = region
            return region

    def close(self) -> None:
        with self._cache_lock:
            self._close_cached_region_locked()

    def _close_cached_region_locked(self) -> None:
        if self._cached_region is not None:
            self._cached_region.close()
        self._cached_region = None
        self._cached_bbox = None


class ERA5LandPrecipitationPlugin(ERA5LandHourlySingleBandPlugin):
    """ERA5-Land hourly precipitation plugin.

    This currently preserves the legacy dataset semantics exactly:
    - fetch direct `tp` values from the upstream ERA5-Land source
    - rely on the template transform pipeline for metres -> millimetres

    Deaccumulation and trailing-boundary handling belong to the dedicated
    precipitation canonicalization work and should be added here later without
    forcing another dataset-template contract change.
    """

    def __init__(self) -> None:
        super().__init__(variable="tp")


class ERA5LandMonthlySingleBandPlugin:
    """Streaming plugin for monthly ERA5-Land means from the Copernicus CDS.

    Fetches one calendar month at a time from the ``reanalysis-era5-land-monthly-means``
    dataset via the CDS API (``ecmwf-datastores``). Credentials are read from
    ``~/.cdsapirc`` or the ``CDSAPI_URL`` / ``CDSAPI_KEY`` environment variables.
    """

    max_concurrency = 1
    commit_batch_size = 12

    def __init__(self, variable: str, **_: Any) -> None:
        if variable not in _CDS_VARIABLE_NAMES:
            raise ValueError(
                f"ERA5LandMonthlySingleBandPlugin: unsupported variable {variable!r}; "
                f"expected one of {list(_CDS_VARIABLE_NAMES)}"
            )
        self.variable = variable

    async def probe(self, bbox: list[float], **_: Any) -> GridSpec:
        xmin, ymin, xmax, ymax = map(float, bbox)
        nx = max(1, math.ceil((xmax - xmin) / _ERA5_LAND_RES_DEG))
        ny = max(1, math.ceil((ymax - ymin) / _ERA5_LAND_RES_DEG))
        return GridSpec(
            shape=(ny, nx),
            crs=4326,
            dtype=np.dtype("float32"),
            nodata=None,
            time_dim="t",
            x_dim="x",
            y_dim="y",
        )

    async def periods(self, start: str, end: str) -> list[str]:
        cutoff = _monthly_availability_cutoff()
        start_dt = _parse_monthly(start)
        end_dt = min(_parse_monthly(end), datetime(cutoff.year, cutoff.month, 1))
        if start_dt > end_dt:
            return []
        result: list[str] = []
        current = start_dt
        while current <= end_dt:
            result.append(f"{current.year:04d}-{current.month:02d}")
            month = current.month % 12 + 1
            year = current.year + (1 if current.month == 12 else 0)
            current = datetime(year, month, 1)
        return result

    async def fetch_period(self, period_id: str, bbox: list[float], **_: Any) -> xr.Dataset:
        return await asyncio.to_thread(self._fetch_sync, period_id, bbox)

    def _fetch_sync(self, period_id: str, bbox: list[float]) -> xr.Dataset:
        year, month = int(period_id[:4]), int(period_id[5:7])
        xmin, ymin, xmax, ymax = map(float, bbox)

        from ecmwf.datastores import Client

        params = {
            "product_type": ["monthly_averaged_reanalysis"],
            "variable": [_CDS_VARIABLE_NAMES[self.variable]],
            "year": [str(year)],
            "month": [str(month).zfill(2)],
            "time": ["00:00"],
            "area": [ymax, xmin, ymin, xmax],  # N, W, S, E
            "data_format": "netcdf",
            "download_format": "unarchived",
        }

        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "era5land_monthly.nc"
            remote = Client().submit("reanalysis-era5-land-monthly-means", params)
            remote.download(str(target))
            ds = xr.open_dataset(target, engine="netcdf4").load()

        ds = ds[[self.variable]]
        time_dim = "valid_time" if "valid_time" in ds.dims else "time"
        rename_map = {"longitude": "x", "latitude": "y", time_dim: "t"}
        ds = ds.rename({k: v for k, v in rename_map.items() if k in ds})

        if self.variable == "t2m":
            ds = kelvin_to_celsius(ds, {"variable": self.variable})
        return ds


class ERA5LandMonthlyPrecipitationPlugin(ERA5LandMonthlySingleBandPlugin):
    """ERA5-Land monthly total precipitation plugin (metres → millimetres)."""

    def __init__(self, **_: Any) -> None:
        super().__init__(variable="tp")

    def _fetch_sync(self, period_id: str, bbox: list[float]) -> xr.Dataset:
        ds = super()._fetch_sync(period_id, bbox)
        return metres_to_mm(ds, {"variable": self.variable})


def _parse_monthly(period: str) -> datetime:
    """Parse a monthly period string (YYYY-MM or full ISO) to the first of the month."""
    if len(period) == 7:
        return datetime(int(period[:4]), int(period[5:7]), 1)
    dt = parse_period_string_to_datetime(period)
    return datetime(dt.year, dt.month, 1)


def _monthly_availability_cutoff() -> date:
    """Return the latest month for which CDS ERA5-Land monthly means are reliably published.

    CDS publishes monthly means approximately 2 months after the reference month.
    """
    today = datetime.now(UTC).date()
    year, month = today.year, today.month
    for _ in range(2):
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    return date(year, month, 1)


def _open_era5_land_region(variable: str, bbox: tuple[float, float, float, float]) -> xr.Dataset:
    module = import_module("dhis2eo.data.destine.era5_land.hourly")
    open_zarr = cast(Callable[[list[str]], xr.Dataset], getattr(module, "open_zarr"))
    get_zarr_region = cast(
        Callable[[xr.Dataset, tuple[float, float, float, float]], xr.Dataset],
        getattr(module, "get_zarr_region"),
    )
    return get_zarr_region(open_zarr([variable]), bbox)
