"""ERA5-Land streaming plugins."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import timedelta
from importlib import import_module
from threading import Lock
from typing import Any, cast

import numpy as np
import xarray as xr

from open_climate_service.shared.time import datetime_to_period_string, parse_period_string_to_datetime
from open_climate_service.streaming.protocol import GridSpec


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


def _open_era5_land_region(variable: str, bbox: tuple[float, float, float, float]) -> xr.Dataset:
    module = import_module("dhis2eo.data.destine.era5_land.hourly")
    open_zarr = cast(Callable[[list[str]], xr.Dataset], getattr(module, "open_zarr"))
    get_zarr_region = cast(
        Callable[[xr.Dataset, tuple[float, float, float, float]], xr.Dataset],
        getattr(module, "get_zarr_region"),
    )
    return get_zarr_region(open_zarr([variable]), bbox)
