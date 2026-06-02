"""CHIRPS3 plugin for per-period streaming ingest.

CHIRPS3 is the Ticket 1 proving source because it keeps the first vertical slice
small:
- daily periods
- cheap probe derived from known resolution
- one remote raster per period
- straightforward bbox clipping into the requested extent

This plugin stays intentionally source-focused. It knows how to derive the
available periods and fetch one day, but it does not know anything about job
state, artifact records, or store mutation beyond returning one dataset.
"""

from __future__ import annotations

import asyncio
import atexit
import calendar
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from typing import Any

import numpy as np
import xarray as xr

from open_climate_service.streaming.protocol import GridSpec

_CHIRPS3_NODATA = -9999.0
_CHIRPS3_RES_DEG = 0.05
_COMPLETE_AFTER_DAY = 20
_executor: ThreadPoolExecutor | None = None


def _shutdown_executor() -> None:
    global _executor
    if _executor is not None:
        _executor.shutdown(wait=False, cancel_futures=True)
        _executor = None


def _get_executor() -> ThreadPoolExecutor:
    global _executor
    if _executor is None:
        _executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="chirps3")
        atexit.register(_shutdown_executor)
    return _executor


class CHIRPS3DailyPlugin:
    """Streaming plugin for CHIRPS3 daily precipitation.

    Args:
        stage: ``final`` or ``prelim`` CHIRPS3 release stage.
        flavor: delivery flavor for final products. ``prelim`` currently only
            supports the ``sat`` path.
    """

    max_concurrency = 4
    commit_batch_size = 30

    def __init__(self, stage: str = "final", flavor: str = "rnl") -> None:
        if stage not in {"final", "prelim"}:
            raise ValueError(f"stage must be 'final' or 'prelim', got {stage!r}")
        if stage == "final" and flavor not in {"rnl", "sat"}:
            raise ValueError(f"For stage='final', flavor must be 'rnl' or 'sat', got {flavor!r}")
        if stage == "prelim" and flavor != "sat":
            raise ValueError(f"For stage='prelim', flavor must be 'sat', got {flavor!r}")
        self.stage = stage
        self.flavor = flavor

    async def probe(self, bbox: list[float], **_: Any) -> GridSpec:
        """Estimate grid metadata directly from CHIRPS3's known 0.05 degree grid."""
        import math

        xmin, ymin, xmax, ymax = map(float, bbox)
        nx = max(1, math.ceil((xmax - xmin) / _CHIRPS3_RES_DEG))
        ny = max(1, math.ceil((ymax - ymin) / _CHIRPS3_RES_DEG))
        return GridSpec(shape=(ny, nx), crs=4326, dtype=np.dtype("float32"), nodata=_CHIRPS3_NODATA)

    async def periods(self, start: str, end: str) -> list[str]:
        """Return ordered daily periods, clamped to the latest complete month."""
        cutoff = self._availability_cutoff()
        start_date = date.fromisoformat(start[:10])
        end_date = min(date.fromisoformat(end[:10]), cutoff)
        if start_date > end_date:
            return []
        periods: list[str] = []
        current = start_date
        while current <= end_date:
            periods.append(current.isoformat())
            current += timedelta(days=1)
        return periods

    async def fetch_period(self, period_id: str, bbox: list[float], **_: Any) -> xr.Dataset:
        """Fetch one day, clip it to the requested bbox, and return a one-step dataset."""
        return await asyncio.get_running_loop().run_in_executor(_get_executor(), self._fetch_sync, period_id, bbox)

    def _availability_cutoff(self) -> date:
        """Return the last day of the latest month treated as complete by this plugin."""
        today = datetime.now(UTC).date()
        months_back = 1 if today.day > _COMPLETE_AFTER_DAY else 2
        year, month = today.year, today.month
        for _ in range(months_back):
            month -= 1
            if month == 0:
                month = 12
                year -= 1
        return date(year, month, calendar.monthrange(year, month)[1])

    def _url_for_day(self, day: date) -> str:
        """Build the remote CHIRPS3 raster URL for one day."""
        if self.stage == "final":
            return (
                f"https://data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/final/{self.flavor}/cogs/{day.year}/"
                f"chirps-v3.0.{self.flavor}.{day.year}.{day.month:02d}.{day.day:02d}.cog"
            )
        return (
            f"https://data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/prelim/sat/{day.year}/"
            f"chirps-v3.0.prelim.{day.year}.{day.month:02d}.{day.day:02d}.tif"
        )

    def _fetch_sync(self, period_id: str, bbox: list[float]) -> xr.Dataset:
        """Blocking raster read used behind the async fetch interface."""
        import rioxarray

        day = date.fromisoformat(period_id)
        da = rioxarray.open_rasterio(self._url_for_day(day), chunks=None, masked=True)
        if not isinstance(da, xr.DataArray):
            raise TypeError(f"Expected DataArray from CHIRPS3 raster read, got {type(da).__name__}")
        xmin, ymin, xmax, ymax = map(float, bbox)
        da = da.rio.clip_box(minx=xmin, miny=ymin, maxx=xmax, maxy=ymax)
        da = da.squeeze("band", drop=True)
        da = da.where(da != _CHIRPS3_NODATA).load()
        ds = da.to_dataset(name="precip")
        return ds.expand_dims(t=[np.datetime64(period_id, "D")])  # type: ignore[no-any-return]
