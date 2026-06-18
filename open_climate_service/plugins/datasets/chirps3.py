"""CHIRPS3 plugin for per-period streaming ingest.

CHIRPS3 keeps the first vertical slice small:
- daily periods
- grid inferred from the first fetched period
- one remote raster per period
- straightforward bbox clipping into the requested extent

This plugin stays intentionally source-focused. It knows how to derive the
available periods and fetch one day, but it does not know anything about job
state, artifact records, or store mutation beyond returning one dataset.
"""

from __future__ import annotations

import asyncio
import calendar
from datetime import UTC, date, datetime
from typing import Any

import xarray as xr

from open_climate_service.streaming import BaseDatasetPlugin, daily_period_ids, normalize_period

_CHIRPS3_NODATA = -9999.0


class CHIRPS3DailyPlugin(BaseDatasetPlugin):
    """Streaming plugin for CHIRPS3 daily precipitation.

    Args:
        stage: ``final`` or ``prelim`` CHIRPS3 release stage.
        flavor: delivery flavor for final products. ``prelim`` currently only
            supports the ``sat`` path.
    """

    max_concurrency = 4
    commit_batch_size = 30

    def __init__(self, stage: str = "final", flavor: str = "rnl", **_: Any) -> None:
        if stage not in {"final", "prelim"}:
            raise ValueError(f"stage must be 'final' or 'prelim', got {stage!r}")
        if stage == "final" and flavor not in {"rnl", "sat"}:
            raise ValueError(f"For stage='final', flavor must be 'rnl' or 'sat', got {flavor!r}")
        if stage == "prelim" and flavor != "sat":
            raise ValueError(f"For stage='prelim', flavor must be 'sat', got {flavor!r}")
        self.stage = stage
        self.flavor = flavor

    async def periods(self, start: str, end: str) -> list[str]:
        """Return ordered daily periods, clamped to the latest complete month."""
        cutoff = await asyncio.to_thread(self._availability_cutoff)
        return daily_period_ids(start, end, cutoff=cutoff)

    def fetch_period(self, period_id: str, bbox: list[float], **_: Any) -> xr.Dataset:
        """Fetch one day, clip it to the requested bbox, and return a one-step dataset.

        A regular (blocking) method — the framework runs it in a worker thread.
        """
        import rioxarray

        day = date.fromisoformat(period_id)
        da = rioxarray.open_rasterio(self._url_for_day(day), chunks=None, masked=True)
        if not isinstance(da, xr.DataArray):
            raise TypeError(f"Expected DataArray from CHIRPS3 raster read, got {type(da).__name__}")
        return normalize_period(da, variable="precip", period=period_id, nodata=_CHIRPS3_NODATA, bbox=bbox)

    def _availability_cutoff(self) -> date:
        """Return the last day of the most recently published complete month.

        Scans backward with a HEAD request on the last-day COG URL so the
        cutoff reflects actual CDN state rather than a hardcoded lag assumption.
        Raises if no published month is found within the last 6 months.
        """
        import httpx

        today = datetime.now(UTC).date()
        y, m = today.year, today.month
        for _ in range(6):
            m -= 1
            if m == 0:
                m, y = 12, y - 1
            last_day = calendar.monthrange(y, m)[1]
            candidate = date(y, m, last_day)
            resp = httpx.head(self._url_for_day(candidate), timeout=10, follow_redirects=True)
            if resp.status_code == 200:
                return candidate
        raise RuntimeError("No published CHIRPS3 month found in the last 6 months")

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
