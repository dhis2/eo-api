"""Base class for dataset plugins.

Subclassing `BaseDatasetPlugin` is the recommended way to write a dataset plugin.
It supplies the concurrency defaults and canonical dimension names; a plugin then
implements just two methods:

- `periods(start, end)` — the ordered list of period ids the source has, and
- `fetch_period(period_id, bbox, **params)` — fetch one period as an
  `xarray.Dataset`. Implement it as a regular method for blocking I/O (the
  framework runs it in a worker thread) or as `async def` for natively-async
  sources such as lazy Zarr access. Either way, return the dataset normalized to
  ``(t, y, x)`` — the `normalize_period` helper does this.

The store grid (shape, dtype, nodata, CRS) is inferred from the first fetched
period; a projected-grid source declares its CRS via the `crs` class attribute.
"""

from __future__ import annotations

from collections.abc import Awaitable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import xarray as xr


class BaseDatasetPlugin:
    """Convenience base implementing the dataset plugin contract.

    Class attributes (override only when the defaults do not fit):
    - `max_concurrency` — how many periods to fetch concurrently (default 1)
    - `commit_batch_size` — cursor-checkpoint cadence in periods (default 1)
    - `time_dim` / `x_dim` / `y_dim` — canonical dimension names (default t/x/y)
    - `crs` — native CRS of the fetched data as an EPSG int or string (default
      4326). Used as the fallback when the orchestrator infers the grid from the
      first fetched period and the data itself carries no CRS — how a projected
      source (e.g. UTM) declares its projection.

    Template-defined `ingestion.params` are passed to the constructor; the default
    `__init__` stores them on `self.params`. Subclasses that need named
    configuration should define their own `__init__`.
    """

    max_concurrency: int = 1
    commit_batch_size: int = 1
    time_dim: str = "t"
    x_dim: str = "x"
    y_dim: str = "y"
    crs: int | str = 4326

    def __init__(self, **params: Any) -> None:
        self.params: dict[str, Any] = params

    async def periods(self, start: str, end: str) -> list[str]:
        """Return ordered available period identifiers for the requested range."""
        raise NotImplementedError("Dataset plugins must implement periods()")

    def fetch_period(self, period_id: str, bbox: list[float], **params: Any) -> "xr.Dataset | Awaitable[xr.Dataset]":
        """Fetch one period as a dataset normalized to (t, y, x).

        Implement as a regular method for blocking I/O (the framework runs it in a
        worker thread) or as ``async def`` for natively-async sources — the return
        type covers both forms.
        """
        raise NotImplementedError("Dataset plugins must implement fetch_period()")
