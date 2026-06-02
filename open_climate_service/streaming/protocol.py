"""Protocol and shared types for per-period streaming ingest.

This package is intentionally internal.

`open_climate_service.ingestions` owns the application-facing ingestion lifecycle
(request handling, artifact records, publication state, and compatibility with
the rest of the API surface). `open_climate_service.streaming` owns the execution
mechanics for one ingestion strategy: probe a source, enumerate periods, fetch
them one at a time, and append them to an Icechunk-backed Zarr v3 store.

The protocol defined here is deliberately narrow so plugins stay source-focused.
They should not manage jobs, artifact persistence, sync policy, or store
mutation outside writing one fetched period as an `xarray.Dataset`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import numpy as np

if TYPE_CHECKING:
    import xarray as xr


@dataclass
class GridSpec:
    """Metadata returned by a plugin probe before the first write.

    This is the minimum store-shaping contract for Ticket 1:
    - spatial shape of one fetched period
    - native CRS
    - value dtype / nodata
    - canonical time/x/y dimension names and any root attrs the writer should keep

    `shape` is ordered as `(y, x)`, matching array axis order:
    `(len(y_dim), len(x_dim))`.

    In other words, the first element of `shape` is the size of the dimension
    named by `y_dim`, and the second element is the size of the dimension named
    by `x_dim`. Plugins should always return `shape` in this order, even if
    downstream metadata serializes axes in a different convention.
    """

    shape: tuple[int, int]  # (y, x) == (len(y_dim), len(x_dim))
    crs: int
    dtype: np.dtype[Any]
    nodata: float | None = None
    time_dim: str = "t"
    x_dim: str = "x"
    y_dim: str = "y"
    attrs: dict[str, Any] = field(default_factory=dict)
    extra_dims: dict[str, int] = field(default_factory=dict)
    """Optional non-spatial, non-time dimensions, e.g. ``{"age_group": 20}``.

    The orchestrator does not use this field; it exists for plugin authors who
    need to document multidimensional stores and for future orchestrator
    extensions.
    """


@runtime_checkable
class IngestionPlugin(Protocol):
    """Minimal source contract for the streaming ingest engine.

    Contract notes:
    - plugin classes may accept template-defined `ingestion.default_params`
      through their constructor for source configuration.
    - `probe(...)` must be cheap and must not transfer full raster data.
    - `periods(...)` must return an ordered, source-valid list of period ids.
    - `fetch_period(...)` must return exactly one logical period normalized to a
      consistent coordinate system for later append.
    - the same `ingestion.default_params` are forwarded to `probe(...)` and
      `fetch_period(...)` as `**params` for sources that prefer per-call
      configuration rather than constructor state.

    The engine remains responsible for concurrency, resume, cancellation,
    cursor persistence, and store writes.
    """

    max_concurrency: int
    commit_batch_size: int

    async def probe(self, bbox: list[float], **params: Any) -> GridSpec:
        """Return source grid metadata without transferring raster data."""
        ...

    async def periods(self, start: str, end: str) -> list[str]:
        """Return ordered available period identifiers for the requested range."""
        ...

    async def fetch_period(self, period_id: str, bbox: list[float], **params: Any) -> "xr.Dataset":
        """Fetch one period as a dataset in the source CRS."""
        ...
