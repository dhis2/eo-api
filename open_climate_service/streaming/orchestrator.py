"""Internal per-period streaming ingest orchestrator.

This module implements the Ticket 1 execution loop:
1. probe the source to shape the store,
2. enumerate source-valid periods,
3. reconcile those periods with committed store state,
4. fetch missing periods with bounded concurrency, and
5. append each fetched period into a flat Icechunk-backed Zarr v3 store.

It is intentionally lower-level than `open_climate_service.ingestions.services`.
The ingestion service decides *when* to use streaming and how to persist
artifact metadata. This module decides *how* one plugin-backed stream is
fetched and committed safely.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import xarray as xr

from open_climate_service.streaming.protocol import GridSpec, IngestionPlugin
from open_climate_service.streaming.store import (
    is_store_empty,
    open_or_create_repo,
    read_committed_period_ids,
    write_geozarr_attrs,
)
from open_climate_service.transforms.pipeline import run_dataset_transforms

logger = logging.getLogger(__name__)


@dataclass
class StreamingIngestResult:
    """Structured result returned to the ingestion service layer.

    The result is intentionally small. Artifact persistence, publication, and
    public API response construction remain the responsibility of
    `open_climate_service.ingestions`.
    """

    store_path: Path
    period_type: str
    periods_written: int


def _strip_cf_encoding(ds: xr.Dataset, period_type: str, *, time_dim: str) -> None:
    """Normalize xarray encodings so repeated zarr appends remain stable.

    Source datasets frequently carry CF encoding hints that are useful for
    one-off writes but unstable across repeated append sessions. The streaming
    path strips them to keep later commits shape-compatible.
    """
    cf_keys = frozenset({"scale_factor", "add_offset", "missing_value", "_FillValue", "coordinates"})
    for name in list(ds.data_vars) + list(ds.coords):
        ds[name].encoding.clear()
        ds[name].attrs = {key: value for key, value in ds[name].attrs.items() if key not in cf_keys}
    if time_dim in ds.coords:
        units = "hours since 1970-01-01" if period_type == "hourly" else "days since 1970-01-01"
        ds[time_dim].encoding.update({"units": units, "dtype": "int32"})


async def run_streaming_ingest(
    *,
    plugin: IngestionPlugin,
    params: dict[str, Any],
    dataset: dict[str, Any] | None = None,
    bbox: list[float],
    start: str,
    end: str,
    store_path: Path,
    period_type: str,
    on_progress: Callable[[int | None, int | None, str | None], None] | None = None,
    is_cancel_requested: Callable[[], bool] | None = None,
    save_cursor: Callable[[dict[str, Any]], None] | None = None,
) -> StreamingIngestResult:
    """Stream one dataset into a flat Zarr v3 store one period at a time.

    Resume policy:
    - committed store state is authoritative

    This avoids replaying already-committed periods after crashes that happen
    between a store commit and a later cursor write.
    """
    from open_climate_service.jobs.models import JobCancelledError

    spec: GridSpec = await plugin.probe(bbox, **params)
    all_periods = await plugin.periods(start, end)
    if not all_periods:
        return StreamingIngestResult(store_path=store_path, period_type=period_type, periods_written=0)

    committed = read_committed_period_ids(store_path, period_type, time_dim=spec.time_dim)
    pending = [period for period in all_periods if period not in committed]
    if not pending:
        return StreamingIngestResult(store_path=store_path, period_type=period_type, periods_written=0)

    if on_progress:
        on_progress(len(all_periods) - len(pending), len(all_periods), f"{len(pending)} periods pending")

    if committed:
        is_first_write = False
    elif is_store_empty(store_path):
        is_first_write = True
    else:
        raise RuntimeError(
            f"Existing store at {store_path} is not empty, but committed periods could not be determined safely"
        )

    repo = open_or_create_repo(store_path)
    period_queue = iter(pending)
    in_flight: deque[tuple[str, asyncio.Task[xr.Dataset]]] = deque()
    max_parallel = max(1, int(plugin.max_concurrency))
    # Ticket 1 still commits every fetched period individually. The plugin's
    # commit_batch_size currently controls cursor checkpoint cadence rather than
    # Icechunk transaction batching.
    commit_batch_size = max(1, int(plugin.commit_batch_size))
    expected_spatial_shape: tuple[int, int] | None = None

    async def _fetch(period_id: str) -> xr.Dataset:
        return await plugin.fetch_period(period_id, bbox, **params)

    # Keep only a rolling fetch window in memory rather than creating one task
    # for every pending period up front. This keeps long backfills bounded.
    for _ in range(min(max_parallel, len(pending))):
        period_id = next(period_queue, None)
        if period_id is None:
            break
        in_flight.append((period_id, asyncio.create_task(_fetch(period_id))))
    written = 0
    try:
        while in_flight:
            if is_cancel_requested and is_cancel_requested():
                raise JobCancelledError("Streaming ingest cancelled")

            period_id, task = in_flight.popleft()
            ds = await task
            if dataset is not None:
                ds = run_dataset_transforms(ds, dataset)
            _strip_cf_encoding(ds, period_type, time_dim=spec.time_dim)
            try:
                spatial_shape = (int(ds.sizes[spec.y_dim]), int(ds.sizes[spec.x_dim]))
            except KeyError as exc:
                raise RuntimeError(
                    f"Fetched dataset for {period_id} is missing expected spatial dimensions "
                    f"'{spec.y_dim}' and '{spec.x_dim}'"
                ) from exc
            if expected_spatial_shape is None:
                expected_spatial_shape = spatial_shape
                if spec.shape != spatial_shape:
                    spec = replace(spec, shape=spatial_shape)
            elif spatial_shape != expected_spatial_shape:
                raise RuntimeError(
                    f"Fetched dataset for {period_id} has spatial shape {spatial_shape}, "
                    f"expected {expected_spatial_shape}"
                )

            session = repo.writable_session("main")
            if is_first_write:
                ds.to_zarr(session.store, mode="w", zarr_format=3)
                is_first_write = False
            else:
                ds.to_zarr(session.store, append_dim=spec.time_dim, zarr_format=3)
            # Root attrs are rewritten on every commit so later append sessions
            # preserve GeoZarr metadata even if the underlying store layer only
            # touches array content for the new period.
            write_geozarr_attrs(session.store, spec=spec, bbox=bbox)
            session.commit(f"ingest: {period_id}")

            written += 1
            if save_cursor and (written % commit_batch_size == 0 or written == len(pending)):
                save_cursor({"last_committed": period_id})
            if on_progress:
                on_progress(
                    len(all_periods) - len(pending) + written,
                    len(all_periods),
                    f"Wrote {period_id}",
                )

            next_period = next(period_queue, None)
            if next_period is not None:
                in_flight.append((next_period, asyncio.create_task(_fetch(next_period))))
    finally:
        tasks_to_cancel = [task for _, task in in_flight if not task.done()]
        for task in tasks_to_cancel:
            task.cancel()
        if tasks_to_cancel:
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
        close_plugin = getattr(plugin, "close", None)
        if callable(close_plugin):
            close_plugin()

    # Prune intermediate ingest snapshots: each period commit created one snapshot;
    # only the final HEAD state needs to be retained.  expire_snapshots marks older
    # snapshots as expired without deleting chunk data — garbage_collect would be
    # needed to reclaim manifest storage.  The "main" branch ref preserves HEAD.
    from datetime import datetime, timezone

    try:
        final_repo = open_or_create_repo(store_path)
        expired = final_repo.expire_snapshots(older_than=datetime.now(tz=timezone.utc))
        if expired:
            logger.info("Expired %d intermediate snapshots from %s", len(expired), store_path)
    except Exception:
        logger.warning("expire_snapshots failed for %s — store remains valid", store_path, exc_info=True)

    return StreamingIngestResult(store_path=store_path, period_type=period_type, periods_written=written)


def run_streaming_ingest_sync(
    *,
    plugin: IngestionPlugin,
    params: dict[str, Any],
    dataset: dict[str, Any] | None = None,
    bbox: list[float],
    start: str,
    end: str,
    store_path: Path,
    period_type: str,
    on_progress: Callable[[int | None, int | None, str | None], None] | None = None,
    is_cancel_requested: Callable[[], bool] | None = None,
    save_cursor: Callable[[dict[str, Any]], None] | None = None,
) -> StreamingIngestResult:
    """Synchronous wrapper for threaded job execution.

    The jobs framework invokes callables in worker threads today, so the
    ingestion service exposes this sync wrapper and lets the orchestrator keep
    its async internal structure.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        raise RuntimeError("run_streaming_ingest_sync cannot be called from a running event loop")

    return asyncio.run(
        run_streaming_ingest(
            plugin=plugin,
            params=params,
            dataset=dataset,
            bbox=bbox,
            start=start,
            end=end,
            store_path=store_path,
            period_type=period_type,
            on_progress=on_progress,
            is_cancel_requested=is_cancel_requested,
            save_cursor=save_cursor,
        )
    )
