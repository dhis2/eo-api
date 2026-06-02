"""Icechunk-backed store helpers for the internal streaming engine.

Ticket 1 is deliberately flat-store-only. These helpers therefore focus on the
minimum store concerns needed for initial per-period append:
open or create an Icechunk repository, inspect committed time steps for resume,
and keep GeoZarr-compatible root attributes stable on the written Zarr v3
store.

Rechunking, multiscale/pyramid behavior, and broader publication concerns are
explicitly deferred to later tickets.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from geozarr_toolkit import create_geozarr_attrs

from open_climate_service.streaming.protocol import GridSpec

logger = logging.getLogger(__name__)


def open_or_create_repo(store_path: Path) -> Any:
    """Open an existing Icechunk repository or create one at ``store_path``."""
    import icechunk

    storage = icechunk.local_filesystem_storage(str(store_path))
    if store_path.exists():
        return icechunk.Repository.open(storage)
    store_path.parent.mkdir(parents=True, exist_ok=True)
    return icechunk.Repository.create(storage)


def read_committed_period_ids(store_path: Path, period_type: str, *, time_dim: str = "t") -> set[str]:
    """Return period ids already committed in the store, or an empty set.

    Resume correctness is store-first: if the repository already contains a
    committed time step, the orchestrator treats that as authoritative even when
    a persisted cursor is stale or missing.
    """
    import pandas as pd
    import xarray as xr

    from open_climate_service.shared.time import datetime_to_period_string

    if not store_path.exists():
        return set()

    try:
        repo = open_or_create_repo(store_path)
        session = repo.readonly_session("main")
        ds = xr.open_zarr(session.store)
        try:
            if time_dim not in ds.coords:
                return set()
            return {
                datetime_to_period_string(pd.Timestamp(item.item()).to_pydatetime(), period_type)
                for item in ds[time_dim].values
            }
        finally:
            ds.close()
    except Exception:
        logger.debug("Could not read committed periods from %s", store_path, exc_info=True)
        return set()


def is_store_empty(store_path: Path) -> bool:
    """Return whether an Icechunk repository has no arrays, groups, or attrs.

    This is a conservative safety check for first-write detection. If the store
    cannot be inspected, we treat it as non-empty to avoid a destructive
    `mode="w"` rewrite of data that may already exist.
    """
    import zarr

    if not store_path.exists():
        return True

    try:
        repo = open_or_create_repo(store_path)
        session = repo.readonly_session("main")
        root = zarr.open_group(session.store, mode="r")
        return not list(root.array_keys()) and not list(root.group_keys()) and not bool(root.attrs.asdict())
    except Exception:
        logger.debug("Could not determine whether %s is empty", store_path, exc_info=True)
        return False


def write_geozarr_attrs(store: Any, *, spec: GridSpec, bbox: list[float]) -> None:
    """Write root metadata for a flat Zarr v3 store.

    The attrs are rewritten after each commit rather than only on first write so
    root metadata stays stable across repeated append sessions.
    """
    import zarr

    attrs = create_geozarr_attrs(
        dimensions=[spec.x_dim, spec.y_dim],
        crs=f"EPSG:{spec.crs}",
        bbox=bbox,
        shape=(spec.shape[1], spec.shape[0]),
    )
    attrs["proj:code"] = f"EPSG:{spec.crs}"
    attrs["spatial:bbox"] = bbox
    attrs.update(spec.attrs)

    root = zarr.open_group(store, mode="r+")
    root.attrs.update(attrs)
