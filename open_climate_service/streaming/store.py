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

from open_climate_service.shared.geozarr import grid_geometry, write_gdal_geotransform
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
            coord = ds[time_dim]
            if coord.dtype.kind != "M":
                # Non-datetime (ordinal) step dimension — e.g. an integer
                # ``dayofyear`` axis. Period ids are the coord values as plain
                # strings, matching what ``plugin.periods()`` emits; parsing them
                # as datetimes would mangle every value and leave ``committed``
                # empty, causing duplicate appends on resume.
                if coord.dtype.kind in "iu":
                    return {str(int(item)) for item in coord.values}
                return {str(item.item()) for item in coord.values}
            return {
                datetime_to_period_string(pd.Timestamp(item.item()).to_pydatetime(), period_type)
                for item in coord.values
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


def _stored_grid_geometry(root: Any, spec: GridSpec) -> dict[str, Any] | None:
    """Grid geometry read from the store's own coordinate arrays, or None if unavailable.

    The coordinate arrays are the ground truth for where the data actually is. The requested
    ``bbox`` is not: a source can return less than was asked for (CHIRPS ends at 60°N, so a
    Norway request to 72.5°N yields a grid that stops at 60°N), and describing the request
    would stretch the raster over ground it does not cover.

    Returns None for a store whose coordinates aren't readable yet — the first-write path
    calls this before any data exists, and a 1-cell axis has no derivable cell size.
    """
    try:
        x_centres = root[spec.x_dim][:]
        y_centres = root[spec.y_dim][:]
    except (KeyError, TypeError):
        return None
    return grid_geometry(x_centres, y_centres)


def write_geozarr_attrs(store: Any, *, spec: GridSpec, bbox: list[float]) -> None:
    """Write root metadata for a flat Zarr v3 store.

    The attrs are rewritten after each commit rather than only on first write so
    root metadata stays stable across repeated append sessions.
    """
    import zarr

    root = zarr.open_group(store, mode="r+")
    geometry = _stored_grid_geometry(root, spec)

    attrs = create_geozarr_attrs(
        # Array order, (y, x) — `spatial:dimensions` and `spatial:shape` are read
        # positionally, so naming them x-first transposes the grid for any client that
        # trusts the convention.
        dimensions=[spec.y_dim, spec.x_dim],
        crs=f"EPSG:{spec.crs}",
        bbox=bbox,
        shape=spec.shape,
    )
    crs_code = f"EPSG:{spec.crs}"
    attrs["proj:code"] = crs_code
    # Native-CRS extent, in the GeoZarr `spatial:bbox` convention. Direct-Zarr clients
    # (GDAL/QGIS, zarr-layer) read the CRS from the CF grid-mapping (`crs_wkt`) / `proj:`
    # convention that create_geozarr_attrs already writes, and the extent from here or the
    # coordinate arrays — no non-standard `proj4`/`bounds` attrs required. The STAC hints
    # (open_climate_service:proj4, proj:bbox) in stac/services.py serve the map viewer.
    attrs["spatial:bbox"] = bbox
    if geometry is not None:
        # The affine is what a client actually places the raster with. Without it, viewers
        # fall back to inferring a grid from the coordinate arrays and assume EPSG:4326 while
        # doing so, which puts every projected store (seNorge's UTM33 metres) off the map.
        attrs["spatial:transform"] = geometry["transform"]
        attrs["spatial:shape"] = geometry["shape"]
        attrs["spatial:bbox"] = geometry["bbox"]
    attrs.update(spec.attrs)

    root.attrs.update(attrs)
    if geometry is not None:
        write_gdal_geotransform(root, geometry["transform"])
