"""What OCS writes into a GeoZarr store's root, and what it advertises about it.

Two halves of one contract. The first writes the ``spatial:`` convention attributes that place
a raster; the second reads them back to decide the media type a STAC client is told, which is
what gates whether that client will render the store at all. Keeping them in one module means
the claim and the thing being claimed can't drift apart.

Both write paths (the streaming per-period appender and the batch downloader) declare the
`spatial:` convention in ``zarr_conventions``, so both must describe the grid the same way.
This module is the single source of that description, so the two can't drift.

Two things matter to direct-Zarr clients, and both were wrong or missing before:

* **Axis order is array order, ``(y, x)``.** ``spatial:dimensions`` and ``spatial:shape``
  are read positionally — a client takes the second-to-last entry as the row (y) axis and
  the last as the column (x) axis. Writing ``["x", "y"]`` / ``[width, height]`` transposes
  the grid for anything that trusts the convention.
* **The affine matters more than the bbox.** ``spatial:transform`` is what a client needs to
  place a raster; without it, viewers fall back to guessing from the coordinate arrays, and
  that guess is usually hardcoded to EPSG:4326 — which silently mislocates every projected
  store. See CLIM-852.

Everything here is derived from the store's *actual* cell-centre coordinates rather than the
requested bbox: a source may deliver a smaller grid than was asked for (CHIRPS stops at 60°N,
so a request up to 72.5°N yields a grid ending at 60°N), and describing the request instead
of the data stretches the raster over ground it does not cover.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Sequence

logger = logging.getLogger(__name__)

# ``spatial:registration: "pixel"`` (what both write paths declare) puts the affine origin on
# the outer *edge* of the first cell, not its centre — so a half-step is subtracted below.
# This matches what topozarr writes for pyramid levels, keeping root and level-0 consistent.
_PIXEL_REGISTRATION_HALF_STEP = 0.5


def _as_floats(values: Any) -> list[float]:
    """Coerce a coordinate array (numpy, xarray, list) to a plain list of floats."""
    data = getattr(values, "values", values)
    return [float(v) for v in data]


def grid_geometry(
    x_centres: Sequence[float] | Any,
    y_centres: Sequence[float] | Any,
) -> dict[str, Any] | None:
    """Affine transform, shape and edge bbox for a regular grid, or None if undetermined.

    ``x_centres`` / ``y_centres`` are the store's 1-D coordinate arrays, holding cell
    *centres*. The step is taken from the first two cells and may be negative — a north-up
    raster stores y descending, and the sign has to survive into the transform or the image
    lands mirrored. At least two cells per axis are needed; a single-cell axis leaves the
    cell size unknowable from coordinates alone, so the whole geometry is reported as
    undetermined rather than guessed.
    """
    x = _as_floats(x_centres)
    y = _as_floats(y_centres)
    if len(x) < 2 or len(y) < 2:
        return None

    step_x = x[1] - x[0]
    step_y = y[1] - y[0]
    if step_x == 0.0 or step_y == 0.0:
        return None

    origin_x = x[0] - step_x * _PIXEL_REGISTRATION_HALF_STEP
    origin_y = y[0] - step_y * _PIXEL_REGISTRATION_HALF_STEP
    far_x = x[-1] + step_x * _PIXEL_REGISTRATION_HALF_STEP
    far_y = y[-1] + step_y * _PIXEL_REGISTRATION_HALF_STEP

    return {
        # GeoZarr `spatial:transform`: [stepX, rotX, originX, rotY, stepY, originY].
        "transform": [step_x, 0.0, origin_x, 0.0, step_y, origin_y],
        # Array order, (rows, columns) == (y, x).
        "shape": [len(y), len(x)],
        # Outer edges, ordered [xmin, ymin, xmax, ymax] regardless of which way the axes run.
        "bbox": [
            min(origin_x, far_x),
            min(origin_y, far_y),
            max(origin_x, far_x),
            max(origin_y, far_y),
        ],
    }


def gdal_geotransform(transform: Sequence[float]) -> str:
    """A GeoZarr ``spatial:transform`` as GDAL's ``GeoTransform`` string.

    GDAL orders the same six coefficients differently — ``originX stepX rotX originY rotY
    stepY`` — and stores them space-separated on the CF grid-mapping variable. Writing it
    makes the store self-describing to GDAL/QGIS, and is what tells a viewer that a store is
    a *projected* grid rather than degrees (zarr-viewer requires the GeoTransform before it
    will route a store to its projected-grid renderer).
    """
    step_x, rot_x, origin_x, rot_y, step_y, origin_y = transform
    return " ".join(repr(float(v)) for v in (origin_x, step_x, rot_x, origin_y, rot_y, step_y))


def write_gdal_geotransform(root: Any, transform: Sequence[float]) -> None:
    """Stamp the GDAL ``GeoTransform`` onto a store's CF ``spatial_ref`` grid-mapping array.

    ``rio.write_crs`` writes ``crs_wkt`` and ``grid_mapping_name`` but not the transform —
    ``rio.write_transform`` does that, and neither write path calls it, so whether a store
    carries a GeoTransform currently depends on whether its *source* happened to have one
    (GeoTIFF-backed CHIRPS does; NetCDF-backed seNorge does not). That makes the attribute
    unreliable exactly where it matters most: it is the signal zarr-viewer uses to recognise
    a projected grid, and its absence sends a UTM store down the degrees-assuming path.

    A no-op when the store has no ``spatial_ref`` array (nothing to attach it to).
    """
    try:
        spatial_ref = root["spatial_ref"]
    except (KeyError, TypeError):
        return
    spatial_ref.attrs.update({"GeoTransform": gdal_geotransform(transform)})


# --- Media type: telling a STAC client whether the store is pyramided ---------------------
#
# STAC has no way to distinguish a flat store from a pyramided one through the plain Zarr
# media type, and clients that only render pyramided stores therefore cannot decide whether an
# asset is safe to open. The `profile` parameter closes that gap. It is a
# STAC Zarr best-practices recommendation, which notes it is not (yet) part of the official
# Zarr media type registration:
#
#     https://github.com/radiantearth/stac-best-practices/blob/main/best-practices-zarr.md
#
# Consumers match it as a literal, not by parsing parameters. stac-js holds
# `wozMediaTypes = ['application/vnd.zarr; version=3; profile=multiscales']` and compares with
# `allowedTypes.includes(type.toLowerCase())` — so case is forgiven but parameter order and
# spacing are not. WEB_OPTIMIZED_ZARR_MEDIA_TYPE must stay byte-identical.

ZARR_V3_MEDIA_TYPE = "application/vnd.zarr; version=3"
"""Media type for a published Zarr v3 store with no resolution pyramid."""

WEB_OPTIMIZED_ZARR_MEDIA_TYPE = f"{ZARR_V3_MEDIA_TYPE}; profile=multiscales"
"""Media type for a pyramided ("web-optimized") Zarr v3 store.

Byte-identical to the string web clients match on; do not reformat or reorder the parameters.
"""

MULTISCALES_CONVENTION_UUID = "d35379db-88df-4056-af3a-620245f8e347"
"""UUID of the Zarr multiscales convention (https://github.com/zarr-conventions/multiscales)."""


def attributes_declare_multiscales(attributes: Mapping[str, Any]) -> bool:
    """True when root group *attributes* describe a real multiscales pyramid.

    Requires both the convention declared in ``zarr_conventions`` *and* a non-empty
    ``multiscales.layout``. Those are the same two conditions pyramid-aware renderers check, so
    the claim matches what a renderer can actually use. A store that declares the convention but
    lists no levels is treated as flat — there is nothing extra for a client to read.
    """
    conventions = attributes.get("zarr_conventions")
    if not isinstance(conventions, list):
        return False
    declared = any(
        isinstance(convention, Mapping)
        and (convention.get("uuid") == MULTISCALES_CONVENTION_UUID or convention.get("name") == "multiscales")
        for convention in conventions
    )
    if not declared:
        return False
    multiscales = attributes.get("multiscales")
    if not isinstance(multiscales, Mapping):
        return False
    layout = multiscales.get("layout")
    return isinstance(layout, list) and len(layout) > 0


def read_root_attributes(store_path: str, *, icechunk: bool) -> Mapping[str, Any]:
    """Root group attributes of a published store; empty mapping when unreadable.

    Reads group metadata only, never chunk data. Deliberately the *root* group: the store
    accessors descend into pyramid level ``0``, whose attributes do not carry the multiscales
    layout, so this needs its own path.

    Remote stores return empty rather than paying a network round trip — the caller degrades to
    the flat media type.
    """
    if icechunk:
        return _read_icechunk_root_attributes(store_path)
    if "://" in store_path:
        return {}
    zarr_json = Path(store_path) / "zarr.json"
    try:
        payload = json.loads(zarr_json.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    attributes = payload.get("attributes")
    return attributes if isinstance(attributes, Mapping) else {}


def _read_icechunk_root_attributes(store_path: str) -> Mapping[str, Any]:
    import icechunk
    import zarr

    if not Path(store_path).exists():
        return {}
    storage = icechunk.local_filesystem_storage(store_path)
    repo = icechunk.Repository.open(storage)
    session = repo.readonly_session("main")
    group = zarr.open_group(session.store, mode="r")
    return dict(group.attrs)


def zarr_media_type(store_path: str, *, icechunk: bool) -> str:
    """The media type to advertise for the store at *store_path*.

    Falls back to the flat :data:`ZARR_V3_MEDIA_TYPE` whenever the pyramid cannot be confirmed.
    The asymmetry is deliberate: understating is harmless — a client opens the store the ordinary
    way — while overstating sends a renderer looking for levels that do not exist. A locked or
    corrupt store must never take the STAC collection down with it.
    """
    try:
        attributes = read_root_attributes(store_path, icechunk=icechunk)
    except Exception:  # noqa: BLE001 — any read failure degrades to the flat type
        logger.debug("Could not read root attributes of store %r", store_path, exc_info=True)
        return ZARR_V3_MEDIA_TYPE
    if attributes_declare_multiscales(attributes):
        return WEB_OPTIMIZED_ZARR_MEDIA_TYPE
    return ZARR_V3_MEDIA_TYPE
