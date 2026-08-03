"""Media type resolution for published GeoZarr stores.

A published store is either flat (one resolution) or pyramided (a multiscales
group of resolution levels). STAC has no way to tell those apart from the plain
Zarr media type, so clients that only render pyramided stores cannot decide
whether an asset is safe to open.

The `profile` media type parameter closes that gap. It is recommended by the
STAC Zarr best practices, which note it is not (yet) part of the official Zarr
media type registration:

    https://github.com/radiantearth/stac-best-practices/blob/main/best-practices-zarr.md

Consumers match it literally rather than parsing parameters — OpenLayers'
``ol/source/GeoZarr`` is reached only for the exact web-optimized string — so
``WEB_OPTIMIZED_ZARR_MEDIA_TYPE`` must stay byte-identical to what they expect.

The profile is only ever advertised for stores that really are pyramided:
claiming it for a flat store would send a renderer looking for levels that do
not exist.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

ZARR_V3_MEDIA_TYPE = "application/vnd.zarr; version=3"
"""Media type for a published Zarr v3 store with no resolution pyramid."""

WEB_OPTIMIZED_ZARR_MEDIA_TYPE = f"{ZARR_V3_MEDIA_TYPE}; profile=multiscales"
"""Media type for a pyramided ("web-optimized") Zarr v3 store.

Byte-identical to the string web clients match on; do not reformat the
parameters or reorder them.
"""

MULTISCALES_CONVENTION_UUID = "d35379db-88df-4056-af3a-620245f8e347"
"""UUID of the Zarr multiscales convention (https://github.com/zarr-conventions/multiscales)."""


def attributes_declare_multiscales(attributes: Mapping[str, Any]) -> bool:
    """Return True when root group *attributes* describe a multiscales pyramid.

    Mirrors what pyramid-aware renderers require: the multiscales convention
    declared in ``zarr_conventions``, plus a non-empty ``multiscales.layout``
    listing the resolution levels. A store that declares the convention but has
    no levels is treated as flat — there is nothing extra for a client to read.
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
    """Read the root group attributes of a published store.

    Reads only group metadata, never chunk data. Deliberately reads the *root*
    group: the store accessors descend into pyramid level ``0``, whose
    attributes do not carry the multiscales layout.

    Returns an empty mapping when the attributes cannot be read (remote store,
    missing path, unreadable metadata).
    """
    if icechunk:
        return _read_icechunk_root_attributes(store_path)
    if "://" in store_path:
        # Remote stores would need a network round trip per request; the caller
        # falls back to the flat media type rather than paying for it here.
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
    """Return the media type to advertise for the store at *store_path*.

    Falls back to the flat :data:`ZARR_V3_MEDIA_TYPE` whenever the pyramid
    cannot be confirmed. Understating is safe (a client opens the store the
    ordinary way); overstating is not.
    """
    try:
        attributes = read_root_attributes(store_path, icechunk=icechunk)
    except Exception:
        logger.debug("Could not read root attributes of store %r", store_path, exc_info=True)
        return ZARR_V3_MEDIA_TYPE
    if attributes_declare_multiscales(attributes):
        return WEB_OPTIMIZED_ZARR_MEDIA_TYPE
    return ZARR_V3_MEDIA_TYPE
