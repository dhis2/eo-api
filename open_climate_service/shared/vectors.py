"""Named vector collections: boundary sets an instance ships, loadable by id (CLIM-836).

Zonal statistics needs geometry, and until now the only way to supply it was a GeoJSON
FeatureCollection in every request. For the boundaries an instance uses over and over —
administrative regions, districts, catchments — that means a client posting the same several
megabytes on each call, and every client having its own copy of what should be one authoritative
boundary set.

A vector collection is a **GeoParquet file** in ``<data_dir>/vector/``. GeoParquet rather than
GeoJSON because it is what `save_result` already writes for vector output, and because a reader
can use the per-row-group bounding boxes in its footer to fetch only the rows it needs over HTTP
range requests — the same access pattern the Zarr stores already rely on.

Discovery is a directory scan rather than a template file. A boundary set has no ingestion
schedule, no cadence and no sync behaviour, so a dataset template would be almost entirely empty
fields; the file itself already carries the CRS, the bounds, the feature count and the column
names, which is everything the metadata below reports.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

GEOMETRY_WKT_COORD = "geometry_wkt"
"""Companion coordinate on a vector cube's geometry dimension, holding each feature's WKT.

Defined here because both sides need it and neither may import the other: `aggregate_spatial` is
a discovered plugin process that writes it, and the openEO job writers read it. The dimension
itself carries feature *labels* (ids) — which the DHIS2 and CHAP exports use as their location
column — so the shapes ride alongside rather than replacing them.
"""

VECTOR_SUFFIX = ".parquet"
# Collection ids come from filenames and end up in URLs and process arguments, so they are held
# to the same shape as a dataset id rather than accepting anything a filesystem permits.
_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]*$")


def vector_dir() -> Path:
    """Where an instance keeps its vector collections."""
    from open_climate_service.data_manager.services.downloader import DOWNLOAD_DIR

    return DOWNLOAD_DIR.parent / "vector"


def _is_valid_id(value: str) -> bool:
    return bool(_ID_PATTERN.match(value))


def _crs_label(crs: Any) -> str | None:
    """A short CRS name for the metadata: ``EPSG:4326`` where resolvable, else the CRS name.

    ``str(crs)`` on a CRS read from GeoParquet yields the whole PROJJSON document, which is not
    something to put in an API listing. ``to_epsg()`` needs a PROJ database lookup and can raise
    on a broken PROJ install, so the name — which comes from the CRS definition itself — is the
    fallback rather than the error.
    """
    if crs is None:
        return None
    try:
        code = crs.to_epsg()
    except Exception:  # pragma: no cover — depends on the PROJ install
        code = None
    if code is not None:
        return f"EPSG:{code}"
    name = getattr(crs, "name", None)
    return str(name) if name else None


def _is_lonlat(crs: Any) -> bool:
    """Whether a CRS is already WGS 84 lon/lat, so reprojection can be skipped.

    Tested on the EPSG code, not ``str(crs)``: stringifying a CRS read from GeoParquet gives the
    PROJJSON document, which never equals ``"EPSG:4326"`` — comparing against it reprojects every
    collection, including the ones already in lon/lat.
    """
    try:
        if crs.to_epsg() == 4326:
            return True
    except Exception:  # pragma: no cover — depends on the PROJ install
        pass
    # No EPSG code resolved: fall back to asking whether the axes are geographic degrees, which
    # is the property `aggregate_spatial` actually depends on.
    return bool(getattr(crs, "is_geographic", False))


def collection_path(collection_id: str) -> Path | None:
    """Resolve a collection id to its file, or None when there is no such collection.

    The id is validated and the resolved path is confirmed to sit inside the vector directory,
    so an id like ``../../etc/passwd`` cannot reach outside it — the id arrives from a process
    argument or a URL.
    """
    if not _is_valid_id(collection_id):
        return None
    root = vector_dir()
    candidate = (root / f"{collection_id}{VECTOR_SUFFIX}").resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError:
        return None
    return candidate if candidate.is_file() else None


def describe(collection_id: str) -> dict[str, Any] | None:
    """Metadata for one collection, read from the file itself.

    Reads the Parquet metadata and geometry column rather than the whole table, so describing a
    large boundary set does not load it.
    """
    path = collection_path(collection_id)
    if path is None:
        return None
    import geopandas as gpd
    import pyarrow.parquet as pq

    try:
        meta = pq.read_metadata(path)
        frame = gpd.read_parquet(path, columns=["geometry"])
    except Exception:
        logger.warning("Vector collection '%s' could not be read; skipping", collection_id, exc_info=True)
        return None

    bounds = [float(v) for v in frame.total_bounds] if len(frame) else None
    return {
        "id": collection_id,
        "feature_count": int(meta.num_rows),
        "properties": [name for name in meta.schema.names if name != "geometry"],
        "geometry_types": sorted({str(t) for t in frame.geom_type.unique()}),
        "crs": _crs_label(frame.crs),
        "bbox": bounds,
        "size_bytes": path.stat().st_size,
    }


def list_collections() -> list[dict[str, Any]]:
    """Every readable vector collection, by id.

    A file that cannot be read is logged and skipped rather than failing the listing: one corrupt
    boundary file should not hide the others, which is the opposite of the dataset-template rule
    where a bad template aborts its whole file.
    """
    root = vector_dir()
    if not root.is_dir():
        return []
    described = []
    for path in sorted(root.glob(f"*{VECTOR_SUFFIX}")):
        if not _is_valid_id(path.stem):
            logger.warning("Ignoring vector file '%s': not a valid collection id", path.name)
            continue
        info = describe(path.stem)
        if info is not None:
            described.append(info)
    return described


def load_feature_collection(
    collection_id: str,
    *,
    id_property: str | None = None,
    properties: list[str] | None = None,
) -> dict[str, Any]:
    """Read a collection as a GeoJSON FeatureCollection.

    GeoJSON rather than a geopandas frame because that is what `aggregate_spatial` already
    accepts, so a named collection is interchangeable with a client-supplied one and needs no
    change to the aggregation path.

    ``id_property`` chooses which column becomes each feature's ``id``. That matters beyond
    cosmetics: the feature id becomes the label on the geometry dimension, which is what the
    DHIS2 and CHAP exports use as their location column — so pointing it at an org-unit code
    column is what makes a named collection usable for a DHIS2 push.
    """
    path = collection_path(collection_id)
    if path is None:
        available = ", ".join(info["id"] for info in list_collections()) or "none"
        raise ValueError(f"Unknown vector collection {collection_id!r}. Available: {available}")

    import geopandas as gpd

    columns: list[str] | None = None
    if properties is not None:
        wanted: set[str] = {*properties, "geometry"}
        if id_property:
            wanted.add(id_property)
        columns = sorted(wanted)
    frame = gpd.read_parquet(path, columns=columns)

    if frame.crs is not None and not _is_lonlat(frame.crs):
        # aggregate_spatial masks against the raster's own grid and assumes lon/lat, so a
        # projected boundary set is reprojected here rather than silently missing every pixel.
        logger.info("Reprojecting vector collection '%s' from %s to EPSG:4326", collection_id, _crs_label(frame.crs))
        frame = frame.to_crs("EPSG:4326")

    if id_property is not None:
        if id_property not in frame.columns:
            raise ValueError(
                f"Vector collection {collection_id!r} has no property {id_property!r}. "
                f"Available: {', '.join(c for c in frame.columns if c != 'geometry')}"
            )
        frame = frame.set_index(id_property, drop=False)

    payload: dict[str, Any] = frame.to_geo_dict()
    if id_property is not None:
        for feature, value in zip(payload.get("features", []), frame[id_property], strict=False):
            feature["id"] = str(value)
    return payload
