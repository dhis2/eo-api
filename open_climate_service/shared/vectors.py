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

import json
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


def _crs_label_from_projjson(crs: Any) -> str | None:
    """A short CRS label from the PROJJSON in GeoParquet metadata, without building a CRS object.

    GeoParquet stores the CRS as PROJJSON, whose ``id`` carries the authority and code directly —
    so the common case needs no PROJ lookup at all. ``crs: null`` means OGC:CRS84 per the spec
    (lon/lat WGS 84), which is why absent metadata maps to EPSG:4326 rather than to unknown.
    """
    if crs is None:
        return "EPSG:4326"
    if not isinstance(crs, dict):
        return None
    identifier = crs.get("id")
    if isinstance(identifier, dict):
        authority, code = identifier.get("authority"), identifier.get("code")
        if authority and code is not None:
            return f"{authority}:{code}"
    name = crs.get("name")
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


def _geo_metadata(meta: Any) -> dict[str, Any]:
    """The GeoParquet ``geo`` metadata for the primary geometry column, or an empty dict.

    Read from the Parquet footer, which is a few kilobytes however large the file is.
    """
    raw = (meta.metadata or {}).get(b"geo")
    if raw is None:
        return {}
    try:
        geo = json.loads(raw)
        column = geo["columns"][geo["primary_column"]]
    except Exception:  # noqa: BLE001 — malformed metadata is the same as absent metadata here
        return {}
    return column if isinstance(column, dict) else {}


def describe(collection_id: str) -> dict[str, Any] | None:
    """Metadata for one collection, read from the file's footer where possible.

    Bounds and geometry types come from the GeoParquet ``geo`` metadata rather than from the
    geometry column. Both fields are written by every GeoParquet writer we care about — with or
    without a covering bbox — and reading the footer is ~400x faster than reading every geometry
    (0.4 ms vs 150 ms for 300k footprints). This matters because `list_collections` describes
    every collection on every request to `GET /vector-collections`, so the cost of doing it the
    slow way scales with the total feature count of the instance, not with the response size.

    An older file whose metadata lacks either field falls back to reading the geometry column,
    which is correct but proportional to the collection.
    """
    path = collection_path(collection_id)
    if path is None:
        return None
    import pyarrow.parquet as pq

    try:
        meta = pq.read_metadata(path)
    except Exception:
        logger.warning("Vector collection '%s' could not be read; skipping", collection_id, exc_info=True)
        return None

    geo = _geo_metadata(meta)
    bbox = geo.get("bbox")
    geometry_types = geo.get("geometry_types")
    crs_label: str | None
    # `covering` marks a per-row bbox column, which is what makes a spatial read cheap.
    supports_bbox_filter = bool((geo.get("covering") or {}).get("bbox"))

    if isinstance(bbox, list) and len(bbox) == 4 and isinstance(geometry_types, list):
        bounds = [float(v) for v in bbox] if meta.num_rows else None
        types = sorted({str(t) for t in geometry_types})
        crs_label = _crs_label_from_projjson(geo.get("crs"))
    else:
        logger.debug("Vector collection '%s' has incomplete geo metadata; reading geometry", collection_id)
        import geopandas as gpd

        try:
            frame = gpd.read_parquet(path, columns=["geometry"])
        except Exception:
            logger.warning("Vector collection '%s' could not be read; skipping", collection_id, exc_info=True)
            return None
        bounds = [float(v) for v in frame.total_bounds] if len(frame) else None
        types = sorted({str(t) for t in frame.geom_type.unique()})
        crs_label = _crs_label(frame.crs)

    return {
        "id": collection_id,
        "feature_count": int(meta.num_rows),
        "properties": [name for name in meta.schema.names if name != "geometry"],
        "geometry_types": types,
        "crs": crs_label,
        "bbox": bounds,
        "size_bytes": path.stat().st_size,
        # Whether a windowed read can skip row groups — the difference between a viewport query
        # touching a few hundred rows and one reading the whole collection.
        "supports_bbox_filter": supports_bbox_filter,
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


MAX_FEATURES = 50_000
"""Refuse to build a FeatureCollection larger than this without a ``bbox``.

This function materialises every feature as Python dicts, which costs far more than reading the
file: for 300k polygons, ``read_parquet`` takes 0.13 s and ``to_geo_dict`` 5.6 s for ~105 MB of
dicts. That is fine for the boundary sets this exists to serve (tens to thousands of features)
and hopeless for a collection of building footprints, which a single unqualified call would
otherwise pull into memory. The limit is generous for any administrative hierarchy and small
enough to stop that; a windowed read via ``bbox`` is not subject to it beyond its own result size.
"""


def load_feature_collection(
    collection_id: str,
    *,
    id_property: str | None = None,
    properties: list[str] | None = None,
    bbox: tuple[float, float, float, float] | list[float] | None = None,
) -> dict[str, Any]:
    """Read a collection as a GeoJSON FeatureCollection.

    GeoJSON rather than a geopandas frame because that is what `aggregate_spatial` already
    accepts, so a named collection is interchangeable with a client-supplied one and needs no
    change to the aggregation path.

    ``id_property`` chooses which column becomes each feature's ``id``. That matters beyond
    cosmetics: the feature id becomes the label on the geometry dimension, which is what the
    DHIS2 and CHAP exports use as their location column — so pointing it at an org-unit code
    column is what makes a named collection usable for a DHIS2 push.

    ``bbox`` restricts the read to a lon/lat window. Where the file carries a per-row covering
    bbox, this is pushed down to row-group pruning and only the matching groups are fetched;
    otherwise the file is read and filtered, which is correct but not cheap. Collections above
    :data:`MAX_FEATURES` require it.
    """
    path = collection_path(collection_id)
    if path is None:
        available = ", ".join(info["id"] for info in list_collections()) or "none"
        raise ValueError(f"Unknown vector collection {collection_id!r}. Available: {available}")

    import geopandas as gpd

    info = describe(collection_id) or {}
    feature_count = int(info.get("feature_count") or 0)
    if bbox is None and feature_count > MAX_FEATURES:
        raise ValueError(
            f"Vector collection {collection_id!r} has {feature_count:,} features, more than the "
            f"{MAX_FEATURES:,} that can be loaded as a FeatureCollection. Pass a bbox to read a "
            "window of it."
        )

    columns: list[str] | None = None
    if properties is not None:
        wanted: set[str] = {*properties, "geometry"}
        if id_property:
            wanted.add(id_property)
        columns = sorted(wanted)

    if bbox is None:
        frame = gpd.read_parquet(path, columns=columns)
    elif info.get("supports_bbox_filter"):
        frame = gpd.read_parquet(path, columns=columns, bbox=tuple(bbox))
    else:
        # No covering bbox to prune on, so geopandas would reject `bbox=`. Read and clip instead,
        # and say so: the difference is a whole-file read, which is what the covering bbox exists
        # to avoid.
        logger.info(
            "Vector collection '%s' has no covering bbox; reading in full and filtering. "
            "Rewrite it with write_covering_bbox=True to make windowed reads cheap.",
            collection_id,
        )
        from shapely.geometry import box

        full = gpd.read_parquet(path, columns=columns)
        # Intersects, not clip: the window selects features, it does not cut their geometry —
        # matching what `read_parquet(bbox=...)` returns on the pushdown path above.
        window = box(float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3]))
        frame = full[full.intersects(window)]

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
