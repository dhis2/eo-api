"""Overture Maps as a feature provider: a bbox extract, straight to GeoParquet (CLIM-893).

Overture publishes its themes as GeoParquet on S3, partitioned and with per-row bbox columns, so a
country-sized extract is a predicate pushdown rather than a download-and-clip. DuckDB does the whole
thing in one statement: read the remote partitions, filter by bbox, write a local GeoParquet. The
geometry is never in Python memory, which is what makes buildings tractable at all — a country's
footprints are millions of features, and materialising those as GeoJSON dicts costs orders of
magnitude more than the data itself.

That is why this returns a **path** rather than a FeatureCollection. The store records it without
decoding the geometry; only the id column is read back, to check the identity contract.

Monthly releases are what make the version meaningful: a release *is* a version, so "is there a
newer one than we hold" is answerable without comparing data. The release is passed explicitly
rather than resolved to "latest", so an instance upgrades deliberately and a re-ingest is
reproducible.

**Licence.** The buildings theme is ODbL, because it incorporates OpenStreetMap — attribution plus
share-alike on a derived database. The template that declares this provider carries `license` and
`attribution`; serving an extract without surfacing them is a licence breach, not an oversight.
See CLIM-1010 for where that metadata is published.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
from pathlib import Path
from typing import Any

from open_climate_service.features.provider import feature_provider

logger = logging.getLogger(__name__)

S3_RELEASE_ROOT = "s3://overturemaps-us-west-2/release"
DEFAULT_THEME = "buildings"
DEFAULT_TYPE = "building"

_THEME_TYPES = {
    "buildings": "building",
    "places": "place",
    "transportation": "segment",
    "addresses": "address",
    "base": "land_cover",
    "divisions": "division_area",
}


def source_url(release: str, theme: str, type_: str) -> str:
    """The Overture partition glob for one theme of one release."""
    return f"{S3_RELEASE_ROOT}/{release}/theme={theme}/type={type_}/*"


def _declare_covering_bbox(path: Path, bbox_column: str = "bbox") -> None:
    """Mark the written ``bbox`` struct as the geometry's covering, in GeoParquet metadata.

    DuckDB writes valid GeoParquet but no ``covering`` entry, and without one a windowed read has to
    scan the whole file — precisely the cost this ingestion exists to avoid for a country's
    buildings. The bbox struct is already written as a column; this only says what it is.

    Done by streaming row groups through a new writer rather than reading the table whole, so peak
    memory stays at one row group instead of the entire extract.
    """
    import tempfile

    import pyarrow.parquet as pq

    with pq.ParquetFile(path) as reader:
        schema = reader.schema_arrow
        if bbox_column not in schema.names:
            return

        raw = (schema.metadata or {}).get(b"geo")
        if raw is None:
            logger.warning("Overture extract %s has no GeoParquet metadata; leaving it unmarked", path.name)
            return
        geo = json.loads(raw)
        column = geo["columns"][geo["primary_column"]]
        column["covering"] = {
            "bbox": {edge: [bbox_column, edge] for edge in ("xmin", "ymin", "xmax", "ymax")},
        }
        metadata = {**(schema.metadata or {}), b"geo": json.dumps(geo).encode()}

        # Written outside the feature store, then moved in. A temp inside it would be a stray
        # `*.parquet` if this failed part-way -- picked up by the directory scan, warned about on
        # every listing, and indistinguishable from a real collection.
        handle, temp_name = tempfile.mkstemp(suffix=".parquet", prefix=f"{path.stem}-covering-")
        os.close(handle)
        temp = Path(temp_name)
        try:
            with pq.ParquetWriter(temp, schema.with_metadata(metadata)) as writer:
                for batch in reader.iter_batches():
                    writer.write_batch(batch)
        except BaseException:
            temp.unlink(missing_ok=True)
            raise
    # os.replace is atomic within a filesystem; shutil.move handles the temp dir being elsewhere.
    shutil.move(str(temp), str(path))


@feature_provider("overture")
def overture(
    path: Path,
    release: str,
    bbox: list[float],
    theme: str = DEFAULT_THEME,
    type: str | None = None,  # noqa: A002 — Overture's own field name
    columns: list[str] | None = None,
    source: str | None = None,
) -> tuple[Path, str]:
    """Extract one Overture theme for a bounding box, writing GeoParquet to ``path``.

    ``bbox`` is ``[west, south, east, north]`` in lon/lat — normally the instance extent.
    ``source`` overrides the S3 location, which is what lets this be tested against a local file.
    Returns the path and the release id, which becomes the collection's version.
    """
    import duckdb

    if len(bbox) != 4:
        raise ValueError(f"Overture bbox must be [west, south, east, north], got {bbox!r}")
    west, south, east, north = (float(value) for value in bbox)
    if west >= east or south >= north:
        raise ValueError(f"Overture bbox is empty or inverted: {bbox!r}")

    type_ = type or _THEME_TYPES.get(theme)
    if type_ is None:
        raise ValueError(f"Unknown Overture theme {theme!r}. Known: {', '.join(sorted(_THEME_TYPES))}")

    location = source or source_url(release, theme, type_)
    selected = ", ".join(columns) if columns else "*"

    connection = duckdb.connect()
    connection.execute("INSTALL spatial; LOAD spatial;")
    if source is None:
        # Only needed to reach S3; a local source must not require the extension or network.
        connection.execute("INSTALL httpfs; LOAD httpfs; SET s3_region='us-west-2';")

    path.parent.mkdir(parents=True, exist_ok=True)
    read = f"read_parquet('{location}', filename=false, hive_partitioning=1)"

    # Overture ships a per-row `bbox` struct, and filtering on it is what makes this a pushdown
    # rather than a download: only the row groups whose bbox statistics overlap are fetched. A
    # source without one is still usable — a hand-made extract, or a future theme that drops it —
    # but the predicate then has to touch the geometry, so say which path was taken.
    has_bbox = any(row[0] == "bbox" for row in connection.execute(f"DESCRIBE SELECT * FROM {read}").fetchall())
    if has_bbox:
        where = f"bbox.xmin < {east} AND bbox.xmax > {west} AND bbox.ymin < {north} AND bbox.ymax > {south}"
    else:
        logger.info("Overture source %s has no bbox column; filtering on geometry instead", location)
        where = f"ST_Intersects(geometry, ST_MakeEnvelope({west}, {south}, {east}, {north}))"

    # The bbox struct is written alongside the geometry so a *windowed read of the extract* can
    # prune row groups; `_declare_covering_bbox` then records what it is.
    #
    # The source's own bbox is excluded rather than carried through. `SELECT *` plus a computed
    # `AS bbox` against a source that already has one -- which every real Overture partition does --
    # emits both, so the extract carries two identical structs and the covering reference names an
    # ambiguous column.
    projection = selected if columns else ("* EXCLUDE (bbox)" if has_bbox else "*")
    connection.execute(
        f"""
        COPY (
            SELECT {projection},
                   {{'xmin': ST_XMin(geometry), 'ymin': ST_YMin(geometry),
                     'xmax': ST_XMax(geometry), 'ymax': ST_YMax(geometry)}} AS bbox
            FROM {read}
            WHERE {where}
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    connection.close()

    _declare_covering_bbox(path)
    logger.info("Extracted Overture %s (%s) for %s to %s", theme, release, bbox, path.name)
    return path, release


def theme_types() -> dict[str, Any]:
    """The theme → type mapping, for callers that want to validate before ingesting."""
    return dict(_THEME_TYPES)
