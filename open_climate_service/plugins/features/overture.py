"""Overture Maps as a feature provider: a bbox extract, straight to GeoParquet (CLIM-893).

Overture publishes its themes as GeoParquet on S3, partitioned and carrying a per-row ``bbox``
struct, so a country-sized extract is a predicate pushdown rather than a download-and-clip. The
extract streams batch by batch from the remote partitions to a local file: the geometry is never
decoded, and peak memory is one batch rather than the whole country. That is what makes buildings
tractable at all -- a country's footprints are millions of features, and materialising those as
GeoJSON dicts costs orders of magnitude more than the data itself.

That is why this returns a **path** rather than a FeatureCollection. The store records it without
decoding the geometry; only the id column is read back, in Arrow, to check the identity contract.

Two things fall out of the source already having a ``bbox`` struct, and both are why this needs no
geometry engine:

* The window is a filter on plain numeric struct fields, which Arrow pushes down to row-group
  statistics. No spatial predicate, no geometry decoding.
* The extract inherits the source's ``geo`` metadata, **covering included**, so a windowed read of
  the result prunes row groups too. Nothing has to be recomputed or re-declared.

Monthly releases are what make the version meaningful: a release *is* a version, so "is there a
newer one than we hold" is answerable without comparing data. The release is passed explicitly
rather than resolved to "latest", so an instance upgrades deliberately and a re-ingest is
reproducible.

**Licence.** The buildings theme is ODbL, because it incorporates OpenStreetMap -- attribution plus
share-alike on a derived database. The template that declares this provider carries `license` and
`attribution`; serving an extract without surfacing them is a licence breach, not an oversight.
See CLIM-1010 for where that metadata is published.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from open_climate_service.features.provider import feature_provider

logger = logging.getLogger(__name__)

DEFAULT_THEME = "buildings"
BBOX_COLUMN = "bbox"

_THEME_TYPES = {
    "buildings": "building",
    "places": "place",
    "transportation": "segment",
    "addresses": "address",
    "base": "land_cover",
    "divisions": "division_area",
}


def theme_types() -> dict[str, Any]:
    """The theme -> type mapping, for callers that want to validate before ingesting."""
    return dict(_THEME_TYPES)


@feature_provider("overture")
def overture(
    path: Path,
    release: str,
    bbox: list[float],
    theme: str = DEFAULT_THEME,
    type: str | None = None,  # noqa: A002 -- Overture's own field name
    columns: list[str] | None = None,
    stac_catalog: bool = True,
) -> tuple[Path, str]:
    """Extract one Overture theme for a bounding box, writing GeoParquet to ``path``.

    ``bbox`` is ``[west, south, east, north]`` in lon/lat -- normally the instance extent.
    Returns the path and the release id, which becomes the collection's version.
    """
    import pyarrow.parquet as pq
    from overturemaps import core

    if len(bbox) != 4:
        raise ValueError(f"Overture bbox must be [west, south, east, north], got {bbox!r}")
    west, south, east, north = (float(value) for value in bbox)
    if west >= east or south >= north:
        raise ValueError(f"Overture bbox is empty or inverted: {bbox!r}")

    type_ = type or _THEME_TYPES.get(theme)
    if type_ is None:
        raise ValueError(f"Unknown Overture theme {theme!r}. Known: {', '.join(sorted(_THEME_TYPES))}")

    # stac=True is the whole performance story. Without it, selecting a window means opening the
    # footer of every partition in the theme -- 512 of them for buildings, which measured at ~20
    # minutes for a single city block regardless of the query engine. The STAC catalogue carries a
    # spatial extent per partition, so only the overlapping ones are opened at all: the same query
    # measured at 32 seconds. Range requests make each file cheap; only this makes the *count* cheap.
    reader = core.record_batch_reader(
        type_, bbox=(west, south, east, north), release=release, stac=stac_catalog
    )
    if reader is None:
        raise ValueError(f"Overture returned no data for type {type_!r} in release {release!r}")

    schema = reader.schema
    if BBOX_COLUMN not in schema.names:
        raise ValueError(
            f"Overture {type_!r} has no {BBOX_COLUMN!r} column, so the extract would have no covering "
            "bbox and a windowed read of it would scan the whole file."
        )
    if columns is not None:
        # geometry is the point of the file and bbox is what the covering metadata refers to, so
        # neither can be dropped by a column selection without invalidating the result.
        keep = list(dict.fromkeys([*columns, "geometry", BBOX_COLUMN]))
        missing = [name for name in keep if name not in schema.names]
        if missing:
            raise ValueError(f"Overture {type_!r} has no column(s): {', '.join(missing)}")
        # Preserve the `geo` metadata: it declares the covering, and rebuilding a schema drops it.
        import pyarrow as pa

        schema = pa.schema([schema.field(name) for name in keep], metadata=reader.schema.metadata)

    path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with pq.ParquetWriter(path, schema) as writer:
        for batch in reader:
            if not batch.num_rows:
                continue
            if columns is not None:
                batch = batch.select(schema.names)
            writer.write_batch(batch)
            written += batch.num_rows

    logger.info("Extracted %d Overture %s features (%s) for %s to %s", written, theme, release, bbox, path.name)
    return path, release
