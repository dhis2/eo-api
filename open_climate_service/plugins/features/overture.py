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

S3_RELEASE_ROOT = "s3://overturemaps-us-west-2/release"
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


def source_url(release: str, theme: str, type_: str) -> str:
    """The Overture partition glob for one theme of one release."""
    return f"{S3_RELEASE_ROOT}/{release}/theme={theme}/type={type_}/*"


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
    source: str | None = None,
) -> tuple[Path, str]:
    """Extract one Overture theme for a bounding box, writing GeoParquet to ``path``.

    ``bbox`` is ``[west, south, east, north]`` in lon/lat -- normally the instance extent.
    ``source`` overrides the S3 location, which is what lets this be tested against a local file.
    Returns the path and the release id, which becomes the collection's version.
    """
    import pyarrow.compute as pc
    import pyarrow.dataset as pa_dataset
    import pyarrow.parquet as pq

    if len(bbox) != 4:
        raise ValueError(f"Overture bbox must be [west, south, east, north], got {bbox!r}")
    west, south, east, north = (float(value) for value in bbox)
    if west >= east or south >= north:
        raise ValueError(f"Overture bbox is empty or inverted: {bbox!r}")

    type_ = type or _THEME_TYPES.get(theme)
    if type_ is None:
        raise ValueError(f"Unknown Overture theme {theme!r}. Known: {', '.join(sorted(_THEME_TYPES))}")

    location = source or source_url(release, theme, type_)
    dataset = pa_dataset.dataset(location, format="parquet")

    if BBOX_COLUMN not in dataset.schema.names:
        # Without it the window would have to touch every geometry, which is the cost this whole
        # path exists to avoid -- so say so rather than silently doing the expensive thing.
        raise ValueError(
            f"Overture source {location!r} has no {BBOX_COLUMN!r} column, so a bbox window cannot be "
            "pushed down. Every Overture release ships one; a hand-made extract may need rewriting "
            "with write_covering_bbox=True."
        )

    # Intersects, not contains: a building on the edge of the extent belongs to it. Arrow pushes
    # this down to row-group statistics, so only overlapping groups are fetched over the network.
    window = (
        (pc.field(BBOX_COLUMN, "xmin") < east)
        & (pc.field(BBOX_COLUMN, "xmax") > west)
        & (pc.field(BBOX_COLUMN, "ymin") < north)
        & (pc.field(BBOX_COLUMN, "ymax") > south)
    )

    projection: list[str] | None = None
    if columns is not None:
        # geometry is the point of the file and bbox is what the covering metadata refers to, so
        # neither can be dropped by a column selection without invalidating the result.
        projection = list(dict.fromkeys([*columns, "geometry", BBOX_COLUMN]))
        missing = [name for name in projection if name not in dataset.schema.names]
        if missing:
            raise ValueError(f"Overture source {location!r} has no column(s): {', '.join(missing)}")

    scanner = dataset.scanner(columns=projection, filter=window)
    # Carry the source's `geo` metadata onto the output, so the extract stays valid GeoParquet and
    # keeps its covering. Projection drops schema metadata, hence re-attaching it explicitly.
    schema = scanner.projected_schema.with_metadata(dataset.schema.metadata)

    path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with pq.ParquetWriter(path, schema) as writer:
        for batch in scanner.to_batches():
            if batch.num_rows:
                writer.write_batch(batch)
                written += batch.num_rows

    logger.info("Extracted %d Overture %s features (%s) for %s to %s", written, theme, release, bbox, path.name)
    return path, release
