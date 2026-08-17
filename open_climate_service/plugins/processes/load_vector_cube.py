"""load_vector_cube — load a named vector collection the instance ships (CLIM-836).

The counterpart to `load_collection` for vector data. Without it, every zonal-statistics request
has to carry its own GeoJSON, so the same boundary set is posted repeatedly and each client keeps
its own copy of what should be one authoritative version:

    aggregate_spatial(load_collection("era5land_temperature_daily"), geometries=<megabytes of GeoJSON>)
    aggregate_spatial(load_collection("era5land_temperature_daily"), geometries=load_vector_cube("districts"))

openEO lists `load_vector_cube` as backend-provided, so there is no upstream implementation to
defer to — the collections are the backend's own.
"""

from __future__ import annotations

from typing import Any

from open_climate_service.process import process
from open_climate_service.shared import vectors


@process(
    summary="Load a named vector collection",
    description=(
        "Load a vector collection stored on this instance, by id, for use as the `geometries` "
        "argument of `aggregate_spatial`. Available collections are listed at "
        "`GET /vector-collections`."
    ),
    parameters={
        "id": {"description": "Collection id, as listed by GET /vector-collections."},
        "id_property": {
            "description": (
                "Column whose value becomes each feature's id. The feature id becomes the label "
                "on the geometry dimension, which the DHIS2 and CHAP exports use as their "
                "location column — so set this to an org-unit code column when the result is "
                "destined for DHIS2."
            )
        },
        "properties": {
            "description": (
                "Columns to read, in addition to the geometry. Omit to read them all; naming a "
                "few keeps a wide boundary set from carrying every attribute through the job."
            )
        },
        "bbox": {
            "description": (
                "Optional lon/lat window [west, south, east, north]. Where the collection "
                "carries a covering bbox this prunes row groups instead of reading the whole "
                "file, and it is required for collections too large to load whole."
            )
        },
    },
)
def load_vector_cube(
    id: str,  # noqa: A002 — the openEO parameter is named `id`
    id_property: str | None = None,
    properties: list[str] | None = None,
    bbox: list[float] | None = None,
) -> dict[str, Any]:
    """Return the collection as a GeoJSON FeatureCollection.

    A FeatureCollection rather than an xvec-backed cube, so the result is interchangeable with a
    client-supplied one and `aggregate_spatial` needs no second input path. The geometry survives
    the aggregation either way — see `aggregate_spatial`'s WKT companion coordinate.
    """
    return vectors.load_feature_collection(id, id_property=id_property, properties=properties, bbox=bbox)
