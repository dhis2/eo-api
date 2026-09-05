"""load_features — resolve a declared feature id to geometry at execution time (CLIM-926).

The provider-backed counterpart of `load_vector_cube`. Where `load_vector_cube` names a file in the
instance's feature store, this names a *declaration* — an id the instance config binds to a provider
and its parameters, so the geometry may come from DHIS2, from a stored collection, or from anything
a plugin registers.

It exists so that a reference can survive **into** the process graph instead of being resolved
before it. A scheduled trigger writes:

    {"process_id": "load_features", "arguments": {"id": "districts", "snapshot": "...-20260905T110422Z"}}

which is ~90 bytes in the persisted job record, rather than the megabytes a resolved
FeatureCollection would be. The `snapshot` argument is stamped at submission, so the record still
says exactly which boundaries the run used.
"""

from __future__ import annotations

from typing import Any

from open_climate_service.features import resolver
from open_climate_service.process import process


@process(
    summary="Load a declared feature set by id",
    description=(
        "Resolve a feature set declared in this instance's configuration, for use as the "
        "`geometries` argument of `aggregate_spatial`. The declaration binds the id to a provider "
        "and its parameters; the result is cached, and each cached version has a snapshot id."
    ),
    parameters={
        "id": {"description": "Feature id, as declared under `features:` in the instance config."},
        "snapshot": {
            "description": (
                "Pin to one cached snapshot, as recorded on a triggered job. Omit to use the "
                "current one, fetching from the provider when the cache is past its TTL."
            )
        },
    },
)
def load_features(
    id: str,  # noqa: A002 — the openEO parameter is named `id`, as in load_vector_cube
    snapshot: str | None = None,
) -> dict[str, Any]:
    """Return the declared feature set as a GeoJSON FeatureCollection."""
    return resolver.load(id, snapshot=snapshot)
