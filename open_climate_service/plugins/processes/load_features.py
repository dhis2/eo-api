"""load_features — resolve a declared feature id to geometry at execution time (CLIM-926).

The provider-backed counterpart of `load_vector_cube`. Where `load_vector_cube` names a file in the
instance's feature store, this names a *declaration* — an id the instance config binds to a provider
and its parameters, so the geometry may come from DHIS2, from a stored collection, or from anything
a plugin registers.

It exists so that a reference can survive **into** the process graph instead of being resolved
before it. A scheduled trigger writes:

    {"process_id": "load_features", "arguments": {"id": "districts"}}

which is a few dozen bytes in the persisted job record, rather than the megabytes a resolved
FeatureCollection would be. Which boundaries the run used is recorded on the job itself, as the
version the store held at submission.
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
        "and its parameters; the stored collection is refreshed when it is past its TTL."
    ),
    parameters={
        "id": {"description": "Feature id, as declared under `features:` in the instance config."},
    },
)
def load_features(
    id: str,  # noqa: A002 — the openEO parameter is named `id`, as in load_vector_cube
) -> dict[str, Any]:
    """Return the declared feature set as a GeoJSON FeatureCollection."""
    return resolver.load(id)
