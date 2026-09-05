"""The ``stored`` feature provider: a collection this instance already ships (CLIM-926).

Reads ``<data_dir>/features/`` through the same call `load_vector_cube` makes, so a boundary set is
one artefact with one reader whether it is reached from a process graph or from a trigger.

This is what keeps the standalone case honest. An instance with no DHIS2 configured can still
declare features and schedule zonal aggregation — it drops a GeoParquet file in ``features/`` and
points a declaration at it:

    features:
      - id: catchments
        provider: stored
        params: { id: catchments, id_property: catchment_code }
"""

from __future__ import annotations

from typing import Any

from open_climate_service.features.provider import feature_provider
from open_climate_service.shared import features


@feature_provider("stored", stores_result=False)
def stored(
    id: str,  # noqa: A002 — names the collection, matching load_vector_cube's parameter
    id_property: str | None = None,
    properties: list[str] | None = None,
    bbox: list[float] | None = None,
) -> dict[str, Any]:
    """Load a named collection from this instance's feature store."""
    return features.load_feature_collection(id, id_property=id_property, properties=properties, bbox=bbox)
