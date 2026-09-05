"""Resolve a declared feature id to geometry, through its provider and the store (CLIM-926).

Two entry points:

- :func:`ensure_current` refreshes the stored entry if it is stale and returns the **version** that
  is now in the store. The automation layer calls it at submission so a job can record which
  boundaries it ran against, without the geometry itself ever entering the job record.
- :func:`load` returns the geometry, and is called during execution by the ``load_features`` process.

That split is the design. Resolving geometry at submission would inline megabytes into every
persisted process graph — the problem this ticket exists to remove.
"""

from __future__ import annotations

import logging
from typing import Any

from open_climate_service.features import store
from open_climate_service.features.config import FeatureTemplate, get_feature_templates
from open_climate_service.features.provider import resolve_provider, stores_result
from open_climate_service.shared import features as shared_features

logger = logging.getLogger(__name__)

LIVE_VERSION = "live"
"""Version recorded for a set whose provider resolves in place and stores nothing."""


def declaration(feature_id: str) -> FeatureTemplate:
    """The instance's template for ``feature_id``."""
    return get_feature_templates().get(feature_id)


def _call(declared: FeatureTemplate) -> tuple[dict[str, Any], str | None]:
    """Call the provider and check it returned something usable."""
    provider = resolve_provider(declared.provider)
    result = provider(**declared.params)

    version: str | None = None
    if isinstance(result, tuple):
        collection, version = result
    else:
        collection = result

    if not isinstance(collection, dict) or collection.get("type") != "FeatureCollection":
        raise ValueError(
            f"Feature provider {declared.provider!r} must return a GeoJSON FeatureCollection "
            f"for {declared.id!r}, got {type(collection).__name__}"
        )
    return collection, version


def ensure_current(feature_id: str, *, refresh: bool = False) -> str:
    """Make the stored entry current and return its version.

    Fetching is bounded by the declaration's TTL, so a schedule firing hourly still asks the
    provider once a day — and a briefly unreachable upstream does not fail a submission while the
    stored entry is still within its TTL.

    A provider that stores nothing has nothing to make current: it reads whatever the store holds
    at the moment it runs, so there is no fetch here and no version to record beyond that fact.
    """
    declared = declaration(feature_id)
    if not stores_result(resolve_provider(declared.provider)):
        return LIVE_VERSION
    if not refresh and store.is_fresh(declared):
        recorded = store.metadata(feature_id).get("version")
        if isinstance(recorded, str):
            return recorded
    collection, version = _call(declared)
    return store.write(declared, collection, version=version)


def load(feature_id: str) -> dict[str, Any]:
    """The FeatureCollection for ``feature_id``, refreshing the store first if it is stale.

    A stored set is read through the provider, which goes to the same call `load_vector_cube` makes.
    A provider-maintained one is read from the store it just updated, where the sidecar says which
    column carries the ids — so both come back looking the same.
    """
    declared = declaration(feature_id)
    if not stores_result(resolve_provider(declared.provider)):
        collection, _ = _call(declared)
        return collection
    ensure_current(feature_id)
    return shared_features.load_feature_collection(feature_id)
