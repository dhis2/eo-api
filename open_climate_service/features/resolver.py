"""Resolve a declared feature id to geometry, through its provider and the cache (CLIM-926).

Two entry points, deliberately split:

- :func:`current_snapshot` answers "which snapshot should this job use?" and returns an **id**.
  The automation layer calls it at submission, so the job record names a snapshot without ever
  holding geometry.
- :func:`load` answers "give me the geometry" and is called during execution, by the
  ``load_features`` process.

That split is the whole design. Resolving geometry at submission would inline megabytes into every
persisted process graph — which is the problem this ticket exists to remove.
"""

from __future__ import annotations

import logging
from typing import Any

from open_climate_service.features import cache
from open_climate_service.features.config import FeatureDeclaration, get_features_config
from open_climate_service.features.provider import resolve_provider

logger = logging.getLogger(__name__)


def declaration(feature_id: str) -> FeatureDeclaration:
    """The instance's declaration for ``feature_id``."""
    return get_features_config().get(feature_id)


def _fetch(declared: FeatureDeclaration) -> str:
    """Call the provider and cache the result, returning the new snapshot id."""
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
    return cache.write(declared, collection, version=version)


def current_snapshot(feature_id: str, *, refresh: bool = False) -> str:
    """The snapshot id a job submitted now should use, fetching only when the cache is stale.

    Returns an id rather than geometry so the caller can record which boundaries a run used without
    persisting them. Refetching is bounded by the declaration's TTL, so a schedule that fires hourly
    still calls the provider once a day — and a briefly unreachable upstream does not fail the
    submission while a usable snapshot is still within its TTL.
    """
    declared = declaration(feature_id)
    if not refresh:
        existing = cache.latest(declared)
        if existing is not None:
            return existing
    return _fetch(declared)


def load(feature_id: str, *, snapshot: str | None = None) -> dict[str, Any]:
    """The FeatureCollection for ``feature_id``, pinned to ``snapshot`` when one is named.

    A named snapshot is read as-is and never refreshed: a job that recorded one is asking for the
    boundaries that run saw, and quietly substituting today's would make the result irreproducible
    in exactly the way the snapshot id exists to prevent.
    """
    if snapshot is not None:
        return cache.read(feature_id, snapshot)
    return cache.read(feature_id, current_snapshot(feature_id))
