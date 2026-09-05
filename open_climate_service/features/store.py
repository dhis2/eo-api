"""Writing provider-resolved sets into the feature store (CLIM-926).

There is one store, ``<data_dir>/features/``, and a provider **updates an entry in it** rather than
filling a second cache beside it. That mirrors how datasets already work: an ingestion plugin
fetches from upstream and writes into ``downloads/``, which is then served — there is no separate
"dataset cache", because fetched data is re-fetchable and a version string is enough to explain it.

A collection therefore comes in two flavours, distinguished by whether it has a sidecar:

- **curated** — an admin dropped in a GeoParquet file. No sidecar, or one naming only its id column.
  Nothing refreshes it; deleting it is a config change.
- **provider-maintained** — a ``features/*.yaml`` template names a provider, and the entry is
  rewritten whenever it falls outside its TTL. The sidecar records the *runtime* facts: the
  provider, the params fingerprint, the version and when it was fetched. Authored metadata —
  licence, attribution, description — belongs to the template, not here, so a set can be described
  in the catalogue before it has ever been fetched.

The sidecar is what keeps a refresh from clobbering something it does not own: a provider only
rewrites an entry whose sidecar says it is that provider's.

**What is deliberately not kept:** a history of past versions. Recording *which* boundaries a run
used — enough to explain why yesterday covered 47 districts and today covers 48 — needs a version
string, not an archive of every snapshot. Byte-identical re-execution of an old job is a stronger
property than anything asks for, and re-running a scheduled push is usually a repair that wants
current boundaries anyway.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import UTC, datetime
from typing import Any

from open_climate_service.features.config import FeatureTemplate
from open_climate_service.shared import features as shared_features

logger = logging.getLogger(__name__)

ID_COLUMN = "_ocs_feature_id"
"""Column carrying each feature's id through the GeoParquet round-trip.

A GeoJSON feature's ``id`` is not a property, so writing a FeatureCollection to a frame would drop
it — and the id is the whole point. It is written as an explicit column and recorded in the sidecar
as ``id_property``, so a reader that knows only the collection id still gets the right ids back.
"""


def params_fingerprint(declaration: FeatureTemplate) -> str:
    """A digest of what was asked for, so changing the request forces a refetch.

    Covers the provider and its params: ``level: 2`` and ``level: 3`` are different questions and an
    entry fetched for one must not be served for the other.
    """
    payload = json.dumps(
        {"provider": declaration.provider, "params": declaration.params},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:12]


def metadata(feature_id: str) -> dict[str, Any]:
    """The sidecar for one entry, or an empty dict when it has none."""
    return shared_features.read_sidecar(feature_id)


def is_fresh(declaration: FeatureTemplate) -> bool:
    """Whether the stored entry can be served without asking the provider again.

    False when there is no entry, when it was fetched for different params, when it belongs to a
    different provider, or when it is past its TTL.
    """
    if shared_features.collection_path(declaration.id) is None:
        return False
    sidecar = metadata(declaration.id)
    if sidecar.get("provider") != declaration.provider:
        return False
    if sidecar.get("params_fingerprint") != params_fingerprint(declaration):
        return False
    fetched_at = sidecar.get("fetched_at")
    if not isinstance(fetched_at, str):
        return False
    try:
        stamped = datetime.fromisoformat(fetched_at)
    except ValueError:
        return False
    age = (datetime.now(UTC) - stamped).total_seconds()
    return age < declaration.effective_ttl


def write(declaration: FeatureTemplate, collection: dict[str, Any], *, version: str | None = None) -> str:
    """Write a resolved FeatureCollection into the store, returning the version recorded for it.

    Refuses to overwrite a collection this provider does not own, so a declaration whose id collides
    with a curated file fails loudly instead of silently replacing it.
    """
    import geopandas as gpd

    features = collection.get("features") or []
    if not features:
        raise ValueError(f"Feature provider {declaration.provider!r} returned no features for {declaration.id!r}")

    existing = metadata(declaration.id)
    if shared_features.collection_path(declaration.id) is not None:
        owner = existing.get("provider")
        if owner is None:
            raise ValueError(
                f"Feature collection {declaration.id!r} already exists in the store and is not "
                f"maintained by a provider. Declaration {declaration.id!r} would overwrite it — "
                "give the declaration a different id, or remove the file."
            )
        if owner != declaration.provider:
            raise ValueError(
                f"Feature collection {declaration.id!r} is maintained by provider {owner!r}, "
                f"not {declaration.provider!r}. Give the declaration a different id."
            )

    source = f"Feature set {declaration.id!r} from provider {declaration.provider!r}"
    field = declaration.id_property or "id"
    values = [feature.get(field) if field in feature else feature.get("id") for feature in features]
    labels = shared_features.validate_feature_ids(values, source=source, field=field)

    frame = gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")
    frame[ID_COLUMN] = labels

    now = datetime.now(UTC)
    # A provider's own version wins where it has one. Otherwise the fallback identifies *what* was
    # fetched as well as when: the params fingerprint distinguishes a level-2 fetch from a level-3
    # one, and microseconds keep two fetches in the same second from reading as the same version in
    # a job's provenance line.
    recorded = version or f"{params_fingerprint(declaration)}-{now.strftime('%Y%m%dT%H%M%S%fZ')}"

    root = shared_features.features_dir()
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{declaration.id}{shared_features.FEATURE_SUFFIX}"
    # A covering bbox costs almost nothing to write and is what makes a windowed read cheap later.
    frame.to_parquet(path, write_covering_bbox=True)

    sidecar = shared_features.sidecar_path(declaration.id)
    if sidecar is not None:
        sidecar.write_text(
            json.dumps(
                {
                    "id_property": ID_COLUMN,
                    "provider": declaration.provider,
                    "params_fingerprint": params_fingerprint(declaration),
                    "version": recorded,
                    "fetched_at": now.isoformat(),
                    "feature_count": len(features),
                },
                indent=2,
                default=str,
            )
        )
    logger.info(
        "Updated feature collection '%s' from provider '%s': %d features, version %s",
        declaration.id,
        declaration.provider,
        len(features),
        recorded,
    )
    return recorded
