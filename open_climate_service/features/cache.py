"""On-disk cache of resolved feature sets (CLIM-926).

A cached set is GeoParquet, the same format and the same reader as a stored collection — but it
lives in its **own** store and is never published. The two hold the same kind of content; what
differs is lifecycle:

===================  ==========================  ==============================
                     ``<data_dir>/features/``    ``<data_dir>/cache/features/``
===================  ==========================  ==============================
Origin               an admin placed it          a provider wrote it
Listed at /features  yes                         never
Loadable by id       yes, ``load_vector_cube``   no
Missing file         a config error              refetch
Eviction             never                       free
===================  ==========================  ==============================

That separation is not cosmetic. ``GET /features`` is an unauthenticated listing, and a DHIS2
hierarchy at facility level carries point coordinates — a cache that wrote itself into a published
endpoint would make a disclosure decision nobody took. **The cache must never become id-addressable
in the public API**; sharing the reader is fine, sharing the namespace is not.

A snapshot is immutable and addressed by id, which is what makes a scheduled push reproducible: the
job record names the snapshot it used, so re-running it aggregates the boundaries that run actually
saw, not whatever the hierarchy looks like today.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from open_climate_service import config as api_config
from open_climate_service.features.config import FeatureDeclaration
from open_climate_service.shared import features as shared_features

logger = logging.getLogger(__name__)

ID_COLUMN = "_ocs_feature_id"
"""Column carrying each feature's id through the GeoParquet round-trip.

A GeoJSON feature's ``id`` is not a property, so writing a FeatureCollection to a frame would drop
it — and the id is the whole point. It is written as an explicit column and read back through
``id_property``, which also means the identity contract is re-checked on every read.
"""

_SNAPSHOT_FORMAT = "%Y%m%dT%H%M%S%fZ"
_SNAPSHOT_RE = re.compile(r"^(?P<fingerprint>[0-9a-f]{12})-(?P<stamp>\d{8}T\d{12}Z)$")
# Microseconds, not seconds: two resolutions of one declaration in the same second — a refresh
# straight after a fetch, or two triggers firing on one event — would otherwise share an id while
# holding different geometry, and the second write would silently replace what a job record pins.


def cache_dir() -> Path:
    """Where resolved feature sets are cached. Deliberately not ``features/``."""
    return api_config.get_data_root() / "cache" / "features"


def _feature_dir(feature_id: str) -> Path:
    return cache_dir() / feature_id


def fingerprint(declaration: FeatureDeclaration) -> str:
    """A stable digest of what was asked for, so changing the request invalidates the cache.

    Covers the provider and its params: ``level: 2`` and ``level: 3`` are different questions to
    one provider and must not share an answer.
    """
    payload = json.dumps(
        {"provider": declaration.provider, "params": declaration.params},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:12]


def _snapshot_time(snapshot_id: str) -> datetime | None:
    match = _SNAPSHOT_RE.match(snapshot_id)
    if match is None:
        return None
    return datetime.strptime(match.group("stamp"), _SNAPSHOT_FORMAT).replace(tzinfo=UTC)


def snapshot_path(feature_id: str, snapshot_id: str) -> Path | None:
    """Resolve a snapshot to its file, or None when there is no such snapshot.

    The id arrives from a persisted process graph, so it is validated and the resolved path is
    confirmed to sit inside the feature's own cache directory rather than trusted as a path.
    """
    if _SNAPSHOT_RE.match(snapshot_id) is None:
        return None
    root = _feature_dir(feature_id)
    candidate = (root / f"{snapshot_id}.parquet").resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError:
        return None
    return candidate if candidate.is_file() else None


def latest(declaration: FeatureDeclaration) -> str | None:
    """The newest snapshot matching this declaration and still within its TTL, if any."""
    root = _feature_dir(declaration.id)
    if not root.is_dir():
        return None
    wanted = fingerprint(declaration)
    cutoff = datetime.now(UTC) - timedelta(seconds=declaration.effective_ttl)

    best: tuple[datetime, str] | None = None
    for path in root.glob("*.parquet"):
        match = _SNAPSHOT_RE.match(path.stem)
        if match is None or match.group("fingerprint") != wanted:
            continue
        stamp = _snapshot_time(path.stem)
        if stamp is None or stamp < cutoff:
            continue
        if best is None or stamp > best[0]:
            best = (stamp, path.stem)
    return None if best is None else best[1]


def write(declaration: FeatureDeclaration, collection: dict[str, Any], *, version: str | None = None) -> str:
    """Persist a resolved FeatureCollection as a new snapshot and return its id."""
    import geopandas as gpd

    features = collection.get("features") or []
    if not features:
        raise ValueError(f"Feature provider {declaration.provider!r} returned no features for {declaration.id!r}")

    source = f"Feature set {declaration.id!r} from provider {declaration.provider!r}"
    field = declaration.id_property or "id"
    values = [feature.get(field if field in feature else "id") for feature in features]
    labels = shared_features.validate_feature_ids(values, source=source, field=field)

    frame = gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")
    frame[ID_COLUMN] = labels

    stamp = datetime.now(UTC).strftime(_SNAPSHOT_FORMAT)
    snapshot_id = f"{fingerprint(declaration)}-{stamp}"
    root = _feature_dir(declaration.id)
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{snapshot_id}.parquet"
    # A covering bbox costs almost nothing to write and is what makes a windowed read cheap later.
    frame.to_parquet(path, write_covering_bbox=True)

    (root / f"{snapshot_id}.json").write_text(
        json.dumps(
            {
                "feature_id": declaration.id,
                "provider": declaration.provider,
                "params": declaration.params,
                "version": version,
                "feature_count": len(features),
                "fetched_at": datetime.now(UTC).isoformat(),
            },
            indent=2,
            default=str,
        )
    )
    logger.info("Cached feature set '%s' as snapshot %s (%d features)", declaration.id, snapshot_id, len(features))
    return snapshot_id


def read(feature_id: str, snapshot_id: str) -> dict[str, Any]:
    """Read one snapshot back as a GeoJSON FeatureCollection."""
    path = snapshot_path(feature_id, snapshot_id)
    if path is None:
        raise ValueError(
            f"Feature snapshot {snapshot_id!r} for {feature_id!r} is no longer cached. "
            "Snapshots are evictable; re-run without pinning one to fetch a fresh set."
        )
    return shared_features.read_features(
        path,
        source=f"Feature snapshot {snapshot_id!r} of {feature_id!r}",
        id_property=ID_COLUMN,
    )


def evict(feature_id: str, *, keep: int = 3) -> list[str]:
    """Drop all but the newest ``keep`` snapshots of one feature set, returning what was removed.

    Old snapshots are kept rather than overwritten because a job record may still name one; keeping
    a few bounds the disk cost without breaking the most recent reruns.
    """
    root = _feature_dir(feature_id)
    if not root.is_dir():
        return []
    stems = sorted(
        (path.stem for path in root.glob("*.parquet") if _SNAPSHOT_RE.match(path.stem)),
        key=lambda stem: _snapshot_time(stem) or datetime.min.replace(tzinfo=UTC),
        reverse=True,
    )
    removed = []
    for stem in stems[keep:]:
        (root / f"{stem}.parquet").unlink(missing_ok=True)
        (root / f"{stem}.json").unlink(missing_ok=True)
        removed.append(stem)
    return removed
