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
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
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


def target_path(feature_id: str) -> Path:
    """Where a collection's GeoParquet lives."""
    root = shared_features.features_dir()
    root.mkdir(parents=True, exist_ok=True)
    return root / f"{feature_id}{shared_features.FEATURE_SUFFIX}"


def staging_path(feature_id: str) -> Path:
    """Where a refresh is written before it replaces the live entry.

    Writing straight to the live path lets a reader open a half-written file: a scheduled refresh
    racing a `/result` produces "Parquet magic bytes not found in footer", which is both a failed
    job and an unintelligible reason. Staging then replacing means a reader sees the old file or the
    new one and never a partial one.

    Beside the target so the replace is a same-filesystem rename, and without the `.parquet` suffix
    so the directory scan cannot mistake a leftover for a collection.

    Unique per call, not per collection: two refreshes of one set otherwise share a staging file and
    each moves the other's out from under it, trading a torn read for a FileNotFoundError. With a
    path each, both complete and the later one wins -- which is the same outcome as re-syncing a
    dataset twice.
    """
    root = shared_features.features_dir()
    root.mkdir(parents=True, exist_ok=True)
    handle, name = tempfile.mkstemp(prefix=f"{feature_id}.", suffix=".staging", dir=root)
    os.close(handle)
    return Path(name)


def _commit(staged: Path, feature_id: str) -> Path:
    """Move a staged file into place atomically, and return where it landed."""
    final = target_path(feature_id)
    os.replace(staged, final)
    return final


_MISSING = object()


def _feature_id(feature: dict[str, Any], field: str) -> Any:
    """The value `id_property` names, looked for where GeoJSON actually keeps attributes.

    A GeoJSON feature holds its attributes under ``properties``; only ``id`` is a top-level key. A
    lookup that tested the top level alone would find `id_property` nowhere and quietly fall back to
    the feature's own id — so a template pointing at, say, an org-unit code column would key the
    whole export on the wrong column while looking like it worked.

    ``properties`` wins over the top level so a feature carrying both is read the way the GeoJSON
    spec means it, and a genuinely absent value returns None for the identity contract to reject.
    """
    properties = feature.get("properties")
    if isinstance(properties, dict) and field in properties:
        return properties[field]
    value = feature.get(field, _MISSING)
    return None if value is _MISSING else value


def check_ownership(declaration: FeatureTemplate) -> None:
    """Refuse to overwrite a collection this provider does not own."""
    if shared_features.collection_path(declaration.id) is None:
        return
    owner = metadata(declaration.id).get("provider")
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


def _write_sidecar(
    declaration: FeatureTemplate, *, id_property: str, version: str, count: int, fetched: datetime
) -> None:
    sidecar = shared_features.sidecar_path(declaration.id)
    if sidecar is None:
        return
    payload = json.dumps(
        {
            "id_property": id_property,
            "provider": declaration.provider,
            "params_fingerprint": params_fingerprint(declaration),
            "version": version,
            "fetched_at": fetched.isoformat(),
            "feature_count": count,
        },
        indent=2,
        default=str,
    )
    # Atomic, for the same reason the parquet is. A torn sidecar is worse than a torn collection:
    # it parses as absent, so `id_property` is lost and the collection comes back with the wrong
    # ids -- silently, where a torn parquet at least fails loudly.
    handle, name = tempfile.mkstemp(prefix=f"{declaration.id}.", suffix=".sidecar", dir=sidecar.parent)
    os.close(handle)
    staged = Path(name)
    try:
        staged.write_text(payload)
        os.replace(staged, sidecar)
    except BaseException:
        staged.unlink(missing_ok=True)
        raise


def _validate_id_column(path: Path, id_property: str, declaration: FeatureTemplate) -> int:
    """Check a written file's ids in Arrow, and return how many features it holds.

    Kept in Arrow rather than pulled into Python. `to_pylist()` on a country's buildings is millions
    of Python str objects, which is the exact cost the file-writing path exists to avoid -- the ids
    would end up dominating a read that was supposed to touch one narrow column. `null_count` and a
    distinct count answer the same question in C++, and only a *failing* file pays to materialise
    anything, and then only enough to name the offenders.
    """
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    column = pq.read_table(path, columns=[id_property])[id_property]
    total = len(column)
    source = f"Feature set {declaration.id!r} from provider {declaration.provider!r}"
    if total == 0:
        raise ValueError(f"Feature provider {declaration.provider!r} wrote no features for {declaration.id!r}")

    labels = pc.cast(column, "string")
    if column.null_count or pc.any(pc.is_null(labels)).as_py():
        # Only now is it worth materialising: the message has to name what went wrong.
        shared_features.validate_feature_ids(column.to_pylist(), source=source, field=id_property)
    if pc.count_distinct(labels).as_py() != total:
        shared_features.validate_feature_ids(labels.to_pylist(), source=source, field=id_property)
    return total


def record_written_file(declaration: FeatureTemplate, path: Path, *, version: str | None = None) -> str:
    """Record a GeoParquet file a provider wrote itself, without loading its geometry.

    The counterpart of :func:`write` for sources too large to pass through a FeatureCollection. The
    file is already in place; what remains is to check it can be used and to record the sidecar.

    Ids are validated by reading **only the id column** through Arrow. For a country's buildings that
    is one narrow column rather than millions of decoded geometries, which is the difference between
    this being cheap and being the very cost the file-writing path exists to avoid.
    """
    import pyarrow.parquet as pq

    # Existence is not enough: the staging path is created empty before the provider is handed it,
    # so a provider that returns without writing leaves a zero-byte file rather than none.
    if not path.is_file() or path.stat().st_size == 0:
        raise ValueError(
            f"Feature provider {declaration.provider!r} reported writing {path}, which is missing or empty"
        )

    staged = path != target_path(declaration.id)

    # Ownership is *not* rechecked here. The file is already written, so at this point the store
    # holds the provider's own output — asking whether it may overwrite what is there would compare
    # it against itself and refuse. The resolver checks before the provider runs, which is the only
    # moment the answer is meaningful.
    schema = pq.read_schema(path)
    id_property = declaration.id_property or ("id" if "id" in schema.names else None)
    if id_property is None:
        raise ValueError(
            f"Feature collection {declaration.id!r} has no 'id' column and the template sets no "
            f"id_property. Available: {', '.join(n for n in schema.names if n != 'geometry')}"
        )
    if id_property not in schema.names:
        raise ValueError(
            f"Feature collection {declaration.id!r} has no property {id_property!r}. "
            f"Available: {', '.join(n for n in schema.names if n != 'geometry')}"
        )

    count = _validate_id_column(path, id_property, declaration)

    # Only once the file is known good does it become the live entry: a provider that fails
    # part-way, or writes something that breaks the identity contract, leaves the previous
    # collection untouched rather than a broken one in its place.
    if staged:
        _commit(path, declaration.id)

    now = datetime.now(UTC)
    recorded = version or f"{params_fingerprint(declaration)}-{now.strftime('%Y%m%dT%H%M%S%fZ')}"
    _write_sidecar(declaration, id_property=id_property, version=recorded, count=count, fetched=now)
    logger.info(
        "Recorded feature collection '%s' written by provider '%s': %d features, version %s",
        declaration.id,
        declaration.provider,
        count,
        recorded,
    )
    return recorded


def write(declaration: FeatureTemplate, collection: dict[str, Any], *, version: str | None = None) -> str:
    """Write a resolved FeatureCollection into the store, returning the version recorded for it.

    Refuses to overwrite a collection this provider does not own, so a declaration whose id collides
    with a curated file fails loudly instead of silently replacing it.
    """
    import geopandas as gpd

    features = collection.get("features") or []
    if not features:
        raise ValueError(f"Feature provider {declaration.provider!r} returned no features for {declaration.id!r}")

    check_ownership(declaration)

    source = f"Feature set {declaration.id!r} from provider {declaration.provider!r}"
    field = declaration.id_property or "id"
    values = [_feature_id(feature, field) for feature in features]
    labels = shared_features.validate_feature_ids(values, source=source, field=field)

    frame = gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")
    frame[ID_COLUMN] = labels

    now = datetime.now(UTC)
    # A provider's own version wins where it has one. Otherwise the fallback identifies *what* was
    # fetched as well as when: the params fingerprint distinguishes a level-2 fetch from a level-3
    # one, and microseconds keep two fetches in the same second from reading as the same version in
    # a job's provenance line.
    recorded = version or f"{params_fingerprint(declaration)}-{now.strftime('%Y%m%dT%H%M%S%fZ')}"

    # A covering bbox costs almost nothing to write and is what makes a windowed read cheap later.
    staged = staging_path(declaration.id)
    try:
        frame.to_parquet(staged, write_covering_bbox=True)
        _commit(staged, declaration.id)
    except BaseException:
        staged.unlink(missing_ok=True)
        raise

    _write_sidecar(declaration, id_property=ID_COLUMN, version=recorded, count=len(features), fetched=now)
    logger.info(
        "Updated feature collection '%s' from provider '%s': %d features, version %s",
        declaration.id,
        declaration.provider,
        len(features),
        recorded,
    )
    return recorded
