"""Backend store for generic DHIS2 preview collection rows."""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import re
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

_PREVIEW_COLLECTION_ID = "generic-dhis2-datavalue-preview"
_PREVIEW_PG_TABLE = "generic_dhis2_datavalue_preview"
_PREVIEW_COLLECTION_PATH = Path(
    os.getenv("GENERIC_DHIS2_DATAVALUE_PREVIEW_PATH", "/tmp/generic_dhis2_datavalue_preview.geojson")
)
_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")


def _empty_feature_collection() -> dict[str, Any]:
    return {"type": "FeatureCollection", "features": []}


def _run_async(coro: Any) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    with ThreadPoolExecutor(max_workers=1) as executor:
        return executor.submit(lambda: asyncio.run(coro)).result()


def _validate_table_name(table_name: str) -> str:
    if not _TABLE_RE.fullmatch(table_name):
        raise ValueError(f"Invalid PostgreSQL table name: {table_name}")
    return table_name


def _pg_dsn() -> str:
    return os.getenv("EO_API_PG_DSN", "").strip()


def _as_feature(feature_id: str, properties: dict[str, Any], geometry: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "type": "Feature",
        "id": feature_id,
        "geometry": geometry,
        "properties": properties,
    }


def _asyncpg_module() -> Any:
    import asyncpg

    return asyncpg


def _use_postgres_backend() -> bool:
    return bool(_pg_dsn())


def ensure_preview_store_seeded(*, file_path: Path | None = None) -> str:
    """Ensure configured preview backend is initialized."""
    if _use_postgres_backend():
        _run_async(_ensure_postgres_store_seeded())
        return _validate_table_name(_PREVIEW_PG_TABLE)
    path = file_path or _PREVIEW_COLLECTION_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(json.dumps(_empty_feature_collection()), encoding="utf-8")
    return str(path)


def publish_preview_rows(
    *,
    dataset_type: str,
    rows: list[dict[str, Any]],
    job_id: str | None = None,
    file_path: Path | None = None,
) -> dict[str, Any]:
    """Publish preview rows to the configured backend."""
    if _use_postgres_backend():
        result = _run_async(_publish_preview_rows_postgres(dataset_type=dataset_type, rows=rows, job_id=job_id))
        return cast(dict[str, Any], result)
    return _publish_preview_rows_file(dataset_type=dataset_type, rows=rows, job_id=job_id, file_path=file_path)


def load_preview_features(*, job_id: str | None = None, file_path: Path | None = None) -> list[dict[str, Any]]:
    """Load preview features from configured backend."""
    if _use_postgres_backend():
        result = _run_async(_load_preview_features_postgres(job_id=job_id))
        return cast(list[dict[str, Any]], result)
    return _load_preview_features_file(job_id=job_id, file_path=file_path)


def get_preview_feature(identifier: str, *, file_path: Path | None = None) -> dict[str, Any] | None:
    """Fetch a single preview feature by identifier."""
    if _use_postgres_backend():
        result = _run_async(_get_preview_feature_postgres(identifier))
        return cast(dict[str, Any] | None, result)
    return _get_preview_feature_file(identifier, file_path=file_path)


def infer_preview_fields() -> dict[str, dict[str, str]]:
    """Infer schema fields from preview backend."""
    fields: dict[str, dict[str, str]] = {"job_id": {"type": "string"}, "dataset_type": {"type": "string"}}
    features = load_preview_features()
    if not features:
        return fields
    props = features[0].get("properties", {})
    if not isinstance(props, dict):
        return fields
    for key, value in props.items():
        if isinstance(value, bool):
            ftype = "boolean"
        elif isinstance(value, int):
            ftype = "integer"
        elif isinstance(value, float):
            ftype = "number"
        else:
            ftype = "string"
        fields[str(key)] = {"type": ftype}
    return fields


def _publish_preview_rows_file(
    *,
    dataset_type: str,
    rows: list[dict[str, Any]],
    job_id: str | None = None,
    file_path: Path | None = None,
) -> dict[str, Any]:
    path = file_path or _PREVIEW_COLLECTION_PATH
    ensure_preview_store_seeded(file_path=path)

    effective_job_id = job_id or uuid.uuid4().hex
    published_at = datetime.now(UTC).isoformat()
    appended_features = []
    for idx, row in enumerate(rows):
        properties = dict(row)
        properties["dataset_type"] = dataset_type
        properties["job_id"] = effective_job_id
        properties["published_at"] = published_at
        appended_features.append(_as_feature(f"{effective_job_id}-{idx}", properties, None))

    with path.open("r+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            payload = json.load(handle)
            features = payload.get("features", [])
            if not isinstance(features, list):
                features = []
            features.extend(appended_features)
            payload = {"type": "FeatureCollection", "features": features}
            handle.seek(0)
            json.dump(payload, handle)
            handle.truncate()
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    return {
        "collection_id": _PREVIEW_COLLECTION_ID,
        "path": str(path),
        "job_id": effective_job_id,
        "item_count": len(appended_features),
        "total_item_count": len(payload["features"]),
        "backend": "file",
    }


def _load_preview_features_file(*, job_id: str | None = None, file_path: Path | None = None) -> list[dict[str, Any]]:
    path = file_path or _PREVIEW_COLLECTION_PATH
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    features = payload.get("features", [])
    if not isinstance(features, list):
        return []
    if not job_id:
        return features
    return [f for f in features if str((f.get("properties") or {}).get("job_id")) == job_id]


def _get_preview_feature_file(identifier: str, *, file_path: Path | None = None) -> dict[str, Any] | None:
    for feature in _load_preview_features_file(file_path=file_path):
        if str(feature.get("id")) == identifier:
            return feature
    return None


async def _ensure_postgres_store_seeded() -> None:
    asyncpg = _asyncpg_module()
    table_name = _validate_table_name(_PREVIEW_PG_TABLE)
    conn = await asyncpg.connect(_pg_dsn())
    try:
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                feature_id TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                dataset_type TEXT NOT NULL,
                published_at TIMESTAMPTZ NOT NULL,
                properties JSONB NOT NULL,
                geometry JSONB
            );
            """
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS {table_name.replace('.', '_')}_job_id_idx ON {table_name}(job_id);"
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS {table_name.replace('.', '_')}_dataset_idx ON {table_name}(dataset_type);"
        )
    finally:
        await conn.close()


async def _publish_preview_rows_postgres(
    *,
    dataset_type: str,
    rows: list[dict[str, Any]],
    job_id: str | None = None,
) -> dict[str, Any]:
    asyncpg = _asyncpg_module()
    table_name = _validate_table_name(_PREVIEW_PG_TABLE)
    await _ensure_postgres_store_seeded()

    effective_job_id = job_id or uuid.uuid4().hex
    published_at = datetime.now(UTC)
    records: list[tuple[str, str, str, datetime, str, str]] = []
    for idx, row in enumerate(rows):
        properties = dict(row)
        properties["dataset_type"] = dataset_type
        properties["job_id"] = effective_job_id
        properties["published_at"] = published_at.isoformat()
        records.append(
            (
                f"{effective_job_id}-{idx}",
                effective_job_id,
                dataset_type,
                published_at,
                json.dumps(properties),
                "null",
            )
        )

    conn = await asyncpg.connect(_pg_dsn())
    try:
        if records:
            await conn.executemany(
                f"""
                INSERT INTO {table_name} (feature_id, job_id, dataset_type, published_at, properties, geometry)
                VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb)
                ON CONFLICT (feature_id) DO UPDATE
                SET job_id = EXCLUDED.job_id,
                    dataset_type = EXCLUDED.dataset_type,
                    published_at = EXCLUDED.published_at,
                    properties = EXCLUDED.properties,
                    geometry = EXCLUDED.geometry;
                """,
                records,
            )
        total_count = int(await conn.fetchval(f"SELECT COUNT(*) FROM {table_name};"))
    finally:
        await conn.close()

    return {
        "collection_id": _PREVIEW_COLLECTION_ID,
        "path": table_name,
        "job_id": effective_job_id,
        "item_count": len(records),
        "total_item_count": total_count,
        "backend": "postgresql",
    }


async def _load_preview_features_postgres(*, job_id: str | None = None) -> list[dict[str, Any]]:
    asyncpg = _asyncpg_module()
    table_name = _validate_table_name(_PREVIEW_PG_TABLE)
    await _ensure_postgres_store_seeded()

    conn = await asyncpg.connect(_pg_dsn())
    try:
        if job_id:
            rows = await conn.fetch(
                f"""
                SELECT feature_id, properties, geometry
                FROM {table_name}
                WHERE job_id = $1
                ORDER BY published_at DESC, feature_id ASC;
                """,
                job_id,
            )
        else:
            rows = await conn.fetch(
                f"""
                SELECT feature_id, properties, geometry
                FROM {table_name}
                ORDER BY published_at DESC, feature_id ASC;
                """
            )
    finally:
        await conn.close()

    return [
        _as_feature(
            str(row["feature_id"]),
            dict(row["properties"]) if isinstance(row["properties"], dict) else {},
            dict(row["geometry"]) if isinstance(row["geometry"], dict) else None,
        )
        for row in rows
    ]


async def _get_preview_feature_postgres(identifier: str) -> dict[str, Any] | None:
    asyncpg = _asyncpg_module()
    table_name = _validate_table_name(_PREVIEW_PG_TABLE)
    await _ensure_postgres_store_seeded()

    conn = await asyncpg.connect(_pg_dsn())
    try:
        row = await conn.fetchrow(
            f"""
            SELECT feature_id, properties, geometry
            FROM {table_name}
            WHERE feature_id = $1;
            """,
            identifier,
        )
    finally:
        await conn.close()

    if row is None:
        return None

    props = dict(row["properties"]) if isinstance(row["properties"], dict) else {}
    geom = dict(row["geometry"]) if isinstance(row["geometry"], dict) else None
    return _as_feature(str(row["feature_id"]), props, geom)
