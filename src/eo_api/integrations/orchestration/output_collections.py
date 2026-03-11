"""Runtime helpers for publishing workflow outputs to OGC collection backends."""

from __future__ import annotations

from pathlib import Path

from eo_api.integrations.orchestration import preview_store

# Keep this symbol for tests and existing monkeypatching patterns.
_PREVIEW_COLLECTION_PATH = preview_store._PREVIEW_COLLECTION_PATH


def ensure_output_collections_seeded() -> Path:
    """Ensure configured output collection backend is initialized."""
    backend_path = preview_store.ensure_preview_store_seeded(file_path=_PREVIEW_COLLECTION_PATH)
    # PostgreSQL seeding returns a table name; preserve Path return contract for callers.
    if "/" not in backend_path and "\\" not in backend_path:
        return _PREVIEW_COLLECTION_PATH
    return Path(backend_path)


def publish_dhis2_datavalue_preview(
    *,
    dataset_type: str,
    rows: list[dict[str, object]],
    job_id: str | None = None,
) -> dict[str, object]:
    """Append DHIS2 dataValue preview rows to configured backend."""
    return preview_store.publish_preview_rows(
        dataset_type=dataset_type,
        rows=rows,
        job_id=job_id,
        file_path=_PREVIEW_COLLECTION_PATH,
    )
