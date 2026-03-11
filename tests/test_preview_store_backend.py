from pathlib import Path
from typing import Any

from eo_api.integrations.orchestration import preview_store


def test_publish_preview_rows_uses_file_backend_when_pg_dsn_unset(monkeypatch: Any, tmp_path: Path) -> None:
    monkeypatch.delenv("GENERIC_DHIS2_PREVIEW_PG_DSN", raising=False)
    result = preview_store.publish_preview_rows(
        dataset_type="chirps3",
        rows=[{"orgUnit": "OU_1", "period": "202501", "value": "1.23"}],
        job_id="job-file",
        file_path=tmp_path / "preview.geojson",
    )
    assert result["backend"] == "file"
    assert result["job_id"] == "job-file"


def test_publish_preview_rows_uses_postgres_backend_when_pg_dsn_set(monkeypatch: Any) -> None:
    monkeypatch.setenv("GENERIC_DHIS2_PREVIEW_PG_DSN", "postgresql://user:pass@localhost:5432/db")

    def _fake_run_async(coro: Any) -> dict[str, Any]:
        close = getattr(coro, "close", None)
        if callable(close):
            close()
        return {
            "collection_id": "generic-dhis2-datavalue-preview",
            "path": "generic_dhis2_datavalue_preview",
            "job_id": "job-pg",
            "item_count": 1,
            "total_item_count": 1,
            "backend": "postgresql",
        }

    monkeypatch.setattr(preview_store, "_run_async", _fake_run_async)
    result = preview_store.publish_preview_rows(
        dataset_type="chirps3",
        rows=[{"orgUnit": "OU_1", "period": "202501", "value": "1.23"}],
        job_id="job-pg",
    )
    assert result["backend"] == "postgresql"
    assert result["job_id"] == "job-pg"
