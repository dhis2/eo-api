from datetime import UTC, date, datetime, tzinfo
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from open_climate_service.ingestions import services, sync_engine
from open_climate_service.ingestions.schemas import (
    ArtifactCoverage,
    ArtifactFormat,
    ArtifactPublication,
    ArtifactRecord,
    ArtifactRequestScope,
    CoverageSpatial,
    CoverageTemporal,
    DatasetDetailRecord,
    DatasetPublication,
    PublicationStatus,
    SyncAction,
    SyncDetail,
    SyncKind,
    SyncResponse,
)


def _artifact(
    *,
    artifact_id: str,
    source_dataset_id: str = "chirps3_precipitation_daily",
    managed_dataset_id: str = "chirps3_precipitation_daily_sle",
    created_at: str = "2026-01-10T00:00:00+00:00",
    end: str = "2026-01-10",
    path: str = "/tmp/chirps3_precipitation_daily.zarr",
) -> ArtifactRecord:
    return ArtifactRecord(
        artifact_id=artifact_id,
        dataset_id=source_dataset_id,
        dataset_name="CHIRPS3 precipitation",
        variable="precip",
        format=ArtifactFormat.ZARR,
        path=path,
        asset_paths=[path],
        variables=["precip"],
        request_scope=ArtifactRequestScope(
            start="2026-01-01",
            end=end,
            bbox=(1.0, 2.0, 3.0, 4.0),
        ),
        coverage=ArtifactCoverage(
            temporal=CoverageTemporal(start="2026-01-01", end=end),
            spatial=CoverageSpatial(xmin=1.0, ymin=2.0, xmax=3.0, ymax=4.0),
        ),
        created_at=datetime.fromisoformat(created_at),
        publication=ArtifactPublication(
            status=PublicationStatus.PUBLISHED,
            collection_id=managed_dataset_id,
            pygeoapi_path=f"/ogcapi/collections/{managed_dataset_id}",
        ),
    )


def _dataset_detail(dataset_id: str) -> DatasetDetailRecord:
    return DatasetDetailRecord(
        dataset_id=dataset_id,
        source_dataset_id="chirps3_precipitation_daily",
        dataset_name="CHIRPS3 precipitation",
        short_name="CHIRPS3 precip",
        variable="precip",
        period_type="daily",
        units="mm",
        resolution="5 km x 5 km",
        source="CHIRPS v3",
        source_url="https://example.com/chirps",
        extent=ArtifactCoverage(
            temporal=CoverageTemporal(start="2026-01-01", end="2026-01-11"),
            spatial=CoverageSpatial(xmin=1.0, ymin=2.0, xmax=3.0, ymax=4.0),
        ),
        last_updated=datetime(2026, 1, 11, tzinfo=UTC),
        links=[],
        publication=DatasetPublication(
            status=PublicationStatus.PUBLISHED,
            published_at=datetime(2026, 1, 11, tzinfo=UTC),
        ),
        versions=[],
    )


def test_sync_dataset_returns_up_to_date_when_no_new_period_is_due(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset_id = "chirps3_precipitation_daily_sle"
    monkeypatch.setattr(
        services,
        "get_latest_artifact_for_dataset_or_404",
        lambda _: _artifact(artifact_id="a1", managed_dataset_id=dataset_id, end="2026-01-31"),
    )
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {
            "id": "chirps3_precipitation_daily",
            "period_type": "daily",
            "sync": {"kind": "temporal"},
            "ingestion": {},
        },
    )
    monkeypatch.setattr(services, "get_dataset_or_404", lambda _: _dataset_detail(dataset_id))

    result = services.sync_dataset(dataset_id=dataset_id, end="2026-01-31", publish=True)

    assert result.sync_id is None
    assert result.status == "up_to_date"
    assert result.message == "Managed dataset is already current with upstream source state."
    assert result.dataset is not None
    assert result.dataset.dataset_id == dataset_id
    assert result.sync_detail is not None
    assert result.sync_detail.sync_kind == SyncKind.TEMPORAL
    assert result.sync_detail.action == SyncAction.NO_OP
    assert result.sync_detail.reason == "no_new_period"
    assert (
        result.sync_detail.message
        == "Data already exists through 2026-01-31; target 2026-01-31 does not require a new download."
    )


def test_sync_dataset_creates_new_version_from_next_period(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset_id = "chirps3_precipitation_daily_sle"
    latest = _artifact(artifact_id="a1", managed_dataset_id=dataset_id, end="2026-01-31")
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {"id": "chirps3_precipitation_daily", "period_type": "daily", "sync": {"kind": "temporal"}},
    )

    captured: dict[str, object] = {}

    def fake_create_artifact(**kwargs: object) -> ArtifactRecord:
        captured.update(kwargs)
        return _artifact(artifact_id="a2", managed_dataset_id=dataset_id, end="2026-02-10")

    monkeypatch.setattr(services, "create_artifact", fake_create_artifact)
    monkeypatch.setattr(services, "get_dataset_or_404", lambda _: _dataset_detail(dataset_id))
    result = services.sync_dataset(dataset_id=dataset_id, end="2026-02-10", publish=True)

    assert captured["start"] == "2026-01-01"
    assert captured["end"] == "2026-02-10"
    assert captured["bbox"] == [1.0, 2.0, 3.0, 4.0]
    assert captured["country_code"] == "SLE"
    assert result.sync_id == "a2"
    assert result.status == "completed"
    assert result.message == "Managed dataset was rematerialized against the latest planned upstream state."
    assert result.sync_detail is not None
    assert result.sync_detail.sync_kind == SyncKind.TEMPORAL
    assert result.sync_detail.action == SyncAction.REMATERIALIZE
    assert result.sync_detail.reason == "new_periods_available"
    assert "Data exists through 2026-01-31" in result.sync_detail.message
    assert "Sync will rematerialize the dataset through 2026-02-10" in result.sync_detail.message
    assert result.sync_detail.current_start == "2026-01-01"
    assert result.sync_detail.current_end == "2026-01-31"
    assert result.sync_detail.target_end == "2026-02-10"
    assert result.sync_detail.target_end_source == "request"
    assert result.sync_detail.delta_start == "2026-02-01"
    assert result.sync_detail.delta_end == "2026-02-10"


def test_sync_dataset_append_policy_falls_back_to_rematerialize_without_icechunk_artifact(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    zarr_path = tmp_path / "worldpop_population_yearly.zarr"

    dataset_id = "worldpop_population_yearly_sle"
    latest = _artifact(
        artifact_id="a1",
        source_dataset_id="worldpop_population_yearly",
        managed_dataset_id=dataset_id,
        end="2024",
        path=str(zarr_path),
    )
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {
            "id": "worldpop_population_yearly",
            "period_type": "yearly",
            "sync": {"kind": "temporal", "execution": "append"},
            "ingestion": {},
        },
    )

    captured: dict[str, object] = {}

    def fake_create_artifact(**kwargs: object) -> ArtifactRecord:
        captured.update(kwargs)
        return _artifact(
            artifact_id="a2",
            source_dataset_id="worldpop_population_yearly",
            managed_dataset_id=dataset_id,
            end="2025",
        )

    messages: list[str] = []

    def fake_info(message: str, *args: object) -> None:
        messages.append(message % args if args else message)

    monkeypatch.setattr(services, "create_artifact", fake_create_artifact)
    monkeypatch.setattr(services, "get_dataset_or_404", lambda _: _dataset_detail(dataset_id))
    monkeypatch.setattr(sync_engine.logger, "info", fake_info)

    result = services.sync_dataset(dataset_id=dataset_id, end="2025", publish=True)

    assert "download_start" in captured
    assert captured["download_start"] is None
    assert result.sync_detail is not None
    assert result.sync_detail.action == SyncAction.REMATERIALIZE
    assert any("requires an existing Icechunk artifact" in message for message in messages)


def test_sync_dataset_append_policy_uses_store_based_append_for_plugin_backed_dataset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset_id = "chirps3_precipitation_daily_sle"
    latest = _artifact(
        artifact_id="a1",
        managed_dataset_id=dataset_id,
        end="2026-01-31",
        path="/tmp/chirps3_precipitation_daily.icechunk",
    )
    latest.format = ArtifactFormat.ICECHUNK
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {
            "id": "chirps3_precipitation_daily",
            "period_type": "daily",
            "sync": {"kind": "temporal", "execution": "append"},
            "ingestion": {"plugin": "open_climate_service.streaming.plugins.chirps3.CHIRPS3DailyPlugin"},
        },
    )

    captured: dict[str, object] = {}

    def fake_create_artifact(**kwargs: object) -> ArtifactRecord:
        captured.update(kwargs)
        return _artifact(artifact_id="a2", managed_dataset_id=dataset_id, end="2026-02-10")

    monkeypatch.setattr(services, "create_artifact", fake_create_artifact)
    monkeypatch.setattr(services, "get_dataset_or_404", lambda _: _dataset_detail(dataset_id))

    result = services.sync_dataset(dataset_id=dataset_id, end="2026-02-10", publish=True)

    assert "download_start" in captured
    assert captured["download_start"] == "2026-02-01"
    assert captured["download_end"] == "2026-02-10"
    assert result.sync_detail is not None
    assert result.sync_detail.action == SyncAction.APPEND
    assert result.sync_detail.reason == "new_periods_available_for_append"
    assert "Data exists through 2026-01-31" in result.sync_detail.message
    assert "Sync will append missing periods 2026-02-01 through 2026-02-10" in result.sync_detail.message
    assert "extend coverage through 2026-02-10" in result.sync_detail.message
    assert result.message is not None
    assert "appending missing periods" in result.message
    assert "committed store" in result.message


def test_plan_sync_for_plugin_backed_icechunk_uses_committed_store_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    latest = _artifact(
        artifact_id="a1",
        managed_dataset_id="chirps3_precipitation_daily_sle",
        end="2026-01-31",
        path="/tmp/chirps3_precipitation_daily.icechunk",
    )
    latest.format = ArtifactFormat.ICECHUNK
    latest.coverage.temporal.end = "2026-01-15"

    monkeypatch.setattr(sync_engine, "_artifact_storage_roots", lambda: (Path("/tmp").resolve(),))
    monkeypatch.setattr(sync_engine, "read_committed_period_ids", lambda *args, **kwargs: {"2026-01-31"})

    result = sync_engine.plan_sync(
        source_dataset={
            "id": "chirps3_precipitation_daily",
            "period_type": "daily",
            "sync": {"kind": "temporal", "execution": "append"},
            "ingestion": {"plugin": "open_climate_service.streaming.plugins.chirps3.CHIRPS3DailyPlugin"},
        },
        latest_artifact=latest,
        requested_end="2026-01-31",
    )

    assert result.action == SyncAction.NO_OP
    assert result.reason == "no_new_period"
    assert result.current_end == "2026-01-31"


def test_plan_sync_for_plugin_backed_icechunk_normalizes_committed_period_before_returning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    latest = _artifact(
        artifact_id="a1",
        managed_dataset_id="chirps3_precipitation_daily_sle",
        end="2026-04-21T13",
        path="/tmp/chirps3_precipitation_daily.icechunk",
    )
    latest.format = ArtifactFormat.ICECHUNK
    latest.coverage.temporal.end = "2026-04-21T13"

    monkeypatch.setattr(sync_engine, "_artifact_storage_roots", lambda: (Path("/tmp").resolve(),))
    monkeypatch.setattr(
        sync_engine,
        "read_committed_period_ids",
        lambda *args, **kwargs: {"2026-04-21T13:27:45"},
    )

    result = sync_engine.plan_sync(
        source_dataset={
            "id": "era5land_temperature_hourly",
            "period_type": "hourly",
            "sync": {"kind": "temporal", "execution": "append"},
            "ingestion": {"plugin": "example.HourlyPlugin"},
        },
        latest_artifact=latest,
        requested_end="2026-04-21T13",
    )

    assert result.action == SyncAction.NO_OP
    assert result.reason == "no_new_period"
    assert result.current_end == "2026-04-21T13"


def test_plan_sync_for_plugin_backed_icechunk_falls_back_to_artifact_end_without_store_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    latest = _artifact(
        artifact_id="a1",
        managed_dataset_id="chirps3_precipitation_daily_sle",
        end="2026-01-15",
    )
    latest.format = ArtifactFormat.ICECHUNK
    latest.path = None
    latest.asset_paths = []

    read_calls: list[tuple[object, ...]] = []

    def fake_read_committed_period_ids(*args: object, **kwargs: object) -> set[str]:
        read_calls.append(args)
        return {"2026-01-31"}

    monkeypatch.setattr(sync_engine, "read_committed_period_ids", fake_read_committed_period_ids)

    result = sync_engine.plan_sync(
        source_dataset={
            "id": "chirps3_precipitation_daily",
            "period_type": "daily",
            "sync": {"kind": "temporal", "execution": "append"},
            "ingestion": {"plugin": "open_climate_service.streaming.plugins.chirps3.CHIRPS3DailyPlugin"},
        },
        latest_artifact=latest,
        requested_end="2026-01-31",
    )

    assert result.action == SyncAction.APPEND
    assert result.current_end == "2026-01-15"
    assert read_calls == []


def test_plan_sync_for_plugin_backed_icechunk_skips_non_local_store_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    latest = _artifact(
        artifact_id="a1",
        managed_dataset_id="chirps3_precipitation_daily_sle",
        end="2026-01-15",
        path="s3://bucket/chirps3_precipitation_daily.icechunk",
    )
    latest.format = ArtifactFormat.ICECHUNK

    read_calls: list[tuple[object, ...]] = []
    warnings: list[str] = []

    def fake_read_committed_period_ids(*args: object, **kwargs: object) -> set[str]:
        read_calls.append(args)
        return {"2026-01-31"}

    def fake_warning(message: str, *args: object) -> None:
        warnings.append(message % args if args else message)

    monkeypatch.setattr(sync_engine, "read_committed_period_ids", fake_read_committed_period_ids)
    monkeypatch.setattr(sync_engine.logger, "warning", fake_warning)

    result = sync_engine.plan_sync(
        source_dataset={
            "id": "chirps3_precipitation_daily",
            "period_type": "daily",
            "sync": {"kind": "temporal", "execution": "append"},
            "ingestion": {"plugin": "open_climate_service.streaming.plugins.chirps3.CHIRPS3DailyPlugin"},
        },
        latest_artifact=latest,
        requested_end="2026-01-31",
    )

    assert result.action == SyncAction.APPEND
    assert result.current_end == "2026-01-15"
    assert read_calls == []
    assert any("non-local URI" in message for message in warnings)


def test_plan_sync_for_plugin_backed_icechunk_falls_back_to_artifact_end_for_windows_drive_letter_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Windows drive-letter paths (C:\...) are detected explicitly before urlparse
    # can misread the drive letter as a URI scheme. On Linux the path is not
    # absolute, so _resolve_local_artifact_path returns "relative path" and
    # committed-store inspection falls back to artifact metadata.
    latest = _artifact(
        artifact_id="a1",
        managed_dataset_id="chirps3_precipitation_daily_sle",
        end="2026-01-15",
        path="C:\\data\\downloads\\chirps3_precipitation_daily.icechunk",
    )
    latest.format = ArtifactFormat.ICECHUNK

    read_calls: list[tuple[object, ...]] = []
    warnings: list[str] = []

    def fake_read_committed_period_ids(*args: object, **kwargs: object) -> set[str]:
        read_calls.append(args)
        return {"2026-01-31"}

    def fake_warning(message: str, *args: object) -> None:
        warnings.append(message % args if args else message)

    monkeypatch.setattr(sync_engine, "read_committed_period_ids", fake_read_committed_period_ids)
    monkeypatch.setattr(sync_engine.logger, "warning", fake_warning)

    result = sync_engine.plan_sync(
        source_dataset={
            "id": "chirps3_precipitation_daily",
            "period_type": "daily",
            "sync": {"kind": "temporal", "execution": "append"},
            "ingestion": {"plugin": "open_climate_service.streaming.plugins.chirps3.CHIRPS3DailyPlugin"},
        },
        latest_artifact=latest,
        requested_end="2026-01-31",
    )

    assert result.action == SyncAction.APPEND
    assert result.current_end == "2026-01-15"
    assert read_calls == []
    assert any("relative path" in message for message in warnings)


def test_plan_sync_for_plugin_backed_icechunk_falls_back_to_artifact_end_for_file_uri_with_windows_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # file:///C:/... URIs are valid on Windows but not within any trusted storage
    # root on this Linux service. urlparse produces path "/C:/..." which is
    # absolute on Linux but won't match any configured storage root, so
    # _resolve_local_artifact_path returns "untrusted local path" and planning
    # falls back to artifact metadata.
    latest = _artifact(
        artifact_id="a1",
        managed_dataset_id="chirps3_precipitation_daily_sle",
        end="2026-01-15",
        path="file:///C:/data/downloads/chirps3_precipitation_daily.icechunk",
    )
    latest.format = ArtifactFormat.ICECHUNK

    read_calls: list[tuple[object, ...]] = []
    warnings: list[str] = []

    def fake_read_committed_period_ids(*args: object, **kwargs: object) -> set[str]:
        read_calls.append(args)
        return {"2026-01-31"}

    def fake_warning(message: str, *args: object) -> None:
        warnings.append(message % args if args else message)

    monkeypatch.setattr(sync_engine, "read_committed_period_ids", fake_read_committed_period_ids)
    monkeypatch.setattr(sync_engine.logger, "warning", fake_warning)

    result = sync_engine.plan_sync(
        source_dataset={
            "id": "chirps3_precipitation_daily",
            "period_type": "daily",
            "sync": {"kind": "temporal", "execution": "append"},
            "ingestion": {"plugin": "open_climate_service.streaming.plugins.chirps3.CHIRPS3DailyPlugin"},
        },
        latest_artifact=latest,
        requested_end="2026-01-31",
    )

    assert result.action == SyncAction.APPEND
    assert result.current_end == "2026-01-15"
    assert read_calls == []
    assert any("untrusted local path" in message for message in warnings)


def test_plan_sync_for_plugin_backed_icechunk_falls_back_to_artifact_end_when_store_read_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    latest = _artifact(
        artifact_id="a1",
        managed_dataset_id="chirps3_precipitation_daily_sle",
        end="2026-01-15",
        path="/tmp/chirps3_precipitation_daily.icechunk",
    )
    latest.format = ArtifactFormat.ICECHUNK

    warnings: list[str] = []
    monkeypatch.setattr(sync_engine, "_artifact_storage_roots", lambda: (Path("/tmp").resolve(),))

    def fake_warning(message: str, *args: object) -> None:
        warnings.append(message % args if args else message)

    monkeypatch.setattr(
        sync_engine,
        "read_committed_period_ids",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(sync_engine.logger, "warning", fake_warning)

    result = sync_engine.plan_sync(
        source_dataset={
            "id": "chirps3_precipitation_daily",
            "period_type": "daily",
            "sync": {"kind": "temporal", "execution": "append"},
            "ingestion": {"plugin": "open_climate_service.streaming.plugins.chirps3.CHIRPS3DailyPlugin"},
        },
        latest_artifact=latest,
        requested_end="2026-01-31",
    )

    assert result.action == SyncAction.APPEND
    assert result.current_end == "2026-01-15"
    assert any("falling back to artifact metadata" in message for message in warnings)


def test_plan_sync_for_plugin_backed_icechunk_falls_back_to_artifact_end_when_committed_set_is_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    latest = _artifact(
        artifact_id="a1",
        managed_dataset_id="chirps3_precipitation_daily_sle",
        end="2026-01-15",
        path="/tmp/chirps3_precipitation_daily.icechunk",
    )
    latest.format = ArtifactFormat.ICECHUNK

    monkeypatch.setattr(sync_engine, "_artifact_storage_roots", lambda: (Path("/tmp").resolve(),))
    monkeypatch.setattr(sync_engine, "read_committed_period_ids", lambda *args, **kwargs: set())

    result = sync_engine.plan_sync(
        source_dataset={
            "id": "chirps3_precipitation_daily",
            "period_type": "daily",
            "sync": {"kind": "temporal", "execution": "append"},
            "ingestion": {"plugin": "open_climate_service.streaming.plugins.chirps3.CHIRPS3DailyPlugin"},
        },
        latest_artifact=latest,
        requested_end="2026-01-31",
    )

    assert result.action == SyncAction.APPEND
    assert result.current_end == "2026-01-15"


def test_plan_sync_for_plugin_backed_icechunk_falls_back_to_artifact_end_for_untrusted_local_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    latest = _artifact(
        artifact_id="a1",
        managed_dataset_id="chirps3_precipitation_daily_sle",
        end="2026-01-15",
        path="/tmp/chirps3_precipitation_daily.icechunk",
    )
    latest.format = ArtifactFormat.ICECHUNK

    warnings: list[str] = []

    def fake_warning(message: str, *args: object) -> None:
        warnings.append(message % args if args else message)

    monkeypatch.setattr(sync_engine.logger, "warning", fake_warning)
    monkeypatch.setattr(sync_engine, "_artifact_storage_roots", lambda: (Path("/srv/app/data/downloads"),))

    result = sync_engine.plan_sync(
        source_dataset={
            "id": "chirps3_precipitation_daily",
            "period_type": "daily",
            "sync": {"kind": "temporal", "execution": "append"},
            "ingestion": {"plugin": "open_climate_service.streaming.plugins.chirps3.CHIRPS3DailyPlugin"},
        },
        latest_artifact=latest,
        requested_end="2026-01-31",
    )

    assert result.action == SyncAction.APPEND
    assert result.current_end == "2026-01-15"
    assert any("untrusted local path" in message for message in warnings)


def test_plan_sync_for_plugin_backed_icechunk_falls_back_to_artifact_end_for_relative_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    latest = _artifact(
        artifact_id="a1",
        managed_dataset_id="chirps3_precipitation_daily_sle",
        end="2026-01-15",
        path="data/downloads/chirps3_precipitation_daily.icechunk",
    )
    latest.format = ArtifactFormat.ICECHUNK

    warnings: list[str] = []

    def fake_warning(message: str, *args: object) -> None:
        warnings.append(message % args if args else message)

    monkeypatch.setattr(sync_engine.logger, "warning", fake_warning)

    result = sync_engine.plan_sync(
        source_dataset={
            "id": "chirps3_precipitation_daily",
            "period_type": "daily",
            "sync": {"kind": "temporal", "execution": "append"},
            "ingestion": {"plugin": "open_climate_service.streaming.plugins.chirps3.CHIRPS3DailyPlugin"},
        },
        latest_artifact=latest,
        requested_end="2026-01-31",
    )

    assert result.action == SyncAction.APPEND
    assert result.current_end == "2026-01-15"
    assert any("relative path" in message for message in warnings)


def test_plan_sync_for_plugin_backed_icechunk_falls_back_to_artifact_end_when_committed_period_is_malformed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    latest = _artifact(
        artifact_id="a1",
        managed_dataset_id="chirps3_precipitation_daily_sle",
        end="2026-01-15",
        path="/tmp/chirps3_precipitation_daily.icechunk",
    )
    latest.format = ArtifactFormat.ICECHUNK

    warnings: list[str] = []

    def fake_warning(message: str, *args: object) -> None:
        warnings.append(message % args if args else message)

    monkeypatch.setattr(sync_engine, "_artifact_storage_roots", lambda: (Path("/tmp").resolve(),))
    monkeypatch.setattr(sync_engine, "read_committed_period_ids", lambda *args, **kwargs: {"not-a-period"})
    monkeypatch.setattr(sync_engine.logger, "warning", fake_warning)

    result = sync_engine.plan_sync(
        source_dataset={
            "id": "chirps3_precipitation_daily",
            "period_type": "daily",
            "sync": {"kind": "temporal", "execution": "append"},
            "ingestion": {"plugin": "open_climate_service.streaming.plugins.chirps3.CHIRPS3DailyPlugin"},
        },
        latest_artifact=latest,
        requested_end="2026-01-31",
    )

    assert result.action == SyncAction.APPEND
    assert result.current_end == "2026-01-15"
    assert any("malformed committed periods" in message for message in warnings)


def test_sync_dataset_append_policy_falls_back_for_plugin_backed_non_icechunk_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset_id = "chirps3_precipitation_daily_sle"
    latest = _artifact(
        artifact_id="a1",
        managed_dataset_id=dataset_id,
        end="2026-01-31",
        path="/tmp/chirps3_precipitation_daily.zarr",
    )
    latest.format = ArtifactFormat.ZARR
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {
            "id": "chirps3_precipitation_daily",
            "period_type": "daily",
            "sync": {"kind": "temporal", "execution": "append"},
            "ingestion": {"plugin": "open_climate_service.streaming.plugins.chirps3.CHIRPS3DailyPlugin"},
        },
    )

    captured: dict[str, object] = {}
    infos: list[str] = []

    def fake_create_artifact(**kwargs: object) -> ArtifactRecord:
        captured.update(kwargs)
        return _artifact(artifact_id="a2", managed_dataset_id=dataset_id, end="2026-02-10")

    def fake_info(message: str, *args: object) -> None:
        infos.append(message % args if args else message)

    monkeypatch.setattr(services, "create_artifact", fake_create_artifact)
    monkeypatch.setattr(services, "get_dataset_or_404", lambda _: _dataset_detail(dataset_id))
    monkeypatch.setattr(sync_engine.logger, "info", fake_info)

    result = services.sync_dataset(dataset_id=dataset_id, end="2026-02-10", publish=True)

    assert "download_start" in captured
    assert captured["download_start"] is None
    assert captured["download_end"] is None
    assert result.sync_detail is not None
    assert result.sync_detail.action == SyncAction.REMATERIALIZE
    assert result.sync_detail.reason == "new_periods_available"
    assert any("requires an existing Icechunk artifact" in message for message in infos)


def test_sync_dataset_release_policy_returns_up_to_date_when_release_matches(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset_id = "worldpop_population_yearly_sle"
    latest = _artifact(
        artifact_id="a1",
        source_dataset_id="worldpop_population_yearly",
        managed_dataset_id=dataset_id,
        end="2024",
    )
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {"id": "worldpop_population_yearly", "period_type": "yearly", "sync": {"kind": "release"}},
    )
    monkeypatch.setattr(services, "get_dataset_or_404", lambda _: _dataset_detail(dataset_id))

    result = services.sync_dataset(dataset_id=dataset_id, end="2024", publish=True)

    assert result.sync_id is None
    assert result.status == "up_to_date"
    assert result.sync_detail is not None
    assert result.sync_detail.sync_kind == SyncKind.RELEASE
    assert result.sync_detail.action == SyncAction.NO_OP
    assert result.sync_detail.reason == "no_new_release"


def test_sync_dataset_release_policy_clamps_future_year_by_template_availability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset_id = "release_dataset_sle"
    latest = _artifact(
        artifact_id="a1",
        source_dataset_id="release_dataset_yearly",
        managed_dataset_id=dataset_id,
        end="2024",
    )
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        sync_engine.provider_availability,
        "utc_today",
        lambda: date(2026, 4, 15),
    )
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {
            "id": "release_dataset_yearly",
            "period_type": "yearly",
            "sync": {"kind": "release", "availability": {"latest_year_offset": 1}},
        },
    )

    captured: dict[str, object] = {}

    def fake_create_artifact(**kwargs: object) -> ArtifactRecord:
        captured.update(kwargs)
        return _artifact(
            artifact_id="a2",
            source_dataset_id="release_dataset_yearly",
            managed_dataset_id=dataset_id,
            end="2025",
        )

    monkeypatch.setattr(services, "create_artifact", fake_create_artifact)
    monkeypatch.setattr(services, "get_dataset_or_404", lambda _: _dataset_detail(dataset_id))

    result = services.sync_dataset(dataset_id=dataset_id, end="2026", publish=True)

    assert captured["start"] == "2026-01-01"
    assert captured["end"] == "2025"
    assert result.status == "completed"
    assert result.sync_detail is not None
    assert result.sync_detail.sync_kind == SyncKind.RELEASE
    assert result.sync_detail.action == SyncAction.REMATERIALIZE
    assert result.sync_detail.target_end == "2025"
    assert result.sync_detail.target_end_source == "request_clamped_by_availability"
    assert result.sync_detail.delta_start is None
    assert result.sync_detail.delta_end is None


def test_default_hourly_target_end_is_utc_aware(monkeypatch: pytest.MonkeyPatch) -> None:
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> "FixedDateTime":
            return cls(2026, 4, 21, 13, 47, 31, tzinfo=tz if tz is UTC else None)

    monkeypatch.setattr(sync_engine, "utc_now", lambda: FixedDateTime(2026, 4, 21, 13, 47, 31, tzinfo=UTC))

    result = sync_engine._default_target_end(period_type="hourly")

    assert result == "2026-04-21T13"


def test_default_target_end_rejects_unsupported_period_type() -> None:
    with pytest.raises(ValueError, match="Unsupported period_type 'fortnightly' for sync"):
        sync_engine._default_target_end(period_type="fortnightly")


def test_next_period_start_preserves_hourly_period_format() -> None:
    result = sync_engine._next_period_start("2026-04-21T13", period_type="hourly")

    assert result == "2026-04-21T14"


def test_default_weekly_target_end_uses_iso_week_format(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sync_engine, "utc_now", lambda: datetime(2026, 4, 21, 13, 47, 31, tzinfo=UTC))

    result = sync_engine._default_target_end(period_type="weekly")

    assert result == "2026-W17"


def test_next_period_start_preserves_weekly_period_format() -> None:
    result = sync_engine._next_period_start("2026-W17", period_type="weekly")

    assert result == "2026-W18"


def test_next_period_start_rolls_weekly_period_across_iso_year_boundary() -> None:
    result = sync_engine._next_period_start("2020-W53", period_type="weekly")

    assert result == "2021-W01"


def test_sync_dataset_static_policy_returns_not_syncable_without_period_arithmetic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset_id = "static_dataset_sle"
    latest = _artifact(
        artifact_id="a1",
        source_dataset_id="static_dataset",
        managed_dataset_id=dataset_id,
        end="static-release",
    )
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {"id": "static_dataset", "period_type": "unsupported-static-period", "sync": {"kind": "static"}},
    )
    monkeypatch.setattr(services, "create_artifact", lambda **_: pytest.fail("static sync should not create artifacts"))
    monkeypatch.setattr(services, "get_dataset_or_404", lambda _: _dataset_detail(dataset_id))

    result = services.sync_dataset(dataset_id=dataset_id, end="ignored", publish=True)

    assert result.sync_id is None
    assert result.status == "not_syncable"
    assert result.sync_detail is not None
    assert result.sync_detail.sync_kind == SyncKind.STATIC
    assert result.sync_detail.action == SyncAction.NOT_SYNCABLE
    assert result.sync_detail.reason == "static_dataset"


def test_plan_sync_requires_sync_kind() -> None:
    latest = _artifact(artifact_id="a1", end="2026-01-31")

    with pytest.raises(ValueError, match="must define sync.kind"):
        sync_engine.plan_sync(
            source_dataset={"id": "chirps3_precipitation_daily", "period_type": "daily"},
            latest_artifact=latest,
            requested_end="2026-02-10",
        )


def test_plan_sync_static_policy_ignores_period_normalization() -> None:
    latest = _artifact(
        artifact_id="a1",
        source_dataset_id="static_dataset",
        managed_dataset_id="static_dataset_sle",
        end="static-release",
    )

    result = sync_engine.plan_sync(
        source_dataset={"id": "static_dataset", "period_type": "unsupported-static-period", "sync": {"kind": "static"}},
        latest_artifact=latest,
        requested_end=None,
    )

    assert result.sync_kind == SyncKind.STATIC
    assert result.action == SyncAction.NOT_SYNCABLE
    assert result.reason == "static_dataset"


def test_plan_sync_dataset_returns_plan_without_creating_artifact(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset_id = "chirps3_precipitation_daily_sle"
    latest = _artifact(artifact_id="a1", managed_dataset_id=dataset_id, end="2026-01-31")
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {"id": "chirps3_precipitation_daily", "period_type": "daily", "sync": {"kind": "temporal"}},
    )
    monkeypatch.setattr(services, "create_artifact", lambda **_: pytest.fail("sync plan should not create artifacts"))

    result = services.plan_sync_dataset(dataset_id=dataset_id, end="2026-02-10")

    assert result.sync_kind == SyncKind.TEMPORAL
    assert result.action == SyncAction.REMATERIALIZE
    assert result.reason == "new_periods_available"
    assert result.current_start == "2026-01-01"
    assert result.current_end == "2026-01-31"
    assert result.target_end == "2026-02-10"
    assert result.target_end_source == "request"
    assert result.delta_start == "2026-02-01"
    assert result.delta_end == "2026-02-10"


def test_sync_plan_route_returns_plan_without_creating_artifact(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset_id = "chirps3_precipitation_daily_sle"
    latest = _artifact(artifact_id="a1", managed_dataset_id=dataset_id, end="2026-01-31")
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {"id": "chirps3_precipitation_daily", "period_type": "daily", "sync": {"kind": "temporal"}},
    )
    monkeypatch.setattr(services, "create_artifact", lambda **_: pytest.fail("sync plan should not create artifacts"))

    response = client.get(f"/sync/{dataset_id}/plan", params={"end": "2026-02-10"})

    assert response.status_code == 200
    assert response.json() == {
        "source_dataset_id": "chirps3_precipitation_daily",
        "sync_kind": "temporal",
        "action": "rematerialize",
        "reason": "new_periods_available",
        "message": "Data exists through 2026-01-31. Sync will rematerialize the dataset through 2026-02-10.",
        "current_start": "2026-01-01",
        "current_end": "2026-01-31",
        "target_end": "2026-02-10",
        "target_end_source": "request",
        "delta_start": "2026-02-01",
        "delta_end": "2026-02-10",
    }


def test_sync_plan_route_returns_400_for_invalid_end_period(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset_id = "chirps3_precipitation_daily_sle"
    latest = _artifact(artifact_id="a1", managed_dataset_id=dataset_id, end="2026-01-31")
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {"id": "chirps3_precipitation_daily", "period_type": "daily", "sync": {"kind": "temporal"}},
    )

    response = client.get(f"/sync/{dataset_id}/plan", params={"end": "not-a-period"})

    assert response.status_code == 400


def test_plan_sync_treats_blank_end_as_default_target(monkeypatch: pytest.MonkeyPatch) -> None:
    class FixedDate(date):
        @classmethod
        def today(cls) -> "FixedDate":
            return cls(2026, 4, 20)

    monkeypatch.setattr(sync_engine, "utc_today", lambda: FixedDate(2026, 4, 20))

    result = sync_engine.plan_sync(
        source_dataset={
            "id": "chirps3_precipitation_daily",
            "period_type": "daily",
            "sync": {"kind": "temporal", "execution": "append"},
            "ingestion": {},
        },
        latest_artifact=_artifact(artifact_id="a1", end="2024-02-29"),
        requested_end="",
    )

    assert result.target_end == "2026-04-20"
    assert result.target_end_source == "default_today"


def test_sync_route_executes_rematerialize_and_returns_structured_detail(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset_id = "chirps3_precipitation_daily_sle"
    latest = _artifact(artifact_id="a1", managed_dataset_id=dataset_id, end="2026-01-31")
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {"id": "chirps3_precipitation_daily", "period_type": "daily", "sync": {"kind": "temporal"}},
    )
    monkeypatch.setattr(
        services,
        "create_artifact",
        lambda **_: _artifact(artifact_id="a2", managed_dataset_id=dataset_id, end="2026-02-10"),
    )
    monkeypatch.setattr(services, "get_dataset_or_404", lambda _: _dataset_detail(dataset_id))

    response = client.post(
        f"/sync/{dataset_id}",
        json={"end": "2026-02-10", "publish": True},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["sync_id"] == "a2"
    assert payload["status"] == "completed"
    assert payload["dataset"]["dataset_id"] == dataset_id
    assert payload["sync_detail"]["sync_kind"] == "temporal"
    assert payload["sync_detail"]["action"] == "rematerialize"
    assert payload["sync_detail"]["target_end"] == "2026-02-10"


def test_latest_available_end_preserves_requested_month_without_lag(monkeypatch: pytest.MonkeyPatch) -> None:
    class FixedDate(date):
        @classmethod
        def today(cls) -> "FixedDate":
            return cls(2026, 4, 15)

    monkeypatch.setattr(sync_engine.provider_availability, "utc_today", lambda: FixedDate(2026, 4, 15))

    result = sync_engine._latest_available_end(
        source_dataset={
            "id": "monthly_dataset",
            "period_type": "monthly",
            "sync": {"availability": {"lag_days": 0}},
        },
        requested_end="2026-05",
    )

    assert result == "2026-05"


def test_plan_sync_marks_default_target_end_source(monkeypatch: pytest.MonkeyPatch) -> None:
    class FixedDate(date):
        @classmethod
        def today(cls) -> "FixedDate":
            return cls(2026, 4, 20)

    monkeypatch.setattr(sync_engine, "utc_today", lambda: FixedDate(2026, 4, 20))

    result = sync_engine.plan_sync(
        source_dataset={
            "id": "chirps3_precipitation_daily",
            "period_type": "daily",
            "sync": {"kind": "temporal", "execution": "append"},
            "ingestion": {},
        },
        latest_artifact=_artifact(artifact_id="a1", end="2024-02-29"),
        requested_end=None,
    )

    assert result.target_end == "2026-04-20"
    assert result.target_end_source == "default_today"
    assert result.delta_start == "2024-03-01"
    assert result.delta_end == "2026-04-20"


def test_plan_sync_marks_request_target_clamped_by_availability(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sync_engine, "_get_dynamic_function", lambda _: lambda: "2026-03-31")

    result = sync_engine.plan_sync(
        source_dataset={
            "id": "chirps3_precipitation_daily",
            "period_type": "daily",
            "sync": {
                "kind": "temporal",
                "execution": "append",
                "availability": {
                    "latest_available_function": (
                        "open_climate_service.providers.availability.chirps3_daily_latest_available"
                    )
                },
            },
            "ingestion": {},
        },
        latest_artifact=_artifact(artifact_id="a1", end="2026-02-28"),
        requested_end="2026-04-21",
    )

    assert result.target_end == "2026-03-31"
    assert result.target_end_source == "request_clamped_by_availability"
    assert result.delta_start == "2026-03-01"
    assert result.delta_end == "2026-03-31"


def test_latest_available_end_clamps_monthly_lag_to_month_period(monkeypatch: pytest.MonkeyPatch) -> None:
    class FixedDate(date):
        @classmethod
        def today(cls) -> "FixedDate":
            return cls(2026, 4, 15)

    monkeypatch.setattr(sync_engine.provider_availability, "utc_today", lambda: FixedDate(2026, 4, 15))

    result = sync_engine._latest_available_end(
        source_dataset={
            "id": "monthly_dataset",
            "period_type": "monthly",
            "sync": {"availability": {"lag_days": 1}},
        },
        requested_end="2026-05",
    )

    assert result == "2026-04"


def test_latest_available_end_uses_provider_availability_hook(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_latest_available(*, dataset: dict[str, object], requested_end: str) -> str:
        calls.append({"dataset": dataset, "requested_end": requested_end})
        return "2026-02-05"

    monkeypatch.setattr(sync_engine, "_get_dynamic_function", lambda _: fake_latest_available)

    source_dataset = {
        "id": "provider_dataset",
        "period_type": "daily",
        "sync": {"availability": {"latest_available_function": "provider.latest_available"}},
    }
    result = sync_engine._latest_available_end(source_dataset=source_dataset, requested_end="2026-02-10")

    assert result == "2026-02-05"
    assert calls == [{"dataset": source_dataset, "requested_end": "2026-02-10"}]


def test_latest_available_end_clamps_provider_availability_to_requested_end(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sync_engine, "_get_dynamic_function", lambda _: lambda: "2026-03-01")

    result = sync_engine._latest_available_end(
        source_dataset={
            "id": "provider_dataset",
            "period_type": "daily",
            "sync": {"availability": {"latest_available_function": "provider.latest_available"}},
        },
        requested_end="2026-02-10",
    )

    assert result == "2026-02-10"


def test_latest_available_end_wraps_provider_import_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_import(_: str) -> object:
        raise ImportError("missing provider")

    monkeypatch.setattr(sync_engine, "_get_dynamic_function", fail_import)

    with pytest.raises(
        sync_engine.SyncConfigurationError,
        match="Latest availability function 'provider.latest_available' failed",
    ):
        sync_engine._latest_available_end(
            source_dataset={
                "id": "provider_dataset",
                "period_type": "daily",
                "sync": {"availability": {"latest_available_function": "provider.latest_available"}},
            },
            requested_end="2026-02-10",
        )


def test_latest_available_end_rejects_invalid_provider_period_string(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sync_engine, "_get_dynamic_function", lambda _: lambda: "2026-31-99")

    with pytest.raises(
        sync_engine.SyncConfigurationError,
        match="Latest availability function 'provider.latest_available' returned invalid period",
    ):
        sync_engine._latest_available_end(
            source_dataset={
                "id": "provider_dataset",
                "period_type": "daily",
                "sync": {"availability": {"latest_available_function": "provider.latest_available"}},
            },
            requested_end="2026-02-10",
        )


def test_latest_available_end_wraps_invalid_provider_function_path(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(sync_engine.SyncConfigurationError, match="Latest availability function 'invalid_path' failed"):
        sync_engine._latest_available_end(
            source_dataset={
                "id": "provider_dataset",
                "period_type": "daily",
                "sync": {"availability": {"latest_available_function": "invalid_path"}},
            },
            requested_end="2026-02-10",
        )


def test_sync_plan_route_returns_500_for_provider_hook_misconfiguration(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset_id = "chirps3_precipitation_daily_sle"
    latest = _artifact(artifact_id="a1", managed_dataset_id=dataset_id, end="2026-01-31")
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {
            "id": "chirps3_precipitation_daily",
            "period_type": "daily",
            "sync": {"kind": "temporal", "availability": {"latest_available_function": "provider.latest_available"}},
        },
    )

    def fail_import(_: str) -> object:
        raise ImportError("missing provider")

    monkeypatch.setattr(sync_engine, "_get_dynamic_function", fail_import)

    response = client.get(f"/sync/{dataset_id}/plan", params={"end": "2026-02-10"})

    assert response.status_code == 500
    assert "Latest availability function 'provider.latest_available' failed" in response.json()["detail"]


def test_run_sync_raises_clear_error_when_append_invariants_are_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    latest_artifact = _artifact(
        artifact_id="a1",
        source_dataset_id="chirps3_precipitation_daily",
        managed_dataset_id="chirps3_precipitation_daily_sle",
        end="2026-02-10",
    )

    broken_plan = SyncDetail(
        source_dataset_id="chirps3_precipitation_daily",
        sync_kind=SyncKind.TEMPORAL,
        action=SyncAction.APPEND,
        reason="new_periods_available_for_append",
        message="broken test plan",
        current_start=None,
        current_end="2026-02-10",
        target_end="2026-02-11",
        target_end_source="request",
        delta_start="2026-02-11",
        delta_end="2026-02-11",
    )
    monkeypatch.setattr(sync_engine, "plan_sync", lambda **_: broken_plan)

    with pytest.raises(ValueError, match="Sync execution requires current_start"):
        sync_engine.run_sync(
            latest_artifact=latest_artifact,
            source_dataset={"id": "chirps3_precipitation_daily", "period_type": "daily", "sync": {"kind": "temporal"}},
            requested_end="2026-02-11",
            country_code=None,
            publish=True,
            create_artifact_fn=lambda **_: pytest.fail("create_artifact should not be called"),
            get_dataset_fn=lambda _: pytest.fail("get_dataset should not be called"),
        )


def test_sync_dataset_forwards_country_code_from_extent(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset_id = "worldpop_population_yearly_sle"
    latest = _artifact(
        artifact_id="a1",
        source_dataset_id="worldpop_population_yearly",
        managed_dataset_id=dataset_id,
        end="2020",
    )
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {"id": "worldpop_population_yearly", "period_type": "yearly", "sync": {"kind": "release"}},
    )
    monkeypatch.setattr(
        services,
        "get_extent",
        lambda: {"id": "sle", "bbox": [-13.5, 6.9, -10.1, 10.0], "country_code": "SLE"},
    )

    captured: dict[str, object] = {}

    def fake_run_sync(**kwargs: object) -> SyncResponse:
        captured.update(kwargs)
        return SyncResponse(
            sync_id="a2",
            status="completed",
            message="ok",
            dataset=_dataset_detail(dataset_id),
            sync_detail=SyncDetail(
                source_dataset_id="worldpop_population_yearly",
                sync_kind=SyncKind.RELEASE,
                action=SyncAction.REMATERIALIZE,
                reason="new_release_available",
                message="ok",
                current_start="2020",
                current_end="2020",
                target_end="2021",
                target_end_source="request",
            ),
        )

    monkeypatch.setattr(services, "run_sync", fake_run_sync)

    services.sync_dataset(dataset_id=dataset_id, end="2021", publish=True)

    assert captured["country_code"] == "SLE"
