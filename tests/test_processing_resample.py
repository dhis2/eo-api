from datetime import UTC, datetime
from pathlib import Path

import icechunk
import numpy as np
import pytest
import xarray as xr

from open_climate_service.ingestions import services as ingestion_services
from open_climate_service.ingestions.schemas import (
    ArtifactCoverage,
    ArtifactFormat,
    ArtifactPublication,
    ArtifactRecord,
    ArtifactRequestScope,
    CoverageSpatial,
    CoverageTemporal,
    PublicationStatus,
)
from open_climate_service.processing import resample


@pytest.fixture(autouse=True)
def isolated_artifact_store(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    monkeypatch.setattr(ingestion_services, "ARTIFACTS_DIR", artifacts_dir)
    monkeypatch.setattr(ingestion_services, "ARTIFACTS_INDEX_PATH", artifacts_dir / "records.json")
    monkeypatch.setattr(resample, "DERIVED_DATA_DIR", tmp_path / "derived")


def _artifact(
    *,
    artifact_id: str,
    dataset_id: str,
    managed_dataset_id: str,
    path: Path,
    start: str,
    end: str,
) -> ArtifactRecord:
    return ArtifactRecord(
        artifact_id=artifact_id,
        dataset_id=dataset_id,
        dataset_name=dataset_id,
        variable="value",
        format=ArtifactFormat.ZARR,
        path=str(path),
        asset_paths=[str(path)],
        variables=["value"],
        request_scope=ArtifactRequestScope(
            start=start,
            end=end,
            bbox=(1.0, 2.0, 3.0, 4.0),
        ),
        coverage=ArtifactCoverage(
            temporal=CoverageTemporal(start=start, end=end),
            spatial=CoverageSpatial(xmin=1.0, ymin=2.0, xmax=3.0, ymax=4.0),
        ),
        created_at=datetime(2026, 1, 10, tzinfo=UTC),
        publication=ArtifactPublication(
            status=PublicationStatus.PUBLISHED,
            collection_id=managed_dataset_id,
            pygeoapi_path=f"/ogcapi/collections/{managed_dataset_id}",
        ),
    )


def test_materialize_resampled_artifact_builds_daily_dataset_from_hourly_source(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source_hourly.zarr"
    time = np.array("2026-01-01T00", dtype="datetime64[h]") + np.arange(48)
    ds = xr.Dataset(
        {"value": (("time", "lat", "lon"), np.arange(48, dtype=float).reshape(48, 1, 1))},
        coords={"time": time, "lat": [2.0], "lon": [1.0]},
    )
    ds.to_zarr(source_path, mode="w", consolidated=True)

    source_artifact = _artifact(
        artifact_id="source-hourly",
        dataset_id="era5land_temperature_hourly",
        managed_dataset_id="era5land_temperature_hourly_sle",
        path=source_path,
        start="2026-01-01T00",
        end="2026-01-02T23",
    )

    monkeypatch.setattr(
        resample.registry_datasets,
        "get_dataset",
        lambda dataset_id: (
            {"id": dataset_id, "period_type": "hourly"} if dataset_id == "era5land_temperature_hourly" else None
        ),
    )
    monkeypatch.setattr(
        resample.ingestion_services,
        "get_latest_artifact_for_dataset_or_404",
        lambda _: source_artifact,
    )

    artifact = resample.materialize_resampled_artifact(
        source_dataset_id="era5land_temperature_hourly",
        frequency="1D",
        method="mean",
        start="2026-01-01",
        end="2026-01-02",
        overwrite=False,
        publish=False,
    )

    assert artifact.dataset_id == "era5land_temperature_hourly_1d_mean"
    assert artifact.coverage.temporal.start == "2026-01-01"
    assert artifact.coverage.temporal.end == "2026-01-02"
    result = xr.open_zarr(artifact.path, consolidated=True)
    try:
        assert result["value"].shape == (2, 1, 1)
        assert result["value"].values[:, 0, 0].tolist() == [11.5, 35.5]
    finally:
        result.close()


def test_materialize_resampled_artifact_reads_icechunk_backed_source(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source_hourly.icechunk"
    storage = icechunk.local_filesystem_storage(str(source_path))
    repo = icechunk.Repository.create(storage)
    session = repo.writable_session("main")
    time = np.array("2026-01-01T00", dtype="datetime64[h]") + np.arange(24)
    ds = xr.Dataset(
        {"value": (("time", "lat", "lon"), np.arange(24, dtype=float).reshape(24, 1, 1))},
        coords={"time": time, "lat": [2.0], "lon": [1.0]},
    )
    ds.to_zarr(session.store, mode="w", zarr_format=3)
    session.commit("seed hourly source")
    ds.close()

    source_artifact = _artifact(
        artifact_id="source-hourly-icechunk",
        dataset_id="era5land_temperature_hourly",
        managed_dataset_id="era5land_temperature_hourly_sle",
        path=source_path,
        start="2026-01-01T00",
        end="2026-01-01T23",
    )
    source_artifact.format = ArtifactFormat.ICECHUNK

    monkeypatch.setattr(
        resample.registry_datasets,
        "get_dataset",
        lambda dataset_id: (
            {"id": dataset_id, "period_type": "hourly"} if dataset_id == "era5land_temperature_hourly" else None
        ),
    )
    monkeypatch.setattr(
        resample.ingestion_services,
        "get_latest_artifact_for_dataset_or_404",
        lambda _: source_artifact,
    )

    artifact = resample.materialize_resampled_artifact(
        source_dataset_id="era5land_temperature_hourly",
        frequency="1D",
        method="mean",
        start="2026-01-01",
        end="2026-01-01",
        overwrite=False,
        publish=False,
    )

    result = xr.open_zarr(artifact.path, consolidated=True)
    try:
        assert result["value"].values[:, 0, 0].tolist() == [11.5]
    finally:
        result.close()


def test_materialize_resampled_artifact_supports_custom_frequency_dekadal(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source_daily_dekadal.zarr"
    time = np.array("2026-01-01", dtype="datetime64[D]") + np.arange(10)
    ds = xr.Dataset(
        {"value": (("time", "lat", "lon"), np.ones((10, 1, 1), dtype=float))},
        coords={"time": time, "lat": [2.0], "lon": [1.0]},
    )
    ds.to_zarr(source_path, mode="w", consolidated=True)

    source_artifact = _artifact(
        artifact_id="source-daily-dekadal",
        dataset_id="chirps3_precipitation_daily",
        managed_dataset_id="chirps3_precipitation_daily_sle",
        path=source_path,
        start="2026-01-01",
        end="2026-01-10",
    )

    monkeypatch.setattr(
        resample.registry_datasets,
        "get_dataset",
        lambda dataset_id: (
            {"id": dataset_id, "period_type": "daily"} if dataset_id == "chirps3_precipitation_daily" else None
        ),
    )
    monkeypatch.setattr(
        resample.ingestion_services,
        "get_latest_artifact_for_dataset_or_404",
        lambda _: source_artifact,
    )

    artifact = resample.materialize_resampled_artifact(
        source_dataset_id="chirps3_precipitation_daily",
        frequency="10D",
        method="sum",
        start="2026-01-01",
        end="2026-01-01",
        overwrite=False,
        publish=False,
    )

    assert artifact.dataset_id == "chirps3_precipitation_daily_10d_sum"
    assert artifact.coverage.temporal.start == "2026-01-01"
    result = xr.open_zarr(artifact.path, consolidated=True)
    try:
        assert result["value"].values[:, 0, 0].tolist() == [10.0]
    finally:
        result.close()


def test_materialize_resampled_artifact_returns_404_when_source_dataset_template_is_missing() -> None:
    with pytest.raises(resample.HTTPException, match="Source dataset template 'missing_daily' not found"):
        resample.materialize_resampled_artifact(
            source_dataset_id="missing_daily",
            frequency="W-MON",
            method="sum",
            start="2026-01-05",
            end="2026-01-12",
            overwrite=False,
            publish=False,
        )


def test_materialize_resampled_artifact_drops_incomplete_trailing_week(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source_daily.zarr"
    time = np.array("2026-01-05", dtype="datetime64[D]") + np.arange(10)
    ds = xr.Dataset(
        {"value": (("time", "lat", "lon"), np.ones((10, 1, 1), dtype=float))},
        coords={"time": time, "lat": [2.0], "lon": [1.0]},
    )
    ds.to_zarr(source_path, mode="w", consolidated=True)

    source_artifact = _artifact(
        artifact_id="source-daily",
        dataset_id="chirps3_precipitation_daily",
        managed_dataset_id="chirps3_precipitation_daily_sle",
        path=source_path,
        start="2026-01-05",
        end="2026-01-14",
    )

    monkeypatch.setattr(
        resample.registry_datasets,
        "get_dataset",
        lambda dataset_id: (
            {"id": dataset_id, "period_type": "daily"} if dataset_id == "chirps3_precipitation_daily" else None
        ),
    )
    monkeypatch.setattr(
        resample.ingestion_services,
        "get_latest_artifact_for_dataset_or_404",
        lambda _: source_artifact,
    )

    artifact = resample.materialize_resampled_artifact(
        source_dataset_id="chirps3_precipitation_daily",
        frequency="W-MON",
        method="sum",
        start="2026-01-05",
        end="2026-01-12",
        overwrite=False,
        publish=False,
    )

    # W03 (Jan 12-18) is incomplete — only W02 (Jan 5-11) is covered fully
    assert artifact.coverage.temporal.start == "2026-W02"
    assert artifact.coverage.temporal.end == "2026-W02"
    result = xr.open_zarr(artifact.path, consolidated=True)
    try:
        assert result["value"].values[:, 0, 0].tolist() == [7.0]
    finally:
        result.close()


def test_open_source_dataset_rejects_unsupported_artifact_formats() -> None:
    artifact = _artifact(
        artifact_id="source-hourly-netcdf",
        dataset_id="era5land_temperature_hourly",
        managed_dataset_id="era5land_temperature_hourly_sle",
        path=Path("/tmp/source.nc"),
        start="2026-01-01T00",
        end="2026-01-01T23",
    )
    artifact.format = ArtifactFormat.NETCDF

    with pytest.raises(resample.HTTPException, match="Zarr- or Icechunk-backed source artifact"):
        resample._open_source_dataset(artifact)


def test_materialize_resampled_artifact_drops_incomplete_leading_week(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source_daily_leading_partial.zarr"
    time = np.array("2026-01-07", dtype="datetime64[D]") + np.arange(12)
    ds = xr.Dataset(
        {"value": (("time", "lat", "lon"), np.ones((12, 1, 1), dtype=float))},
        coords={"time": time, "lat": [2.0], "lon": [1.0]},
    )
    ds.to_zarr(source_path, mode="w", consolidated=True)

    source_artifact = _artifact(
        artifact_id="source-daily-leading-partial",
        dataset_id="chirps3_precipitation_daily",
        managed_dataset_id="chirps3_precipitation_daily_sle",
        path=source_path,
        start="2026-01-07",
        end="2026-01-18",
    )

    monkeypatch.setattr(
        resample.registry_datasets,
        "get_dataset",
        lambda dataset_id: (
            {"id": dataset_id, "period_type": "daily"} if dataset_id == "chirps3_precipitation_daily" else None
        ),
    )
    monkeypatch.setattr(
        resample.ingestion_services,
        "get_latest_artifact_for_dataset_or_404",
        lambda _: source_artifact,
    )

    artifact = resample.materialize_resampled_artifact(
        source_dataset_id="chirps3_precipitation_daily",
        frequency="W-MON",
        method="sum",
        start="2026-01-05",
        end="2026-01-12",
        overwrite=False,
        publish=False,
    )

    # W02 (Jan 5-11) starts Wednesday Jan 7 — incomplete leading week dropped
    assert artifact.coverage.temporal.start == "2026-W03"
    assert artifact.coverage.temporal.end == "2026-W03"
    result = xr.open_zarr(artifact.path, consolidated=True)
    try:
        assert result["value"].values[:, 0, 0].tolist() == [7.0]
    finally:
        result.close()


def test_materialize_resampled_artifact_returns_409_when_source_has_no_data_in_requested_range(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source_daily_empty_range.zarr"
    time = np.array("2026-01-01", dtype="datetime64[D]") + np.arange(7)
    ds = xr.Dataset(
        {"value": (("time", "lat", "lon"), np.ones((7, 1, 1), dtype=float))},
        coords={"time": time, "lat": [2.0], "lon": [1.0]},
    )
    ds.to_zarr(source_path, mode="w", consolidated=True)

    source_artifact = _artifact(
        artifact_id="source-daily-empty",
        dataset_id="chirps3_precipitation_daily",
        managed_dataset_id="chirps3_precipitation_daily_sle",
        path=source_path,
        start="2026-01-01",
        end="2026-01-07",
    )

    monkeypatch.setattr(
        resample.registry_datasets,
        "get_dataset",
        lambda dataset_id: (
            {"id": dataset_id, "period_type": "daily"} if dataset_id == "chirps3_precipitation_daily" else None
        ),
    )
    monkeypatch.setattr(
        resample.ingestion_services,
        "get_latest_artifact_for_dataset_or_404",
        lambda _: source_artifact,
    )

    with pytest.raises(
        resample.HTTPException,
        match="Source artifact contains no data for the requested resample range",
    ):
        resample.materialize_resampled_artifact(
            source_dataset_id="chirps3_precipitation_daily",
            frequency="W-MON",
            method="sum",
            start="2026-03-02",  # 2026-W10 — well beyond source data
            end="2026-03-02",
            overwrite=False,
            publish=False,
        )


def test_materialize_resampled_artifact_builds_monthly_dataset_from_daily_source(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source_daily_monthly.zarr"
    time = np.array("2026-01-01", dtype="datetime64[D]") + np.arange(31)
    ds = xr.Dataset(
        {"value": (("time", "lat", "lon"), np.ones((31, 1, 1), dtype=float))},
        coords={"time": time, "lat": [2.0], "lon": [1.0]},
    )
    ds.to_zarr(source_path, mode="w", consolidated=True)

    source_artifact = _artifact(
        artifact_id="source-daily-monthly",
        dataset_id="chirps3_precipitation_daily",
        managed_dataset_id="chirps3_precipitation_daily_sle",
        path=source_path,
        start="2026-01-01",
        end="2026-01-31",
    )

    monkeypatch.setattr(
        resample.registry_datasets,
        "get_dataset",
        lambda dataset_id: (
            {"id": dataset_id, "period_type": "daily"} if dataset_id == "chirps3_precipitation_daily" else None
        ),
    )
    monkeypatch.setattr(
        resample.ingestion_services,
        "get_latest_artifact_for_dataset_or_404",
        lambda _: source_artifact,
    )

    artifact = resample.materialize_resampled_artifact(
        source_dataset_id="chirps3_precipitation_daily",
        frequency="MS",
        method="sum",
        start="2026-01-01",
        end="2026-01-01",
        overwrite=False,
        publish=False,
    )

    # Monthly resampled timestamp is the start of the month
    assert artifact.coverage.temporal.start == "2026-01"
    assert artifact.coverage.temporal.end == "2026-01"
    result = xr.open_zarr(artifact.path, consolidated=True)
    try:
        assert result["value"].values[:, 0, 0].tolist() == [31.0]
    finally:
        result.close()

    summary = ingestion_services.get_dataset_summary_for_artifact_or_404(artifact.artifact_id)
    assert summary.period_type == "monthly"
    assert summary.source_dataset_id == "chirps3_precipitation_daily"


def test_materialize_resampled_artifact_keeps_complete_week_for_daily_non_midnight_timestamps(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source_daily_non_midnight_weekly.zarr"
    time = np.array("2026-01-05T06", dtype="datetime64[h]") + np.arange(7) * np.timedelta64(24, "h")
    ds = xr.Dataset(
        {"value": (("time", "lat", "lon"), np.ones((7, 1, 1), dtype=float))},
        coords={"time": time, "lat": [2.0], "lon": [1.0]},
    )
    ds.to_zarr(source_path, mode="w", consolidated=True)

    source_artifact = _artifact(
        artifact_id="source-daily-non-midnight",
        dataset_id="senorge_temperature_daily",
        managed_dataset_id="senorge_temperature_daily_sle",
        path=source_path,
        start="2026-01-05",
        end="2026-01-11",
    )

    monkeypatch.setattr(
        resample.registry_datasets,
        "get_dataset",
        lambda dataset_id: (
            {"id": dataset_id, "period_type": "daily"} if dataset_id == "senorge_temperature_daily" else None
        ),
    )
    monkeypatch.setattr(
        resample.ingestion_services,
        "get_latest_artifact_for_dataset_or_404",
        lambda _: source_artifact,
    )

    artifact = resample.materialize_resampled_artifact(
        source_dataset_id="senorge_temperature_daily",
        frequency="W-MON",
        method="sum",
        start="2026-01-05",
        end="2026-01-05",
        overwrite=False,
        publish=False,
    )

    assert artifact.coverage.temporal.start == "2026-W02"
    assert artifact.coverage.temporal.end == "2026-W02"
    result = xr.open_zarr(artifact.path, consolidated=True)
    try:
        assert result["value"].values[:, 0, 0].tolist() == [7.0]
    finally:
        result.close()


def test_materialize_resampled_artifact_reuses_existing_artifact_when_overwrite_is_false(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source_hourly.zarr"
    time = np.array("2026-01-01T00", dtype="datetime64[h]") + np.arange(24)
    ds = xr.Dataset(
        {"value": (("time", "lat", "lon"), np.arange(24, dtype=float).reshape(24, 1, 1))},
        coords={"time": time, "lat": [2.0], "lon": [1.0]},
    )
    ds.to_zarr(source_path, mode="w", consolidated=True)

    source_artifact = _artifact(
        artifact_id="source-hourly",
        dataset_id="era5land_temperature_hourly",
        managed_dataset_id="era5land_temperature_hourly_sle",
        path=source_path,
        start="2026-01-01T00",
        end="2026-01-01T23",
    )

    monkeypatch.setattr(
        resample.registry_datasets,
        "get_dataset",
        lambda dataset_id: (
            {"id": dataset_id, "period_type": "hourly"} if dataset_id == "era5land_temperature_hourly" else None
        ),
    )
    monkeypatch.setattr(
        resample.ingestion_services,
        "get_latest_artifact_for_dataset_or_404",
        lambda _: source_artifact,
    )

    first = resample.materialize_resampled_artifact(
        source_dataset_id="era5land_temperature_hourly",
        frequency="1D",
        method="mean",
        start="2026-01-01",
        end="2026-01-01",
        overwrite=False,
        publish=False,
    )

    monkeypatch.setattr(
        resample.ingestion_services,
        "get_latest_artifact_for_dataset_or_404",
        lambda _: pytest.fail("existing derived artifact should be reused before resolving source artifact"),
    )
    second = resample.materialize_resampled_artifact(
        source_dataset_id="era5land_temperature_hourly",
        frequency="1D",
        method="mean",
        start="2026-01-01",
        end="2026-01-01",
        overwrite=False,
        publish=False,
    )

    assert second.artifact_id == first.artifact_id


def test_materialize_resampled_artifact_reuses_existing_artifact_by_realized_end(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source_daily_realized_reuse.zarr"
    time = np.array("2026-01-05", dtype="datetime64[D]") + np.arange(10)
    ds = xr.Dataset(
        {"value": (("time", "lat", "lon"), np.ones((10, 1, 1), dtype=float))},
        coords={"time": time, "lat": [2.0], "lon": [1.0]},
    )
    ds.to_zarr(source_path, mode="w", consolidated=True)

    source_artifact = _artifact(
        artifact_id="source-daily-realized-reuse",
        dataset_id="chirps3_precipitation_daily",
        managed_dataset_id="chirps3_precipitation_daily_sle",
        path=source_path,
        start="2026-01-05",
        end="2026-01-14",
    )

    monkeypatch.setattr(
        resample.registry_datasets,
        "get_dataset",
        lambda dataset_id: (
            {"id": dataset_id, "period_type": "daily"} if dataset_id == "chirps3_precipitation_daily" else None
        ),
    )
    monkeypatch.setattr(
        resample.ingestion_services,
        "get_latest_artifact_for_dataset_or_404",
        lambda _: source_artifact,
    )

    first = resample.materialize_resampled_artifact(
        source_dataset_id="chirps3_precipitation_daily",
        frequency="W-MON",
        method="sum",
        start="2026-01-05",
        end="2026-01-12",
        overwrite=False,
        publish=False,
    )
    second = resample.materialize_resampled_artifact(
        source_dataset_id="chirps3_precipitation_daily",
        frequency="W-MON",
        method="sum",
        start="2026-01-05",
        end="2026-01-12",
        overwrite=False,
        publish=False,
    )

    assert second.artifact_id == first.artifact_id


def test_materialize_resampled_artifact_publishes_reused_existing_artifact_when_requested(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source_hourly_publish_existing.zarr"
    time = np.array("2026-01-01T00", dtype="datetime64[h]") + np.arange(24)
    ds = xr.Dataset(
        {"value": (("time", "lat", "lon"), np.arange(24, dtype=float).reshape(24, 1, 1))},
        coords={"time": time, "lat": [2.0], "lon": [1.0]},
    )
    ds.to_zarr(source_path, mode="w", consolidated=True)

    source_artifact = _artifact(
        artifact_id="source-hourly-publish-existing",
        dataset_id="era5land_temperature_hourly",
        managed_dataset_id="era5land_temperature_hourly_sle",
        path=source_path,
        start="2026-01-01T00",
        end="2026-01-01T23",
    )

    monkeypatch.setattr(
        resample.registry_datasets,
        "get_dataset",
        lambda dataset_id: (
            {"id": dataset_id, "period_type": "hourly"} if dataset_id == "era5land_temperature_hourly" else None
        ),
    )
    monkeypatch.setattr(
        resample.ingestion_services,
        "get_latest_artifact_for_dataset_or_404",
        lambda _: source_artifact,
    )

    existing = resample.materialize_resampled_artifact(
        source_dataset_id="era5land_temperature_hourly",
        frequency="1D",
        method="mean",
        start="2026-01-01",
        end="2026-01-01",
        overwrite=False,
        publish=False,
    )

    published = existing.model_copy(
        update={
            "publication": existing.publication.model_copy(update={"status": PublicationStatus.PUBLISHED}),
        }
    )
    publish_calls: list[str] = []

    def _publish_artifact_record(artifact_id: str) -> ArtifactRecord:
        publish_calls.append(artifact_id)
        return published

    monkeypatch.setattr(resample.ingestion_services, "publish_artifact_record", _publish_artifact_record)

    reused = resample.materialize_resampled_artifact(
        source_dataset_id="era5land_temperature_hourly",
        frequency="1D",
        method="mean",
        start="2026-01-01",
        end="2026-01-01",
        overwrite=False,
        publish=True,
    )

    assert publish_calls == [existing.artifact_id]
    assert reused.publication.status == PublicationStatus.PUBLISHED


def test_materialize_resampled_artifact_rematerializes_when_overwrite_is_true(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source_hourly_overwrite.zarr"
    time = np.array("2026-01-01T00", dtype="datetime64[h]") + np.arange(24)
    initial_ds = xr.Dataset(
        {"value": (("time", "lat", "lon"), np.arange(24, dtype=float).reshape(24, 1, 1))},
        coords={"time": time, "lat": [2.0], "lon": [1.0]},
    )
    initial_ds.to_zarr(source_path, mode="w", consolidated=True)

    source_artifact = _artifact(
        artifact_id="source-hourly-overwrite",
        dataset_id="era5land_temperature_hourly",
        managed_dataset_id="era5land_temperature_hourly_sle",
        path=source_path,
        start="2026-01-01T00",
        end="2026-01-01T23",
    )

    monkeypatch.setattr(
        resample.registry_datasets,
        "get_dataset",
        lambda dataset_id: (
            {"id": dataset_id, "period_type": "hourly"} if dataset_id == "era5land_temperature_hourly" else None
        ),
    )
    monkeypatch.setattr(
        resample.ingestion_services,
        "get_latest_artifact_for_dataset_or_404",
        lambda _: source_artifact,
    )

    first = resample.materialize_resampled_artifact(
        source_dataset_id="era5land_temperature_hourly",
        frequency="1D",
        method="mean",
        start="2026-01-01",
        end="2026-01-01",
        overwrite=False,
        publish=False,
    )

    updated_ds = xr.Dataset(
        {"value": (("time", "lat", "lon"), (np.arange(24, dtype=float) + 24).reshape(24, 1, 1))},
        coords={"time": time, "lat": [2.0], "lon": [1.0]},
    )
    updated_ds.to_zarr(source_path, mode="w", consolidated=True)

    second = resample.materialize_resampled_artifact(
        source_dataset_id="era5land_temperature_hourly",
        frequency="1D",
        method="mean",
        start="2026-01-01",
        end="2026-01-01",
        overwrite=True,
        publish=False,
    )

    assert second.artifact_id == first.artifact_id
    result = xr.open_zarr(second.path, consolidated=True)
    try:
        assert result["value"].values[:, 0, 0].tolist() == [35.5]
    finally:
        result.close()
