from datetime import UTC, datetime

import pytest

from eo_api.ingestions import services
from eo_api.ingestions.schemas import (
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
)
from eo_api.publications.services import managed_dataset_id_for


def _artifact(
    *,
    artifact_id: str,
    source_dataset_id: str = "chirps3_precipitation_daily",
    managed_dataset_id: str = "chirps3_precipitation_daily_sle",
    created_at: str = "2026-01-10T00:00:00+00:00",
    end: str = "2026-01-10",
) -> ArtifactRecord:
    return ArtifactRecord(
        artifact_id=artifact_id,
        dataset_id=source_dataset_id,
        dataset_name="CHIRPS3 precipitation",
        variable="precip",
        format=ArtifactFormat.ZARR,
        path="/tmp/chirps3_precipitation_daily.zarr",
        asset_paths=["/tmp/chirps3_precipitation_daily.zarr"],
        variables=["precip"],
        request_scope=ArtifactRequestScope(
            start="2026-01-01",
            end=end,
            extent_id="sle",
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


def test_list_datasets_groups_artifacts_by_managed_dataset_id(monkeypatch: pytest.MonkeyPatch) -> None:
    records = [
        _artifact(artifact_id="a1", created_at="2026-01-10T00:00:00+00:00", end="2026-01-10"),
        _artifact(artifact_id="a2", created_at="2026-01-11T00:00:00+00:00", end="2026-01-11"),
    ]
    monkeypatch.setattr(services, "_load_records", lambda: records)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {
            "id": "chirps3_precipitation_daily",
            "short_name": "CHIRPS3 precip",
            "period_type": "daily",
            "units": "mm",
            "resolution": "5 km x 5 km",
            "source": "CHIRPS v3",
            "source_url": "https://example.com/chirps",
        },
    )

    result = services.list_datasets()

    assert len(result.items) == 1
    dataset = result.items[0]
    assert dataset.dataset_id == "chirps3_precipitation_daily_sle"
    assert dataset.source_dataset_id == "chirps3_precipitation_daily"
    assert dataset.period_type == "daily"
    assert dataset.units == "mm"
    assert dataset.extent.temporal.end == "2026-01-11"
    assert dataset.publication.status == PublicationStatus.PUBLISHED
    assert any(link.href == f"/zarr/{dataset.dataset_id}" for link in dataset.links)


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
        lambda _: {"id": "chirps3_precipitation_daily", "period_type": "daily"},
    )
    monkeypatch.setattr(services, "get_dataset_or_404", lambda _: _dataset_detail(dataset_id))

    result = services.sync_dataset(dataset_id=dataset_id, end="2026-01-31", prefer_zarr=True, publish=True)

    assert result.sync_id is None
    assert result.status == "up_to_date"
    assert result.dataset.dataset_id == dataset_id


def test_sync_dataset_creates_new_version_from_next_period(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset_id = "chirps3_precipitation_daily_sle"
    latest = _artifact(artifact_id="a1", managed_dataset_id=dataset_id, end="2026-01-31")
    monkeypatch.setattr(services, "get_latest_artifact_for_dataset_or_404", lambda _: latest)
    monkeypatch.setattr(
        services.registry_datasets,
        "get_dataset",
        lambda _: {"id": "chirps3_precipitation_daily", "period_type": "daily"},
    )

    captured: dict[str, object] = {}

    def fake_create_artifact(**kwargs: object) -> ArtifactRecord:
        captured.update(kwargs)
        return _artifact(artifact_id="a2", managed_dataset_id=dataset_id, end="2026-02-10")

    monkeypatch.setattr(services, "create_artifact", fake_create_artifact)
    monkeypatch.setattr(services, "get_dataset_or_404", lambda _: _dataset_detail(dataset_id))

    result = services.sync_dataset(dataset_id=dataset_id, end="2026-02-10", prefer_zarr=True, publish=True)

    assert captured["start"] == "2026-02-01"
    assert captured["end"] == "2026-02-10"
    assert captured["extent_id"] == "sle"
    assert captured["bbox"] == [1.0, 2.0, 3.0, 4.0]
    assert captured["country_code"] is None
    assert result.sync_id == "a2"
    assert result.status == "completed"


def test_managed_dataset_id_prefers_extent_id_when_present() -> None:
    artifact = _artifact(artifact_id="a1")

    assert managed_dataset_id_for(artifact) == "chirps3_precipitation_daily_sle"
