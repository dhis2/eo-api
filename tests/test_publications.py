from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

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
from open_climate_service.publications import services


def test_load_base_config_returns_mapping() -> None:
    config = services._load_base_config()
    assert isinstance(config, dict)
    assert "server" in config


def test_resolve_pygeoapi_dir_uses_data_dir_from_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from open_climate_service import config as api_config

    monkeypatch.setattr(api_config, "get_data_dir", lambda: tmp_path / "data")
    result = services._resolve_pygeoapi_dir()
    assert result == tmp_path / "data" / "pygeoapi"


def test_resolve_pygeoapi_dir_uses_xdg_data_home(monkeypatch: pytest.MonkeyPatch) -> None:
    import tempfile

    from open_climate_service import config as api_config

    monkeypatch.setattr(api_config, "get_data_dir", lambda: None)
    with tempfile.TemporaryDirectory() as xdg:
        monkeypatch.setenv("XDG_DATA_HOME", xdg)
        result = services._resolve_pygeoapi_dir()
        assert str(result) == f"{xdg}/climate-service/pygeoapi"


def test_native_dataset_href_defaults_to_relative_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CLIMATE_SERVICE_BASE_URL", raising=False)
    monkeypatch.delenv("OGCAPI_BASE_URL", raising=False)

    assert services._native_dataset_href("dataset-1") == "/datasets/dataset-1"


def test_native_dataset_href_uses_ogcapi_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CLIMATE_SERVICE_BASE_URL", raising=False)
    monkeypatch.setenv("OGCAPI_BASE_URL", "https://example.org/ogcapi")

    assert services._native_dataset_href("dataset-1") == "https://example.org/datasets/dataset-1"


def test_native_dataset_href_uses_open_climate_service_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLIMATE_SERVICE_BASE_URL", "https://climate.example.org")
    monkeypatch.delenv("OGCAPI_BASE_URL", raising=False)

    assert services._native_dataset_href("dataset-1") == "https://climate.example.org/datasets/dataset-1"


def test_build_collection_resource_keeps_singleton_time_dimension_for_zarr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(services, "_provider_axes", lambda record: ("x", "y", "time"))

    record = ArtifactRecord(
        artifact_id="artifact-1",
        dataset_id="chirps3_precipitation_daily_ms_sum",
        dataset_name="CHIRPS monthly total precipitation",
        variable="precip",
        format=ArtifactFormat.ZARR,
        path="/tmp/chirps3_precipitation_daily_ms_sum.zarr",
        asset_paths=["/tmp/chirps3_precipitation_daily_ms_sum.zarr"],
        variables=["precip"],
        request_scope=ArtifactRequestScope(start="2024-01-01", end="2024-01-31"),
        coverage=ArtifactCoverage(
            temporal=CoverageTemporal(start="2024-01", end="2024-01"),
            spatial=CoverageSpatial(xmin=-13.5, ymin=6.9, xmax=-10.1, ymax=10.0),
        ),
        created_at=datetime.now(UTC),
        publication=ArtifactPublication(
            status=PublicationStatus.PUBLISHED,
            collection_id="chirps3_precipitation_daily_ms_sum_sle",
        ),
    )

    resource = services._build_collection_resource(record)
    provider = resource["providers"][0]

    assert provider["options"] == {"zarr": {"consolidated": True}}
    assert "squeeze" not in provider["options"]


def test_publish_icechunk_artifact_removes_existing_pygeoapi_collection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old_zarr = ArtifactRecord(
        artifact_id="artifact-zarr",
        dataset_id="chirps3_precipitation_daily",
        dataset_name="CHIRPS3 precipitation",
        variable="precip",
        format=ArtifactFormat.ZARR,
        path="/tmp/chirps3.zarr",
        asset_paths=["/tmp/chirps3.zarr"],
        variables=["precip"],
        request_scope=ArtifactRequestScope(start="2026-01-01", end="2026-01-03"),
        coverage=ArtifactCoverage(
            temporal=CoverageTemporal(start="2026-01-01", end="2026-01-03"),
            spatial=CoverageSpatial(xmin=1.0, ymin=2.0, xmax=3.0, ymax=4.0),
        ),
        created_at=datetime.now(UTC),
        publication=ArtifactPublication(
            status=PublicationStatus.PUBLISHED,
            collection_id="chirps3_precipitation_daily",
            pygeoapi_path="/ogcapi/collections/chirps3_precipitation_daily",
        ),
    )
    new_icechunk = ArtifactRecord(
        artifact_id="artifact-icechunk",
        dataset_id="chirps3_precipitation_daily",
        dataset_name="CHIRPS3 precipitation",
        variable="precip",
        format=ArtifactFormat.ICECHUNK,
        path="/tmp/chirps3.icechunk",
        asset_paths=["/tmp/chirps3.icechunk"],
        variables=["precip"],
        request_scope=ArtifactRequestScope(start="2026-01-01", end="2026-01-05"),
        coverage=ArtifactCoverage(
            temporal=CoverageTemporal(start="2026-01-01", end="2026-01-05"),
            spatial=CoverageSpatial(xmin=1.0, ymin=2.0, xmax=3.0, ymax=4.0),
        ),
        created_at=datetime.now(UTC),
        publication=ArtifactPublication(status=PublicationStatus.UNPUBLISHED),
    )

    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        "open_climate_service.ingestions.services.list_artifacts",
        lambda: type("Artifacts", (), {"items": [old_zarr, new_icechunk]})(),
    )
    monkeypatch.setattr(
        services,
        "_sync_pygeoapi_documents",
        lambda *, resources: captured.setdefault("resources", resources),
    )
    monkeypatch.setattr(services, "_refresh_mounted_pygeoapi", lambda: captured.setdefault("refreshed", True))

    published = services.publish_artifact(new_icechunk)

    assert published.publication.status == PublicationStatus.PUBLISHED
    assert published.publication.pygeoapi_path is None
    assert captured["resources"] == {}
    assert captured["refreshed"] is True
