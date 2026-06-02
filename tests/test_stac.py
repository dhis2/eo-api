from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pystac
import pytest
import xarray as xr
from fastapi.testclient import TestClient

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
from open_climate_service.openeo import collections as openeo_collections
from open_climate_service.stac import services as stac_services


@pytest.fixture(autouse=True)
def _clear_xstac_collection_cache() -> None:
    stac_services._clear_xstac_collection_cache()


def _minimal_xstac_payload(dataset_id: str = "chirps3_precipitation_daily") -> dict[str, object]:
    """Return a minimal xstac-style collection payload for tests that don't need full metadata."""
    return {
        "type": "Collection",
        "id": dataset_id,
        "extent": {"spatial": {"bbox": [[0, 0, 0, 0]]}, "temporal": {"interval": [[None, None]]}},
        "cube:dimensions": {"time": {"type": "temporal", "extent": ["2026-01-01", "2026-01-10"]}},
        "cube:variables": {"precip": {"type": "data", "dimensions": ["time", "y", "x"]}},
        "assets": {"zarr": {}},
    }


def _artifact(
    *,
    artifact_id: str,
    dataset_id: str = "chirps3_precipitation_daily",
    dataset_name: str = "CHIRPS3 precipitation",
    variable: str = "precip",
    managed_dataset_id: str = "chirps3_precipitation_daily",
    status: PublicationStatus = PublicationStatus.PUBLISHED,
    format: ArtifactFormat = ArtifactFormat.ZARR,
    path: str | None = "/tmp/chirps3_precipitation_daily.zarr",
    asset_paths: list[str] | None = None,
    temporal_start: str = "2026-01-01",
    temporal_end: str = "2026-01-10",
    created_at: datetime | None = None,
) -> ArtifactRecord:
    return ArtifactRecord(
        artifact_id=artifact_id,
        dataset_id=dataset_id,
        dataset_name=dataset_name,
        variable=variable,
        format=format,
        path=path,
        asset_paths=[path] if asset_paths is None and path is not None else (asset_paths or []),
        variables=[variable],
        request_scope=ArtifactRequestScope(
            start=temporal_start,
            end=temporal_end,
            bbox=(1.0, 2.0, 3.0, 4.0),
        ),
        coverage=ArtifactCoverage(
            temporal=CoverageTemporal(start=temporal_start, end=temporal_end),
            spatial=CoverageSpatial(xmin=1.0, ymin=2.0, xmax=3.0, ymax=4.0),
        ),
        created_at=created_at or datetime(2026, 1, 10, tzinfo=UTC),
        publication=ArtifactPublication(
            status=status,
            collection_id=managed_dataset_id,
            pygeoapi_path=f"/ogcapi/collections/{managed_dataset_id}",
        ),
    )


def test_stac_landing_returns_catalog(client: TestClient) -> None:
    response = client.get("/stac/catalog.json")

    assert response.status_code == 200
    payload = response.json()
    assert payload["type"] == "Catalog"
    assert any(link["rel"] == "self" for link in payload["links"])
    assert any(link["rel"] == "root" for link in payload["links"])


def test_stac_catalog_lists_child_collections(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        stac_services,
        "_eligible_artifacts_by_dataset",
        lambda: {"chirps3_precipitation_daily": _artifact(artifact_id="a1")},
    )

    response = client.get("/stac/catalog.json")

    assert response.status_code == 200
    payload = response.json()
    child_links = [link for link in payload["links"] if link["rel"] == "child"]
    assert len(child_links) == 1
    assert child_links[0]["href"].endswith("/stac/collections/chirps3_precipitation_daily")


def test_stac_landing_links_to_collections(client: TestClient) -> None:
    response = client.get("/stac")

    assert response.status_code == 200
    payload = response.json()
    assert any(link["href"].endswith("/stac/catalog.json") for link in payload["links"] if link["rel"] == "root")


def test_catalog_self_link_reflects_request_path(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(items=[_artifact(artifact_id="a1")]),
    )

    response = client.get("/stac")

    assert response.status_code == 200
    payload = response.json()
    assert payload["links"][0]["href"].endswith("/stac")


def test_catalog_excludes_unpublished_and_netcdf(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(
            items=[
                _artifact(artifact_id="a1", status=PublicationStatus.UNPUBLISHED),
                _artifact(artifact_id="a2", format=ArtifactFormat.NETCDF),
            ]
        ),
    )

    response = client.get("/stac/catalog.json")

    assert response.status_code == 200
    payload = response.json()
    assert [link for link in payload["links"] if link["rel"] == "child"] == []


def test_collection_uses_xstac_and_adds_expected_fields(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(items=[_artifact(artifact_id="a1")]),
    )
    monkeypatch.setattr(
        stac_services.registry_datasets,
        "get_dataset",
        lambda _: {
            "period_type": "daily",
            "units": "mm",
            "source": "CHIRPS v3",
            "extents": {"temporal": {"resolution": "P1D"}},
        },
    )
    monkeypatch.setattr(
        stac_services,
        "_build_collection_with_xstac",
        lambda **_: {
            "type": "Collection",
            "id": "chirps3_precipitation_daily",
            "extent": {"spatial": {"bbox": [[0, 0, 0, 0]]}, "temporal": {"interval": [[None, None]]}},
            "cube:dimensions": {
                "x": {
                    "type": "spatial",
                    "axis": "x",
                    "extent": [1.0, 3.0],
                    "step": 0.05000000074505806,
                    "reference_system": 4326,
                },
                "y": {
                    "type": "spatial",
                    "axis": "y",
                    "extent": [2.0, 4.0],
                    "step": -0.05000000074505806,
                    "reference_system": 4326,
                },
                "time": {"type": "temporal", "extent": ["2026-01-01", "2026-01-10"]},
            },
            "stac_extensions": ["https://stac-extensions.github.io/projection/v2.0.0/schema.json"],
            "cube:variables": {
                "precip": {
                    "type": "data",
                    "dimensions": ["time", "y", "x"],
                    "attrs": {
                        "long_name": "Precipitation",
                        "units": "mm/day",
                        "TIFFTAG_SOFTWARE": "IDL 9.0.0",
                    },
                },
            },
            "assets": {"zarr": {}},
        },
    )
    monkeypatch.setattr(
        stac_services,
        "_zarr_asset_metadata",
        lambda _: {"zarr:consolidated": True, "zarr:zarr_format": 3},
    )
    monkeypatch.setattr(stac_services, "_zarr_open_kwargs", lambda _: {"consolidated": True})

    response = client.get("/collections/chirps3_precipitation_daily")

    assert response.status_code == 200
    payload = response.json()
    assert payload["type"] == "Collection"
    assert payload["description"] == "Published GeoZarr dataset for CHIRPS3 precipitation"
    assert payload["assets"]["zarr"]["href"].endswith("/zarr/chirps3_precipitation_daily")
    assert payload["assets"]["zarr"]["xarray:open_kwargs"] == {"consolidated": True}
    assert payload["extent"]["temporal"]["interval"] == [["2026-01-01T00:00:00Z", "2026-01-10T00:00:00Z"]]
    assert payload["cube:dimensions"]["x"]["step"] == 0.05
    assert payload["cube:dimensions"]["y"]["step"] == -0.05
    assert payload["cube:dimensions"]["t"]["extent"] == ["2026-01-01T00:00:00Z", "2026-01-10T00:00:00Z"]
    assert payload["cube:dimensions"]["t"]["step"] == "P1D"
    assert payload["cube:variables"]["precip"]["unit"] == "mm/day"
    assert payload["cube:variables"]["precip"]["attrs"] == {
        "long_name": "Precipitation",
        "units": "mm/day",
    }
    assert "https://stac-extensions.github.io/projection/v2.0.0/schema.json" in payload["stac_extensions"]


def test_stac_collection_compatibility_route_builds_collection(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        stac_services,
        "build_collection",
        lambda dataset_id, request: {"type": "Collection", "id": dataset_id, "links": []},
    )

    response = client.get("/stac/collections/chirps3_precipitation_daily")

    assert response.status_code == 200
    assert response.json()["id"] == "chirps3_precipitation_daily"


def test_collections_logs_skipped_dataset_failures(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str, str]] = []

    monkeypatch.setattr(
        stac_services,
        "_eligible_artifacts_by_dataset",
        lambda: {"broken_dataset": _artifact(artifact_id="a1", dataset_id="broken_dataset")},
    )

    def _raise(dataset_id: str, request: object) -> dict[str, object]:
        raise stac_services.HTTPException(status_code=503, detail="store unavailable")

    monkeypatch.setattr(stac_services, "build_collection", _raise)
    monkeypatch.setattr(openeo_collections.logger, "warning", lambda *args: calls.append(args))

    response = client.get("/collections")

    assert response.status_code == 200
    assert response.json()["collections"] == []
    assert calls == [("Skipping collection '%s' from openEO listing: %s", "broken_dataset", "store unavailable")]


def test_collection_uses_configured_base_url(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLIMATE_SERVICE_BASE_URL", "https://climate.example.org")
    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(items=[_artifact(artifact_id="a1")]),
    )
    monkeypatch.setattr(
        stac_services.registry_datasets,
        "get_dataset",
        lambda _: {"period_type": "daily", "units": "mm"},
    )
    monkeypatch.setattr(
        stac_services,
        "_build_collection_with_xstac",
        lambda **_: {
            "type": "Collection",
            "id": "chirps3_precipitation_daily",
            "extent": {"spatial": {"bbox": [[0, 0, 0, 0]]}, "temporal": {"interval": [[None, None]]}},
            "cube:dimensions": {"time": {"type": "temporal", "extent": ["2026-01-01", "2026-01-10"]}},
            "cube:variables": {"precip": {"type": "data", "dimensions": ["time", "y", "x"]}},
            "assets": {"zarr": {}},
        },
    )
    monkeypatch.setattr(stac_services, "_zarr_asset_metadata", lambda _: {"zarr:consolidated": True})
    monkeypatch.setattr(stac_services, "_zarr_open_kwargs", lambda _: {"consolidated": True})

    response = client.get("/collections/chirps3_precipitation_daily")

    assert response.status_code == 200
    payload = response.json()
    assert payload["links"][0]["href"] == "https://climate.example.org/collections/chirps3_precipitation_daily"


def test_collection_sets_hourly_step_to_pt1h(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    hourly_artifact = _artifact(
        artifact_id="a1",
        dataset_id="era5land_temperature_hourly",
        dataset_name="ERA5-Land temperature",
        variable="t2m",
        managed_dataset_id="era5land_temperature_hourly",
        path="/tmp/era5land_temperature_hourly.zarr",
        temporal_start="2026-01-01T00",
        temporal_end="2026-01-01T12",
    )
    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(items=[hourly_artifact]),
    )
    monkeypatch.setattr(
        stac_services.registry_datasets,
        "get_dataset",
        lambda _: {
            "period_type": "hourly",
            "source": "ERA5-Land",
            "short_name": "2m temperature",
            "extents": {"temporal": {"resolution": "PT1H"}},
        },
    )
    monkeypatch.setattr(
        stac_services,
        "_build_collection_with_xstac",
        lambda **_: {
            "type": "Collection",
            "id": "era5land_temperature_hourly",
            "extent": {"spatial": {"bbox": [[0, 0, 0, 0]]}, "temporal": {"interval": [[None, None]]}},
            "cube:dimensions": {
                "valid_time": {"type": "temporal", "extent": ["2026-01-01T00", "2026-01-01T12"]},
            },
            "cube:variables": {"t2m": {"type": "data", "dimensions": ["valid_time", "y", "x"]}},
            "assets": {"zarr": {}},
        },
    )
    monkeypatch.setattr(stac_services, "_zarr_asset_metadata", lambda _: {"zarr:consolidated": True})
    monkeypatch.setattr(stac_services, "_zarr_open_kwargs", lambda _: {"consolidated": True})

    response = client.get("/collections/era5land_temperature_hourly")

    assert response.status_code == 200
    payload = response.json()
    assert payload["cube:dimensions"]["t"]["step"] == "PT1H"


def test_collection_uses_level0_href_for_pyramid_zarr_store(
    client: TestClient, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    zarr_path = tmp_path / "chirps3_precipitation_daily.zarr"
    (zarr_path / "0").mkdir(parents=True)
    artifact = _artifact(artifact_id="a1", path=str(zarr_path))
    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(items=[artifact]),
    )
    monkeypatch.setattr(
        stac_services.registry_datasets,
        "get_dataset",
        lambda _: {"period_type": "daily", "source": "CHIRPS v3", "ingestion": {}},
    )
    monkeypatch.setattr(
        stac_services,
        "_build_collection_with_xstac",
        lambda **_: {
            "type": "Collection",
            "id": "chirps3_precipitation_daily",
            "extent": {"spatial": {"bbox": [[0, 0, 0, 0]]}, "temporal": {"interval": [[None, None]]}},
            "cube:dimensions": {"time": {"type": "temporal", "extent": ["2026-01-01", "2026-01-10"]}},
            "cube:variables": {"precip": {"type": "data", "dimensions": ["time", "y", "x"]}},
            "assets": {"zarr": {}},
        },
    )
    monkeypatch.setattr(stac_services, "_zarr_asset_metadata", lambda _: {"zarr:consolidated": True})
    monkeypatch.setattr(stac_services, "_zarr_open_kwargs", lambda _: {"consolidated": None})

    response = client.get("/collections/chirps3_precipitation_daily")

    assert response.status_code == 200
    payload = response.json()
    assert payload["assets"]["zarr"]["href"].endswith("/zarr/chirps3_precipitation_daily/0")


def test_collection_uses_root_href_for_icechunk_store(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    artifact = _artifact(artifact_id="a1", format=ArtifactFormat.ICECHUNK, path="/tmp/chirps3.icechunk")
    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(items=[artifact]),
    )
    monkeypatch.setattr(
        stac_services.registry_datasets,
        "get_dataset",
        lambda _: {"period_type": "daily", "source": "CHIRPS v3", "ingestion": {}},
    )
    monkeypatch.setattr(
        stac_services,
        "_build_collection_with_xstac",
        lambda **_: {
            "type": "Collection",
            "id": "chirps3_precipitation_daily",
            "extent": {"spatial": {"bbox": [[0, 0, 0, 0]]}, "temporal": {"interval": [[None, None]]}},
            "cube:dimensions": {"time": {"type": "temporal", "extent": ["2026-01-01", "2026-01-10"]}},
            "cube:variables": {"precip": {"type": "data", "dimensions": ["time", "y", "x"]}},
            "assets": {"zarr": {}},
        },
    )

    response = client.get("/collections/chirps3_precipitation_daily")

    assert response.status_code == 200
    payload = response.json()
    assert payload["assets"]["zarr"]["href"].endswith("/zarr/chirps3_precipitation_daily")
    assert payload["assets"]["zarr"]["xarray:open_kwargs"] == {"consolidated": None}
    assert payload["assets"]["zarr"]["zarr:zarr_format"] == 3


def test_collection_uses_level0_href_for_remote_pyramid_zarr_store(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact = _artifact(artifact_id="a1", path="s3://example-bucket/chirps3_precipitation_daily.zarr")
    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(items=[artifact]),
    )
    monkeypatch.setattr(
        stac_services.registry_datasets,
        "get_dataset",
        lambda _: {"period_type": "daily", "source": "CHIRPS v3", "ingestion": {}},
    )
    monkeypatch.setattr(
        stac_services,
        "_build_collection_with_xstac",
        lambda **_: {
            "type": "Collection",
            "id": "chirps3_precipitation_daily",
            "extent": {"spatial": {"bbox": [[0, 0, 0, 0]]}, "temporal": {"interval": [[None, None]]}},
            "cube:dimensions": {"time": {"type": "temporal", "extent": ["2026-01-01", "2026-01-10"]}},
            "cube:variables": {"precip": {"type": "data", "dimensions": ["time", "y", "x"]}},
            "assets": {"zarr": {}},
        },
    )
    monkeypatch.setattr(stac_services, "_zarr_asset_metadata", lambda _: {"zarr:consolidated": True})
    monkeypatch.setattr(stac_services, "_zarr_open_kwargs", lambda _: {"consolidated": None})
    monkeypatch.setattr(stac_services, "_is_pyramid_zarr", lambda _: True)

    response = client.get("/collections/chirps3_precipitation_daily")

    assert response.status_code == 200
    payload = response.json()
    assert payload["assets"]["zarr"]["href"].endswith("/zarr/chirps3_precipitation_daily/0")


def test_collection_returns_404_for_unknown_dataset(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(items=[]),
    )

    response = client.get("/collections/unknown-dataset")

    assert response.status_code == 404


def test_collections_prefers_latest_artifact_per_managed_dataset(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    older = _artifact(artifact_id="a1", created_at=datetime(2026, 1, 9, tzinfo=UTC))
    newer = _artifact(artifact_id="a2", created_at=datetime(2026, 1, 10, tzinfo=UTC))
    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(items=[older, newer]),
    )
    monkeypatch.setattr(stac_services, "_build_collection_with_xstac", lambda **_: _minimal_xstac_payload())
    monkeypatch.setattr(stac_services.registry_datasets, "get_dataset", lambda _: {"period_type": "daily"})
    monkeypatch.setattr(stac_services, "_zarr_asset_metadata", lambda _: {})
    monkeypatch.setattr(stac_services, "_zarr_open_kwargs", lambda _: {})

    response = client.get("/collections")

    assert response.status_code == 200
    payload = response.json()
    collections = payload["collections"]
    assert len(collections) == 1
    assert collections[0]["id"] == "chirps3_precipitation_daily"


def test_collections_sorts_by_managed_dataset_id(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(
            items=[
                _artifact(artifact_id="a1", managed_dataset_id="worldpop_population_yearly"),
                _artifact(
                    artifact_id="a2",
                    dataset_id="aa_dataset",
                    dataset_name="AA Dataset",
                    variable="value",
                    managed_dataset_id="aa_dataset",
                    path="/tmp/aa_dataset.zarr",
                ),
            ]
        ),
    )
    monkeypatch.setattr(stac_services, "_build_collection_with_xstac", lambda **_: _minimal_xstac_payload())
    monkeypatch.setattr(stac_services.registry_datasets, "get_dataset", lambda _: {"period_type": "daily"})
    monkeypatch.setattr(stac_services, "_zarr_asset_metadata", lambda _: {})
    monkeypatch.setattr(stac_services, "_zarr_open_kwargs", lambda _: {})

    response = client.get("/collections")

    assert response.status_code == 200
    payload = response.json()
    collection_ids = [col["id"] for col in payload["collections"]]
    assert collection_ids == sorted(collection_ids)


def test_build_collection_with_xstac_normalizes_pystac_collection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DummyDataset:
        def close(self) -> None:
            pass

    artifact = _artifact(artifact_id="a1")
    template = pystac.Collection(
        id="chirps3_precipitation_daily",
        description="template",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[1.0, 2.0, 3.0, 4.0]]),
            temporal=pystac.TemporalExtent([[datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 10, tzinfo=UTC)]]),
        ),
        title="CHIRPS3 precipitation",
        license="proprietary",
    )
    template.add_asset("zarr", pystac.Asset(href="http://example.test/zarr"))
    monkeypatch.setattr(stac_services, "open_zarr_dataset", lambda _: DummyDataset())
    monkeypatch.setattr(stac_services, "get_x_y_dims", lambda _: ("x", "y"))
    monkeypatch.setattr(stac_services, "get_time_dim", lambda _: "time")
    monkeypatch.setattr(stac_services, "xarray_to_stac", lambda *args, **kwargs: template)

    payload = stac_services._build_collection_with_xstac(artifact=artifact, template=template)

    assert isinstance(payload, dict)
    assert payload["type"] == "Collection"
    assert payload["id"] == "chirps3_precipitation_daily"


def test_collection_reuses_cached_xstac_payload(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DummyDataset:
        def close(self) -> None:
            pass

    open_count = 0

    def fake_open(_: str) -> DummyDataset:
        nonlocal open_count
        open_count += 1
        return DummyDataset()

    def fake_xarray_to_stac(*args: object, **kwargs: object) -> pystac.Collection:
        template = kwargs["template"] if "template" in kwargs else args[1]
        assert isinstance(template, pystac.Collection)
        template.extra_fields["cube:dimensions"] = {
            "time": {"type": "temporal", "extent": ["2026-01-01", "2026-01-10"]}
        }
        template.extra_fields["cube:variables"] = {"precip": {"type": "data", "dimensions": ["time", "y", "x"]}}
        return template

    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(items=[_artifact(artifact_id="a1")]),
    )
    monkeypatch.setattr(stac_services.registry_datasets, "get_dataset", lambda _: {"period_type": "daily"})
    monkeypatch.setattr(stac_services, "open_zarr_dataset", fake_open)
    monkeypatch.setattr(stac_services, "get_x_y_dims", lambda _: ("x", "y"))
    monkeypatch.setattr(stac_services, "get_time_dim", lambda _: "time")
    monkeypatch.setattr(stac_services, "xarray_to_stac", fake_xarray_to_stac)
    monkeypatch.setattr(stac_services, "_zarr_asset_metadata", lambda _: {"zarr:consolidated": True})
    monkeypatch.setattr(stac_services, "_zarr_open_kwargs", lambda _: {"consolidated": True})

    first_response = client.get("/collections/chirps3_precipitation_daily")
    second_response = client.get("/collections/chirps3_precipitation_daily")

    assert first_response.status_code == 200
    assert second_response.status_code == 200
    assert first_response.json() == second_response.json()
    assert open_count == 1


def test_zarr_consolidated_flag_detects_v3_and_v2_markers(tmp_path: Path) -> None:
    v3_consolidated = tmp_path / "v3_consolidated.zarr"
    v3_consolidated.mkdir()
    (v3_consolidated / "zarr.json").write_text('{"consolidated_metadata": {}}', encoding="utf-8")

    v3_unconsolidated = tmp_path / "v3_unconsolidated.zarr"
    v3_unconsolidated.mkdir()
    (v3_unconsolidated / "zarr.json").write_text("{}", encoding="utf-8")

    v2_consolidated = tmp_path / "v2_consolidated.zarr"
    v2_consolidated.mkdir()
    (v2_consolidated / ".zmetadata").write_text("{}", encoding="utf-8")

    assert stac_services._zarr_consolidated_flag(str(v3_consolidated)) is True
    assert stac_services._zarr_consolidated_flag(str(v3_unconsolidated)) is False
    assert stac_services._zarr_consolidated_flag(str(v2_consolidated)) is True
    assert stac_services._zarr_consolidated_flag("s3://example-bucket/store.zarr") is None


def test_collection_preserves_template_links_when_xstac_mutates_template(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DummyDataset:
        def close(self) -> None:
            pass

    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(items=[_artifact(artifact_id="a1")]),
    )
    monkeypatch.setattr(stac_services.registry_datasets, "get_dataset", lambda _: {"period_type": "daily"})
    monkeypatch.setattr(stac_services, "open_zarr_dataset", lambda _: DummyDataset())
    monkeypatch.setattr(stac_services, "get_x_y_dims", lambda _: ("x", "y"))
    monkeypatch.setattr(stac_services, "get_time_dim", lambda _: "time")

    def fake_xarray_to_stac(*args: object, **kwargs: object) -> pystac.Collection:
        template = kwargs["template"] if "template" in kwargs else args[1]
        assert isinstance(template, pystac.Collection)
        template.extra_fields["cube:dimensions"] = {
            "time": {"type": "temporal", "extent": ["2026-01-01", "2026-01-10"]}
        }
        template.extra_fields["cube:variables"] = {"precip": {"type": "data", "dimensions": ["time", "y", "x"]}}
        return template

    monkeypatch.setattr(stac_services, "xarray_to_stac", fake_xarray_to_stac)
    monkeypatch.setattr(stac_services, "_zarr_asset_metadata", lambda _: {"zarr:consolidated": True})
    monkeypatch.setattr(stac_services, "_zarr_open_kwargs", lambda _: {"consolidated": True})

    response = client.get("/collections/chirps3_precipitation_daily")

    assert response.status_code == 200
    payload = response.json()
    links = {link["rel"]: link["href"] for link in payload["links"]}
    assert links["self"].endswith("/collections/chirps3_precipitation_daily")
    # root/parent links from stac_services point to /stac (rewritten from /stac/catalog.json)
    assert links["root"].endswith("/stac")
    assert links["parent"].endswith("/stac")
    assert links["alternate"].endswith("/datasets/chirps3_precipitation_daily")


def test_collection_returns_500_for_missing_artifact_store_metadata(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact = _artifact(artifact_id="a1", path=None, asset_paths=[])
    monkeypatch.setattr(ingestion_services, "list_artifacts", lambda: SimpleNamespace(items=[artifact]))
    monkeypatch.setattr(stac_services.registry_datasets, "get_dataset", lambda _: {"period_type": "daily"})

    response = client.get("/collections/chirps3_precipitation_daily")

    assert response.status_code == 500
    assert "no readable storage path metadata" in response.json()["detail"]


def test_collection_returns_503_when_zarr_store_cannot_be_opened(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    def raise_file_not_found(_: str) -> None:
        raise FileNotFoundError("missing")

    monkeypatch.setattr(
        ingestion_services,
        "list_artifacts",
        lambda: SimpleNamespace(items=[_artifact(artifact_id="a1")]),
    )
    monkeypatch.setattr(stac_services.registry_datasets, "get_dataset", lambda _: {"period_type": "daily"})
    monkeypatch.setattr(
        stac_services,
        "open_zarr_dataset",
        raise_file_not_found,
    )

    response = client.get("/collections/chirps3_precipitation_daily")

    assert response.status_code == 503
    assert "temporarily unavailable" in response.json()["detail"]


def test_build_collection_with_xstac_reads_normalised_zarr_coordinates(tmp_path: Path) -> None:
    """STAC collection metadata is derived correctly from a normalised longitude/latitude/time store."""
    zarr_path = tmp_path / "chirps3_precipitation_daily.zarr"
    ds = xr.Dataset(
        {"precip": (["time", "latitude", "longitude"], np.ones((5, 3, 3), dtype="float32"))},
        coords={
            "time": pd.date_range("2026-01-01", periods=5, freq="D"),
            "latitude": [4.0, 3.0, 2.0],
            "longitude": [1.0, 2.0, 3.0],
        },
    )
    ds.to_zarr(str(zarr_path), mode="w", consolidated=True)

    artifact = _artifact(artifact_id="a1", path=str(zarr_path))

    template = pystac.Collection(
        id="chirps3_precipitation_daily",
        description="test",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[1.0, 2.0, 3.0, 4.0]]),
            temporal=pystac.TemporalExtent([[datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 5, tzinfo=UTC)]]),
        ),
    )

    payload = stac_services._build_collection_with_xstac(artifact=artifact, template=template)

    assert payload["type"] == "Collection"
    cube_dims = payload["cube:dimensions"]
    assert "longitude" in cube_dims, f"expected 'longitude' in cube:dimensions, got {list(cube_dims)}"
    assert "latitude" in cube_dims, f"expected 'latitude' in cube:dimensions, got {list(cube_dims)}"
    assert "time" in cube_dims, f"expected 'time' in cube:dimensions, got {list(cube_dims)}"
    assert cube_dims["longitude"]["axis"] == "x"
    assert cube_dims["latitude"]["axis"] == "y"
    assert cube_dims["time"]["type"] == "temporal"
