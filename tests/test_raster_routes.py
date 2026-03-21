from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
import xarray as xr
from fastapi.testclient import TestClient

from eo_api.main import app
from eo_api.raster import routes as raster_routes


def test_raster_capabilities_report_missing_zarr_archive(monkeypatch: pytest.MonkeyPatch) -> None:
    client = TestClient(app)
    with monkeypatch.context() as patcher:
        patcher.setattr(raster_routes, "get_zarr_path", lambda dataset: None)
        response = client.get("/raster/chirps3_precipitation_daily/capabilities")

    assert response.status_code == 200
    body = response.json()
    assert body["collection_id"] == "chirps3_precipitation_daily"
    assert body["kind"] == "coverage"
    assert body["titiler"]["eligible"] is False
    assert body["titiler"]["reader"] == "xarray"
    assert "build_zarr" in body["titiler"]["reason"]


def test_raster_variables_route_rejects_resource_without_zarr_archive(monkeypatch: pytest.MonkeyPatch) -> None:
    client = TestClient(app)
    with monkeypatch.context() as patcher:
        patcher.setattr(raster_routes, "get_zarr_path", lambda dataset: None)
        response = client.get("/raster/chirps3_precipitation_daily/variables")

    assert response.status_code == 422
    body = response.json()["detail"]
    assert body["error"] == "raster_publication_unsupported"
    assert body["error_code"] == "RASTER_PUBLICATION_UNSUPPORTED"


def test_raster_variables_route_uses_zarr_backed_xarray_reader(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    zarr_path = tmp_path / "chirps3_precipitation_daily.zarr"
    xr.Dataset(
        data_vars={
            "precip": (("time", "lat", "lon"), np.arange(4, dtype=float).reshape(1, 2, 2)),
        },
        coords={
            "time": np.array(["2024-01-01"], dtype="datetime64[ns]"),
            "lat": [9.5, 10.5],
            "lon": [39.5, 40.5],
        },
    ).to_zarr(zarr_path, mode="w")

    monkeypatch.setattr(raster_routes, "get_zarr_path", lambda dataset: zarr_path)

    client = TestClient(app)
    response = client.get("/raster/chirps3_precipitation_daily/variables")

    assert response.status_code == 200
    assert response.json() == ["precip"]


def test_raster_preview_requires_datetime_for_temporal_dataset(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    zarr_path = tmp_path / "chirps3_precipitation_daily.zarr"
    xr.Dataset(
        data_vars={
            "precip": (("time", "lat", "lon"), np.arange(8, dtype=float).reshape(2, 2, 2)),
        },
        coords={
            "time": np.array(["2024-01-01", "2024-01-02"], dtype="datetime64[ns]"),
            "lat": [9.5, 10.5],
            "lon": [39.5, 40.5],
        },
    ).to_zarr(zarr_path, mode="w")

    monkeypatch.setattr(raster_routes, "get_zarr_path", lambda dataset: zarr_path)

    client = TestClient(app)
    response = client.get("/raster/chirps3_precipitation_daily/preview.png?variable=precip")

    assert response.status_code == 422
    body = response.json()["detail"]
    assert body["error"] == "raster_datetime_required"
    assert body["error_code"] == "RASTER_DATETIME_REQUIRED"


def test_raster_preview_with_datetime_renders_single_time_slice(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    zarr_path = tmp_path / "chirps3_precipitation_daily.zarr"
    xr.Dataset(
        data_vars={
            "precip": (("time", "lat", "lon"), np.arange(8, dtype=float).reshape(2, 2, 2)),
        },
        coords={
            "time": np.array(["2024-01-01", "2024-01-02"], dtype="datetime64[ns]"),
            "lat": [9.5, 10.5],
            "lon": [39.5, 40.5],
        },
    ).to_zarr(zarr_path, mode="w")

    monkeypatch.setattr(raster_routes, "get_zarr_path", lambda dataset: zarr_path)

    client = TestClient(app)
    response = client.get("/raster/chirps3_precipitation_daily/preview.png?variable=precip&datetime=2024-01-01")

    assert response.status_code == 200
    assert response.headers["content-type"] == "image/png"
    assert response.content


def test_raster_preview_with_aggregation_renders_time_reduced_image(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    zarr_path = tmp_path / "chirps3_precipitation_daily.zarr"
    xr.Dataset(
        data_vars={
            "precip": (("time", "lat", "lon"), np.arange(8, dtype=float).reshape(2, 2, 2)),
        },
        coords={
            "time": np.array(["2024-01-01", "2024-01-02"], dtype="datetime64[ns]"),
            "lat": [9.5, 10.5],
            "lon": [39.5, 40.5],
        },
    ).to_zarr(zarr_path, mode="w")

    monkeypatch.setattr(raster_routes, "get_zarr_path", lambda dataset: zarr_path)

    client = TestClient(app)
    response = client.get(
        "/raster/chirps3_precipitation_daily/preview.png"
        "?variable=precip&aggregation=sum&start=2024-01-01&end=2024-01-02"
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == "image/png"
    assert response.content


def test_raster_preview_rejects_aggregation_without_range(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    zarr_path = tmp_path / "chirps3_precipitation_daily.zarr"
    xr.Dataset(
        data_vars={
            "precip": (("time", "lat", "lon"), np.arange(8, dtype=float).reshape(2, 2, 2)),
        },
        coords={
            "time": np.array(["2024-01-01", "2024-01-02"], dtype="datetime64[ns]"),
            "lat": [9.5, 10.5],
            "lon": [39.5, 40.5],
        },
    ).to_zarr(zarr_path, mode="w")

    monkeypatch.setattr(raster_routes, "get_zarr_path", lambda dataset: zarr_path)

    client = TestClient(app)
    response = client.get("/raster/chirps3_precipitation_daily/preview.png?variable=precip&aggregation=sum")

    assert response.status_code == 422
    body = response.json()["detail"]
    assert body["error"] == "raster_temporal_query_invalid"
    assert body["error_code"] == "RASTER_TEMPORAL_QUERY_INVALID"


def test_raster_tile_outside_bounds_returns_404(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    zarr_path = tmp_path / "chirps3_precipitation_daily.zarr"
    xr.Dataset(
        data_vars={
            "precip": (("time", "lat", "lon"), np.arange(8, dtype=float).reshape(2, 2, 2)),
        },
        coords={
            "time": np.array(["2024-01-01", "2024-01-02"], dtype="datetime64[ns]"),
            "lat": [9.5, 10.5],
            "lon": [39.5, 40.5],
        },
    ).to_zarr(zarr_path, mode="w")

    monkeypatch.setattr(raster_routes, "get_zarr_path", lambda dataset: zarr_path)

    client = TestClient(app)
    response = client.get(
        "/raster/chirps3_precipitation_daily/tiles/WebMercatorQuad/6/30/31.png"
        "?variable=precip&aggregation=sum&start=2024-01-01&end=2024-01-02"
    )

    assert response.status_code == 404
