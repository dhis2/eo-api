from __future__ import annotations

import numpy as np
import pytest
import xarray as xr
from fastapi.testclient import TestClient

from eo_api.data_accessor.services.accessor import (
    get_coverage_summary,
    get_point_values,
    get_preview_summary,
)
from eo_api.main import app


def test_get_point_values_returns_time_series(monkeypatch: pytest.MonkeyPatch) -> None:
    ds = xr.Dataset(
        {"precip": (("time", "lat", "lon"), np.array([[[1.0, 2.0]], [[3.0, 4.0]]]))},
        coords={
            "time": np.array(["2024-01-01", "2024-02-01"], dtype="datetime64[ns]"),
            "lat": [8.0],
            "lon": [1.0, 2.0],
        },
    )
    monkeypatch.setattr("eo_api.data_accessor.services.accessor.get_data", lambda *args, **kwargs: ds)

    result = get_point_values(
        {"id": "chirps3_precipitation_daily", "variable": "precip", "period_type": "monthly"},
        lon=1.9,
        lat=8.0,
        start="2024-01",
        end="2024-02",
    )

    assert result["dataset_id"] == "chirps3_precipitation_daily"
    assert result["variable"] == "precip"
    assert result["value_count"] == 2
    assert result["resolved_point"] == {"lon": 2.0, "lat": 8.0}
    assert result["values"] == [{"period": "2024-01", "value": 2.0}, {"period": "2024-02", "value": 4.0}]


def test_point_query_outside_coverage_returns_typed_error(monkeypatch: pytest.MonkeyPatch) -> None:
    ds = xr.Dataset(
        {"precip": (("time", "lat", "lon"), np.array([[[1.0, 2.0]], [[3.0, 4.0]]]))},
        coords={
            "time": np.array(["2024-01-01", "2024-02-01"], dtype="datetime64[ns]"),
            "lat": [8.0],
            "lon": [1.0, 2.0],
        },
    )
    monkeypatch.setattr(
        "eo_api.data_registry.services.datasets.get_dataset",
        lambda dataset_id: {"id": dataset_id, "variable": "precip", "period_type": "monthly"},
    )
    monkeypatch.setattr("eo_api.data_accessor.services.accessor.get_data", lambda *args, **kwargs: ds)

    client = TestClient(app)
    response = client.get(
        "/retrieve/chirps3_precipitation_daily/point",
        params={"lon": 99.0, "lat": 99.0, "start": "2024-01", "end": "2024-02"},
    )

    assert response.status_code == 422
    body = response.json()["detail"]
    assert body["error"] == "point_query_invalid"
    assert body["error_code"] == "POINT_QUERY_INVALID"
    assert body["resource_id"] == "chirps3_precipitation_daily"


def test_get_preview_summary_returns_stats_and_sample(monkeypatch: pytest.MonkeyPatch) -> None:
    ds = xr.Dataset(
        {"precip": (("time", "lat", "lon"), np.array([[[1.0, 2.0]], [[3.0, 4.0]]]))},
        coords={
            "time": np.array(["2024-01-01", "2024-02-01"], dtype="datetime64[ns]"),
            "lat": [8.0],
            "lon": [1.0, 2.0],
        },
    )
    monkeypatch.setattr("eo_api.data_accessor.services.accessor.get_data", lambda *args, **kwargs: ds)

    result = get_preview_summary(
        {"id": "chirps3_precipitation_daily", "variable": "precip", "period_type": "monthly"},
        start="2024-01",
        end="2024-02",
        bbox=[1.0, 8.0, 2.0, 8.0],
        max_cells=3,
    )

    assert result["dataset_id"] == "chirps3_precipitation_daily"
    assert result["stats"] == {"min": 1.0, "max": 4.0, "mean": 2.5, "value_count": 4}
    assert result["dims"] == {"time": 2, "lat": 1, "lon": 2}
    assert len(result["sample"]) == 3
    assert result["sample"][0]["period"] == "2024-01"


def test_preview_endpoint_requires_complete_bbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "eo_api.data_registry.services.datasets.get_dataset",
        lambda dataset_id: {"id": dataset_id, "variable": "precip", "period_type": "monthly"},
    )

    client = TestClient(app)
    response = client.get(
        "/retrieve/chirps3_precipitation_daily/preview",
        params={"start": "2024-01", "end": "2024-02", "xmin": 1.0, "ymin": 8.0},
    )

    assert response.status_code == 422
    body = response.json()["detail"]
    assert body["error"] == "preview_invalid"
    assert body["error_code"] == "PREVIEW_INVALID"
    assert body["resource_id"] == "chirps3_precipitation_daily"


def test_get_coverage_summary_wraps_preview_and_full_coverage(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "eo_api.data_accessor.services.accessor.get_preview_summary",
        lambda *args, **kwargs: {
            "dataset_id": "chirps3_precipitation_daily",
            "variable": "precip",
            "requested": {"start": "2024-01", "end": "2024-02", "bbox": [1.0, 8.0, 2.0, 8.0]},
            "dims": {"time": 2, "lat": 1, "lon": 2},
            "stats": {"min": 1.0, "max": 4.0, "mean": 2.5, "value_count": 4},
            "sample": [{"period": "2024-01", "lat": 8.0, "lon": 1.0, "value": 1.0}],
        },
    )
    monkeypatch.setattr(
        "eo_api.data_accessor.services.accessor.get_data_coverage",
        lambda dataset: {
            "coverage": {
                "temporal": {"start": "2024-01", "end": "2024-12"},
                "spatial": {"xmin": 1.0, "ymin": 8.0, "xmax": 2.0, "ymax": 9.0},
            }
        },
    )

    result = get_coverage_summary(
        {"id": "chirps3_precipitation_daily", "variable": "precip", "period_type": "monthly"},
        start="2024-01",
        end="2024-02",
        bbox=[1.0, 8.0, 2.0, 8.0],
        max_cells=3,
    )

    assert result["coverage"]["temporal"] == {"start": "2024-01", "end": "2024-12"}
    assert result["coverage"]["spatial"] == {"xmin": 1.0, "ymin": 8.0, "xmax": 2.0, "ymax": 9.0}
    assert result["subset"]["stats"]["mean"] == 2.5
    assert result["subset"]["sample"][0]["value"] == 1.0


def test_coverage_endpoint_requires_complete_bbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "eo_api.data_registry.services.datasets.get_dataset",
        lambda dataset_id: {"id": dataset_id, "variable": "precip", "period_type": "monthly"},
    )

    client = TestClient(app)
    response = client.get(
        "/retrieve/chirps3_precipitation_daily/coverage",
        params={"start": "2024-01", "end": "2024-02", "xmin": 1.0, "ymin": 8.0},
    )

    assert response.status_code == 422
    body = response.json()["detail"]
    assert body["error"] == "coverage_invalid"
    assert body["error_code"] == "COVERAGE_INVALID"
    assert body["resource_id"] == "chirps3_precipitation_daily"
