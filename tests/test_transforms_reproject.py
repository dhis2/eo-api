"""Tests for the reproject_to_instance_crs transform."""

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import xarray as xr

from open_climate_service.transforms.reproject import reproject_to_instance_crs

_DATASET: dict[str, Any] = {"id": "test", "variable": "value"}


def _make_wgs84_dataset() -> xr.Dataset:
    """Minimal WGS84 raster dataset with x/y dims in degrees."""
    return xr.Dataset(
        {"value": (["time", "y", "x"], np.ones((1, 4, 4), dtype="float32"))},
        coords={
            "time": ["2024-01-01"],
            "y": [60.0, 59.0, 58.0, 57.0],
            "x": [10.0, 11.0, 12.0, 13.0],
        },
    )


def test_noop_when_instance_crs_matches_source(monkeypatch: pytest.MonkeyPatch) -> None:
    """No reprojection when the instance CRS equals the source CRS."""
    monkeypatch.delenv("CLIMATE_SERVICE_CONFIG", raising=False)
    ds = _make_wgs84_dataset()
    result = reproject_to_instance_crs(ds, _DATASET, source_crs="EPSG:4326")
    # Dataset returned unchanged — same object, no reprojection called
    assert result is ds


def test_noop_returns_dataset_with_original_coords(monkeypatch: pytest.MonkeyPatch) -> None:
    """Coordinates remain in degrees when no reprojection is needed."""
    monkeypatch.delenv("CLIMATE_SERVICE_CONFIG", raising=False)
    ds = _make_wgs84_dataset()
    result = reproject_to_instance_crs(ds, _DATASET, source_crs="EPSG:4326")
    assert float(result.x.max()) == pytest.approx(13.0)
    assert float(result.y.max()) == pytest.approx(60.0)


def test_calls_reproject_with_target_crs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """When instance CRS differs from source, rio.reproject is called with the target CRS."""
    config_file = tmp_path / "climate-service.yaml"
    config_file.write_text("data_dir: ./data\ncrs: EPSG:25833\n", encoding="utf-8")
    monkeypatch.setenv("CLIMATE_SERVICE_CONFIG", str(config_file))

    ds = _make_wgs84_dataset()
    reprojected_ds = _make_wgs84_dataset()

    rio_mock = MagicMock()
    rio_mock.set_spatial_dims.return_value = ds
    rio_mock.write_crs.return_value = ds
    rio_mock.reproject.return_value = reprojected_ds

    with patch.object(type(ds), "rio", new_callable=lambda: property(lambda self: rio_mock)):
        result = reproject_to_instance_crs(ds, _DATASET, source_crs="EPSG:4326")

    rio_mock.set_spatial_dims.assert_called_once_with(x_dim="x", y_dim="y")
    rio_mock.write_crs.assert_called_once_with("EPSG:4326")
    rio_mock.reproject.assert_called_once_with("EPSG:25833")
    assert result is reprojected_ds


def test_skips_reproject_when_source_equals_target(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """When source_crs is already the instance CRS, reproject is never called."""
    config_file = tmp_path / "climate-service.yaml"
    config_file.write_text("data_dir: ./data\ncrs: EPSG:25833\n", encoding="utf-8")
    monkeypatch.setenv("CLIMATE_SERVICE_CONFIG", str(config_file))

    ds = _make_wgs84_dataset()
    result = reproject_to_instance_crs(ds, _DATASET, source_crs="EPSG:25833")
    assert result is ds
