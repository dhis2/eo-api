"""Tests for the /normals endpoint and service."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import xarray as xr

from open_climate_service.normals.services import (
    _EDH_DAILY_SOURCES,
    _circular_rolling_mean,
)


def _make_daily_region(variable: str, value: float, n_days: int = 365) -> xr.Dataset:
    """Minimal daily EDH-style dataset for testing."""
    import pandas as pd

    times = pd.date_range("1991-01-01", periods=n_days, freq="D")
    data = np.full((n_days, 2, 2), value, dtype=np.float32)
    return xr.Dataset(
        {variable: (("valid_time", "latitude", "longitude"), data)},
        coords={"valid_time": times, "latitude": [9.0, 8.9], "longitude": [346.6, 346.7]},
    )


def test_edh_daily_sources_includes_temperature_and_precipitation() -> None:
    assert "era5land_temperature_daily" in _EDH_DAILY_SOURCES
    assert "era5land_precipitation_daily" in _EDH_DAILY_SOURCES
    assert _EDH_DAILY_SOURCES["era5land_temperature_daily"] == ("t2m", "kelvin_to_celsius")
    assert _EDH_DAILY_SOURCES["era5land_precipitation_daily"] == ("tp", "metres_to_mm")


def test_circular_rolling_mean_preserves_shape() -> None:
    da = xr.DataArray(
        np.random.rand(366, 3, 4).astype(np.float32),
        dims=["dayofyear", "y", "x"],
    )
    result = _circular_rolling_mean(da, window=31)
    assert result.shape == da.shape


def test_circular_rolling_mean_smooths_spike() -> None:
    vals = np.zeros((366, 1, 1), dtype=np.float32)
    vals[180] = 100.0  # spike at day 181
    da = xr.DataArray(vals, dims=["dayofyear", "y", "x"])
    result = _circular_rolling_mean(da, window=31)
    assert float(result[180, 0, 0]) < 10.0  # spike is smoothed
    assert float(result[0, 0, 0]) < 1.0     # days far from spike near zero


def test_compute_normals_temperature(tmp_path: pytest.TempPathFactory) -> None:
    from open_climate_service.normals.schemas import NormalsRequest
    from open_climate_service.normals.services import compute_normals

    region = _make_daily_region("t2m", 300.0, n_days=365 * 5)  # 5 years

    request = NormalsRequest(
        source_dataset_id="era5land_temperature_daily",
        period=(1991, 1995),
        smoothing_window=0,
    )
    bbox = [-13.5, 6.9, -10.1, 10.0]

    with (
        patch("open_climate_service.normals.services._edh_open_daily", return_value=region),
        patch("open_climate_service.normals.services._normals_store_path", return_value=tmp_path / "t2m.zarr"),
        patch("open_climate_service.normals.services._register_normals_artifact") as mock_reg,
        patch(  # noqa: E501
            "open_climate_service.normals.services.artifact_services.get_dataset_summary_for_artifact_or_404"
        ) as mock_ds,
    ):
        mock_reg.return_value = MagicMock(artifact_id="test-id")
        mock_ds.return_value = MagicMock(model_dump=lambda: {})
        response = compute_normals(request, bbox)

    assert response.normals_id == "era5land_temperature_daily_normals_1991_1995"
    assert response.status == "completed"
    # Verify the store was written with dayofyear dimension
    ds = xr.open_zarr(tmp_path / "t2m.zarr", consolidated=False)
    assert "dayofyear" in ds.dims
    assert ds.sizes["dayofyear"] == 366
    # K→°C: 300 - 273.15 = 26.85
    assert abs(float(ds.t2m.mean()) - 26.85) < 0.01


def test_compute_normals_precipitation(tmp_path: pytest.TempPathFactory) -> None:
    from open_climate_service.normals.schemas import NormalsRequest
    from open_climate_service.normals.services import compute_normals

    region = _make_daily_region("tp", 0.005, n_days=365 * 5)  # 0.005 m/day

    request = NormalsRequest(
        source_dataset_id="era5land_precipitation_daily",
        period=(1991, 1995),
        smoothing_window=0,
    )
    bbox = [-13.5, 6.9, -10.1, 10.0]

    with (
        patch("open_climate_service.normals.services._edh_open_daily", return_value=region),
        patch("open_climate_service.normals.services._normals_store_path", return_value=tmp_path / "tp.zarr"),
        patch("open_climate_service.normals.services._register_normals_artifact") as mock_reg,
        patch(  # noqa: E501
            "open_climate_service.normals.services.artifact_services.get_dataset_summary_for_artifact_or_404"
        ) as mock_ds,
    ):
        mock_reg.return_value = MagicMock(artifact_id="test-id")
        mock_ds.return_value = MagicMock(model_dump=lambda: {})
        response = compute_normals(request, bbox)

    assert response.normals_id == "era5land_precipitation_daily_normals_1991_1995"
    ds = xr.open_zarr(tmp_path / "tp.zarr", consolidated=False)
    assert "dayofyear" in ds.dims
    # m→mm: 0.005 × 1000 = 5.0 mm/day
    assert abs(float(ds.tp.mean()) - 5.0) < 0.01


def test_compute_normals_rejects_unsupported_source() -> None:
    from open_climate_service.normals.schemas import NormalsRequest
    from open_climate_service.normals.services import compute_normals

    request = NormalsRequest(source_dataset_id="chirps3_precipitation_daily", period=(1991, 2020))
    with pytest.raises(ValueError, match="not supported"):
        compute_normals(request, [-13.5, 6.9, -10.1, 10.0])
