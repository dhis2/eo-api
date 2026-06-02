"""Tests for the SPI climate index plugin process."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from fastapi.testclient import TestClient

from open_climate_service.plugins.processes.climate_indices import spi
from open_climate_service.process import get_process_metadata


@pytest.fixture()
def daily_precip() -> xr.DataArray:
    """Five years of synthetic daily precipitation (kg m-2 s-1)."""
    rng = np.random.default_rng(42)
    time = pd.date_range("2000-01-01", periods=365 * 5, freq="D")
    data = rng.exponential(scale=3.0, size=len(time)).astype(np.float32)
    da = xr.DataArray(data, dims=["time"], coords={"time": time})
    da.attrs["units"] = "kg m-2 s-1"
    return da


def test_spi_is_registered_as_process() -> None:
    meta = get_process_metadata(spi)
    assert meta is not None
    assert meta["id"] == "spi"
    assert "SPI" in meta["summary"]


def test_spi_parameters_include_window_and_calibration() -> None:
    meta = get_process_metadata(spi)
    assert meta is not None
    param_names = {p["name"] for p in meta["parameters"]}
    assert {"pr", "window", "cal_start", "cal_end", "freq"} <= param_names


def test_spi_window_defaults() -> None:
    meta = get_process_metadata(spi)
    assert meta is not None
    window_param = next(p for p in meta["parameters"] if p["name"] == "window")
    assert window_param["optional"] is True
    assert window_param["default"] == 1


def test_spi1_returns_dataarray_with_time_dimension(daily_precip: xr.DataArray) -> None:
    result = spi(daily_precip, window=1)
    assert isinstance(result, xr.DataArray)
    assert "time" in result.dims


def test_spi1_output_length_matches_monthly_resampling(daily_precip: xr.DataArray) -> None:
    result = spi(daily_precip, window=1)
    # 5 years of daily data → 60 monthly output steps
    assert result.sizes["time"] == 60


def test_spi3_produces_different_values_than_spi1(daily_precip: xr.DataArray) -> None:
    r1 = spi(daily_precip, window=1)
    r3 = spi(daily_precip, window=3)
    assert not np.allclose(r1.values, r3.values, equal_nan=True)


def test_spi_with_calibration_period(daily_precip: xr.DataArray) -> None:
    result = spi(daily_precip, window=1, cal_start="2000-01-01", cal_end="2002-12-31")
    assert isinstance(result, xr.DataArray)
    assert result.sizes["time"] == 60


def test_spi_appears_in_get_processes_endpoint(client: TestClient) -> None:
    response = client.get("/processes")
    assert response.status_code == 200
    ids = {p["id"] for p in response.json()["processes"]}
    assert "spi" in ids


def test_spi_process_summary_in_catalog(client: TestClient) -> None:
    response = client.get("/processes")
    procs = {p["id"]: p for p in response.json()["processes"]}
    assert "spi" in procs
    assert "SPI" in procs["spi"]["summary"]
