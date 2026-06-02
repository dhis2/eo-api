"""Tests for the SPI, CDD, CWD, and TX-days-above climate index plugin processes."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from fastapi.testclient import TestClient

from open_climate_service.plugins.processes.climate_indices import cdd, cwd, spi, tx_days_above
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


# ---------------------------------------------------------------------------
# CDD
# ---------------------------------------------------------------------------


def test_cdd_is_registered_as_process() -> None:
    meta = get_process_metadata(cdd)
    assert meta is not None
    assert meta["id"] == "cdd"
    assert "CDD" in meta["summary"] or "consecutive dry" in meta["summary"].lower()


def test_cdd_thresh_default() -> None:
    meta = get_process_metadata(cdd)
    assert meta is not None
    thresh_param = next(p for p in meta["parameters"] if p["name"] == "thresh")
    assert thresh_param["default"] == "1 mm/day"


def test_cdd_returns_annual_dataarray(daily_precip: xr.DataArray) -> None:
    result = cdd(daily_precip)
    assert isinstance(result, xr.DataArray)
    assert "time" in result.dims
    # 5 years of data → 5 annual values (last may be NaN for incomplete year)
    assert result.sizes["time"] >= 4


def test_cdd_monthly_freq(daily_precip: xr.DataArray) -> None:
    result = cdd(daily_precip, freq="MS")
    assert isinstance(result, xr.DataArray)
    # Monthly resampling → ~60 steps for 5 years
    assert result.sizes["time"] >= 12


# ---------------------------------------------------------------------------
# CWD
# ---------------------------------------------------------------------------


def test_cwd_is_registered_as_process() -> None:
    meta = get_process_metadata(cwd)
    assert meta is not None
    assert meta["id"] == "cwd"
    assert "CWD" in meta["summary"] or "consecutive wet" in meta["summary"].lower()


def test_cwd_returns_annual_dataarray(daily_precip: xr.DataArray) -> None:
    result = cwd(daily_precip)
    assert isinstance(result, xr.DataArray)
    assert "time" in result.dims


def test_cdd_cwd_are_complementary(daily_precip: xr.DataArray) -> None:
    r_cdd = cdd(daily_precip)
    r_cwd = cwd(daily_precip)
    assert not np.allclose(r_cdd.values, r_cwd.values, equal_nan=True)


# ---------------------------------------------------------------------------
# TX days above
# ---------------------------------------------------------------------------


@pytest.fixture()
def daily_tasmax() -> xr.DataArray:
    """Five years of synthetic daily maximum temperature (degC)."""
    rng = np.random.default_rng(7)
    time = pd.date_range("2000-01-01", periods=365 * 5, freq="D")
    data = rng.normal(loc=20.0, scale=8.0, size=len(time)).astype(np.float32)
    da = xr.DataArray(data, dims=["time"], coords={"time": time})
    da.attrs["units"] = "degC"
    return da


def test_tx_days_above_is_registered_as_process() -> None:
    meta = get_process_metadata(tx_days_above)
    assert meta is not None
    assert meta["id"] == "tx_days_above"
    assert "TX" in meta["summary"] or "maximum temperature" in meta["summary"].lower()


def test_tx_days_above_thresh_default() -> None:
    meta = get_process_metadata(tx_days_above)
    assert meta is not None
    thresh_param = next(p for p in meta["parameters"] if p["name"] == "thresh")
    assert thresh_param["default"] == "25 degC"


def test_tx_days_above_returns_annual_dataarray(daily_tasmax: xr.DataArray) -> None:
    result = tx_days_above(daily_tasmax)
    assert isinstance(result, xr.DataArray)
    assert "time" in result.dims


def test_tx_days_above_higher_threshold_gives_fewer_days(daily_tasmax: xr.DataArray) -> None:
    r25 = tx_days_above(daily_tasmax, thresh="25 degC")
    r35 = tx_days_above(daily_tasmax, thresh="35 degC")
    # Fewer days exceed 35°C than 25°C
    assert float(r35.mean()) <= float(r25.mean())


# ---------------------------------------------------------------------------
# API catalog — all four processes appear
# ---------------------------------------------------------------------------


def test_all_four_indices_appear_in_get_processes(client: TestClient) -> None:
    response = client.get("/processes")
    assert response.status_code == 200
    ids = {p["id"] for p in response.json()["processes"]}
    assert {"spi", "cdd", "cwd", "tx_days_above"} <= ids
