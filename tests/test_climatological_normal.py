"""Tests for the built-in `climatological_normal` openEO process."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from open_climate_service.plugins.processes.climatological_normal import climatological_normal


def _daily_cube() -> xr.DataArray:
    """Two years of daily data where each step equals its day-of-year."""
    times = pd.date_range("1991-01-01", "1992-12-31", freq="D")
    doy = times.dayofyear.to_numpy().astype("float32")
    data = np.broadcast_to(doy[:, None, None], (len(times), 2, 2)).copy()
    return xr.DataArray(data, dims=("t", "y", "x"), coords={"t": times, "y": [1.0, 2.0], "x": [1.0, 2.0]})


def test_climatological_normal_reduces_time_to_dayofyear() -> None:
    out = climatological_normal(_daily_cube(), smoothing_window=0)

    assert "dayofyear" in out.dims
    assert "t" not in out.dims
    assert out.sizes["dayofyear"] == 366  # leap day present (1992)
    assert float(out.sel(dayofyear=100).isel(y=0, x=0)) == pytest.approx(100.0)
    assert float(out.sel(dayofyear=366).isel(y=0, x=0)) == pytest.approx(366.0)


def test_climatological_normal_applies_smoothing() -> None:
    out = climatological_normal(_daily_cube(), smoothing_window=31)
    # Circular wrap makes day-of-year 1 differ markedly from its raw value (1.0).
    assert float(out.sel(dayofyear=1).isel(y=0, x=0)) != pytest.approx(1.0)


def test_climatological_normal_requires_temporal_dimension() -> None:
    flat = xr.DataArray(np.zeros((2, 2), dtype="float32"), dims=("y", "x"), coords={"y": [1.0, 2.0], "x": [1.0, 2.0]})
    with pytest.raises(ValueError, match="temporal dimension"):
        climatological_normal(flat)
