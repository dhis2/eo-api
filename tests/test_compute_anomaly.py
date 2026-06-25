"""Tests for the built-in `compute_anomaly` openEO process."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from open_climate_service.plugins.processes.compute_anomaly import compute_anomaly


def _observed_doy(scale: float = 1.0, offset: float = 0.0) -> xr.DataArray:
    """Daily 2024 cube where each day's value is ``scale*dayofyear + offset``."""
    times = pd.date_range("2024-01-01", "2024-12-31", freq="D")  # leap year → 366 days
    doy = times.dayofyear.to_numpy().astype("float32")
    data = np.broadcast_to((scale * doy + offset)[:, None, None], (len(times), 2, 2)).copy()
    return xr.DataArray(data, dims=("t", "y", "x"), coords={"t": times, "y": [1.0, 2.0], "x": [1.0, 2.0]})


def _normal_doy() -> xr.DataArray:
    """Day-of-year normal where each value equals its day-of-year (1..366)."""
    doy = np.arange(1, 367, dtype="float32")
    data = np.broadcast_to(doy[:, None, None], (366, 2, 2)).copy()
    return xr.DataArray(
        data,
        dims=("dayofyear", "y", "x"),
        coords={"dayofyear": list(range(1, 367)), "y": [1.0, 2.0], "x": [1.0, 2.0]},
    )


def test_absolute_anomaly_aligns_dayofyear_and_subtracts() -> None:
    out = compute_anomaly(_observed_doy(offset=5.0), _normal_doy(), method="absolute")

    assert "dayofyear" not in out.dims and set(out.dims) >= {"t", "y", "x"}
    assert out.sizes["t"] == 366
    # observed(d) = dayofyear+5, normal(d) = dayofyear  →  anomaly = 5 everywhere
    assert float(out.isel(t=99, y=0, x=0)) == pytest.approx(5.0)
    assert float(out.max()) == pytest.approx(5.0) and float(out.min()) == pytest.approx(5.0)


def test_relative_anomaly_is_percent_of_normal() -> None:
    # observed = 2 × normal  →  relative = 100·(2n − n)/n = 100 %
    out = compute_anomaly(_observed_doy(scale=2.0), _normal_doy(), method="relative")
    assert float(out.isel(t=180, y=0, x=0)) == pytest.approx(100.0)


def test_month_aligned_anomaly() -> None:
    # Monthly observed + monthly normal: each month's value is month+1 and the normal is
    # month, so the anomaly is 1 everywhere. earthkit aligns on the normal's `month` axis
    # and keeps the (monthly) observed time axis.
    times = pd.date_range("2024-01-01", "2024-12-01", freq="MS")  # 12 monthly steps
    month = times.month.to_numpy().astype("float32")
    obs = xr.DataArray(
        np.broadcast_to((month + 1.0)[:, None, None], (len(times), 2, 2)).copy(),
        dims=("t", "y", "x"),
        coords={"t": times, "y": [1.0, 2.0], "x": [1.0, 2.0]},
    )
    normal = xr.DataArray(
        np.broadcast_to(np.arange(1, 13, dtype="float32")[:, None, None], (12, 2, 2)).copy(),
        dims=("month", "y", "x"),
        coords={"month": list(range(1, 13)), "y": [1.0, 2.0], "x": [1.0, 2.0]},
    )

    out = compute_anomaly(obs, normal, method="absolute")

    assert "month" not in out.dims and out.sizes["t"] == len(times)
    assert float(out.max()) == pytest.approx(1.0) and float(out.min()) == pytest.approx(1.0)


def test_rejects_unsupported_method() -> None:
    with pytest.raises(ValueError, match="method"):
        compute_anomaly(_observed_doy(), _normal_doy(), method="bogus")


def test_standardised_method_points_to_std_normal() -> None:
    with pytest.raises(ValueError, match="standard-deviation|standardised"):
        compute_anomaly(_observed_doy(), _normal_doy(), method="standardised")


def test_requires_datetime_observed_axis() -> None:
    obs = xr.DataArray(
        np.zeros((3, 2, 2), dtype="float32"),
        dims=("t", "y", "x"),
        coords={"t": [0, 1, 2], "y": [1.0, 2.0], "x": [1.0, 2.0]},
    )
    with pytest.raises(ValueError, match="datetime"):
        compute_anomaly(obs, _normal_doy())


def test_requires_ordinal_normal_axis() -> None:
    bad_normal = xr.DataArray(
        np.zeros((2, 2), dtype="float32"), dims=("y", "x"), coords={"y": [1.0, 2.0], "x": [1.0, 2.0]}
    )
    with pytest.raises(ValueError, match="dayofyear|month"):
        compute_anomaly(_observed_doy(), bad_normal)


def test_aligns_grids_differing_by_float_noise() -> None:
    # Observed and normal can be produced independently (e.g. CDS-derived observed vs an
    # EDH normal whose lon was remapped from [0, 360)), leaving spatial coords off by
    # ~1e-11. Exact-equality alignment would intersect to an empty grid; the process must
    # snap the normal onto the observed grid so the subtraction survives.
    normal = _normal_doy()
    noisy = normal.assign_coords(x=normal.x.values - 1e-11, y=normal.y.values + 1e-11)

    out = compute_anomaly(_observed_doy(offset=5.0), noisy, method="absolute")

    assert out.sizes["x"] == 2 and out.sizes["y"] == 2  # not collapsed to an empty grid
    assert bool(np.isfinite(out).all())
    assert float(out.isel(t=99, y=0, x=0)) == pytest.approx(5.0)


def test_anomaly_preserves_dask_laziness() -> None:
    out = compute_anomaly(_observed_doy(offset=5.0).chunk({"y": 1, "x": 1}), _normal_doy().chunk({"y": 1, "x": 1}))
    assert out.chunks is not None
