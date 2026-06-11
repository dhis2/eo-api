"""Tests for the climate-normals dataset plugin (ClimateNormalsPlugin)."""

from __future__ import annotations

import asyncio

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from open_climate_service.plugins.datasets import climate_normals as cn
from open_climate_service.plugins.datasets.climate_normals import ClimateNormalsPlugin


def _synthetic_region(*, var: str = "t2m", time_dim: str = "valid_time", years: tuple[str, str] = ("1991", "1992")):
    """Daily data where each step's value equals its day-of-year, so the day-of-year
    climatology is exactly the day-of-year (handy for assertions)."""
    times = pd.date_range(f"{years[0]}-01-01", f"{years[1]}-12-31", freq="D")
    doy = times.dayofyear.to_numpy().astype("float32")
    data = np.broadcast_to(doy[:, None, None], (len(times), 2, 2)).copy()
    return xr.Dataset(
        {var: ((time_dim, "y", "x"), data)},
        coords={time_dim: times, "y": [1.0, 2.0], "x": [10.0, 11.0]},
    )


def _edh_plugin(**overrides: object) -> ClimateNormalsPlugin:
    kwargs: dict[str, object] = dict(edh_variable="t2m", variable="t2m", smoothing_window=0, unit_transform=None)
    kwargs.update(overrides)
    return ClimateNormalsPlugin(**kwargs)  # type: ignore[arg-type]


def test_periods_span_a_nominal_leap_year() -> None:
    periods = asyncio.run(_edh_plugin().periods("ignored", "ignored"))
    assert len(periods) == 366
    assert periods[0] == "2000-01-01"
    assert periods[-1] == "2000-12-31"


def test_compute_climatology_groups_by_dayofyear(monkeypatch: pytest.MonkeyPatch) -> None:
    plugin = _edh_plugin()
    monkeypatch.setattr(plugin, "_load_reference", lambda bbox: _synthetic_region())

    clim = plugin._compute_climatology([10.0, 1.0, 11.0, 2.0])

    assert "dayofyear" in clim.dims
    assert clim.sizes["dayofyear"] == 366  # leap day present (1992)
    assert float(clim["t2m"].sel(dayofyear=100).isel(y=0, x=0)) == pytest.approx(100.0)
    assert float(clim["t2m"].sel(dayofyear=366).isel(y=0, x=0)) == pytest.approx(366.0)


def test_fetch_period_maps_nominal_date_to_dayofyear(monkeypatch: pytest.MonkeyPatch) -> None:
    plugin = _edh_plugin()
    monkeypatch.setattr(plugin, "_load_reference", lambda bbox: _synthetic_region())

    ds = plugin._fetch_sync("2000-04-09", [10.0, 1.0, 11.0, 2.0])  # 2000-04-09 is day-of-year 100

    assert ds.sizes["t"] == 1
    assert str(ds["t"].values[0])[:10] == "2000-04-09"
    assert "dayofyear" not in ds.dims  # collapsed to the single fetched day
    assert float(ds["t2m"].isel(t=0, y=0, x=0)) == pytest.approx(100.0)


def test_unit_transform_is_applied(monkeypatch: pytest.MonkeyPatch) -> None:
    plugin = _edh_plugin(unit_transform="kelvin_to_celsius")
    monkeypatch.setattr(plugin, "_load_reference", lambda bbox: _synthetic_region())

    clim = plugin._compute_climatology([10.0, 1.0, 11.0, 2.0])

    assert float(clim["t2m"].sel(dayofyear=300).isel(y=0, x=0)) == pytest.approx(300.0 - 273.15)


def test_smoothing_window_changes_values(monkeypatch: pytest.MonkeyPatch) -> None:
    plugin = _edh_plugin(smoothing_window=31)
    monkeypatch.setattr(plugin, "_load_reference", lambda bbox: _synthetic_region())

    clim = plugin._compute_climatology([10.0, 1.0, 11.0, 2.0])

    # A 31-day circular mean around a linear ramp shifts mid-year values only slightly,
    # but the wrap-around makes day-of-year 1 differ markedly from its raw value (1.0).
    assert float(clim["t2m"].sel(dayofyear=1).isel(y=0, x=0)) != pytest.approx(1.0)


def test_load_reference_edh_normalises_longitude_and_renames(monkeypatch: pytest.MonkeyPatch) -> None:
    times = pd.date_range("1991-01-01", "1991-12-31", freq="D")
    edh = xr.Dataset(
        {"t2m": (("valid_time", "latitude", "longitude"), np.ones((len(times), 2, 2), dtype="float32"))},
        coords={"valid_time": times, "latitude": [2.0, 1.0], "longitude": [350.0, 351.0]},  # 0–360
    )
    monkeypatch.setattr(cn, "_edh_open_daily", lambda: edh)
    plugin = _edh_plugin(period=[1991, 1991])

    region = plugin._load_reference([-10.0, 1.0, -9.0, 2.0])  # bbox in -180/180; lon 350 -> -10

    assert "x" in region.dims and "y" in region.dims
    assert float(region["x"].min()) < 0  # 0–360 normalised to -180/180


def test_param_validation() -> None:
    with pytest.raises(ValueError, match="edh_variable"):
        ClimateNormalsPlugin(variable="t2m", edh_variable="")
