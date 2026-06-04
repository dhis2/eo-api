import asyncio
from datetime import date, datetime
from typing import cast
from unittest.mock import patch

import numpy as np
import pytest
import xarray as xr

from open_climate_service.plugins.datasets.era5_land import (
    ERA5LandHourlySingleBandPlugin,
    ERA5LandMonthlyPrecipitationPlugin,
    ERA5LandMonthlySingleBandPlugin,
    ERA5LandPrecipitationPlugin,
    _monthly_availability_cutoff,
)


def test_era5_land_periods_enumerate_hours() -> None:
    plugin = ERA5LandHourlySingleBandPlugin(variable="t2m")

    periods = asyncio.run(plugin.periods("2026-01-01T00", "2026-01-01T02"))

    assert periods == ["2026-01-01T00", "2026-01-01T01", "2026-01-01T02"]


def test_era5_land_probe_uses_region_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    plugin = ERA5LandHourlySingleBandPlugin(variable="t2m")

    def fake_region_for_bbox(bbox: list[float]) -> xr.Dataset:
        _ = bbox
        return xr.Dataset(
            {"t2m": (("valid_time", "latitude", "longitude"), np.ones((1, 3, 4), dtype=np.float32))},
            coords={
                "valid_time": ["2026-01-01T00:00:00"],
                "latitude": [3.0, 2.0, 1.0],
                "longitude": [4.0, 5.0, 6.0, 7.0],
            },
        )

    monkeypatch.setattr(plugin, "_region_for_bbox", fake_region_for_bbox)

    spec = asyncio.run(plugin.probe([1.0, 2.0, 3.0, 4.0]))

    assert spec.shape == (3, 4)
    assert spec.time_dim == "t"
    assert spec.x_dim == "x"
    assert spec.y_dim == "y"


def test_era5_land_fetch_period_normalizes_coordinates(monkeypatch: pytest.MonkeyPatch) -> None:
    plugin = ERA5LandHourlySingleBandPlugin(variable="t2m")

    def fake_region_for_bbox(bbox: list[float]) -> xr.Dataset:
        _ = bbox
        return xr.Dataset(
            {"t2m": (("valid_time", "latitude", "longitude"), np.array([[[280.0]], [[281.0]]], dtype=np.float32))},
            coords={
                "valid_time": np.array(["2026-01-01T00:00:00", "2026-01-01T01:00:00"], dtype="datetime64[ns]"),
                "latitude": [9.0],
                "longitude": [30.0],
            },
        )

    monkeypatch.setattr(plugin, "_region_for_bbox", fake_region_for_bbox)

    dataset = asyncio.run(plugin.fetch_period("2026-01-01T01", [1.0, 2.0, 3.0, 4.0]))

    assert "t" in dataset.dims
    assert "x" in dataset.dims
    assert "y" in dataset.dims
    assert "longitude" not in dataset.dims
    assert "latitude" not in dataset.dims
    assert dataset["t2m"].values.tolist() == [[[281.0]]]


def test_era5_land_precipitation_plugin_defaults_to_tp(monkeypatch: pytest.MonkeyPatch) -> None:
    plugin = ERA5LandPrecipitationPlugin()

    def fake_region_for_bbox(bbox: list[float]) -> xr.Dataset:
        _ = bbox
        return xr.Dataset(
            {"tp": (("valid_time", "latitude", "longitude"), np.array([[[0.002]]], dtype=np.float32))},
            coords={
                "valid_time": np.array(["2026-01-01T00:00:00"], dtype="datetime64[ns]"),
                "latitude": [9.0],
                "longitude": [30.0],
            },
        )

    monkeypatch.setattr(plugin, "_region_for_bbox", fake_region_for_bbox)

    dataset = asyncio.run(plugin.fetch_period("2026-01-01T00", [1.0, 2.0, 3.0, 4.0]))

    assert list(dataset.data_vars) == ["tp"]
    np.testing.assert_allclose(dataset["tp"].values, [[[0.002]]])


def test_era5_land_cached_region_closes_previous_dataset_when_bbox_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plugin = ERA5LandHourlySingleBandPlugin(variable="t2m")
    opened: list[object] = []

    class FakeRegion:
        def __init__(self, label: str) -> None:
            self.label = label
            self.closed = False

        def close(self) -> None:
            self.closed = True

    def fake_open_region(variable: str, bbox: tuple[float, float, float, float]) -> object:
        region = FakeRegion(f"{variable}:{bbox}")
        opened.append(region)
        return region

    monkeypatch.setattr("open_climate_service.plugins.datasets.era5_land._open_era5_land_region", fake_open_region)

    first = plugin._region_for_bbox([1.0, 2.0, 3.0, 4.0])
    second = plugin._region_for_bbox([2.0, 3.0, 4.0, 5.0])

    assert first is opened[0]
    assert second is opened[1]
    assert cast(FakeRegion, first).closed is True
    assert cast(FakeRegion, second).closed is False


def test_era5_land_monthly_plugin_rejects_unknown_variable() -> None:
    with pytest.raises(ValueError, match="unsupported variable"):
        ERA5LandMonthlySingleBandPlugin(variable="unknown")


def test_era5_land_monthly_probe_derives_shape_from_bbox() -> None:
    plugin = ERA5LandMonthlySingleBandPlugin(variable="t2m")
    spec = asyncio.run(plugin.probe([-13.5, 6.9, -10.1, 10.0]))
    assert spec.shape == (31, 34)
    assert spec.crs == 4326
    assert spec.time_dim == "t"
    assert spec.x_dim == "x"
    assert spec.y_dim == "y"


def test_era5_land_monthly_periods_enumerates_months() -> None:
    plugin = ERA5LandMonthlySingleBandPlugin(variable="t2m")
    with patch(
        "open_climate_service.plugins.datasets.era5_land._monthly_availability_cutoff",
        return_value=date(2024, 3, 1),
    ):
        periods = asyncio.run(plugin.periods("2024-01", "2024-06"))
    assert periods == ["2024-01", "2024-02", "2024-03"]


def test_era5_land_monthly_periods_empty_when_start_after_cutoff() -> None:
    plugin = ERA5LandMonthlySingleBandPlugin(variable="t2m")
    with patch(
        "open_climate_service.plugins.datasets.era5_land._monthly_availability_cutoff",
        return_value=date(2023, 12, 1),
    ):
        periods = asyncio.run(plugin.periods("2024-01", "2024-06"))
    assert periods == []


def _make_monthly_nc(variable: str, value: float, kelvin: bool = False) -> xr.Dataset:
    data = np.array([[[[value]]]], dtype=np.float32)
    return xr.Dataset(
        {variable: (("valid_time", "latitude", "longitude"), data[0])},
        coords={
            "valid_time": np.array(["2024-01-01"], dtype="datetime64[ns]"),
            "latitude": [9.0],
            "longitude": [30.0],
        },
    )


def test_era5_land_monthly_fetch_renames_coords_and_converts_temperature(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pytest.TempPathFactory
) -> None:
    plugin = ERA5LandMonthlySingleBandPlugin(variable="t2m")
    raw_ds = _make_monthly_nc("t2m", 300.0)

    def fake_fetch_sync(period_id: str, bbox: list[float]) -> xr.Dataset:
        ds = raw_ds[["t2m"]]
        ds = ds.rename({"longitude": "x", "latitude": "y", "valid_time": "t"})
        from open_climate_service.transforms.unit_conversion import kelvin_to_celsius

        return kelvin_to_celsius(ds, {"variable": "t2m"})

    monkeypatch.setattr(plugin, "_fetch_sync", fake_fetch_sync)
    ds = asyncio.run(plugin.fetch_period("2024-01", [-1.0, 8.0, 31.0, 10.0]))

    assert "t" in ds.dims
    assert "x" in ds.dims
    assert "y" in ds.dims
    np.testing.assert_allclose(ds["t2m"].values, [[[300.0 - 273.15]]], atol=1e-3)


def test_era5_land_monthly_precipitation_converts_metres_to_mm(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plugin = ERA5LandMonthlyPrecipitationPlugin()
    raw_ds = xr.Dataset(
        {"tp": (("t", "y", "x"), np.array([[[0.05]]], dtype=np.float32))},
        coords={"t": np.array(["2024-01-01"], dtype="datetime64[ns]"), "y": [9.0], "x": [30.0]},
    )

    def fake_parent_fetch(self: object, period_id: str, bbox: list[float]) -> xr.Dataset:
        return raw_ds

    monkeypatch.setattr(ERA5LandMonthlySingleBandPlugin, "_fetch_sync", fake_parent_fetch)
    ds = asyncio.run(plugin.fetch_period("2024-01", [-1.0, 8.0, 31.0, 10.0]))

    np.testing.assert_allclose(ds["tp"].values, [[[50.0]]], atol=1e-3)


def test_monthly_availability_cutoff_is_two_months_behind_today(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "open_climate_service.plugins.datasets.era5_land.datetime",
        type("FakeDatetime", (), {"now": staticmethod(lambda tz=None: datetime(2024, 4, 15))})(),
    )
    cutoff = _monthly_availability_cutoff()
    assert cutoff == date(2024, 2, 1)


def test_era5_land_plugin_close_releases_cached_region(monkeypatch: pytest.MonkeyPatch) -> None:
    plugin = ERA5LandHourlySingleBandPlugin(variable="t2m")

    class FakeRegion:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    region = FakeRegion()
    monkeypatch.setattr(
        "open_climate_service.plugins.datasets.era5_land._open_era5_land_region",
        lambda variable, bbox: region,
    )

    plugin._region_for_bbox([1.0, 2.0, 3.0, 4.0])
    plugin.close()

    assert region.closed is True
