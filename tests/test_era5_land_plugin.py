import asyncio
from typing import cast

import numpy as np
import pytest
import xarray as xr

from open_climate_service.plugins.datasets.era5_land import ERA5LandHourlySingleBandPlugin, ERA5LandPrecipitationPlugin


def test_era5_land_periods_enumerate_hours(monkeypatch: pytest.MonkeyPatch) -> None:
    plugin = ERA5LandHourlySingleBandPlugin(variable="t2m")
    monkeypatch.setattr(plugin, "_latest_available", lambda: "2099-12-31T23")

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
    # 281.0 K → 281.0 - 273.15 = 7.85 °C
    np.testing.assert_allclose(dataset["t2m"].values, [[[281.0 - 273.15]]], rtol=1e-4)
    assert dataset["t2m"].attrs.get("units") == "degC"


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
    # 0.002 m → 0.002 * 1000 = 2.0 mm
    np.testing.assert_allclose(dataset["tp"].values, [[[2.0]]], rtol=1e-4)
    assert dataset["tp"].attrs.get("units") == "mm"


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


def test_era5_land_plugin_accepts_extra_kwargs() -> None:
    plugin = ERA5LandHourlySingleBandPlugin(variable="t2m", unknown_future_field="ignored")
    assert plugin.variable == "t2m"


def test_era5_land_plugin_still_rejects_empty_variable_with_extra_kwargs() -> None:
    with pytest.raises(ValueError, match="non-empty variable"):
        ERA5LandHourlySingleBandPlugin(variable="", unknown_future_field="ignored")


def test_era5_land_precipitation_plugin_accepts_extra_kwargs() -> None:
    plugin = ERA5LandPrecipitationPlugin(unknown_future_field="ignored")
    assert plugin.variable == "tp"
