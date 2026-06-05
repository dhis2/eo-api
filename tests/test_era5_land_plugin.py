import asyncio
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import xarray as xr

from open_climate_service.plugins.datasets.era5_land import (
    ERA5LandCDSHourlyPlugin,
    ERA5LandDailyTemperaturePlugin,
    ERA5LandEDHDailyPlugin,
    ERA5LandMonthlyPlugin,
    ERA5LandMonthlyPrecipitationPlugin,
    ERA5LandPrecipitationPlugin,
    _daily_availability_cutoff,
    _edh_open_zarr,
    _era5land_monthly_product_type,
    _hourly_availability_cutoff,
    _monthly_availability_cutoff,
)


def test_era5_land_hourly_plugin_rejects_unknown_variable() -> None:
    with pytest.raises(ValueError, match="unsupported variable"):
        ERA5LandCDSHourlyPlugin(variable="unknown")


def test_era5_land_hourly_probe_derives_shape_from_bbox() -> None:
    plugin = ERA5LandCDSHourlyPlugin(variable="t2m")
    spec = asyncio.run(plugin.probe([-13.5, 6.9, -10.1, 10.0]))
    assert spec.shape == (31, 34)
    assert spec.time_dim == "t"
    assert spec.x_dim == "x"
    assert spec.y_dim == "y"


def test_era5_land_hourly_periods_enumerates_hours() -> None:
    from datetime import timezone

    plugin = ERA5LandCDSHourlyPlugin(variable="t2m")
    cutoff = datetime(2026, 1, 1, 2, tzinfo=timezone.utc)
    with patch(
        "open_climate_service.plugins.datasets.era5_land._hourly_availability_cutoff",
        return_value=cutoff,
    ):
        periods = asyncio.run(plugin.periods("2026-01-01T00", "2026-01-01T05"))
    assert periods == ["2026-01-01T00", "2026-01-01T01", "2026-01-01T02"]


def test_era5_land_hourly_fetch_uses_cached_monthly_batch(monkeypatch: pytest.MonkeyPatch) -> None:
    plugin = ERA5LandCDSHourlyPlugin(variable="t2m")
    fetch_calls: list[tuple[int, int]] = []

    def fake_fetch_month(self: object, year: int, month: int, bbox: tuple[float, float, float, float]) -> xr.Dataset:
        fetch_calls.append((year, month))
        return xr.Dataset(
            {"t2m": (("t", "y", "x"), np.array([[[20.0]], [[21.0]]], dtype=np.float32))},
            coords={
                "t": np.array(["2026-01-01T00", "2026-01-01T01"], dtype="datetime64[h]").astype("datetime64[ns]"),
                "y": [9.0],
                "x": [30.0],
            },
        )

    monkeypatch.setattr(ERA5LandCDSHourlyPlugin, "_fetch_month", fake_fetch_month)

    asyncio.run(plugin.fetch_period("2026-01-01T00", [-1.0, 8.0, 31.0, 10.0]))
    asyncio.run(plugin.fetch_period("2026-01-01T01", [-1.0, 8.0, 31.0, 10.0]))

    assert fetch_calls == [(2026, 1)]


def test_era5_land_hourly_fetch_converts_temperature(monkeypatch: pytest.MonkeyPatch) -> None:
    plugin = ERA5LandCDSHourlyPlugin(variable="t2m")

    def fake_fetch_month(self: object, year: int, month: int, bbox: tuple[float, float, float, float]) -> xr.Dataset:
        ds = xr.Dataset(
            {"t2m": (("t", "y", "x"), np.array([[[300.0]]], dtype=np.float32))},
            coords={
                "t": np.array(["2026-01-01T00"], dtype="datetime64[h]").astype("datetime64[ns]"),
                "y": [9.0],
                "x": [30.0],
            },
        )
        from open_climate_service.transforms.unit_conversion import kelvin_to_celsius

        return kelvin_to_celsius(ds, {"variable": "t2m"})

    monkeypatch.setattr(ERA5LandCDSHourlyPlugin, "_fetch_month", fake_fetch_month)
    ds = asyncio.run(plugin.fetch_period("2026-01-01T00", [-1.0, 8.0, 31.0, 10.0]))
    np.testing.assert_allclose(ds["t2m"].values, [[300.0 - 273.15]], atol=1e-3)


def test_era5_land_precipitation_plugin_uses_tp_and_converts_to_mm(monkeypatch: pytest.MonkeyPatch) -> None:
    plugin = ERA5LandPrecipitationPlugin()

    def fake_fetch_month(self: object, year: int, month: int, bbox: tuple[float, float, float, float]) -> xr.Dataset:
        return xr.Dataset(
            {"tp": (("t", "y", "x"), np.array([[[0.002]]], dtype=np.float32))},
            coords={
                "t": np.array(["2026-01-01T00"], dtype="datetime64[h]").astype("datetime64[ns]"),
                "y": [9.0],
                "x": [30.0],
            },
        )

    monkeypatch.setattr(ERA5LandCDSHourlyPlugin, "_fetch_month", fake_fetch_month)
    ds = asyncio.run(plugin.fetch_period("2026-01-01T00", [-1.0, 8.0, 31.0, 10.0]))
    assert list(ds.data_vars) == ["tp"]
    np.testing.assert_allclose(ds["tp"].values, [[2.0]], atol=1e-3)


def test_era5_land_hourly_availability_cutoff_uses_cds() -> None:
    from datetime import timezone

    fake_col = MagicMock()
    fake_col.end_datetime = datetime(2026, 5, 29, 23, tzinfo=timezone.utc)
    fake_client = MagicMock()
    fake_client.get_collection.return_value = fake_col

    with patch("open_climate_service.plugins.datasets.era5_land._CdsClient", return_value=fake_client):
        cutoff = _hourly_availability_cutoff()

    assert cutoff == datetime(2026, 5, 29, 23, tzinfo=timezone.utc)


def test_era5_land_daily_probe_derives_shape_from_bbox() -> None:
    plugin = ERA5LandDailyTemperaturePlugin()
    spec = asyncio.run(plugin.probe([-13.5, 6.9, -10.1, 10.0]))
    assert spec.shape == (31, 34)
    assert spec.crs == 4326
    assert spec.time_dim == "t"


def test_era5_land_daily_periods_enumerates_days() -> None:
    plugin = ERA5LandDailyTemperaturePlugin()
    with patch(
        "open_climate_service.plugins.datasets.era5_land._daily_availability_cutoff",
        return_value=date(2024, 1, 3),
    ):
        periods = asyncio.run(plugin.periods("2024-01-01", "2024-01-10"))
    assert periods == ["2024-01-01", "2024-01-02", "2024-01-03"]


def test_era5_land_daily_fetch_uses_cached_monthly_batch(monkeypatch: pytest.MonkeyPatch) -> None:
    plugin = ERA5LandDailyTemperaturePlugin()
    fetch_calls: list[tuple[int, int]] = []

    def fake_fetch_month(self: object, year: int, month: int, bbox: tuple[float, float, float, float]) -> xr.Dataset:
        fetch_calls.append((year, month))
        return xr.Dataset(
            {"t2m": (("t", "y", "x"), np.zeros((3, 1, 1), dtype=np.float32))},
            coords={
                "t": np.array(["2024-01-01", "2024-01-02", "2024-01-03"], dtype="datetime64[D]").astype(
                    "datetime64[ns]"
                ),
                "y": [9.0],
                "x": [30.0],
            },
        )

    monkeypatch.setattr(ERA5LandDailyTemperaturePlugin, "_fetch_month", fake_fetch_month)

    asyncio.run(plugin.fetch_period("2024-01-01", [-1.0, 8.0, 31.0, 10.0]))
    asyncio.run(plugin.fetch_period("2024-01-02", [-1.0, 8.0, 31.0, 10.0]))
    asyncio.run(plugin.fetch_period("2024-01-03", [-1.0, 8.0, 31.0, 10.0]))

    # Three days in the same month → only one CDS API call
    assert fetch_calls == [(2024, 1)]


def test_era5_land_daily_availability_cutoff_uses_cds() -> None:
    fake_col = MagicMock()
    fake_col.end_datetime = datetime(2026, 5, 29, tzinfo=None)
    fake_client = MagicMock()
    fake_client.get_collection.return_value = fake_col

    with patch("open_climate_service.plugins.datasets.era5_land._CdsClient", return_value=fake_client):
        cutoff = _daily_availability_cutoff()

    assert cutoff == date(2026, 5, 29)


def test_era5_land_monthly_plugin_rejects_unknown_variable() -> None:
    with pytest.raises(ValueError, match="unsupported variable"):
        ERA5LandMonthlyPlugin(variable="unknown")


def test_era5_land_monthly_probe_derives_shape_from_bbox() -> None:
    plugin = ERA5LandMonthlyPlugin(variable="t2m")
    spec = asyncio.run(plugin.probe([-13.5, 6.9, -10.1, 10.0]))
    assert spec.shape == (31, 34)
    assert spec.crs == 4326
    assert spec.time_dim == "t"
    assert spec.x_dim == "x"
    assert spec.y_dim == "y"


def test_era5_land_monthly_periods_enumerates_months() -> None:
    plugin = ERA5LandMonthlyPlugin(variable="t2m")
    with patch(
        "open_climate_service.plugins.datasets.era5_land._monthly_availability_cutoff",
        return_value=date(2024, 3, 1),
    ):
        periods = asyncio.run(plugin.periods("2024-01", "2024-06"))
    assert periods == ["2024-01", "2024-02", "2024-03"]


def test_era5_land_monthly_periods_empty_when_start_after_cutoff() -> None:
    plugin = ERA5LandMonthlyPlugin(variable="t2m")
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
    plugin = ERA5LandMonthlyPlugin(variable="t2m")
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

    monkeypatch.setattr(ERA5LandMonthlyPlugin, "_fetch_sync", fake_parent_fetch)
    ds = asyncio.run(plugin.fetch_period("2024-01", [-1.0, 8.0, 31.0, 10.0]))

    # 0.05 m/day × 1000 mm/m × 31 days (January 2024) = 1550 mm monthly total
    np.testing.assert_allclose(ds["tp"].values, [[[1550.0]]], atol=1e-1)


def test_monthly_availability_cutoff_uses_cds_end_datetime() -> None:
    from datetime import timezone

    fake_col = MagicMock()
    fake_col.end_datetime = datetime(2026, 4, 1, tzinfo=timezone.utc)
    fake_client = MagicMock()
    fake_client.get_collection.return_value = fake_col

    with patch("open_climate_service.plugins.datasets.era5_land._CdsClient", return_value=fake_client):
        cutoff = _monthly_availability_cutoff()

    assert cutoff == date(2026, 4, 1)


def test_monthly_availability_cutoff_raises_when_cds_unavailable() -> None:
    with (
        patch("open_climate_service.plugins.datasets.era5_land._CdsClient", side_effect=Exception("network error")),
        pytest.raises(Exception, match="network error"),
    ):
        _monthly_availability_cutoff()


def test_era5_land_hourly_refetches_when_month_changes(monkeypatch: pytest.MonkeyPatch) -> None:
    plugin = ERA5LandCDSHourlyPlugin(variable="t2m")
    fetch_calls: list[tuple[int, int]] = []

    def fake_fetch_month(self: object, year: int, month: int, bbox: tuple[float, float, float, float]) -> xr.Dataset:
        fetch_calls.append((year, month))
        ts = f"{year:04d}-{month:02d}-01T00"
        return xr.Dataset(
            {"t2m": (("t", "y", "x"), np.zeros((1, 1, 1), dtype=np.float32))},
            coords={
                "t": np.array([ts], dtype="datetime64[h]").astype("datetime64[ns]"),
                "y": [9.0],
                "x": [30.0],
            },
        )

    monkeypatch.setattr(ERA5LandCDSHourlyPlugin, "_fetch_month", fake_fetch_month)

    asyncio.run(plugin.fetch_period("2026-01-01T00", [-1.0, 8.0, 31.0, 10.0]))
    asyncio.run(plugin.fetch_period("2026-02-01T00", [-1.0, 8.0, 31.0, 10.0]))

    assert fetch_calls == [(2026, 1), (2026, 2)]


# ---------------------------------------------------------------------------
# EDH plugin tests
# ---------------------------------------------------------------------------


def _fake_edh_hourly_region() -> xr.Dataset:
    return xr.Dataset(
        {"t2m": (("valid_time", "latitude", "longitude"), np.array([[[280.0]], [[281.0]]], dtype=np.float32))},
        coords={
            "valid_time": np.array(["2026-01-01T00", "2026-01-01T01"], dtype="datetime64[h]").astype("datetime64[ns]"),
            "latitude": [9.0],
            "longitude": [30.0],
        },
    )


def _fake_edh_daily_region() -> xr.Dataset:
    return xr.Dataset(
        {"t2m": (("valid_time", "latitude", "longitude"), np.array([[[285.0]], [[286.0]]], dtype=np.float32))},
        coords={
            "valid_time": np.array(["2026-01-01", "2026-01-02"], dtype="datetime64[D]").astype("datetime64[ns]"),
            "latitude": [9.0],
            "longitude": [30.0],
        },
    )




def test_edh_daily_plugin_rejects_unknown_variable() -> None:
    with pytest.raises(ValueError, match="unsupported variable"):
        ERA5LandEDHDailyPlugin(variable="unknown")


def test_edh_daily_fetch_renames_coords_and_converts_temperature(monkeypatch: pytest.MonkeyPatch) -> None:
    plugin = ERA5LandEDHDailyPlugin(variable="t2m")
    monkeypatch.setattr(plugin, "_region_for_bbox", lambda bbox: _fake_edh_daily_region())
    ds = asyncio.run(plugin.fetch_period("2026-01-01", [-1.0, 8.0, 31.0, 10.0]))
    assert set(ds.dims) == {"t", "y", "x"}
    np.testing.assert_allclose(ds["t2m"].values, [[[285.0 - 273.15]]], atol=1e-3)


def test_edh_daily_periods_capped_to_latest_available(monkeypatch: pytest.MonkeyPatch) -> None:
    plugin = ERA5LandEDHDailyPlugin(variable="t2m")
    monkeypatch.setattr(plugin, "_latest_available", lambda: "2026-01-03")
    periods = asyncio.run(plugin.periods("2026-01-01", "2026-01-10"))
    assert periods == ["2026-01-01", "2026-01-02", "2026-01-03"]


def test_edh_open_zarr_injects_api_key_in_url(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[str] = []

    def fake_open_zarr(url: str, **kwargs: object) -> xr.Dataset:
        captured.append(url)
        return MagicMock()

    monkeypatch.setenv("EDH_API_KEY", "mytoken")
    with patch("open_climate_service.plugins.datasets.era5_land.xr.open_zarr", fake_open_zarr):
        _edh_open_zarr("https://api.earthdatahub.destine.eu/era5/test.zarr")

    assert captured[0] == "https://edh:mytoken@api.earthdatahub.destine.eu/era5/test.zarr"


def test_era5land_monthly_product_type_uses_workaround_for_affected_period() -> None:
    # Affected period: September 2022 – February 2024 (CDS data bug, forum.ecmwf.int/t/2370)
    assert _era5land_monthly_product_type(2022, 9) == "monthly_averaged_reanalysis_by_hour_of_day"
    assert _era5land_monthly_product_type(2023, 6) == "monthly_averaged_reanalysis_by_hour_of_day"
    assert _era5land_monthly_product_type(2024, 2) == "monthly_averaged_reanalysis_by_hour_of_day"
    # Outside affected period: standard product
    assert _era5land_monthly_product_type(2022, 8) == "monthly_averaged_reanalysis"
    assert _era5land_monthly_product_type(2024, 3) == "monthly_averaged_reanalysis"
    assert _era5land_monthly_product_type(2026, 1) == "monthly_averaged_reanalysis"
