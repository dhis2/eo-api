import asyncio

import pytest
import xarray as xr

from open_climate_service.streaming.plugins.worldpop import WorldPopYearlyPlugin, _resolve_variant


def test_worldpop_plugin_periods_enumerates_years() -> None:
    plugin = WorldPopYearlyPlugin()

    periods = asyncio.run(plugin.periods("2020", "2023"))

    assert periods == ["2020", "2021", "2022", "2023"]


def test_worldpop_plugin_fetch_period_uses_country_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plugin = WorldPopYearlyPlugin(version="global2")
    captured: dict[str, object] = {}

    def fake_fetch_year(year: int, country_code: str) -> xr.Dataset:
        captured["year"] = year
        captured["country_code"] = country_code
        return xr.Dataset(
            {"pop_total": (("time", "y", "x"), [[[1.0]]])},
            coords={"time": ["2022-01-01"], "x": [1.0], "y": [2.0]},
        )

    monkeypatch.setattr(plugin, "_fetch_year", fake_fetch_year)

    dataset = asyncio.run(plugin.fetch_period("2022", [1.0, 2.0, 3.0, 4.0], country_code="SLE"))

    assert captured == {"year": 2022, "country_code": "SLE"}
    assert list(dataset.data_vars) == ["pop_total"]


def test_worldpop_plugin_requires_country_code() -> None:
    plugin = WorldPopYearlyPlugin()

    with pytest.raises(ValueError, match="country_code"):
        asyncio.run(plugin.fetch_period("2022", [1.0, 2.0, 3.0, 4.0]))


def test_worldpop_plugin_variant_resolver_supports_total_product() -> None:
    variant = _resolve_variant(product="total", variable="pop_total")

    assert variant.product == "total"
    assert variant.source_variable == "pop_total"
    assert variant.output_variable == "pop_total"


def test_worldpop_plugin_variant_resolver_rejects_unknown_product() -> None:
    with pytest.raises(ValueError, match="Unsupported WorldPop product"):
        _resolve_variant(product="female", variable="pop_female")


def test_worldpop_plugin_fetch_period_can_rename_output_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plugin = WorldPopYearlyPlugin(product="total", variable="population_total")

    def fake_fetch_year(year: int, country_code: str) -> xr.Dataset:
        return xr.Dataset(
            {"pop_total": (("time", "lon", "lat"), [[[1.0]]])},
            coords={"time": ["2022-01-01"], "lon": [1.0], "lat": [2.0]},
        )

    monkeypatch.setattr(plugin, "_fetch_year", fake_fetch_year)

    dataset = asyncio.run(plugin.fetch_period("2022", [1.0, 2.0, 3.0, 4.0], country_code="SLE"))

    assert list(dataset.data_vars) == ["population_total"]
    assert "x" in dataset.dims and "y" in dataset.dims


def test_worldpop_plugin_masks_nodata_sentinel_to_nan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Values equal to the WorldPop -99999 sentinel must become NaN, not be stored."""
    import numpy as np

    plugin = WorldPopYearlyPlugin()

    def fake_fetch_year(year: int, country_code: str) -> xr.Dataset:
        return xr.Dataset(
            {"pop_total": (("time", "lon", "lat"), [[[5.0, -99999.0, 12.0]]])},
            coords={"time": ["2020-01-01"], "lon": [1.0], "lat": [1.0, 2.0, 3.0]},
        )

    monkeypatch.setattr(plugin, "_fetch_year", fake_fetch_year)

    dataset = asyncio.run(plugin.fetch_period("2020", [0.0, 0.0, 4.0, 4.0], country_code="SLE"))

    values = dataset["pop_total"].values.flatten()
    assert np.isnan(values[1]), "sentinel -99999 must be masked to NaN"
    assert values[0] == pytest.approx(5.0), "valid values must be preserved"
    assert values[2] == pytest.approx(12.0), "valid values must be preserved"
