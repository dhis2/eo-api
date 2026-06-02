import asyncio
from datetime import date

import pytest

from open_climate_service.streaming.plugins.chirps3 import CHIRPS3DailyPlugin


def test_chirps3_plugin_probe_estimates_grid_from_bbox() -> None:
    plugin = CHIRPS3DailyPlugin()

    spec = asyncio.run(plugin.probe([30.0, -2.0, 31.0, -1.0]))

    assert spec.crs == 4326
    assert spec.shape == (20, 20)
    assert spec.nodata == -9999.0


def test_chirps3_plugin_periods_are_clamped_to_complete_month(monkeypatch: pytest.MonkeyPatch) -> None:
    plugin = CHIRPS3DailyPlugin()
    monkeypatch.setattr(plugin, "_availability_cutoff", lambda: date(2026, 1, 31))

    periods = asyncio.run(plugin.periods("2026-01-30", "2026-02-03"))

    assert periods == ["2026-01-30", "2026-01-31"]
