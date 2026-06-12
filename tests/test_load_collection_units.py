"""load_collection backfills the cube's units from the dataset's declared units.

Without this, unit-aware processes (e.g. xclim's spi / climate indices) fail with a
confusing ``KeyError 'units'`` because the stored Zarr variable carries no units attr.
"""

from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
import xarray as xr

import open_climate_service.openeo.execution as ex


def _dataset(units: str | None = None) -> xr.Dataset:
    times = pd.date_range("2020-01-01", periods=3, freq="D")
    attrs = {"units": units} if units else {}
    return xr.Dataset(
        {"precip": (("t", "y", "x"), np.ones((3, 2, 2), dtype="float32"), attrs)},
        coords={"t": times, "y": [1.0, 2.0], "x": [1.0, 2.0]},
    )


def test_load_collection_backfills_units_from_artifact(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ex, "_get_published_artifact", lambda _id: SimpleNamespace(units="mm"))
    monkeypatch.setattr(ex, "_open_artifact", lambda _a: _dataset())
    monkeypatch.setattr(ex, "_ensure_crs", lambda ds: ds)

    cube = ex._load_collection_impl("chirps3_precipitation_daily")

    assert cube.attrs.get("units") == "mm"


def test_load_collection_preserves_existing_units(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ex, "_get_published_artifact", lambda _id: SimpleNamespace(units="mm"))
    monkeypatch.setattr(ex, "_open_artifact", lambda _a: _dataset(units="kg m-2 s-1"))
    monkeypatch.setattr(ex, "_ensure_crs", lambda ds: ds)

    cube = ex._load_collection_impl("some_dataset")

    assert cube.attrs.get("units") == "kg m-2 s-1"  # data's own units win
