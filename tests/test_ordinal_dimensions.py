"""Tests for non-temporal (ordinal) stepping-dimension support.

Covers the STAC ``cube:dimensions`` declaration and the ingest orchestrator's CF-encoding
handling for an integer day-of-year axis (vs a datetime time axis).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import xarray as xr

from open_climate_service.stac.services import _build_ordinal_dimensions
from open_climate_service.streaming.orchestrator import _strip_cf_encoding


def _dayofyear_cube() -> xr.Dataset:
    return xr.Dataset(
        {"t2m": (("dayofyear", "y", "x"), np.zeros((366, 2, 2), dtype="float32"))},
        coords={"dayofyear": list(range(1, 367)), "y": [1.0, 2.0], "x": [1.0, 2.0]},
    )


def _daily_cube() -> xr.Dataset:
    times = pd.date_range("2020-01-01", periods=3, freq="D")
    return xr.Dataset(
        {"v": (("t", "y", "x"), np.zeros((3, 2, 2), dtype="float32"))},
        coords={"t": times, "y": [1.0, 2.0], "x": [1.0, 2.0]},
    )


def test_build_ordinal_dimensions_declares_dayofyear() -> None:
    dims = _build_ordinal_dimensions(_dayofyear_cube(), "x", "y", None)

    assert "dayofyear" in dims
    doy = dims["dayofyear"]
    assert doy["type"] == "other"
    assert doy["values"][0] == 1 and doy["values"][-1] == 366
    assert doy["step"] == 1


def test_build_ordinal_dimensions_excludes_spatial_and_temporal() -> None:
    # Only x/y (spatial) and t (temporal) present — nothing ordinal to declare.
    assert _build_ordinal_dimensions(_daily_cube(), "x", "y", "t") == {}


def test_strip_cf_encoding_skips_datetime_for_ordinal_coord() -> None:
    ds = _dayofyear_cube()
    _strip_cf_encoding(ds, "daily", time_dim="dayofyear")
    # An integer day-of-year axis must not get datetime CF encoding.
    assert "units" not in ds["dayofyear"].encoding


def test_strip_cf_encoding_applies_datetime_for_time_coord() -> None:
    ds = _daily_cube()
    _strip_cf_encoding(ds, "daily", time_dim="t")
    assert ds["t"].encoding.get("units") == "days since 1970-01-01"
