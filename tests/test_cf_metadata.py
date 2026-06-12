"""Tests for CF-metadata stamping/backfill and units validation (issue #280)."""

from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
import xarray as xr

import open_climate_service.openeo.execution as ex
from open_climate_service.shared.cf import apply_cf_metadata, cf_attrs_from_template, validate_units


def test_cf_attrs_from_template_extracts_cf_fields() -> None:
    template = {
        "units": "mm",
        "standard_name": "lwe_thickness_of_precipitation_amount",
        "cell_methods": "time: sum",
        "name": "x",
    }
    assert cf_attrs_from_template(template) == {
        "units": "mm",
        "standard_name": "lwe_thickness_of_precipitation_amount",
        "cell_methods": "time: sum",
    }
    assert cf_attrs_from_template(None) == {}
    assert cf_attrs_from_template({"units": ""}) == {"units": ""}  # dimensionless kept


def test_apply_cf_metadata_sets_and_preserves() -> None:
    da = xr.DataArray(np.zeros((2, 2), dtype="float32"), dims=("y", "x"))
    apply_cf_metadata(da, {"units": "mm", "standard_name": "lwe_thickness_of_precipitation_amount"})
    assert da.attrs["units"] == "mm"
    assert da.attrs["standard_name"] == "lwe_thickness_of_precipitation_amount"

    # existing attrs win unless overwrite=True
    da2 = xr.DataArray(np.zeros(2), dims=("x",), attrs={"units": "kg m-2 s-1"})
    apply_cf_metadata(da2, {"units": "mm"})
    assert da2.attrs["units"] == "kg m-2 s-1"
    apply_cf_metadata(da2, {"units": "mm"}, overwrite=True)
    assert da2.attrs["units"] == "mm"


def test_apply_cf_metadata_targets_dataset_variable() -> None:
    ds = xr.Dataset({"precip": (("y", "x"), np.zeros((1, 1), dtype="float32"))}, coords={"y": [0.0], "x": [0.0]})
    apply_cf_metadata(ds, {"units": "mm"}, variable="precip")
    assert ds["precip"].attrs["units"] == "mm"


@pytest.mark.parametrize("units", ["mm", "mm/d", "degC", "kg m-2 s-1", ""])
def test_validate_units_accepts_valid(units: str) -> None:
    assert validate_units(units) is None


@pytest.mark.parametrize("units", ["people", "not-a-unit"])
def test_validate_units_rejects_invalid(units: str) -> None:
    msg = validate_units(units)
    assert msg is not None and "not a recognised" in msg


def test_load_collection_backfills_cf_from_template(monkeypatch: pytest.MonkeyPatch) -> None:
    times = pd.date_range("2020-01-01", periods=2, freq="D")
    ds = xr.Dataset(
        {"precip": (("t", "y", "x"), np.ones((2, 1, 1), dtype="float32"))},
        coords={"t": times, "y": [0.0], "x": [0.0]},
    )
    monkeypatch.setattr(
        ex, "_get_published_artifact", lambda _id: SimpleNamespace(source_dataset_id="chirps3_precipitation_daily")
    )
    monkeypatch.setattr(ex, "_open_artifact", lambda _a: ds)
    monkeypatch.setattr(ex, "_ensure_crs", lambda d: d)
    from open_climate_service.data_registry.services import datasets as registry

    monkeypatch.setattr(
        registry,
        "get_dataset",
        lambda i: {
            "units": "mm",
            "standard_name": "lwe_thickness_of_precipitation_amount",
            "cell_methods": "time: sum",
        },
    )

    cube = ex._load_collection_impl("chirps3_precipitation_daily")

    assert cube.attrs.get("units") == "mm"
    assert cube.attrs.get("standard_name") == "lwe_thickness_of_precipitation_amount"
    assert cube.attrs.get("cell_methods") == "time: sum"
