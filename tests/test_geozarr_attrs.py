"""Store-level CRS attributes written by ``write_geozarr_attrs``.

Direct-Zarr clients (GeoLibre's native Zarr panel, GDAL/QGIS) never see OCS's STAC,
so a projected store must embed a ``proj4`` string + native-CRS ``bounds`` in its root
attributes to auto-reproject. This is the store-side counterpart of the STAC render
hints in ``stac/services.py``.
"""

from __future__ import annotations

import numpy as np
import pytest
import zarr

from open_climate_service.shared.crs import crs_to_proj4, is_builtin_crs
from open_climate_service.streaming.protocol import GridSpec
from open_climate_service.streaming.store import write_geozarr_attrs


def _init_group(path) -> str:
    store = str(path / "s.zarr")
    zarr.open_group(store, mode="w", zarr_format=3)
    return store


def test_write_geozarr_attrs_adds_proj4_and_bounds_for_projected(tmp_path) -> None:
    store = _init_group(tmp_path)
    spec = GridSpec(shape=(3, 3), crs=32633, dtype=np.dtype("float32"), x_dim="x", y_dim="y")
    bbox = [-74500.0, 6450500.0, 1119500.0, 7999500.0]  # native UTM33 metres

    write_geozarr_attrs(store, spec=spec, bbox=bbox)

    attrs = dict(zarr.open_group(store, mode="r").attrs)
    assert attrs["proj:code"] == "EPSG:32633"
    assert "proj=utm" in attrs["proj4"] and "zone=33" in attrs["proj4"]
    assert attrs["bounds"] == bbox  # native-CRS [xMin, yMin, xMax, yMax] for the client


def test_write_geozarr_attrs_omits_proj4_for_wgs84(tmp_path) -> None:
    store = _init_group(tmp_path)
    spec = GridSpec(shape=(3, 3), crs=4326, dtype=np.dtype("float32"), x_dim="x", y_dim="y")

    write_geozarr_attrs(store, spec=spec, bbox=[-13.5, 6.9, -10.1, 10.0])

    attrs = dict(zarr.open_group(store, mode="r").attrs)
    assert attrs["proj:code"] == "EPSG:4326"
    assert "proj4" not in attrs  # built-in → client resolves from the code
    assert "bounds" not in attrs


@pytest.mark.parametrize(
    "code,is_builtin",
    [("EPSG:4326", True), ("EPSG:3857", True), ("CRS84", True), ("CRS:84", True), ("EPSG:32633", False)],
)
def test_is_builtin_crs(code: str, is_builtin: bool) -> None:
    assert is_builtin_crs(code) is is_builtin


def test_crs_to_proj4() -> None:
    assert crs_to_proj4("EPSG:4326") is None  # built-in → no proj4 hint
    proj4 = crs_to_proj4("EPSG:32633")
    assert proj4 is not None and "proj=utm" in proj4 and "zone=33" in proj4
    assert crs_to_proj4("not-a-crs") is None
