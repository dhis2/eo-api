"""Regression test for the managed-store multiscale pyramid write.

Guards against the topozarr API drift that broke publishing of large workflow
outputs (`'Pyramid' object has no attribute 'dt'`): exercises the full
`write_to_icechunk_store` pyramid path against the current `topozarr` API.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

xr = pytest.importorskip("xarray")
pytest.importorskip("icechunk")
pytest.importorskip("topozarr")
pytest.importorskip("rioxarray")
zarr = pytest.importorskip("zarr")

from open_climate_service.data_manager.services.downloader import (  # noqa: E402
    needs_pyramid,
    write_to_icechunk_store,
)


def _pyramid_sized_cube() -> xr.Dataset:
    # Larger than the 2048x2048 pyramid threshold; integer var exercises the
    # no-data fill_value handling that used to be pinned via pyramid.encoding.
    ny = nx = 2100
    data = np.ones((1, ny, nx), dtype="uint8")
    return xr.Dataset(
        {"hotspot": (("t", "y", "x"), data)},
        coords={
            "t": np.array(["2020-01-01"], dtype="datetime64[ns]"),
            "y": np.linspace(-2.9, -1.0, ny),
            "x": np.linspace(28.8, 30.9, nx),
        },
    )


def test_write_to_icechunk_store_builds_pyramid(tmp_path: Path) -> None:
    import icechunk

    ds = _pyramid_sized_cube()
    assert needs_pyramid(ds)

    store_path = tmp_path / "pyramid.icechunk"
    # Must not raise (regression: topozarr Pyramid.write API, not pyramid.dt.*).
    write_to_icechunk_store(ds, store_path, crs="EPSG:4326")

    store = icechunk.Repository.open(icechunk.local_filesystem_storage(str(store_path))).readonly_session("main").store
    root = zarr.open_group(store, mode="r")

    # Root carries the multiscales metadata, and the level groups exist.
    assert "multiscales" in dict(root.attrs)
    level_groups = {k for k, _ in root.groups()}
    assert {"0", "1"} <= level_groups

    # Full-resolution level reads back at the original size with the variable intact.
    lvl0 = xr.open_zarr(store, group="0", consolidated=False, zarr_format=3)
    assert "hotspot" in lvl0.data_vars
    assert lvl0.sizes["y"] == 2100 and lvl0.sizes["x"] == 2100
