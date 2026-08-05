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
    _coarsen_native,
    _normalize_resampling_method,
    needs_pyramid,
    resampling_method_from_template,
    write_to_icechunk_store,
)


def _pyramid_sized_cube():
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

    repo = icechunk.Repository.open(icechunk.local_filesystem_storage(str(store_path)))
    store = repo.readonly_session("main").store
    root = zarr.open_group(store, mode="r")

    # Root carries the multiscales metadata, and the level groups exist.
    assert "multiscales" in dict(root.attrs)
    level_groups = {k for k, _ in root.groups()}
    assert {"0", "1"} <= level_groups

    # Full-resolution level reads back at the original size with the variable intact.
    with xr.open_zarr(store, group="0", consolidated=False, zarr_format=3) as lvl0:
        assert "hotspot" in lvl0.data_vars
        assert lvl0.sizes["y"] == 2100 and lvl0.sizes["x"] == 2100


def test_resampling_method_from_template() -> None:
    assert resampling_method_from_template(None) == "mean"
    assert resampling_method_from_template({}) == "mean"
    assert resampling_method_from_template({"ingestion": {"plugin": "x"}}) == "mean"
    assert resampling_method_from_template({"ingestion": {"resampling": "mode"}}) == "mode"
    assert resampling_method_from_template({"ingestion": {"resampling": "MAX"}}) == "max"
    # Unrecognised values fall back to mean rather than being passed downstream.
    assert resampling_method_from_template({"ingestion": {"resampling": "bilinear"}}) == "mean"
    # A display-block value is ignored — resampling is an ingestion concern.
    assert resampling_method_from_template({"display": {"resampling": "mode"}}) == "mean"


def test_normalize_resampling_method() -> None:
    # Case/whitespace are normalized, and unknown or missing values fall back to mean —
    # so a direct write_to_icechunk_store(pyramid_method=...) caller can't smuggle an
    # invalid CoarseningMethod through to topozarr.
    assert _normalize_resampling_method("MAX") == "max"
    assert _normalize_resampling_method("  mean ") == "mean"
    assert _normalize_resampling_method("Mode") == "mode"
    assert _normalize_resampling_method(None) == "mean"
    assert _normalize_resampling_method("bilinear") == "mean"


def _categorical_block_da():
    # 4x4 class-code grid. Each 2x2 block has a clear majority that differs from its
    # top-left cell, so mode and nearest give distinguishable results (and mean would
    # invent codes that don't exist, e.g. mean(99,10,10,10)=32.25).
    data = np.array(
        [
            [99, 10, 20, 20],
            [10, 10, 20, 30],
            [30, 30, 40, 41],
            [30, 31, 40, 40],
        ],
        dtype="uint8",
    )
    return xr.DataArray(
        data,
        dims=("y", "x"),
        coords={"y": np.arange(4.0), "x": np.arange(4.0)},
    )


def test_coarsen_native_mode_picks_block_majority() -> None:
    out = _coarsen_native(_categorical_block_da(), "x", "y", 2, "mode").transpose("y", "x")
    np.testing.assert_array_equal(out.values, np.array([[10, 20], [30, 40]], dtype="uint8"))


def test_nearest_is_delegated_to_topozarr() -> None:
    """`nearest` arrived upstream in topozarr 0.1.3, so OCS no longer computes it.

    It is composable — corner-of-corners equals corner-of-native — so level-from-previous
    (what topozarr does) and level-from-native (what OCS used to do) agree. Verified
    byte-identical across a real 3-level pyramid when the delegation was made.
    """
    from open_climate_service.data_manager.services.downloader import (
        _COMPOSABLE_METHODS,
        _NATIVE_RESAMPLE_METHODS,
    )

    assert "nearest" in _COMPOSABLE_METHODS
    assert "nearest" not in _NATIVE_RESAMPLE_METHODS
    # Still a valid template value — it just takes the upstream path now.
    assert _normalize_resampling_method("nearest") == "nearest"


def test_coarsen_native_only_handles_mode() -> None:
    """`mode` is the sole remaining local method; anything else is a programming error."""
    with pytest.raises(ValueError, match="unsupported native-resample method"):
        _coarsen_native(_categorical_block_da(), "x", "y", 2, "nearest")


def test_topozarr_nearest_keeps_the_top_left_cell() -> None:
    """Pin the upstream kernel's corner choice, since our equivalence argument rests on it.

    Also guards the packaging trap: `nearest` only exists from topozarr-core 0.1.2, and
    topozarr's own constraint (`>=0.1.0`) would allow an older core where this raises
    "method must be one of 'mean', 'max', 'min', 'sum'" — at ingest time, not on import.
    """
    from topozarr_core import block_reduce

    block = np.arange(16, dtype="int16").reshape(4, 4)
    out = np.asarray(block_reduce(block, (2, 2), "nearest", 0, True))
    np.testing.assert_array_equal(out, np.array([[0, 2], [8, 10]], dtype="int16"))


def _categorical_pyramid_cube():
    # Pyramid-sized grid of a few land-cover-style class codes (no 0 background).
    ny = nx = 2100
    codes = np.array([10, 20, 30, 40, 50], dtype="uint8")
    rng = np.random.default_rng(0)
    data = codes[rng.integers(0, len(codes), size=(ny, nx))]
    return xr.Dataset(
        {"landcover": (("y", "x"), data)},
        coords={"y": np.linspace(-2.9, -1.0, ny), "x": np.linspace(28.8, 30.9, nx)},
    ), set(int(c) for c in codes)


def test_write_to_icechunk_store_mode_keeps_class_codes(tmp_path: Path) -> None:
    import icechunk

    ds, codes = _categorical_pyramid_cube()
    assert needs_pyramid(ds)

    store_path = tmp_path / "landcover.icechunk"
    write_to_icechunk_store(ds, store_path, t_dim=None, crs="EPSG:4326", pyramid_method="mode")

    repo = icechunk.Repository.open(icechunk.local_filesystem_storage(str(store_path)))
    store = repo.readonly_session("main").store

    # A coarsened level must contain only real class codes — never an averaged value
    # (which is exactly what the previous hardcoded "mean" produced for categorical data).
    with xr.open_zarr(store, group="1", consolidated=False, zarr_format=3) as lvl1:
        values = np.unique(lvl1["landcover"].values)
    assert set(int(v) for v in values) <= codes
