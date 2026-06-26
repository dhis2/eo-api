import tempfile
from pathlib import Path

import icechunk
import numpy as np
import pandas as pd
import pytest
import xarray as xr
import zarr

from open_climate_service.data_accessor.services.accessor import (
    _coverage_from_dataset,
    open_icechunk_dataset,
    open_zarr_dataset,
)
from open_climate_service.data_manager.services import downloader
from open_climate_service.ingestions import services as ingestion_services


def test_resolve_download_dir_uses_data_dir_from_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_file = tmp_path / "climate-service.yaml"
    config_file.write_text("data_dir: ./data\nextent:\n  id: test\n", encoding="utf-8")
    monkeypatch.setenv("CLIMATE_SERVICE_CONFIG", str(config_file))
    monkeypatch.delenv("XDG_DATA_HOME", raising=False)
    assert downloader._resolve_download_dir() == tmp_path / "data" / "downloads"


def test_resolve_download_dir_uses_xdg_when_no_config(monkeypatch: pytest.MonkeyPatch) -> None:
    with tempfile.TemporaryDirectory() as xdg:
        monkeypatch.delenv("CLIMATE_SERVICE_CONFIG", raising=False)
        monkeypatch.setenv("XDG_DATA_HOME", xdg)
        assert downloader._resolve_download_dir() == Path(xdg) / "climate-service" / "downloads"


def test_resolve_artifacts_dir_uses_data_dir_from_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_file = tmp_path / "climate-service.yaml"
    config_file.write_text("data_dir: ./data\nextent:\n  id: test\n", encoding="utf-8")
    monkeypatch.setenv("CLIMATE_SERVICE_CONFIG", str(config_file))
    monkeypatch.delenv("XDG_DATA_HOME", raising=False)
    assert ingestion_services._resolve_artifacts_dir() == tmp_path / "data" / "artifacts"


def test_resolve_artifacts_dir_uses_xdg_when_no_config(monkeypatch: pytest.MonkeyPatch) -> None:
    with tempfile.TemporaryDirectory() as xdg:
        monkeypatch.delenv("CLIMATE_SERVICE_CONFIG", raising=False)
        monkeypatch.setenv("XDG_DATA_HOME", xdg)
        assert ingestion_services._resolve_artifacts_dir() == Path(xdg) / "climate-service" / "artifacts"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_dataset() -> xr.Dataset:
    return xr.Dataset(
        {"pop_total": (["t", "lat", "lon"], np.ones((2, 3, 3), dtype="float32"))},
        coords={
            "t": pd.date_range("2020-01-01", periods=2, freq="YS"),
            "lat": [10.0, 9.0, 8.0],
            "lon": [30.0, 31.0, 32.0],
        },
    )


# ---------------------------------------------------------------------------
# open_zarr_dataset
# ---------------------------------------------------------------------------


def test_open_zarr_dataset_flat(tmp_path: Path) -> None:
    """Flat zarr store is opened directly and exposes its data variables."""
    ds = _make_dataset()
    zarr_path = tmp_path / "flat.zarr"
    ds.to_zarr(str(zarr_path), mode="w")

    result = open_zarr_dataset(str(zarr_path))
    try:
        assert "pop_total" in result.data_vars
    finally:
        result.close()


def test_open_zarr_dataset_pyramid_falls_back_to_level_0(tmp_path: Path) -> None:
    """Pyramid zarr store (no root data vars) opens level 0 automatically."""
    ds = _make_dataset()
    zarr_path = tmp_path / "pyramid.zarr"
    ds.to_zarr(str(zarr_path) + "/0", mode="w")
    zarr.open_group(str(zarr_path), mode="a", zarr_format=3)

    result = open_zarr_dataset(str(zarr_path))
    try:
        assert "pop_total" in result.data_vars
        assert result.sizes["t"] == 2
    finally:
        result.close()


def test_open_zarr_dataset_pyramid_with_root_time_still_opens_level_0(tmp_path: Path) -> None:
    """Root time coord in a pyramid store does not prevent level 0 fallback."""
    ds = _make_dataset()
    zarr_path = tmp_path / "pyramid-with-time.zarr"
    ds.to_zarr(str(zarr_path) + "/0", mode="w")
    ds[["t"]].to_zarr(str(zarr_path), mode="a")

    result = open_zarr_dataset(str(zarr_path))
    try:
        assert "pop_total" in result.data_vars
        assert result.sizes["t"] == 2
    finally:
        result.close()


# ---------------------------------------------------------------------------
# open_icechunk_dataset
# ---------------------------------------------------------------------------


def test_open_icechunk_dataset_falls_back_to_level_0_when_root_has_no_data_vars(tmp_path: Path) -> None:
    """Icechunk pyramid store: level 0 is opened when root has no data variables."""
    store_path = tmp_path / "pyramid.icechunk"
    storage = icechunk.local_filesystem_storage(str(store_path))
    repo = icechunk.Repository.create(storage)
    session = repo.writable_session("main")
    ds = _make_dataset()
    ds.to_zarr(session.store, group="0", mode="w", zarr_format=3)
    session.commit("seed level 0")

    result = open_icechunk_dataset(store_path)
    try:
        assert "pop_total" in result.data_vars
        assert result.sizes["t"] == 2
    finally:
        result.close()


def test_open_icechunk_dataset_with_root_time_still_opens_level_0(tmp_path: Path) -> None:
    """Root-level time coord does not confuse the Icechunk fallback."""
    store_path = tmp_path / "pyramid.icechunk"
    storage = icechunk.local_filesystem_storage(str(store_path))
    repo = icechunk.Repository.create(storage)
    session = repo.writable_session("main")
    ds = _make_dataset()
    ds.to_zarr(session.store, group="0", mode="w", zarr_format=3)
    ds[["t"]].to_zarr(session.store, mode="a", zarr_format=3)
    session.commit("seed pyramid root time and level 0")

    result = open_icechunk_dataset(store_path)
    try:
        assert "pop_total" in result.data_vars
        assert result.sizes["t"] == 2
    finally:
        result.close()


# ---------------------------------------------------------------------------
# _write_root_time_coordinate
# ---------------------------------------------------------------------------


def test_write_root_time_coordinate_skips_when_time_dimension_missing(tmp_path: Path) -> None:
    zarr_path = tmp_path / "no-time.zarr"
    root = zarr.open_group(str(zarr_path), mode="w", zarr_format=3)
    root.attrs["sentinel"] = "kept"
    ds = xr.Dataset(
        {"pop_total": (["y", "x"], np.ones((2, 2), dtype="float32"))},
        coords={"y": [1.0, 2.0], "x": [3.0, 4.0]},
    )

    downloader._write_root_time_coordinate(zarr_path, ds, time_dim="t")

    root = zarr.open_group(str(zarr_path), mode="r", zarr_format=3)
    assert "t" not in root
    assert root.attrs["sentinel"] == "kept"


def test_write_root_time_coordinate_skips_when_time_coordinate_missing(tmp_path: Path) -> None:
    zarr_path = tmp_path / "missing-time-coord.zarr"
    root = zarr.open_group(str(zarr_path), mode="w", zarr_format=3)
    root.attrs["sentinel"] = "kept"
    ds = xr.Dataset(
        {"pop_total": (["t", "y", "x"], np.ones((2, 2, 2), dtype="float32"))},
        coords={"y": [1.0, 2.0], "x": [3.0, 4.0]},
    )

    downloader._write_root_time_coordinate(zarr_path, ds, time_dim="t")

    root = zarr.open_group(str(zarr_path), mode="r", zarr_format=3)
    assert "t" not in root
    assert root.attrs["sentinel"] == "kept"


def test_write_root_time_coordinate_replaces_existing_root_time(tmp_path: Path) -> None:
    zarr_path = tmp_path / "existing-time.zarr"
    root = zarr.open_group(str(zarr_path), mode="w", zarr_format=3)
    root.attrs["sentinel"] = "kept"
    first = xr.Dataset(coords={"t": pd.date_range("2020-01-01", periods=3, freq="D")})
    second = xr.Dataset(coords={"t": pd.date_range("2020-02-01", periods=2, freq="D")})

    downloader._write_root_time_coordinate(zarr_path, first, time_dim="t")
    downloader._write_root_time_coordinate(zarr_path, second, time_dim="t")

    root_time = zarr.open_array(str(zarr_path / "t"), mode="r", zarr_format=3)
    assert root_time.shape == (2,)
    assert root_time.chunks == (2,)
    root = zarr.open_group(str(zarr_path), mode="r", zarr_format=3)
    assert root.attrs["sentinel"] == "kept"


# ---------------------------------------------------------------------------
# _coverage_from_dataset — WGS84 reprojection
# ---------------------------------------------------------------------------


def test_coverage_from_dataset_populates_spatial_wgs84_for_projected_crs() -> None:
    # Small UTM33N (EPSG:25833) bounding box covering south-central Norway.
    x = np.array([100_000.0, 200_000.0])  # easting metres
    y = np.array([6_500_000.0, 6_600_000.0])  # northing metres
    times = pd.date_range("2020-01-01", periods=1, freq="D")
    data = np.ones((1, len(y), len(x)))
    ds = xr.Dataset(
        {"temperature": (["time", "y", "x"], data)},
        coords={"time": times, "y": y, "x": x},
    )

    result = _coverage_from_dataset(ds=ds, period_type="daily", native_crs="EPSG:25833")

    wgs84 = result["coverage"]["spatial_wgs84"]
    assert wgs84 is not None
    # Reprojected bounds must be in WGS84 degree range.
    assert -180 <= wgs84["xmin"] <= 180
    assert -180 <= wgs84["xmax"] <= 180
    assert -90 <= wgs84["ymin"] <= 90
    assert -90 <= wgs84["ymax"] <= 90
    # Rough sanity check: UTM33N easting ~100–200 km, northing ~6500–6600 km.
    assert 7.0 < wgs84["xmin"] < 10.0
    assert 8.0 < wgs84["xmax"] < 12.0
    assert 58.0 < wgs84["ymin"] < 62.0
    assert 58.0 < wgs84["ymax"] < 62.0


def test_coverage_from_dataset_leaves_spatial_wgs84_none_for_wgs84() -> None:
    x = np.array([-10.0, -9.0])
    y = np.array([7.0, 8.0])
    times = pd.date_range("2020-01-01", periods=1, freq="D")
    data = np.ones((1, len(y), len(x)))
    ds = xr.Dataset(
        {"temperature": (["time", "y", "x"], data)},
        coords={"time": times, "y": y, "x": x},
    )

    result = _coverage_from_dataset(ds=ds, period_type="daily", native_crs="EPSG:4326")

    assert result["coverage"]["spatial_wgs84"] is None


def test_coverage_from_dataset_handles_non_temporal_ordinal_store() -> None:
    # A day-of-year climatology has a `dayofyear` axis, not a datetime one. Coverage
    # must report spatial extent with null temporal bounds rather than raising
    # "Unable to find time dimension" (which 500'd the publish step).
    x = np.array([-13.0, -11.0])
    y = np.array([7.0, 9.0])
    data = np.ones((366, len(y), len(x)), dtype="float32")
    ds = xr.Dataset(
        {"t2m": (["dayofyear", "y", "x"], data)},
        coords={"dayofyear": list(range(1, 367)), "y": y, "x": x},
    )

    result = _coverage_from_dataset(ds=ds, period_type="climatology", native_crs="EPSG:4326")

    assert result["has_data"] is True
    assert result["coverage"]["temporal"] == {"start": None, "end": None}
    assert result["coverage"]["spatial"] == {"xmin": -13.0, "ymin": 7.0, "xmax": -11.0, "ymax": 9.0}


def test_write_to_icechunk_store_preserves_native_crs_ignoring_instance_crs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Data is stored in its native CRS; the instance config CRS is never used.

    Regression for ERA5-Land (native WGS84) being mislabeled with the instance
    UTM CRS, which put degree coordinates off-map when rendered as metres.
    """
    from open_climate_service import config as api_config

    # Force a non-WGS84 instance CRS — the writer must ignore it entirely.
    monkeypatch.setattr(api_config, "get_crs", lambda: "EPSG:32633")

    # WGS84 dataset carrying its native CRS as the streaming store writes it.
    y = np.linspace(57.0, 72.5, 8)
    x = np.linspace(3.0, 32.0, 10)
    data = np.random.default_rng(0).random((1, len(y), len(x)), dtype="float32")
    ds = xr.Dataset(
        {"t2m": (("t", "y", "x"), data)},
        coords={"t": [np.datetime64("2024-01-01")], "y": y, "x": x},
    )
    ds.attrs["proj:code"] = "EPSG:4326"

    store = tmp_path / "native_crs.icechunk"
    downloader.write_to_icechunk_store(ds, store, commit_message="test")

    out = open_icechunk_dataset(str(store))
    assert out.attrs.get("proj:code") == "EPSG:4326"
