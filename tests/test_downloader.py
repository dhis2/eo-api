import tempfile
from pathlib import Path
from typing import Any

import icechunk
import numpy as np
import pandas as pd
import pytest
import xarray as xr
import zarr
from fastapi import HTTPException
from topozarr.pyramid import Pyramid
from xarray import DataTree

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


def test_download_dataset_returns_400_when_country_code_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_download(
        *,
        start: str,
        end: str,
        dirname: object,
        prefix: str,
        overwrite: bool,
        country_code: str,
    ) -> None:
        del start, end, dirname, prefix, overwrite, country_code

    dataset: dict[str, Any] = {
        "id": "worldpop_population_yearly",
        "ingestion": {"function": "ignored.path"},
    }
    monkeypatch.delenv("COUNTRY_CODE", raising=False)
    monkeypatch.setattr(downloader, "_get_dynamic_function", lambda _: fake_download)

    with pytest.raises(HTTPException) as exc_info:
        downloader.download_dataset(
            dataset=dataset,
            start="2020-01-01",
            end="2020-12-31",
            bbox=None,
            country_code=None,
            overwrite=False,
            background_tasks=None,
        )

    assert exc_info.value.status_code == 400
    assert "requires a country code" in str(exc_info.value.detail)


def test_download_dataset_returns_400_for_missing_bbox(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_download(
        *,
        start: str,
        end: str,
        dirname: object,
        prefix: str,
        overwrite: bool,
        bbox: list[float],
    ) -> None:
        del start, end, dirname, prefix, overwrite, bbox

    dataset: dict[str, Any] = {
        "id": "chirps3_precipitation_daily",
        "ingestion": {"function": "ignored.path"},
    }
    monkeypatch.delenv("DOWNLOAD_BBOX", raising=False)
    monkeypatch.delenv("DEFAULT_DOWNLOAD_BBOX", raising=False)
    monkeypatch.setattr(downloader, "_get_dynamic_function", lambda _: fake_download)

    with pytest.raises(HTTPException) as exc_info:
        downloader.download_dataset(
            dataset=dataset,
            start="2020-01-01",
            end="2020-01-31",
            bbox=None,
            country_code=None,
            overwrite=False,
            background_tasks=None,
        )

    assert exc_info.value.status_code == 400
    assert "A bbox is required" in str(exc_info.value.detail)


def test_download_dataset_returns_502_for_upstream_provider_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_download(
        *,
        start: str,
        end: str,
        dirname: object,
        prefix: str,
        overwrite: bool,
        country_code: str,
    ) -> None:
        del start, end, dirname, prefix, overwrite, country_code
        raise RuntimeError("provider timeout")

    dataset: dict[str, Any] = {
        "id": "worldpop_population_yearly",
        "ingestion": {"function": "ignored.path"},
    }
    monkeypatch.setattr(downloader, "_get_dynamic_function", lambda _: fake_download)

    with pytest.raises(HTTPException) as exc_info:
        downloader.download_dataset(
            dataset=dataset,
            start="2020-01-01",
            end="2020-12-31",
            bbox=None,
            country_code="SLE",
            overwrite=False,
            background_tasks=None,
        )

    assert exc_info.value.status_code == 502
    assert "Upstream dataset download failed: provider timeout" == str(exc_info.value.detail)


# ---------------------------------------------------------------------------
# _get_cache_prefix
# ---------------------------------------------------------------------------


def test_get_cache_prefix_uses_dataset_id() -> None:
    dataset: dict[str, Any] = {"id": "chirps3_precipitation_daily", "ingestion": {}}
    assert downloader._get_cache_prefix(dataset) == "chirps3_precipitation_daily"


# ---------------------------------------------------------------------------
# _validate_spatial_coverage
# ---------------------------------------------------------------------------


_CHIRPS3_EXTENTS: dict[str, Any] = {
    "spatial": {"bbox": [-180, -50, 180, 50], "crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"}
}
_LIMITED_LON_EXTENTS: dict[str, Any] = {
    "spatial": {"bbox": [-180, -90, 60, 90], "crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"}
}


def test_validate_spatial_coverage_passes_when_no_extents_declared() -> None:
    dataset: dict[str, Any] = {"id": "worldpop_population_yearly", "ingestion": {}}
    downloader._validate_spatial_coverage(dataset, bbox=[4.5, 57.9, 31.1, 71.2])


def test_validate_spatial_coverage_passes_when_no_bbox() -> None:
    dataset: dict[str, Any] = {"id": "chirps3_precipitation_daily", "ingestion": {}, "extents": _CHIRPS3_EXTENTS}
    downloader._validate_spatial_coverage(dataset, bbox=None)


def test_validate_spatial_coverage_passes_when_template_bbox_malformed() -> None:
    extents: dict[str, Any] = {"spatial": {"bbox": "not-a-list"}}
    dataset: dict[str, Any] = {"id": "bad_template", "ingestion": {}, "extents": extents}
    downloader._validate_spatial_coverage(dataset, bbox=[-10.0, -10.0, 10.0, 10.0])


def test_validate_spatial_coverage_passes_when_bbox_inside_extents() -> None:
    dataset: dict[str, Any] = {"id": "chirps3_precipitation_daily", "ingestion": {}, "extents": _CHIRPS3_EXTENTS}
    downloader._validate_spatial_coverage(dataset, bbox=[-10.0, -10.0, 10.0, 10.0])


def test_validate_spatial_coverage_raises_when_bbox_outside_lat_extents() -> None:
    dataset: dict[str, Any] = {
        "id": "chirps3_precipitation_daily",
        "ingestion": {},
        "extents": _CHIRPS3_EXTENTS,
    }
    with pytest.raises(HTTPException) as exc_info:
        downloader._validate_spatial_coverage(dataset, bbox=[4.5, 57.9, 31.1, 71.2])
    assert exc_info.value.status_code == 400
    assert "does not cover this extent" in str(exc_info.value.detail)
    assert "Latitude" in str(exc_info.value.detail)


def test_validate_spatial_coverage_raises_when_bbox_outside_lon_extents() -> None:
    dataset: dict[str, Any] = {
        "id": "some_dataset",
        "ingestion": {},
        "extents": _LIMITED_LON_EXTENTS,
    }
    with pytest.raises(HTTPException) as exc_info:
        downloader._validate_spatial_coverage(dataset, bbox=[70.0, -10.0, 90.0, 10.0])
    assert exc_info.value.status_code == 400
    assert "Longitude" in str(exc_info.value.detail)


def test_download_dataset_validates_env_bbox_against_extents(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Coverage validation uses the env fallback bbox when no bbox is passed in the request."""
    dataset: dict[str, Any] = {
        "id": "chirps3_precipitation_daily",
        "ingestion": {"function": "ignored.path"},
        "extents": _CHIRPS3_EXTENTS,
    }
    monkeypatch.setenv("DOWNLOAD_BBOX", "4.5,57.9,31.1,71.2")

    with pytest.raises(HTTPException) as exc_info:
        downloader.download_dataset(
            dataset=dataset,
            start="2020-01-01",
            end="2020-01-31",
            bbox=None,
            country_code=None,
            overwrite=False,
            background_tasks=None,
        )
    assert exc_info.value.status_code == 400
    assert "does not cover this extent" in str(exc_info.value.detail)


def test_download_dataset_returns_400_when_bbox_outside_dataset_extents(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset: dict[str, Any] = {
        "id": "chirps3_precipitation_daily",
        "ingestion": {"function": "ignored.path"},
        "extents": _CHIRPS3_EXTENTS,
    }

    with pytest.raises(HTTPException) as exc_info:
        downloader.download_dataset(
            dataset=dataset,
            start="2020-01-01",
            end="2020-01-31",
            bbox=[4.5, 57.9, 31.1, 71.2],
            country_code=None,
            overwrite=False,
            background_tasks=None,
        )
    assert exc_info.value.status_code == 400
    assert "does not cover this extent" in str(exc_info.value.detail)


def test_download_dataset_returns_409_for_plugin_only_templates() -> None:
    dataset: dict[str, Any] = {
        "id": "chirps3_precipitation_daily",
        "ingestion": {"plugin": "open_climate_service.streaming.plugins.chirps3.CHIRPS3DailyPlugin"},
    }

    with pytest.raises(HTTPException) as exc_info:
        downloader.download_dataset(
            dataset=dataset,
            start="2020-01-01",
            end="2020-01-31",
            bbox=[-10.0, -10.0, 10.0, 10.0],
            country_code=None,
            overwrite=False,
            background_tasks=None,
        )

    assert exc_info.value.status_code == 409
    assert "legacy download path" in str(exc_info.value.detail)


def test_download_dataset_returns_409_for_empty_legacy_function_string() -> None:
    dataset: dict[str, Any] = {
        "id": "chirps3_precipitation_daily",
        "ingestion": {"function": ""},
    }

    with pytest.raises(HTTPException) as exc_info:
        downloader.download_dataset(
            dataset=dataset,
            start="2020-01-01",
            end="2020-01-31",
            bbox=[-10.0, -10.0, 10.0, 10.0],
            country_code=None,
            overwrite=False,
            background_tasks=None,
        )

    assert exc_info.value.status_code == 409
    assert "legacy download path" in str(exc_info.value.detail)


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


def _write_nc_files(tmp_path: Path) -> list[Path]:
    paths = []
    for year in (2020, 2021):
        ds = xr.Dataset(
            {"pop_total": (["t", "lat", "lon"], np.ones((1, 3, 3), dtype="float32"))},
            coords={
                "t": [pd.Timestamp(f"{year}-01-01")],
                "lat": [10.0, 9.0, 8.0],
                "lon": [30.0, 31.0, 32.0],
            },
        )
        path = tmp_path / f"my_dataset_{year}.nc"
        ds.to_netcdf(path)
        paths.append(path)
    return paths


def _write_daily_nc_file(tmp_path: Path) -> list[Path]:
    ds = xr.Dataset(
        {"precip": (["t", "lat", "lon"], np.ones((29, 3, 3), dtype="float32"))},
        coords={
            "t": pd.date_range("2024-02-01", "2024-02-29", freq="D"),
            "lat": [10.0, 9.0, 8.0],
            "lon": [30.0, 31.0, 32.0],
        },
    )
    path = tmp_path / "chirps3_precipitation_daily_2024-02.nc"
    ds.to_netcdf(path)
    return [path]


_FLAT_DATASET: dict[str, Any] = {
    "id": "my_dataset",
    "variable": "pop_total",
    "period_type": "yearly",
    "ingestion": {},
}

_PYRAMID_DATASET: dict[str, Any] = {
    "id": "my_dataset",
    "variable": "pop_total",
    "period_type": "yearly",
    "ingestion": {},
}


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
        assert result.sizes["t"] == 2
    finally:
        result.close()


def test_open_zarr_dataset_pyramid_falls_back_to_level_0(tmp_path: Path) -> None:
    """Pyramid zarr with no data vars at root falls back to opening /0."""
    zarr_path = tmp_path / "pyramid.zarr"
    zarr.open_group(str(zarr_path), mode="w", zarr_format=3)
    _make_dataset().to_zarr(str(zarr_path / "0"), mode="w", zarr_format=3)

    result = open_zarr_dataset(str(zarr_path))
    try:
        assert "pop_total" in result.data_vars
        assert result.sizes["t"] == 2
    finally:
        result.close()


def test_open_zarr_dataset_pyramid_with_root_time_still_opens_level_0(tmp_path: Path) -> None:
    """Root-level time coord (copied for zarr-layer) does not confuse the fallback.

    The fallback triggers on empty data_vars, not empty dims, so a root group
    that only has a 't' coordinate array still falls back to /0.
    """
    ds = _make_dataset()
    zarr_path = tmp_path / "pyramid.zarr"
    zarr.open_group(str(zarr_path), mode="w", zarr_format=3)
    ds.to_zarr(str(zarr_path / "0"), mode="w", zarr_format=3)
    # Simulate what build_dataset_zarr does: copy time to root
    import shutil

    shutil.copytree(str(zarr_path / "0" / "t"), str(zarr_path / "t"))

    result = open_zarr_dataset(str(zarr_path))
    try:
        assert "pop_total" in result.data_vars
    finally:
        result.close()


def test_open_icechunk_dataset_falls_back_to_level_0_when_root_has_no_data_vars(tmp_path: Path) -> None:
    """Icechunk store with data only under group 0 falls back to the base level."""
    store_path = tmp_path / "pyramid.icechunk"
    storage = icechunk.local_filesystem_storage(str(store_path))
    repo = icechunk.Repository.create(storage)
    session = repo.writable_session("main")
    _make_dataset().to_zarr(session.store, group="0", mode="w", zarr_format=3)
    session.commit("seed pyramid level 0")

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
# build_dataset_zarr — flat path
# ---------------------------------------------------------------------------


def test_build_dataset_zarr_flat_creates_zarr(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Flat zarr is written with the correct variable and no pyramid level dirs."""
    nc_files = _write_nc_files(tmp_path)
    monkeypatch.setattr(downloader, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(downloader, "get_cache_files", lambda _: nc_files)

    downloader.build_dataset_zarr(_FLAT_DATASET)

    zarr_path = tmp_path / "my_dataset.zarr"
    assert zarr_path.exists()
    assert not (zarr_path / "0").exists()

    result = open_zarr_dataset(str(zarr_path))
    try:
        assert "pop_total" in result.data_vars
        assert result.sizes["t"] == 2
    finally:
        result.close()

    root = zarr.open_group(str(zarr_path), mode="r")
    assert root.attrs["spatial:dimensions"] == ["x", "y"]
    assert root.attrs["spatial:shape"] == [3, 3]


def test_build_dataset_zarr_normalises_coordinate_names(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Source coordinates (lat/lon, valid_time) are renamed to x/y/time."""
    # Simulate ERA5-Land source with valid_time and lon/lat
    ds_era5 = xr.Dataset(
        {"t2m": (["valid_time", "lat", "lon"], np.ones((2, 3, 3), dtype="float32"))},
        coords={
            "valid_time": pd.date_range("2024-01-01", periods=2, freq="h"),
            "lat": [10.0, 9.0, 8.0],
            "lon": [30.0, 31.0, 32.0],
        },
    )
    path = tmp_path / "era5_t2m_2024-01.nc"
    ds_era5.to_netcdf(path)

    dataset: dict[str, Any] = {
        "id": "era5land_temperature_hourly",
        "variable": "t2m",
        "period_type": "hourly",
        "ingestion": {},
    }
    monkeypatch.setattr(downloader, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(downloader, "get_cache_files", lambda _: [path])

    downloader.build_dataset_zarr(dataset)

    result = open_zarr_dataset(str(tmp_path / "era5land_temperature_hourly.zarr"))
    try:
        assert "t" in result.coords
        assert "x" in result.coords
        assert "y" in result.coords
        assert "valid_time" not in result.coords
        assert "lat" not in result.coords
        assert "lon" not in result.coords
    finally:
        result.close()


def test_build_dataset_zarr_normalises_xy_coordinate_names(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Source coordinates already named x/y are preserved as x/y."""
    ds_xy = xr.Dataset(
        {"precip": (["t", "y", "x"], np.ones((2, 3, 3), dtype="float32"))},
        coords={
            "t": pd.date_range("2024-01-01", periods=2, freq="D"),
            "y": [10.0, 9.0, 8.0],
            "x": [30.0, 31.0, 32.0],
        },
    )
    path = tmp_path / "chirps_xy_2024-01.nc"
    ds_xy.to_netcdf(path)

    dataset: dict[str, Any] = {
        "id": "chirps3_precipitation_daily",
        "variable": "precip",
        "period_type": "daily",
        "ingestion": {},
    }
    monkeypatch.setattr(downloader, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(downloader, "get_cache_files", lambda _: [path])

    downloader.build_dataset_zarr(dataset)

    result = open_zarr_dataset(str(tmp_path / "chirps3_precipitation_daily.zarr"))
    try:
        assert "t" in result.coords
        assert "x" in result.coords
        assert "y" in result.coords
    finally:
        result.close()


def test_build_dataset_zarr_clips_to_requested_daily_range(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Provider cache files may contain full months; canonical Zarr honors request scope."""
    nc_files = _write_daily_nc_file(tmp_path)
    dataset: dict[str, Any] = {
        "id": "chirps3_precipitation_daily",
        "variable": "precip",
        "period_type": "daily",
        "ingestion": {},
    }
    monkeypatch.setattr(downloader, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(downloader, "get_cache_files", lambda _: nc_files)

    downloader.build_dataset_zarr(dataset, start="2024-02-01", end="2024-02-10")

    result = open_zarr_dataset(str(tmp_path / "chirps3_precipitation_daily.zarr"))
    try:
        assert result.sizes["t"] == 10
        assert pd.Timestamp(result.t.min().item()).strftime("%Y-%m-%d") == "2024-02-01"
        assert pd.Timestamp(result.t.max().item()).strftime("%Y-%m-%d") == "2024-02-10"
    finally:
        result.close()


# ---------------------------------------------------------------------------
# build_dataset_zarr — pyramid path
# ---------------------------------------------------------------------------


def _make_fake_pyramid(ds: xr.Dataset, zarr_path: Path) -> Pyramid:
    """Return a Pyramid whose .dt.to_zarr writes a minimal two-level DataTree store."""
    level0 = ds
    level1 = ds.coarsen(y=2, x=2, boundary="trim").mean()  # pyright: ignore[reportAttributeAccessIssue]
    dt = DataTree.from_dict({"0": level0, "1": level1})
    return Pyramid(datatree=dt, encoding={})


def test_build_dataset_zarr_pyramid_copies_time_to_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Pyramid zarr build copies the time coordinate to the store root for zarr-layer."""
    nc_files = _write_nc_files(tmp_path)
    monkeypatch.setattr(downloader, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(downloader, "get_cache_files", lambda _: nc_files)
    monkeypatch.setattr(downloader, "_needs_pyramid", lambda *_: True)

    def fake_create_pyramid(ds: xr.Dataset, levels: int, x_dim: str, y_dim: str, method: str) -> Pyramid:
        return _make_fake_pyramid(ds, tmp_path / "my_dataset.zarr")

    monkeypatch.setattr(downloader, "create_pyramid", fake_create_pyramid)

    downloader.build_dataset_zarr(_PYRAMID_DATASET)

    zarr_path = tmp_path / "my_dataset.zarr"
    assert (zarr_path / "0").exists(), "pyramid level 0 should exist"
    assert (zarr_path / "t").exists(), "t coordinate must be copied to zarr root"


def test_build_dataset_zarr_pyramid_is_openable_via_level_0(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """open_zarr_dataset returns the dataset from level 0 of the pyramid store."""
    nc_files = _write_nc_files(tmp_path)
    monkeypatch.setattr(downloader, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(downloader, "get_cache_files", lambda _: nc_files)
    monkeypatch.setattr(downloader, "_needs_pyramid", lambda *_: True)

    def fake_create_pyramid(ds: xr.Dataset, levels: int, x_dim: str, y_dim: str, method: str) -> Pyramid:
        return _make_fake_pyramid(ds, tmp_path / "my_dataset.zarr")

    monkeypatch.setattr(downloader, "create_pyramid", fake_create_pyramid)

    downloader.build_dataset_zarr(_PYRAMID_DATASET)

    result = open_zarr_dataset(str(tmp_path / "my_dataset.zarr"))
    try:
        assert "pop_total" in result.data_vars
        assert result.sizes["t"] == 2
    finally:
        result.close()


def test_build_dataset_zarr_pyramid_normalises_coordinate_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pyramid zarr store uses canonical x/y/time coordinate names."""
    # Source files use lat/lon (WorldPop-style); canonical names must appear in the written store.
    nc_files = _write_nc_files(tmp_path)
    monkeypatch.setattr(downloader, "DOWNLOAD_DIR", tmp_path)
    monkeypatch.setattr(downloader, "get_cache_files", lambda _: nc_files)
    monkeypatch.setattr(downloader, "_needs_pyramid", lambda *_: True)

    received: list[xr.Dataset] = []

    def fake_create_pyramid(ds: xr.Dataset, levels: int, x_dim: str, y_dim: str, method: str) -> Pyramid:
        received.append(ds)
        return _make_fake_pyramid(ds, tmp_path / "my_dataset.zarr")

    monkeypatch.setattr(downloader, "create_pyramid", fake_create_pyramid)

    downloader.build_dataset_zarr(_PYRAMID_DATASET)

    # The dataset handed to create_pyramid must already carry canonical names.
    assert len(received) == 1
    ds_in = received[0]
    assert "x" in ds_in.coords
    assert "y" in ds_in.coords
    assert "t" in ds_in.coords
    assert "lon" not in ds_in.coords
    assert "lat" not in ds_in.coords

    # The written store must also expose canonical names when opened.
    result = open_zarr_dataset(str(tmp_path / "my_dataset.zarr"))
    try:
        assert "x" in result.coords
        assert "y" in result.coords
        assert "t" in result.coords
    finally:
        result.close()


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
