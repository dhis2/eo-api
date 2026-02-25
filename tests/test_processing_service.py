from pathlib import Path

import pytest

from eoapi.processing.providers.base import RasterFetchResult
from eoapi.processing.service import execute_skeleton_process

try:
    import xarray as xr  # type: ignore

    HAS_XARRAY = True
except ImportError:
    xr = None
    HAS_XARRAY = False


class _FakeProvider:
    provider_id = "fake-provider"

    def fetch(self, request):
        return RasterFetchResult(
            provider=self.provider_id,
            asset_paths=[f"/tmp/{request.parameter}.nc"],
            from_cache=True,
        )


def test_skeleton_zonal_stats_includes_stub_rows_csv_and_dhis2(monkeypatch) -> None:
    monkeypatch.setattr("eoapi.processing.service.build_provider", lambda dataset: _FakeProvider())

    job = execute_skeleton_process(
        "raster.zonal_stats",
        {
            "dataset_id": "chirps-daily",
            "params": ["precip"],
            "time": "2026-01-15",
            "aoi": [30.0, -10.0, 31.0, -9.0],
        },
    )

    outputs = job["outputs"]
    assert outputs["provider"] == "fake-provider"
    assert outputs["rows"][0]["operation"] == "zonal_stats"
    assert outputs["rows"][0]["status"] in {"missing_assets", "read_error", "no_data", "computed", "missing_dependency"}
    assert "parameter" in outputs["csv"].splitlines()[0]
    assert outputs["dhis2"]["status"] == "stub"
    assert outputs["implementation"]["provider"]["id"] == "fake-provider"
    assert "xarray" in outputs["implementation"]["compute"]["libs"]


def test_skeleton_point_timeseries_includes_stub_rows(monkeypatch) -> None:
    monkeypatch.setattr("eoapi.processing.service.build_provider", lambda dataset: _FakeProvider())

    job = execute_skeleton_process(
        "raster.point_timeseries",
        {
            "dataset_id": "chirps-daily",
            "params": ["precip"],
            "time": "2026-01-16",
            "aoi": {"bbox": [30.0, -10.0, 32.0, -8.0]},
        },
    )

    outputs = job["outputs"]
    assert outputs["rows"][0]["operation"] == "point_timeseries"
    assert outputs["rows"][0]["point"] == [31.0, -9.0]


@pytest.mark.skipif(not HAS_XARRAY, reason="xarray not available")
def test_skeleton_zonal_stats_computes_numeric_value(monkeypatch, tmp_path: Path) -> None:
    path = tmp_path / "precip_20260131_20260131_test.nc"
    ds = xr.Dataset(
        {"precip": (("lat", "lon"), [[1.0, 2.0], [3.0, 4.0]])},
        coords={"lat": [-9.75, -9.25], "lon": [30.25, 30.75]},
    )
    ds.to_netcdf(path, engine="scipy")

    class RealFileProvider:
        provider_id = "file-provider"

        def fetch(self, request):
            return RasterFetchResult(
                provider=self.provider_id,
                asset_paths=[str(path)],
                from_cache=True,
            )

    monkeypatch.setattr("eoapi.processing.service.build_provider", lambda dataset: RealFileProvider())

    job = execute_skeleton_process(
        "raster.zonal_stats",
        {
            "dataset_id": "chirps-daily",
            "params": ["precip"],
            "time": "2026-01-31",
            "aoi": [30.0, -10.0, 31.0, -9.0],
            "aggregation": "mean",
        },
    )

    row = job["outputs"]["rows"][0]
    assert row["operation"] == "zonal_stats"
    assert row["status"] == "computed"
    assert row["value"] == pytest.approx(2.5, abs=1e-6)
