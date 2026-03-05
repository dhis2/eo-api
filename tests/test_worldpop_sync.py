from pathlib import Path

import pytest

from eo_api.integrations.worldpop_sync import build_sync_plan, sync_worldpop
from eo_api.routers.ogcapi.plugins.processes.schemas import WorldPopSyncInput


def test_worldpop_sync_input_requires_exactly_one_scope() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        WorldPopSyncInput.model_validate({"start_year": 2015, "end_year": 2016})

    with pytest.raises(ValueError, match="exactly one"):
        WorldPopSyncInput.model_validate(
            {
                "country_code": "SLE",
                "bbox": [-13.3, 6.9, -10.2, 10.0],
                "start_year": 2015,
                "end_year": 2016,
            }
        )


def test_build_sync_plan_by_country(tmp_path: Path) -> None:
    plan = build_sync_plan(
        country_code="sle",
        bbox=None,
        start_year=2025,
        end_year=2026,
        output_format="geotiff",
        root_dir=tmp_path,
    )

    assert plan["scope_key"] == "iso_SLE"
    assert plan["years"] == [2025, 2026]
    assert len(plan["planned_files"]) == 2
    assert plan["planned_files"][0].endswith("worldpop-global-2-total-population-1km_iso_SLE_2025.tif")
    assert plan["manifest_path"].endswith("iso_SLE_2025_2026_geotiff.json")


def test_sync_worldpop_writes_manifest_when_not_dry_run(tmp_path: Path) -> None:
    plan = sync_worldpop(
        country_code=None,
        bbox=[-13.3, 6.9, -10.2, 10.0],
        start_year=2025,
        end_year=2025,
        output_format="zarr",
        root_dir=tmp_path,
        dry_run=False,
    )

    manifest_path = Path(plan["manifest_path"])
    assert manifest_path.exists()


def test_sync_worldpop_downloads_netcdf_for_country_scope(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    expected_file = tmp_path / "iso_ETH" / "netcdf" / "downloaded.nc"
    expected_file.parent.mkdir(parents=True, exist_ok=True)
    expected_file.write_text("stub", encoding="utf-8")

    def _fake_download(**_: object) -> list[str]:
        return [str(expected_file)]

    from eo_api.integrations import worldpop_sync

    monkeypatch.setattr(worldpop_sync, "_download_worldpop_yearly", _fake_download)
    plan = sync_worldpop(
        country_code="ETH",
        bbox=None,
        start_year=2025,
        end_year=2025,
        output_format="netcdf",
        root_dir=tmp_path,
        dry_run=False,
    )

    assert plan["implementation_status"] == "country_netcdf_download"
    assert plan["planned_files"] == [str(expected_file)]
    assert plan["existing_files"] == [str(expected_file)]
    assert plan["missing_files"] == []
    assert Path(plan["manifest_path"]).exists()


def test_sync_worldpop_netcdf_rejects_bbox_only_scope(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="requires country_code"):
        sync_worldpop(
            country_code=None,
            bbox=[-13.3, 6.9, -10.2, 10.0],
            start_year=2025,
            end_year=2025,
            output_format="netcdf",
            root_dir=tmp_path,
            dry_run=False,
        )
