"""WorldPop sync planning utilities shared by API wrappers."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

from dhis2eo.data.worldpop.pop_total import yearly as pop_total_yearly

from eo_api.utils.cache import bbox_token, write_manifest

WORLDPOP_DATASET_ID = "worldpop-global-2-total-population-1km"
WORLDPOP_MIN_YEAR = 2015
WORLDPOP_MAX_YEAR = 2030


def _scope_key(country_code: str | None, bbox: Sequence[float] | None) -> str:
    """Build a stable scope key from country or bbox input."""
    if country_code:
        return f"iso_{country_code.upper()}"
    if bbox is None:
        raise ValueError("Expected bbox when country_code is not set")
    return f"bbox_{bbox_token(bbox)}"


def build_sync_plan(
    *,
    country_code: str | None,
    bbox: Sequence[float] | None,
    start_year: int,
    end_year: int,
    output_format: str,
    root_dir: Path,
) -> dict[str, Any]:
    """Build a deterministic sync plan for WorldPop data artifacts."""
    scope_key = _scope_key(country_code, bbox)
    years = list(range(start_year, end_year + 1))
    suffix = ".nc" if output_format == "netcdf" else (".tif" if output_format == "geotiff" else ".zarr")
    target_dir = root_dir / scope_key / output_format
    planned_files = [target_dir / f"{WORLDPOP_DATASET_ID}_{scope_key}_{year}{suffix}" for year in years]
    existing_files = [path for path in planned_files if path.exists()]
    missing_files = [path for path in planned_files if not path.exists()]
    manifest_path = root_dir / "manifests" / f"{scope_key}_{start_year}_{end_year}_{output_format}.json"

    return {
        "dataset_id": WORLDPOP_DATASET_ID,
        "scope_key": scope_key,
        "years": years,
        "output_format": output_format,
        "target_dir": str(target_dir),
        "planned_files": [str(path) for path in planned_files],
        "existing_files": [str(path) for path in existing_files],
        "missing_files": [str(path) for path in missing_files],
        "manifest_path": str(manifest_path),
    }


def _download_worldpop_yearly(
    *,
    start_year: int,
    end_year: int,
    country_code: str,
    dirname: str,
    prefix: str,
) -> list[str]:
    """Download WorldPop yearly files using dhis2eo."""
    files = pop_total_yearly.download(
        start=f"{start_year:04d}",
        end=f"{end_year:04d}",
        country_code=country_code,
        dirname=dirname,
        prefix=prefix,
        version="global2",
        overwrite=False,
    )
    return [str(path) for path in files]


def sync_worldpop(
    *,
    country_code: str | None,
    bbox: Sequence[float] | None,
    start_year: int,
    end_year: int,
    output_format: str,
    root_dir: Path,
    dry_run: bool,
) -> dict[str, Any]:
    """Create/update sync manifest for a WorldPop request scope."""
    plan = build_sync_plan(
        country_code=country_code,
        bbox=bbox,
        start_year=start_year,
        end_year=end_year,
        output_format=output_format,
        root_dir=root_dir,
    )

    target_dir = Path(plan["target_dir"])
    manifest_path = Path(plan["manifest_path"])
    if not dry_run:
        target_dir.mkdir(parents=True, exist_ok=True)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)

        if output_format == "netcdf":
            if country_code is None:
                raise ValueError("Real WorldPop netcdf sync currently requires country_code scope")
            downloaded_files = _download_worldpop_yearly(
                start_year=start_year,
                end_year=end_year,
                country_code=country_code.upper(),
                dirname=str(target_dir),
                prefix=f"{WORLDPOP_DATASET_ID}_{plan['scope_key']}",
            )
            plan["planned_files"] = downloaded_files
            plan["existing_files"] = [path for path in downloaded_files if Path(path).exists()]
            plan["missing_files"] = [path for path in downloaded_files if not Path(path).exists()]
            implementation_status = "country_netcdf_download"
        else:
            implementation_status = "planning_only"

        write_manifest(
            manifest_path,
            {
                "dataset_id": plan["dataset_id"],
                "scope_key": plan["scope_key"],
                "years": plan["years"],
                "output_format": plan["output_format"],
                "planned_files": plan["planned_files"],
                "existing_files": plan["existing_files"],
                "missing_files": plan["missing_files"],
                "implementation_status": implementation_status,
            },
        )
        plan["implementation_status"] = implementation_status
    else:
        plan["implementation_status"] = "planning_only"

    return plan
