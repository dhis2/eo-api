"""Climate normals computation service."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import numpy as np
import xarray as xr

from open_climate_service import config as api_config
from open_climate_service.ingestions import services as artifact_services
from open_climate_service.ingestions.schemas import ArtifactFormat, ArtifactRecord
from open_climate_service.normals.schemas import NormalsRequest, NormalsResponse
from open_climate_service.shared.time import utc_now

_EDH_DAILY_URL = "https://api.earthdatahub.destine.eu/era5/era5-land-daily-utc-v1.zarr"
_EDH_API_KEY_ENV = "EDH_API_KEY"

# Map from source dataset ID to (edh_variable, unit_transform)
_EDH_DAILY_SOURCES: dict[str, tuple[str, str]] = {
    "era5land_temperature_daily": ("t2m", "kelvin_to_celsius"),
    "era5land_precipitation_daily": ("tp", "metres_to_mm"),
}


def _edh_open_daily() -> xr.Dataset:
    url = _EDH_DAILY_URL
    token = os.environ.get(_EDH_API_KEY_ENV, "")
    if token:
        parts = urlparse(url)
        url = urlunparse(parts._replace(netloc=f"edh:{token}@{parts.netloc}"))
    return xr.open_zarr(url, consolidated=True, storage_options={"client_kwargs": {"trust_env": True}})  # type: ignore[no-any-return]


def _circular_rolling_mean(da: xr.DataArray, window: int) -> xr.DataArray:
    """31-day circular rolling mean over dayofyear (wraps Dec→Jan)."""
    # Tile three copies to handle wrap-around, take the middle
    vals = np.concatenate([da.values, da.values, da.values], axis=0)
    result = np.empty_like(da.values)
    half = window // 2
    n = da.sizes["dayofyear"]
    for i in range(n):
        centre = n + i  # offset into the tripled array
        result[i] = vals[centre - half : centre + half + 1].mean(axis=0)
    return da.copy(data=result)


def _normals_store_path(normals_id: str) -> Path:
    data_dir = api_config.get_data_dir()
    if data_dir is None:
        raise RuntimeError("data_dir must be set in climate-service.yaml")
    path = data_dir / "normals" / f"{normals_id}.zarr"
    path.mkdir(parents=True, exist_ok=True)
    return path


def compute_normals(request: NormalsRequest, bbox: list[float]) -> NormalsResponse:
    """Compute day-of-year climate normals and register as a managed artifact."""
    source_id = request.source_dataset_id
    if source_id not in _EDH_DAILY_SOURCES:
        raise ValueError(
            f"'{source_id}' is not supported for direct EDH normals computation. Supported: {list(_EDH_DAILY_SOURCES)}"
        )

    edh_var, transform = _EDH_DAILY_SOURCES[source_id]
    start_year, end_year = request.period
    eps = 0.05
    xmin, ymin, xmax, ymax = map(float, bbox)
    lon_min = (xmin % 360) - eps
    lon_max = (xmax % 360) + eps

    # Load 1991-2020 slice directly from EDH
    ds = _edh_open_daily()
    region = (
        ds[[edh_var]]
        .sel(
            latitude=slice(ymax + eps, ymin - eps),
            longitude=slice(lon_min, lon_max),
            valid_time=slice(f"{start_year}-01-01", f"{end_year}-12-31"),
        )
        .load()
    )
    ds.close()

    # Normalise longitude 0–360 → –180/180
    region = region.assign_coords(longitude=((region.longitude + 180) % 360) - 180)

    # Day-of-year climatology
    normals = region.groupby("valid_time.dayofyear").mean("valid_time")

    if request.smoothing_window > 0:
        normals[edh_var] = _circular_rolling_mean(normals[edh_var], request.smoothing_window)

    # Apply unit transform
    if transform == "kelvin_to_celsius":
        normals[edh_var] = normals[edh_var] - 273.15
        normals[edh_var].attrs["units"] = "degC"
    elif transform == "metres_to_mm":
        normals[edh_var] = normals[edh_var] * 1000
        normals[edh_var].attrs["units"] = "mm"

    normals = normals.rename({"longitude": "x", "latitude": "y"})

    # Build dataset ID and persist
    normals_id = f"{source_id}_normals_{start_year}_{end_year}"
    store_path = _normals_store_path(normals_id)
    normals.to_zarr(store_path, mode="w", zarr_format=3)

    # Register as artifact
    variable = edh_var
    artifact = _register_normals_artifact(
        normals_id=normals_id,
        source_dataset_id=source_id,
        store_path=store_path,
        variable=variable,
        period=request.period,
        normals=normals,
        publish=request.publish,
    )

    dataset_summary = artifact_services.get_dataset_summary_for_artifact_or_404(artifact.artifact_id)

    return NormalsResponse(
        normals_id=normals_id,
        source_dataset_id=source_id,
        period=request.period,
        smoothing_window=request.smoothing_window,
        status="completed",
        dataset=dataset_summary.model_dump(),
    )


def _register_normals_artifact(
    *,
    normals_id: str,
    source_dataset_id: str,
    store_path: Path,
    variable: str,
    period: tuple[int, int],
    normals: xr.Dataset,
    publish: bool,
) -> ArtifactRecord:
    from uuid import uuid4

    from open_climate_service.ingestions.schemas import (
        ArtifactCoverage,
        ArtifactPublication,
        ArtifactRecord,
        ArtifactRequestScope,
        CoverageSpatial,
        CoverageTemporal,
        PublicationStatus,
    )
    from open_climate_service.ingestions.services import _load_records, _save_records

    xmin = float(normals.x.min())
    xmax = float(normals.x.max())
    ymin = float(normals.y.min())
    ymax = float(normals.y.max())

    artifact = ArtifactRecord(
        artifact_id=str(uuid4()),
        dataset_id=normals_id,
        source_dataset_id=normals_id,
        dataset_name=f"{source_dataset_id} normals ({period[0]}–{period[1]})",
        variable=variable,
        period_type="climatology",
        format=ArtifactFormat.ZARR,
        path=str(store_path),
        request_scope=ArtifactRequestScope(
            start=str(period[0]),
            end=str(period[1]),
            bbox=(xmin, ymin, xmax, ymax),
        ),
        coverage=ArtifactCoverage(
            spatial=CoverageSpatial(xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax),
            temporal=CoverageTemporal(start=str(period[0]), end=str(period[1])),
        ),
        publication=ArtifactPublication(
            status=PublicationStatus.PUBLISHED if publish else PublicationStatus.UNPUBLISHED,
        ),
        created_at=utc_now(),
    )

    records = _load_records()
    records.append(artifact)
    _save_records(records)

    return artifact
