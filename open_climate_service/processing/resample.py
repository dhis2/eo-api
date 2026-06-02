"""Temporal resampling for derived managed datasets."""

from __future__ import annotations

import logging
import os
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd
import xarray as xr
from fastapi import HTTPException

from open_climate_service.data_accessor.services.accessor import open_icechunk_dataset, open_zarr_dataset
from open_climate_service.data_manager.services.utils import get_time_dim
from open_climate_service.data_registry.services import datasets as registry_datasets
from open_climate_service.ingestions import services as ingestion_services
from open_climate_service.ingestions.schemas import (
    ArtifactFormat,
    ArtifactRecord,
    ArtifactRequestScope,
    PublicationStatus,
)
from open_climate_service.publications.services import managed_dataset_id_for_scope
from open_climate_service.shared.time import _coerce_numpy_datetime, utc_today

logger = logging.getLogger(__name__)


def _derived_data_dir() -> Path:
    """Return the directory used to store derived Zarr artifacts."""
    from open_climate_service import config as api_config

    data_dir = api_config.get_data_dir()
    if data_dir is not None:
        return data_dir / "derived"
    return Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share")) / "climate-service" / "derived"


DERIVED_DATA_DIR: Path = _derived_data_dir()


def _frequency_to_period_type(frequency: str) -> str:
    """Map a pandas frequency alias to a Open Climate Service period_type string."""
    name = type(pd.tseries.frequencies.to_offset(frequency)).__name__
    if any(x in name for x in ("Hour", "Minute", "Second")):
        return "hourly"
    if "Week" in name:
        return "weekly"
    if any(x in name for x in ("Month", "Quarter")):
        return "monthly"
    if any(x in name for x in ("Year", "Annual")):
        return "yearly"
    return "daily"


def derived_dataset_id(*, source_dataset_id: str, frequency: str, method: str) -> str:
    """Return the stable derived dataset id auto-generated from source + parameters."""
    freq_slug = re.sub(r"[^a-z0-9]", "_", frequency.lower()).strip("_")
    return f"{source_dataset_id}_{freq_slug}_{method}"


def materialize_resampled_artifact(
    *,
    source_dataset_id: str,
    frequency: str,
    method: str,
    start: str,
    end: str | None,
    overwrite: bool,
    publish: bool,
) -> ArtifactRecord:
    """Materialize a derived dataset by resampling an existing managed source dataset."""
    target_dataset_id = derived_dataset_id(source_dataset_id=source_dataset_id, frequency=frequency, method=method)
    resolved_end = end or utc_today().isoformat()

    existing = ingestion_services._find_existing_artifact(
        dataset_id=target_dataset_id,
        request_scope=ArtifactRequestScope(start=start, end=resolved_end),
    )
    if existing is not None and not overwrite:
        if publish and existing.publication.status != PublicationStatus.PUBLISHED:
            return ingestion_services.publish_artifact_record(existing.artifact_id)
        return existing

    source_dataset = registry_datasets.get_dataset(source_dataset_id)
    if source_dataset is None:
        raise HTTPException(status_code=404, detail=f"Source dataset template '{source_dataset_id}' not found")

    source_artifact = _resolve_source_artifact(source_dataset_id=source_dataset_id)
    target_managed_dataset_id = managed_dataset_id_for_scope(target_dataset_id)
    zarr_path = DERIVED_DATA_DIR / f"{target_managed_dataset_id}.zarr"

    source_ds = _open_source_dataset(source_artifact)
    try:
        resampled = _resample_dataset(
            source_ds=source_ds,
            source_period_type=str(source_dataset["period_type"]),
            frequency=frequency,
            method=method,
            start=start,
            end=resolved_end,
        )
        if resampled.sizes.get(get_time_dim(resampled), 0) == 0:
            raise HTTPException(status_code=409, detail="Source artifact does not contain any complete target periods")
        existing_realized = _find_existing_resampled_artifact(
            target_dataset_id=target_dataset_id,
            start=start,
            resampled=resampled,
        )
        if existing_realized is not None and not overwrite:
            if publish and existing_realized.publication.status != PublicationStatus.PUBLISHED:
                return ingestion_services.publish_artifact_record(existing_realized.artifact_id)
            return existing_realized
        _write_resampled_zarr(resampled, zarr_path)
    finally:
        source_ds.close()

    target_dataset: dict[str, object] = {
        "id": target_dataset_id,
        "source_dataset_id": source_dataset_id,
        "name": target_dataset_id,
        "variable": source_dataset.get("variable", "value"),
        "period_type": _frequency_to_period_type(frequency),
    }
    return ingestion_services.store_materialized_zarr_artifact(
        dataset=target_dataset,
        start=start,
        end=resolved_end,
        bbox=None,
        zarr_path=zarr_path,
        overwrite=overwrite,
        publish=publish,
    )


def _resolve_source_artifact(*, source_dataset_id: str) -> ArtifactRecord:
    managed_dataset_id = managed_dataset_id_for_scope(source_dataset_id)
    return ingestion_services.get_latest_artifact_for_dataset_or_404(managed_dataset_id)


def _open_source_dataset(source_artifact: ArtifactRecord) -> xr.Dataset:
    source_path = source_artifact.path or (source_artifact.asset_paths[0] if source_artifact.asset_paths else None)
    if source_path is None:
        raise HTTPException(status_code=409, detail="Source artifact has no resolvable store path")
    if source_artifact.format == ArtifactFormat.ICECHUNK:
        return open_icechunk_dataset(source_path)
    if source_artifact.format == ArtifactFormat.ZARR:
        return open_zarr_dataset(source_path)
    raise HTTPException(
        status_code=409,
        detail="Resampling currently requires a Zarr- or Icechunk-backed source artifact",
    )


def _resample_dataset(
    *,
    source_ds: xr.Dataset,
    source_period_type: str,
    frequency: str,
    method: str,
    start: str,
    end: str,
) -> xr.Dataset:
    time_dim = get_time_dim(source_ds)
    offset = pd.tseries.frequencies.to_offset(frequency)
    target_start = pd.Timestamp(start).to_pydatetime().replace(tzinfo=None)
    target_end_exclusive = (pd.Timestamp(end) + offset).to_pydatetime().replace(tzinfo=None)
    subset = source_ds.where(source_ds[time_dim] >= np.datetime64(target_start), drop=True)
    subset = subset.where(subset[time_dim] < np.datetime64(target_end_exclusive), drop=True)
    if subset.sizes.get(time_dim, 0) == 0:
        raise HTTPException(status_code=409, detail="Source artifact contains no data for the requested resample range")
    source_start = _coerce_numpy_datetime(subset[time_dim].values[0])
    source_end = _coerce_numpy_datetime(subset[time_dim].values[-1])

    with xr.set_options(keep_attrs=True):
        resampler = subset.resample(
            {time_dim: frequency},
            label="left",
            closed="left",
        )
        result = cast(xr.Dataset, getattr(resampler, method)())
    result = _drop_incomplete_edge_periods(
        result=result,
        source_start=source_start,
        source_end=source_end,
        source_period_type=source_period_type,
        frequency=frequency,
    )
    return result


def _drop_incomplete_edge_periods(
    *,
    result: xr.Dataset,
    source_start: datetime,
    source_end: datetime,
    source_period_type: str,
    frequency: str,
) -> xr.Dataset:
    time_dim = get_time_dim(result)
    if result.sizes.get(time_dim, 0) == 0:
        return result

    first_output_start = _coerce_numpy_datetime(result[time_dim].values[0])
    normalized_source_start = _normalize_source_period_start(source_start, source_period_type=source_period_type)
    if normalized_source_start > first_output_start:
        result = result.isel({time_dim: slice(1, None)})
        if result.sizes.get(time_dim, 0) == 0:
            return result

    last_output_start = _coerce_numpy_datetime(result[time_dim].values[-1])
    offset = pd.tseries.frequencies.to_offset(frequency)
    next_target_start = (pd.Timestamp(last_output_start) + offset).to_pydatetime().replace(tzinfo=None)
    required_source_end = _previous_source_period_start(next_target_start, source_period_type=source_period_type)
    normalized_source_end = _normalize_source_period_start(source_end, source_period_type=source_period_type)
    if normalized_source_end < required_source_end:
        return result.isel({time_dim: slice(0, -1)})
    return result


def _normalize_source_period_start(value: datetime, *, source_period_type: str) -> datetime:
    if source_period_type == "hourly":
        return value.replace(minute=0, second=0, microsecond=0)
    if source_period_type == "daily":
        return value.replace(hour=0, minute=0, second=0, microsecond=0)
    if source_period_type == "weekly":
        start = value - timedelta(days=value.weekday())
        return start.replace(hour=0, minute=0, second=0, microsecond=0)
    if source_period_type == "monthly":
        return value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if source_period_type == "yearly":
        return value.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    raise HTTPException(status_code=400, detail=f"Unsupported source period_type '{source_period_type}' for resampling")


def _find_existing_resampled_artifact(
    *,
    target_dataset_id: str,
    start: str,
    resampled: xr.Dataset,
) -> ArtifactRecord | None:
    time_dim = get_time_dim(resampled)
    realized_end = _coerce_numpy_datetime(resampled[time_dim].values[-1]).strftime("%Y-%m-%d")
    return ingestion_services._find_existing_artifact(
        dataset_id=target_dataset_id,
        request_scope=ArtifactRequestScope(start=start, end=realized_end),
    )


def _previous_source_period_start(boundary: datetime, *, source_period_type: str) -> datetime:
    if source_period_type == "hourly":
        return boundary - timedelta(hours=1)
    if source_period_type == "daily":
        return boundary - timedelta(days=1)
    if source_period_type == "weekly":
        return boundary - timedelta(days=7)
    if source_period_type == "monthly":
        previous_month_last_day = boundary.replace(day=1) - timedelta(days=1)
        return previous_month_last_day.replace(day=1)
    if source_period_type == "yearly":
        return boundary.replace(year=boundary.year - 1, month=1, day=1)
    raise HTTPException(status_code=400, detail=f"Unsupported source period_type '{source_period_type}' for resampling")


def _write_resampled_zarr(ds: xr.Dataset, zarr_path: Path) -> None:
    zarr_path.parent.mkdir(parents=True, exist_ok=True)
    if zarr_path.exists():
        shutil.rmtree(zarr_path)
    ds_chunked = ds.chunk("auto").unify_chunks()
    try:
        ds_chunked.to_zarr(zarr_path, mode="w", consolidated=True)
    finally:
        ds_chunked.close()
        ds.close()
