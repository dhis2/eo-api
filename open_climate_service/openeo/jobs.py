"""openEO job persistence and execution service."""

from __future__ import annotations

import json
import logging
import os
import re
import threading
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path
from typing import Any, TypeVar
from uuid import uuid4

import portalocker
from fastapi import HTTPException

from open_climate_service import config as api_config
from open_climate_service.openeo.schemas import (
    OpenEOJobCreate,
    OpenEOJobListResponse,
    OpenEOJobRecord,
    OpenEOJobResults,
    OpenEOJobStatus,
    OpenEOJobUpdate,
)
from open_climate_service.shared.time import utc_now

_T = TypeVar("_T")

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------


def _resolve_openeo_jobs_dir() -> Path:
    data_dir = api_config.get_data_dir()
    if data_dir is not None:
        return data_dir / "openeo_jobs"
    xdg_data = Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share"))
    return xdg_data / "climate-service" / "openeo_jobs"


_JOBS_DIR = _resolve_openeo_jobs_dir()
_JOBS_INDEX = _JOBS_DIR / "jobs.json"


def _ensure_store() -> None:
    _JOBS_DIR.mkdir(parents=True, exist_ok=True)
    if not _JOBS_INDEX.exists():
        _JOBS_INDEX.write_text("[]\n", encoding="utf-8")


def _load_raw_records() -> list[dict[str, object]]:
    _ensure_store()
    with open(_JOBS_INDEX, encoding="utf-8") as fh:
        portalocker.lock(fh, portalocker.LOCK_SH)
        try:
            payload = json.load(fh)
        finally:
            portalocker.unlock(fh)
    if not isinstance(payload, list):
        raise ValueError("openeo jobs.json must contain a list")
    return payload


def _mutate_store(mutation: Callable[[list[dict[str, object]]], _T]) -> _T:
    _ensure_store()
    with open(_JOBS_INDEX, "r+", encoding="utf-8") as fh:
        portalocker.lock(fh, portalocker.LOCK_EX)
        try:
            payload = json.load(fh)
            records: list[dict[str, object]] = payload if isinstance(payload, list) else []
            result = mutation(records)
            fh.seek(0)
            json.dump(records, fh, indent=2, default=str)
            fh.write("\n")
            fh.truncate()
            return result
        finally:
            portalocker.unlock(fh)


def store_list_jobs() -> list[OpenEOJobRecord]:
    """Return all persisted openEO job records."""
    return [OpenEOJobRecord.model_validate(r) for r in _load_raw_records()]


def store_get_job(job_id: str) -> OpenEOJobRecord | None:
    """Return one job record, or None if not found."""
    for raw in _load_raw_records():
        if raw.get("id") == job_id:
            return OpenEOJobRecord.model_validate(raw)
    return None


def store_create_job(record: OpenEOJobRecord) -> OpenEOJobRecord:
    """Persist a newly created job; raises ValueError if id already exists."""

    def _mutation(records: list[dict[str, object]]) -> OpenEOJobRecord:
        if any(r.get("id") == record.id for r in records):
            raise ValueError(f"Job '{record.id}' already exists")
        records.append(_serialize(record))
        return record

    return _mutate_store(_mutation)


def store_update_job(job_id: str, mutation: Callable[[OpenEOJobRecord], OpenEOJobRecord]) -> OpenEOJobRecord:
    """Load, mutate, and persist one existing job record."""

    def _apply(records: list[dict[str, object]]) -> OpenEOJobRecord:
        for idx, raw in enumerate(records):
            if raw.get("id") != job_id:
                continue
            updated = mutation(OpenEOJobRecord.model_validate(raw))
            records[idx] = _serialize(updated)
            return updated
        raise KeyError(job_id)

    return _mutate_store(_apply)


def store_delete_job(job_id: str) -> bool:
    """Delete a job; returns True if it existed."""

    def _mutation(records: list[dict[str, object]]) -> bool:
        for idx, raw in enumerate(records):
            if raw.get("id") == job_id:
                records.pop(idx)
                return True
        return False

    return _mutate_store(_mutation)


def _serialize(record: OpenEOJobRecord) -> dict[str, object]:
    # model_dump() respects Field(exclude=True) on error_message and cancel_requested,
    # which is correct for HTTP responses but wrong for disk persistence.
    # Explicitly re-add those fields so they survive a server restart.
    data: dict[str, object] = record.model_dump(mode="json", exclude_none=False)
    data["error_message"] = record.error_message
    data["cancel_requested"] = record.cancel_requested
    return data


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class OpenEOJobService:
    """Manages openEO job lifecycle and asynchronous execution."""

    def __init__(self, *, max_workers: int = 4) -> None:
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="openeo-job")
        self._futures: dict[str, Future[None]] = {}
        self._lock = threading.Lock()

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)

    def recover_pending_jobs(self) -> None:
        """Recover jobs left in a non-terminal state from a previous server run.

        QUEUED jobs are re-enqueued.  RUNNING jobs are marked ERROR because their
        executor thread no longer exists after the restart.
        """
        for record in store_list_jobs():
            if record.status == OpenEOJobStatus.RUNNING:
                logger.warning("openEO job %s was RUNNING at restart — marking as error", record.id)
                try:
                    store_update_job(
                        record.id,
                        lambda r: r.model_copy(
                            update={
                                "status": OpenEOJobStatus.ERROR,
                                "error_message": "Interrupted by server restart",
                                "updated": utc_now(),
                            }
                        ),
                    )
                except KeyError:
                    pass
            elif record.status == OpenEOJobStatus.QUEUED:
                logger.info("openEO job %s was QUEUED at restart — re-enqueueing", record.id)
                try:
                    self._enqueue(record.id)
                except Exception:
                    logger.exception("Failed to re-enqueue openEO job %s", record.id)

    # ------------------------------------------------------------------
    # HTTP-layer helpers
    # ------------------------------------------------------------------

    def list_jobs(self) -> OpenEOJobListResponse:
        records = sorted(store_list_jobs(), key=lambda r: r.created, reverse=True)
        return OpenEOJobListResponse(
            jobs=records,
            links=[{"rel": "self", "href": "/jobs", "type": "application/json"}],
        )

    def create_job(self, body: OpenEOJobCreate) -> OpenEOJobRecord:
        if not isinstance(body.process.get("process_graph"), dict):
            raise HTTPException(
                status_code=422,
                detail="process.process_graph must be an object",
            )
        job_id = str(uuid4())
        now = utc_now()
        record = OpenEOJobRecord(
            id=job_id,
            title=body.title if body.title is not None else _derive_job_title(body.process),
            description=body.description,
            process=body.process,
            status=OpenEOJobStatus.CREATED,
            created=now,
            updated=now,
            plan=body.plan,
            budget=body.budget,
            links=[
                {"rel": "self", "href": f"/jobs/{job_id}", "type": "application/json"},
                {"rel": "results", "href": f"/jobs/{job_id}/results", "type": "application/json"},
            ],
        )
        return store_create_job(record)

    def get_job_or_404(self, job_id: str) -> OpenEOJobRecord:
        record = store_get_job(job_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
        return record

    def update_job(self, job_id: str, body: OpenEOJobUpdate) -> OpenEOJobRecord:
        record = self.get_job_or_404(job_id)
        if record.status in {OpenEOJobStatus.QUEUED, OpenEOJobStatus.RUNNING}:
            raise HTTPException(status_code=400, detail="Cannot update a job that is queued or running")
        updates: dict[str, Any] = {}
        if body.title is not None:
            updates["title"] = body.title
        if body.description is not None:
            updates["description"] = body.description
        if body.process is not None:
            if not isinstance(body.process.get("process_graph"), dict):
                raise HTTPException(status_code=422, detail="process.process_graph must be an object")
            updates["process"] = body.process
        if body.plan is not None:
            updates["plan"] = body.plan
        if body.budget is not None:
            updates["budget"] = body.budget
        if updates:
            updates["updated"] = utc_now()
            return store_update_job(job_id, lambda r: r.model_copy(update=updates))
        return record

    def delete_job(self, job_id: str) -> None:
        record = self.get_job_or_404(job_id)
        if record.status in {OpenEOJobStatus.QUEUED, OpenEOJobStatus.RUNNING}:
            raise HTTPException(status_code=400, detail="Cannot delete a running job; cancel it first")
        store_delete_job(job_id)
        import shutil

        job_dir = _JOBS_DIR / job_id
        if job_dir.exists():
            shutil.rmtree(job_dir, ignore_errors=True)

    def start_job(self, job_id: str) -> None:
        """Queue a job for processing (POST /jobs/{id}/results)."""
        record = self.get_job_or_404(job_id)
        if record.status == OpenEOJobStatus.RUNNING:
            raise HTTPException(status_code=400, detail="Job is already running")
        if record.status == OpenEOJobStatus.QUEUED:
            return
        store_update_job(
            job_id,
            lambda r: r.model_copy(update={"status": OpenEOJobStatus.QUEUED, "updated": utc_now()}),
        )
        self._enqueue(job_id)

    def cancel_job(self, job_id: str) -> None:
        """Request cancellation (DELETE /jobs/{id}/results)."""
        record = self.get_job_or_404(job_id)
        if record.status not in {OpenEOJobStatus.QUEUED, OpenEOJobStatus.RUNNING}:
            raise HTTPException(status_code=400, detail="Job is not running or queued")

        # Read the future and attempt cancel while holding the lock so we don't race
        # with the executor thread transitioning QUEUED→RUNNING between our status
        # read and the future.cancel() call.
        with self._lock:
            future = self._futures.get(job_id)
            cancelled_before_start = future is not None and future.cancel()

        if cancelled_before_start:
            # future.cancel() returned True — the job was still queued in the thread
            # pool and will never start.  Transition the store atomically: only if the
            # status is still QUEUED (guards against the edge case where the worker
            # already set it to RUNNING before we got the lock).
            def _mark_canceled_if_queued(r: OpenEOJobRecord) -> OpenEOJobRecord:
                if r.status == OpenEOJobStatus.QUEUED:
                    return r.model_copy(update={"status": OpenEOJobStatus.CANCELED, "updated": utc_now()})
                # Race lost — worker already started; fall back to cooperative cancellation.
                return r.model_copy(update={"cancel_requested": True, "updated": utc_now()})

            store_update_job(job_id, _mark_canceled_if_queued)
        else:
            # Job is running (or no future registered yet) — set flag for cooperative
            # cancellation; the worker checks this before marking FINISHED.
            store_update_job(
                job_id,
                lambda r: r.model_copy(update={"cancel_requested": True, "updated": utc_now()}),
            )

    def get_results(self, job_id: str) -> OpenEOJobResults:
        """Return result asset links for a finished job."""
        record = self.get_job_or_404(job_id)
        if record.status == OpenEOJobStatus.ERROR:
            raise HTTPException(
                status_code=424,
                detail=record.error_message or "Job finished with an error",
            )
        if record.status != OpenEOJobStatus.FINISHED:
            raise HTTPException(
                status_code=400,
                detail=f"Results not available yet; job status is '{record.status}'",
            )
        assets = _result_assets(record)
        return OpenEOJobResults(
            stac_version="1.1.0",
            id=job_id,
            assets=assets,
            links=[{"rel": "self", "href": f"/jobs/{job_id}/results", "type": "application/json"}],
        )

    # ------------------------------------------------------------------
    # Internal execution
    # ------------------------------------------------------------------

    def _enqueue(self, job_id: str) -> None:
        with self._lock:
            existing = self._futures.get(job_id)
            if existing is not None and not existing.done():
                return
            future = self._pool.submit(self._run_job, job_id)
            self._futures[job_id] = future

    def _run_job(self, job_id: str) -> None:
        try:
            self._execute(job_id)
        finally:
            with self._lock:
                self._futures.pop(job_id, None)

    def _execute(self, job_id: str) -> None:
        from open_climate_service.openeo.execution import run_process_graph

        record = store_get_job(job_id)
        if record is None:
            return
        if record.cancel_requested:
            store_update_job(
                job_id,
                lambda r: r.model_copy(update={"status": OpenEOJobStatus.CANCELED, "updated": utc_now()}),
            )
            return

        store_update_job(
            job_id,
            lambda r: r.model_copy(update={"status": OpenEOJobStatus.RUNNING, "updated": utc_now()}),
        )

        try:
            result = run_process_graph(record.process)
            # Re-read record — cancellation may have been requested while running.
            current = store_get_job(job_id)
            if current is not None and current.cancel_requested:
                store_update_job(
                    job_id,
                    lambda r: r.model_copy(update={"status": OpenEOJobStatus.CANCELED, "updated": utc_now()}),
                )
                return
            output_path = self._persist_result(job_id, result)
            store_update_job(
                job_id,
                lambda r: r.model_copy(
                    update={
                        "status": OpenEOJobStatus.FINISHED,
                        "updated": utc_now(),
                        "usage": {"output_path": output_path} if output_path else {},
                    }
                ),
            )
        except Exception as job_exc:
            logger.exception("openEO job %s failed", job_id)
            error_msg = f"{type(job_exc).__name__}: {job_exc}"
            store_update_job(
                job_id,
                lambda r: r.model_copy(
                    update={
                        "status": OpenEOJobStatus.ERROR,
                        "error_message": error_msg,
                        "updated": utc_now(),
                    }
                ),
            )

    def _persist_result(self, job_id: str, result: Any) -> str | None:
        import xarray as xr

        from open_climate_service.openeo.execution import SaveResultEnvelope

        results_dir = _JOBS_DIR / job_id / "results"
        results_dir.mkdir(parents=True, exist_ok=True)

        # Unwrap format envelope from save_result
        fmt = "ZARR"
        options: dict[str, Any] = {}
        if isinstance(result, SaveResultEnvelope):
            fmt = result.format
            options = result.options
            result = result.data

        # Resolve DataArray → Dataset for raster formats
        if isinstance(result, xr.DataArray):
            result = result.to_dataset(name=result.name or "result")

        if isinstance(result, xr.Dataset):
            # Zarr format with dataset_id → write directly to managed Icechunk/Zarr store
            if fmt == "ZARR" and options.get("dataset_id"):
                _write_managed_zarr(result, options)
                # Managed datasets are not served as job-local files; advertise the
                # managed dataset via a marker that _result_assets expands into
                # /datasets, /zarr (and /stac when published) result links.
                return f"managed://{options['dataset_id']}"
            if fmt in _TABULAR_EXPORT_FORMATS:
                return _write_dataset_tabular_export(result, results_dir, fmt, options)
            return _write_raster(result, results_dir, fmt)

        # Tabular: resolve dask_geopandas → GeoDataFrame
        try:
            import dask_geopandas

            if isinstance(result, dask_geopandas.GeoDataFrame):
                result = result.compute()
        except ImportError:
            pass

        try:
            import geopandas as gpd

            if isinstance(result, gpd.GeoDataFrame):
                if fmt in _TABULAR_EXPORT_FORMATS:
                    return _write_tabular_export(
                        result.drop(columns="geometry", errors="ignore"),
                        results_dir,
                        fmt,
                        options,
                    )
                return _write_vector(result, results_dir, fmt)
        except ImportError:
            pass

        # Unrecognised result type — raise so the job is marked ERROR rather than
        # silently finishing with an empty assets dict and no indication of failure.
        raise TypeError(
            f"Unsupported result type '{type(result).__name__}': expected xr.Dataset, xr.DataArray, or GeoDataFrame"
        )


def _strip_non_serializable_attrs(ds: Any) -> Any:
    """Return a copy of ds with any non-JSON-serializable attrs removed.

    openeo-processes-dask injects numpy scalars and datetime64 values into
    variable attrs (e.g. reduced_dimensions_min_values) after reduce_dimension.
    Zarr requires all attrs to be JSON-serializable; strip the offenders so the
    write succeeds while keeping the data and coordinates intact.
    """
    import json

    def _safe(attrs: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for k, v in attrs.items():
            try:
                json.dumps(v)
                out[k] = v
            except (TypeError, ValueError):
                pass
        return out

    ds = ds.copy()
    ds.attrs = _safe(ds.attrs)
    for name in list(ds.data_vars) + list(ds.coords):
        if ds[name].attrs:
            ds[name].attrs = _safe(ds[name].attrs)
    return ds


def _write_managed_zarr(ds: Any, options: dict[str, Any]) -> None:
    """Write a computed xr.Dataset to the managed Icechunk/Zarr store and register it."""
    import uuid
    from datetime import UTC, datetime

    import xarray as xr

    from open_climate_service.data_manager.services import downloader
    from open_climate_service.data_manager.services.utils import get_time_dim, get_x_y_dims
    from open_climate_service.ingestions import services as ingestion_services
    from open_climate_service.ingestions.schemas import (
        ArtifactFormat,
        ArtifactPublication,
        ArtifactRecord,
        ArtifactRequestScope,
    )

    dataset_id = options["dataset_id"]
    if not isinstance(dataset_id, str) or not dataset_id:
        raise ValueError(f"'dataset_id' option must be a non-empty string, got {type(dataset_id).__name__}")
    if Path(dataset_id).name != dataset_id:
        raise ValueError(
            f"Invalid dataset_id '{dataset_id}': must be a plain name with no path separators or traversal segments"
        )

    from open_climate_service.data_registry.services import datasets as _reg

    template = _reg.get_dataset(dataset_id)
    if template is None:
        raise ValueError(
            f"No dataset template found for '{dataset_id}'. "
            "Register the dataset template before writing managed artifacts."
        )

    # Prefer an explicit option, then the registered template's display name, and
    # only fall back to the raw id so published collections read as e.g.
    # "Mosquito hotspots (Rwanda 2018 Q1)" rather than "mosquito_hotspots".
    dataset_name: str = options.get("dataset_name") or template.get("name") or dataset_id

    if not isinstance(ds, xr.Dataset):
        raise TypeError(f"Managed Zarr write requires an xr.Dataset, got {type(ds).__name__}")

    # Rename the variable in the store to match the user-specified variable name,
    # so the on-disk name matches what is advertised in the STAC collection.
    if options.get("variable") and len(ds.data_vars) == 1:
        current_name = next(iter(ds.data_vars))
        desired_name = str(options["variable"])
        if current_name != desired_name:
            ds = ds.rename({current_name: desired_name})

    try:
        x_dim, y_dim = get_x_y_dims(ds)
    except ValueError as exc:
        raise ValueError(f"Cannot write managed dataset '{dataset_id}': {exc}") from exc

    try:
        t_dim: str | None = get_time_dim(ds)
    except ValueError:
        t_dim = None

    coverage = _derive_coverage(ds, x_dim, y_dim, t_dim)
    variable = _derive_variable(ds, options)
    period_type: str | None = options.get("period_type") or (
        _infer_period_type(ds, t_dim) if t_dim is not None else None
    )

    from open_climate_service import config as api_config

    crs: str = ds.attrs.get("proj:code") or api_config.get_crs()
    store_path = downloader.DOWNLOAD_DIR / f"{dataset_id}.icechunk"
    store_path.parent.mkdir(parents=True, exist_ok=True)

    downloader.write_to_icechunk_store(
        _strip_non_serializable_attrs(ds),
        store_path,
        x_dim,
        y_dim,
        t_dim,
        crs=crs,
        commit_message=f"Published from openEO job: {dataset_id}",
    )

    record = ArtifactRecord(
        artifact_id=str(uuid.uuid4()),
        dataset_id=dataset_id,
        dataset_name=dataset_name,
        variable=variable,
        period_type=period_type,
        format=ArtifactFormat.ICECHUNK,
        path=str(store_path),
        asset_paths=[str(store_path)],
        variables=[str(v) for v in ds.data_vars],
        request_scope=ArtifactRequestScope(
            start=coverage.temporal.start,
            end=coverage.temporal.end,
        ),
        coverage=coverage,
        created_at=datetime.now(UTC),
        publication=ArtifactPublication(),
    )
    _publish_raw = options.get("publish", True)
    if not isinstance(_publish_raw, bool):
        raise ValueError(f"'publish' option must be a boolean, got {type(_publish_raw).__name__!r}: {_publish_raw!r}")
    ingestion_services.register_artifact_record(record, publish=_publish_raw)


def _recover_temporal_from_attrs(ds: Any) -> tuple[str, str]:
    """Extract temporal extent from reduce_dimension min/max attrs.

    openeo-processes-dask stores the reduced dimension's value range in
    ``reduced_dimensions_min_values`` / ``reduced_dimensions_max_values`` on each
    variable's attrs after ``reduce_dimension``.  Fall back to ("", "") when not found.
    """
    import numpy as np

    _TIME_NAMES = ("t", "time", "valid_time")
    sources: list[dict[str, Any]] = [ds.attrs]
    for name in list(ds.data_vars) + list(ds.coords):
        attrs = getattr(ds[name], "attrs", {})
        if attrs:
            sources.append(attrs)
    for attrs in sources:
        min_vals = attrs.get("reduced_dimensions_min_values", {})
        max_vals = attrs.get("reduced_dimensions_max_values", {})
        if not isinstance(min_vals, dict) or not isinstance(max_vals, dict):
            continue
        for tname in _TIME_NAMES:
            if tname in min_vals and tname in max_vals:
                try:
                    t_start = str(np.datetime_as_string(np.datetime64(min_vals[tname]), unit="D"))
                    t_end = str(np.datetime_as_string(np.datetime64(max_vals[tname]), unit="D"))
                    return t_start, t_end
                except Exception:
                    pass
    return "", ""


def _derive_coverage(ds: Any, x_dim: str, y_dim: str, t_dim: str | None) -> Any:
    """Derive ArtifactCoverage from an xr.Dataset's coordinates."""
    import numpy as np
    import pyproj

    from open_climate_service.ingestions.schemas import (
        ArtifactCoverage,
        CoverageSpatial,
        CoverageTemporal,
    )

    xmin = float(ds[x_dim].min())
    xmax = float(ds[x_dim].max())
    ymin = float(ds[y_dim].min())
    ymax = float(ds[y_dim].max())
    spatial = CoverageSpatial(xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax)

    native_crs: str = ds.attrs.get("proj:code", "EPSG:4326")
    spatial_wgs84: Any = None
    if native_crs not in ("EPSG:4326", "CRS84", "OGC:CRS84"):
        try:
            transformer = pyproj.Transformer.from_crs(native_crs, "EPSG:4326", always_xy=True)
            # transform_bounds is more accurate than transforming individual corners —
            # it densifies the edges, which matters for non-rectilinear projections.
            wx_min, wy_min, wx_max, wy_max = transformer.transform_bounds(xmin, ymin, xmax, ymax)
            spatial_wgs84 = CoverageSpatial(xmin=wx_min, ymin=wy_min, xmax=wx_max, ymax=wy_max)
        except Exception:
            pass

    if t_dim is not None and t_dim in ds.coords and ds.sizes.get(t_dim, 0) > 0:
        # min/max rather than first/last so coverage is correct for a non-monotonic time axis.
        t_values = ds[t_dim].values
        t_start = str(np.datetime_as_string(t_values.min(), unit="D"))
        t_end = str(np.datetime_as_string(t_values.max(), unit="D"))
    else:
        # Dataset has no time dimension (e.g. after reduce_dimension); recover the
        # original temporal range from attrs that openeo-processes-dask attaches.
        t_start, t_end = _recover_temporal_from_attrs(ds)

    return ArtifactCoverage(
        spatial=spatial,
        spatial_wgs84=spatial_wgs84,
        temporal=CoverageTemporal(start=t_start, end=t_end),
    )


def _infer_period_type(ds: Any, t_dim: str) -> str | None:
    """Infer period type from the median time step of a dataset."""
    import numpy as np

    if t_dim not in ds.coords or ds.sizes.get(t_dim, 0) < 2:
        return None

    # Sort first: streaming/append can leave a non-monotonic time axis, and an
    # unsorted np.diff yields negative/irregular steps that skew the median.
    t_values = np.sort(ds[t_dim].values)
    deltas = np.diff(t_values).astype("timedelta64[s]").astype(float)
    median_seconds = float(np.median(deltas))

    if median_seconds <= 3600:
        return "hourly"
    if median_seconds <= 86400:
        return "daily"
    if median_seconds <= 8 * 86400:
        return "weekly"
    if median_seconds <= 32 * 86400:
        return "monthly"
    if median_seconds <= 100 * 86400:
        return "quarterly"
    return "yearly"


def _derive_variable(ds: Any, options: dict[str, Any]) -> str:
    """Return the primary variable name from options or the sole data variable."""
    if options.get("variable"):
        name = str(options["variable"])
        if name not in ds.data_vars:
            raise ValueError(
                f"Variable {name!r} specified in options not found in dataset; available: {list(ds.data_vars)!r}"
            )
        return name
    vars_list = list(ds.data_vars)
    if len(vars_list) == 1:
        return str(vars_list[0])
    raise ValueError(f"Dataset has multiple variables {vars_list!r}; specify 'variable' in save_result options")


def _result_assets(record: OpenEOJobRecord) -> dict[str, Any]:
    usage = record.usage or {}
    output_path = usage.get("output_path")
    if not output_path or not isinstance(output_path, str):
        return {}
    if output_path.startswith("managed://"):
        dataset_id = output_path[len("managed://") :]
        assets: dict[str, Any] = {
            "dataset": {
                "href": f"/datasets/{dataset_id}",
                "type": "application/json",
                "title": "Managed dataset",
                "roles": ["metadata"],
            },
            "zarr": {
                "href": f"/zarr/{dataset_id}",
                "type": "application/vnd.zarr; version=3",
                "title": "Zarr store",
                "roles": ["data"],
                "xarray:open_kwargs": {"consolidated": True},
            },
        }
        # Advertise the STAC collection only when the dataset is actually published.
        try:
            from open_climate_service.ingestions import services as _ingestion_services

            if dataset_id in _ingestion_services.latest_published_zarr_artifacts_by_dataset():
                assets["stac"] = {
                    "href": f"/stac/collections/{dataset_id}",
                    "type": "application/json",
                    "title": "STAC collection",
                    "roles": ["metadata"],
                }
        except Exception:
            logger.debug("Could not resolve STAC publication for managed dataset '%s'", dataset_id, exc_info=True)
        return assets
    if output_path.endswith(".zarr"):
        return {
            "result": {
                # Trailing slash signals a directory root; Zarr HTTP clients
                # append chunk paths (e.g. .zmetadata, t/0.0) to this href.
                "href": f"/jobs/{record.id}/results/result.zarr/",
                "type": "application/x-zarr",
                "title": "Zarr result store",
                "roles": ["data"],
            }
        }
    if output_path.endswith(".geojson"):
        return {
            "result": {
                "href": f"/jobs/{record.id}/results/result.geojson",
                "type": "application/geo+json",
                "title": "GeoJSON result",
                "roles": ["data"],
            }
        }
    ext_map = {
        ".nc": ("application/netcdf", "NetCDF result"),
        ".tif": ("image/tiff; subtype=geotiff", "GeoTIFF result"),
        ".png": ("image/png", "PNG result"),
        ".csv": ("text/csv", "CSV result"),
        ".json": ("application/json", "JSON result"),
        ".parquet": ("application/vnd.apache.parquet", "GeoParquet result"),
    }
    for ext, (mime, title) in ext_map.items():
        if output_path.endswith(ext):
            fname = output_path.rsplit("/", 1)[-1]
            return {
                "result": {
                    "href": f"/jobs/{record.id}/results/{fname}",
                    "type": mime,
                    "title": title,
                    "roles": ["data"],
                }
            }
    return {}


# ---------------------------------------------------------------------------
# Format writers
# ---------------------------------------------------------------------------

_RASTER_FORMATS: dict[str, tuple[str, str]] = {
    "ZARR": (".zarr", "application/x-zarr"),
    "NETCDF": (".nc", "application/netcdf"),
    "NC": (".nc", "application/netcdf"),
    "NETCDF4": (".nc", "application/netcdf"),
    "GTIFF": (".tif", "image/tiff; subtype=geotiff"),
    "GEOTIFF": (".tif", "image/tiff; subtype=geotiff"),
    "TIFF": (".tif", "image/tiff; subtype=geotiff"),  # common alias
    "TIF": (".tif", "image/tiff; subtype=geotiff"),
    "PNG": (".png", "image/png"),
    "CSV": (".csv", "text/csv"),
}

_VECTOR_FORMATS: dict[str, tuple[str, str]] = {
    "GEOJSON": (".geojson", "application/geo+json"),
    "CSV": (".csv", "text/csv"),
    "PARQUET": (".parquet", "application/vnd.apache.parquet"),
}

_TABULAR_EXPORT_FORMATS: dict[str, tuple[str, str]] = {
    "CHAPCSV": (".csv", "text/csv"),
    "DHIS2JSON": (".json", "application/json"),
}


def _write_raster(ds: Any, results_dir: Any, fmt: str) -> str | None:
    """Write an xr.Dataset to disk in the requested format. Returns the output path."""
    # aggregate_spatial returns a Dataset with a 'geometry' dimension — convert
    # to GeoDataFrame so GEOJSON/PARQUET/CSV produce tabular vector output.
    if "geometry" in getattr(ds, "dims", {}):
        try:
            import geopandas as gpd
            from shapely import wkt as shapely_wkt

            df = ds.to_dataframe().reset_index()
            # geometry column may contain Shapely objects or WKT strings
            geoms = df["geometry"].apply(lambda g: g if hasattr(g, "geom_type") else shapely_wkt.loads(str(g)))
            gdf = gpd.GeoDataFrame(df.drop(columns=["geometry"]), geometry=geoms, crs="EPSG:4326")
            return _write_vector(gdf, results_dir, fmt if fmt in _VECTOR_FORMATS else "GEOJSON")
        except Exception:
            logger.debug("geometry→GeoDataFrame conversion failed", exc_info=True)

    ext, _ = _RASTER_FORMATS.get(fmt, (".zarr", "application/vnd+zarr"))

    if ext == ".zarr":
        path = str(results_dir / "result.zarr")
        ds.to_zarr(path, mode="w")
        return path

    if ext == ".nc":
        path = str(results_dir / "result.nc")
        ds.to_netcdf(path)
        return path

    if ext == ".tif":
        import rioxarray  # noqa: F401  # pyright: ignore[reportUnusedImport]

        path = str(results_dir / "result.tif")
        # GeoTIFF requires a 2-D or 3-D array; use the first variable
        var = list(ds.data_vars)[0]
        da = ds[var]
        if "spatial_ref" in da.coords:
            da = da.drop_vars("spatial_ref")
        if da.rio.crs is None:
            da = da.rio.write_crs("EPSG:4326")
        da.rio.to_raster(path)
        return path

    if ext == ".png":
        return _write_png(ds, results_dir)

    if ext == ".csv":
        path = str(results_dir / "result.csv")
        df = ds.to_dataframe().reset_index()
        # Drop internal Zarr artefacts (spatial_ref, index) that add noise for consumers
        drop = [c for c in df.columns if c in ("spatial_ref", "index") or c.startswith("level_")]
        df.drop(columns=drop, errors="ignore").to_csv(path, index=False)
        return path

    # Unknown format — raise so the caller can surface a clear 400/500 rather than
    # silently writing a .zarr directory that read_bytes() would crash on.
    known = ", ".join(sorted(_RASTER_FORMATS))
    raise ValueError(f"Unsupported raster format '{fmt}'. Known formats: {known}")


def _write_vector(gdf: Any, results_dir: Any, fmt: str) -> str | None:
    """Write a GeoDataFrame to disk in the requested format. Returns the output path."""
    ext, _ = _VECTOR_FORMATS.get(fmt, (".geojson", "application/geo+json"))

    if ext == ".geojson":
        path = str(results_dir / "result.geojson")
        gdf.to_file(path, driver="GeoJSON")
        return path

    if ext == ".parquet":
        path = str(results_dir / "result.parquet")
        gdf.to_parquet(path)
        return path

    if ext == ".csv":
        path = str(results_dir / "result.csv")
        gdf.drop(columns="geometry", errors="ignore").to_csv(path, index=False)
        return path

    # Fallback to GeoJSON
    path = str(results_dir / "result.geojson")
    gdf.to_file(path, driver="GeoJSON")
    return path


def _write_dataset_tabular_export(ds: Any, results_dir: Any, fmt: str, options: dict[str, Any]) -> str | None:
    import pandas as pd

    inferred_options = dict(options)
    period_field = _optional_str_option(inferred_options, "period_field") or "t"
    if "period_type" not in inferred_options and period_field in getattr(ds, "coords", {}):
        inferred = _infer_period_type(ds, period_field)
        if inferred in {"daily", "weekly", "monthly", "quarterly", "yearly"}:
            inferred_options["period_type"] = inferred
    if hasattr(ds, "to_dataframe"):
        df = ds.to_dataframe().reset_index()
    elif isinstance(ds, pd.DataFrame):
        df = ds
    else:
        raise TypeError(f"Unsupported data type for tabular export: {type(ds).__name__}")
    return _write_tabular_export(df, results_dir, fmt, inferred_options)


def _write_tabular_export(df: Any, results_dir: Any, fmt: str, options: dict[str, Any]) -> str | None:
    if fmt == "CHAPCSV":
        return _write_chap_csv(df, results_dir, options)
    if fmt == "DHIS2JSON":
        return _write_dhis2_json(df, results_dir, options)
    known = ", ".join(sorted(_TABULAR_EXPORT_FORMATS))
    raise ValueError(f"Unsupported tabular export format '{fmt}'. Known formats: {known}")


def _write_chap_csv(df: Any, results_dir: Any, options: dict[str, Any]) -> str:
    frame = _build_chap_csv_frame(df, options)
    path = str(results_dir / "result.csv")
    frame.to_csv(path, index=False)
    return path


def _build_chap_csv_frame(df: Any, options: dict[str, Any]) -> Any:
    import pandas as pd

    period_field = _optional_str_option(options, "period_field") or "t"
    location_field = _optional_str_option(options, "location_field") or "geometry"
    period_type = _optional_str_option(options, "period_type")

    frame = pd.DataFrame(df).copy()
    if location_field not in frame.columns:
        raise ValueError(f"Missing location field '{location_field}' in aggregated result")
    if period_field not in frame.columns:
        raise ValueError(f"Missing period field '{period_field}' in aggregated result")

    value_fields = _select_chap_value_fields(frame, location_field, period_field)
    rows: list[dict[str, str]] = []
    for row_index, record in enumerate(frame.to_dict(orient="records")):
        period_value = record.get(period_field)
        if _is_nullish(period_value):
            raise ValueError(f"Null period value in field '{period_field}' at row {row_index}")
        location = record.get(location_field)
        if _is_nullish(location):
            raise ValueError(f"Null location value in field '{location_field}' at row {row_index}")

        row: dict[str, str] = {
            "time_period": _to_dhis2_period_string(period_value, period_type),
            "location": str(location),
        }
        for value_field in value_fields:
            value = record.get(value_field)
            row[value_field] = "" if _is_nullish(value) else _to_dhis2_value_string(value)
        rows.append(row)

    return pd.DataFrame(rows, columns=["time_period", "location", *value_fields])


def _select_chap_value_fields(frame: Any, location_field: str, period_field: str) -> list[str]:
    excluded = {
        location_field,
        period_field,
        "geometry",
        "spatial_ref",
        "index",
        "band",
        "bands",
    }
    candidates = [str(c) for c in frame.columns if c not in excluded and not str(c).startswith("level_")]
    if not candidates:
        raise ValueError("CHAPCSV export requires at least one value column")
    return candidates


def _write_dhis2_json(df: Any, results_dir: Any, options: dict[str, Any]) -> str:
    payload = _build_dhis2_json_payload(df, options)
    path = str(results_dir / "result.json")
    Path(path).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _build_dhis2_json_payload(df: Any, options: dict[str, Any]) -> dict[str, list[dict[str, str]]]:
    import pandas as pd

    data_element_id = _required_str_option(options, "data_element_id")
    org_unit_field = _required_str_option(options, "org_unit_field")
    period_field = _optional_str_option(options, "period_field") or "t"
    period_type = _optional_str_option(options, "period_type")
    category_option_combo = _optional_str_option(options, "category_option_combo")

    frame = pd.DataFrame(df).copy()
    if org_unit_field not in frame.columns:
        raise ValueError(f"Missing org unit field '{org_unit_field}' in aggregated result")
    if period_field not in frame.columns:
        raise ValueError(f"Missing period field '{period_field}' in aggregated result")

    value_field = _select_dhis2_value_field(frame, org_unit_field, period_field)

    data_values: list[dict[str, str]] = []
    for record in frame.to_dict(orient="records"):
        value = record.get(value_field)
        if _is_nullish(value):
            continue

        org_unit = record.get(org_unit_field)
        if _is_nullish(org_unit):
            raise ValueError(f"Null org unit value in field '{org_unit_field}'")

        period_value = record.get(period_field)
        if _is_nullish(period_value):
            raise ValueError(f"Null period value in field '{period_field}'")

        item = {
            "dataElement": data_element_id,
            "orgUnit": str(org_unit),
            "period": _to_dhis2_period_string(period_value, period_type),
            "value": _to_dhis2_value_string(value),
        }
        if category_option_combo is not None:
            item["categoryOptionCombo"] = category_option_combo
        data_values.append(item)

    return {"dataValues": data_values}


def _required_str_option(options: dict[str, Any], key: str) -> str:
    value = _optional_str_option(options, key)
    if value is None:
        raise ValueError(f"Missing required export option '{key}'")
    return value


def _optional_str_option(options: dict[str, Any], key: str) -> str | None:
    raw = options.get(key)
    if raw is None:
        return None
    value = str(raw).strip()
    return value or None


def _select_dhis2_value_field(frame: Any, org_unit_field: str, period_field: str) -> str:
    excluded = {
        org_unit_field,
        period_field,
        "geometry",
        "spatial_ref",
        "index",
        "band",
        "bands",
    }
    candidates = [str(c) for c in frame.columns if c not in excluded and not str(c).startswith("level_")]
    if len(candidates) != 1:
        raise ValueError(
            "DHIS2JSON export requires exactly one value column after excluding "
            f"'{org_unit_field}' and '{period_field}', found {candidates}"
        )
    return candidates[0]


def _is_nullish(value: Any) -> bool:
    import numpy as np
    import pandas as pd

    result = pd.isna(value)
    if isinstance(result, (bool, np.bool_)):
        return bool(result)
    if isinstance(result, np.ndarray):
        if result.ndim == 0:
            return bool(result.item())
        raise ValueError("Array-like values are not supported in tabular export cells")
    if hasattr(result, "shape") and getattr(result, "shape", ()) not in [(), None]:
        raise ValueError("Array-like values are not supported in tabular export cells")
    if hasattr(result, "item"):
        return bool(result.item())
    return bool(result)


def _normalise_period_type(period_type: str | None) -> str | None:
    if period_type is None:
        return None
    value = period_type.strip().lower()
    aliases = {
        "day": "daily",
        "daily": "daily",
        "week": "weekly",
        "weekly": "weekly",
        "month": "monthly",
        "monthly": "monthly",
        "quarter": "quarterly",
        "quarterly": "quarterly",
        "year": "yearly",
        "yearly": "yearly",
    }
    kind = aliases.get(value)
    if kind is None:
        raise ValueError(f"Unsupported period_type '{period_type}'")
    return kind


def _direct_dhis2_period_string(value: str) -> str | None:
    patterns = (
        re.compile(r"^\d{8}$"),
        re.compile(r"^\d{6}$"),
        re.compile(r"^\d{4}$"),
        re.compile(r"^\d{4}W\d{2}$"),
        re.compile(r"^\d{4}Q[1-4]$"),
    )
    if any(pattern.fullmatch(value) for pattern in patterns):
        return value
    return None


def _to_dhis2_period_string(value: Any, period_type: str | None = None) -> str:
    import pandas as pd

    if _is_nullish(value):
        raise ValueError("Cannot serialize null period value")

    kind = _normalise_period_type(period_type)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            raise ValueError("Cannot serialize blank period value")
        direct = _direct_dhis2_period_string(stripped)
        if direct is not None:
            if kind is None:
                return direct
            pattern_map = {
                "daily": re.compile(r"^\d{8}$"),
                "weekly": re.compile(r"^\d{4}W\d{2}$"),
                "monthly": re.compile(r"^\d{6}$"),
                "quarterly": re.compile(r"^\d{4}Q[1-4]$"),
                "yearly": re.compile(r"^\d{4}$"),
            }
            if pattern_map[kind].fullmatch(stripped):
                return direct
            for known_type, pattern in pattern_map.items():
                if known_type != kind and pattern.fullmatch(stripped):
                    raise ValueError(f"Period value appears to be {known_type}, but period_type={kind}")
        if kind is None:
            raise ValueError(
                "Ambiguous period value; provide save_result option 'period_type' "
                "for date-like values that are not already in DHIS2 format"
            )
        try:
            timestamp = pd.Timestamp(stripped)
        except Exception as exc:
            raise ValueError(f"Could not parse period value {stripped!r} for period_type={kind}") from exc
        return _format_dhis2_timestamp(timestamp, kind)

    if kind is None:
        raise ValueError(
            "Ambiguous period value; provide save_result option 'period_type' "
            "for date-like values that are not already in DHIS2 format"
        )

    try:
        timestamp = pd.Timestamp(value)
    except Exception as exc:
        raise ValueError(f"Could not parse period value {value!r} for period_type={kind}") from exc
    return _format_dhis2_timestamp(timestamp, kind)


def _format_dhis2_timestamp(timestamp: Any, period_type: str) -> str:
    if period_type == "daily":
        return str(timestamp.strftime("%Y%m%d"))
    if period_type == "weekly":
        iso = timestamp.isocalendar()
        return f"{iso.year}W{iso.week:02d}"
    if period_type == "monthly":
        return str(timestamp.strftime("%Y%m"))
    if period_type == "quarterly":
        return f"{timestamp.year}Q{timestamp.quarter}"
    if period_type == "yearly":
        return str(timestamp.strftime("%Y"))
    raise ValueError(f"Unsupported period_type '{period_type}'")


def _to_dhis2_value_string(value: Any) -> str:
    import numpy as np

    if _is_nullish(value):
        raise ValueError("Cannot serialize null value")
    if isinstance(value, (bool, np.bool_)):
        return "true" if bool(value) else "false"
    if isinstance(value, (int, np.integer)) and not isinstance(value, bool):
        return str(int(value))
    if isinstance(value, (float, np.floating)):
        as_float = float(value)
        as_float32 = float(np.float32(as_float))
        if as_float == as_float32:
            return np.format_float_positional(np.float32(as_float), trim="-")
        return np.format_float_positional(as_float, trim="-")
    if isinstance(value, Decimal):
        normalized = format(value, "f")
        if "." in normalized:
            normalized = normalized.rstrip("0").rstrip(".")
        return normalized or "0"
    return str(value)


def _write_png(ds: Any, results_dir: Any) -> str | None:
    """Render an xr.Dataset as a styled PNG using the collection's render settings.

    Applies the same colormap, rescale range, and NaN transparency as the /map
    viewer.  Squeezes to a 2-D slice (first time step if temporal).
    """
    import matplotlib
    import numpy as np

    matplotlib.use("agg")  # non-interactive backend — safe on worker threads
    import matplotlib.pyplot as plt
    from matplotlib.colors import Normalize

    var = list(ds.data_vars)[0]
    arr = ds[var]

    # Squeeze to 2-D (first step of each leading dim)
    while arr.ndim > 2:
        arr = arr.isel({arr.dims[0]: 0})

    data = arr.values.astype(float)

    # Look up render settings from the published collection via the dataset registry
    colormap_name = "viridis"
    vmin, vmax = float(np.nanmin(data)), float(np.nanmax(data))
    try:
        from open_climate_service.data_registry.services import datasets as reg

        for _ds_meta in reg.list_datasets():
            display = _ds_meta.get("display", {})
            ds_var = _ds_meta.get("variable", "")
            if ds_var == var or _ds_meta.get("id", "").endswith(var):
                colormap_name = display.get("colormap", colormap_name)
                rng = display.get("range")
                if isinstance(rng, list) and len(rng) == 2:
                    vmin, vmax = float(rng[0]), float(rng[1])
                break
    except Exception:
        pass

    cmap = plt.get_cmap(colormap_name).copy()
    cmap.set_bad(alpha=0)  # NaN → transparent

    norm = Normalize(vmin=vmin, vmax=vmax, clip=False)

    # Render at the natural aspect ratio of the data
    height, width = data.shape
    dpi = 150
    fig_w = max(4, width / dpi)
    fig_h = max(3, height / dpi)

    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=dpi)
    fig.patch.set_alpha(0)
    ax.imshow(data, origin="upper", cmap=cmap, norm=norm, interpolation="nearest")
    ax.axis("off")
    fig.tight_layout(pad=0)

    path = str(results_dir / "result.png")
    fig.savefig(path, bbox_inches="tight", dpi=dpi, transparent=True, pad_inches=0)
    plt.close(fig)
    return path


def _derive_job_title(process: dict[str, Any]) -> str | None:
    """Generate a human-readable job title from a process graph when none is provided.

    Looks for a load_collection node and uses the collection id (plus temporal
    extent if present) to build a short label, e.g. "chirps3_precipitation_daily
    2023-01-01–2023-12-31". Returns None if no load_collection is found.
    """
    graph = process.get("process_graph")
    if not isinstance(graph, dict):
        return None
    for node in graph.values():
        if not isinstance(node, dict) or node.get("process_id") != "load_collection":
            continue
        args = node.get("arguments", {})
        collection_id = args.get("id")
        if not isinstance(collection_id, str):
            continue
        temporal = args.get("temporal_extent")
        if isinstance(temporal, (list, tuple)) and len(temporal) == 2:
            start, end = temporal[0], temporal[1]
            if start and end:
                return f"{collection_id} {start}–{end}"
            if start:
                return f"{collection_id} from {start}"
            if end:
                return f"{collection_id} until {end}"
        return collection_id
    return None


_service: OpenEOJobService | None = None


def get_openeo_job_service() -> OpenEOJobService:
    """Return the singleton openEO job service."""
    global _service
    if _service is None:
        _service = OpenEOJobService()
    return _service


def reset_openeo_job_service() -> None:
    """Reset singleton for tests."""
    global _service
    if _service is not None:
        _service.shutdown()
    _service = None
