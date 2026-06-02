"""openEO job persistence and execution service."""

from __future__ import annotations

import json
import logging
import os
import threading
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
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
            title=body.title,
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
        if isinstance(result, SaveResultEnvelope):
            fmt = result.format
            result = result.data

        # Resolve DataArray → Dataset for raster formats
        if isinstance(result, xr.DataArray):
            result = result.to_dataset(name=result.name or "result")

        if isinstance(result, xr.Dataset):
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
                return _write_vector(result, results_dir, fmt)
        except ImportError:
            pass

        # Unrecognised result type — raise so the job is marked ERROR rather than
        # silently finishing with an empty assets dict and no indication of failure.
        raise TypeError(
            f"Unsupported result type '{type(result).__name__}': expected xr.Dataset, xr.DataArray, or GeoDataFrame"
        )


def _result_assets(record: OpenEOJobRecord) -> dict[str, Any]:
    usage = record.usage or {}
    output_path = usage.get("output_path")
    if not output_path or not isinstance(output_path, str):
        return {}
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
