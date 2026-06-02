"""Services for ingestion, dataset persistence, and publication metadata."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import mimetypes
import os
import shutil
import threading
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime
from importlib import import_module
from pathlib import Path
from typing import Any, Protocol, cast
from uuid import uuid4

import portalocker
import pyproj
from fastapi import HTTPException
from fastapi.responses import FileResponse, JSONResponse
from starlette.responses import Response
from zarr.core.buffer import default_buffer_prototype

from open_climate_service import config as api_config
from open_climate_service.data_accessor.services.accessor import get_data_coverage_for_paths
from open_climate_service.data_manager.services import downloader
from open_climate_service.data_registry.services import datasets as registry_datasets
from open_climate_service.extents.services import get_extent
from open_climate_service.ingestions.schemas import (
    ArtifactCoverage,
    ArtifactFormat,
    ArtifactListResponse,
    ArtifactPublication,
    ArtifactRecord,
    ArtifactRequestScope,
    CoverageSpatial,
    CoverageTemporal,
    DatasetAccessLink,
    DatasetDetailRecord,
    DatasetListResponse,
    DatasetPublication,
    DatasetRecord,
    DatasetVersionRecord,
    IngestionListResponse,
    IngestionResponse,
    PublicationStatus,
    SyncDetail,
    SyncResponse,
)
from open_climate_service.ingestions.sync_engine import SyncConfigurationError, plan_sync, run_sync
from open_climate_service.publications.services import managed_dataset_id_for, publish_artifact
from open_climate_service.shared.time import datetime_to_period_string, normalize_period_string, utc_now, utc_today
from open_climate_service.streaming.orchestrator import run_streaming_ingest_sync
from open_climate_service.streaming.protocol import IngestionPlugin

logger = logging.getLogger(__name__)

# Per-store threading locks prevent two concurrent ingest/sync runs from writing
# to the same Icechunk store simultaneously (which causes MVCC commit conflicts).
_store_locks: dict[str, threading.Lock] = {}
_store_locks_mutex = threading.Lock()


def _acquire_store_lock(store_path: Path) -> threading.Lock:
    """Return the exclusive lock for store_path, creating it if needed."""
    key = str(store_path.resolve())
    with _store_locks_mutex:
        if key not in _store_locks:
            _store_locks[key] = threading.Lock()
        return _store_locks[key]


class _IcechunkReadableStore(Protocol):
    def list_dir(self, prefix: str) -> AsyncIterator[str]: ...
    def exists(self, key: str) -> Awaitable[bool]: ...
    def get(self, key: str, prototype: Any) -> Awaitable[Any]: ...


def _resolve_artifacts_dir() -> Path:
    from open_climate_service import config as api_config

    data_dir = api_config.get_data_dir()
    if data_dir is not None:
        return data_dir / "artifacts"
    xdg_data = Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share"))
    return xdg_data / "climate-service" / "artifacts"


ARTIFACTS_DIR = _resolve_artifacts_dir()
ARTIFACTS_INDEX_PATH = ARTIFACTS_DIR / "records.json"


def ensure_store() -> None:
    """Create the artifact metadata store if it does not exist."""
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    if not ARTIFACTS_INDEX_PATH.exists():
        ARTIFACTS_INDEX_PATH.write_text("[]\n", encoding="utf-8")


def list_artifacts() -> ArtifactListResponse:
    """Return all stored artifacts."""
    return ArtifactListResponse(items=_materialized_records(_load_records()))


def group_datasets() -> dict[str, list[ArtifactRecord]]:
    """Return artifact records grouped by stable managed dataset id."""
    grouped: dict[str, list[ArtifactRecord]] = {}
    for record in list_artifacts().items:
        grouped.setdefault(managed_dataset_id_for(record), []).append(record)
    return grouped


def latest_published_zarr_artifacts_by_dataset() -> dict[str, ArtifactRecord]:
    """Return the latest published Zarr/Icechunk artifact for each dataset id."""
    result: dict[str, ArtifactRecord] = {}
    for dataset_id, artifacts in group_datasets().items():
        latest = max(artifacts, key=lambda artifact: artifact.created_at)
        if latest.publication.status != PublicationStatus.PUBLISHED:
            continue
        if latest.format not in {ArtifactFormat.ZARR, ArtifactFormat.ICECHUNK}:
            continue
        result[dataset_id] = latest
    return dict(sorted(result.items()))


def list_ingestions() -> IngestionListResponse:
    """Return ingestion run records for operational/admin use."""
    records = sorted(_materialized_records(_load_records()), key=lambda record: record.created_at, reverse=True)
    items = [_build_ingestion_response(record) for record in records]
    return IngestionListResponse(items=items)


def get_artifact_or_404(artifact_id: str) -> ArtifactRecord:
    """Return a single artifact or raise 404."""
    for record in _materialized_records(_load_records()):
        if record.artifact_id == artifact_id:
            return record
    raise HTTPException(status_code=404, detail=f"Artifact '{artifact_id}' not found")


def get_dataset_for_artifact_or_404(artifact_id: str) -> DatasetDetailRecord:
    """Return the managed dataset view corresponding to an internal artifact id."""
    artifact = get_artifact_or_404(artifact_id)
    return get_dataset_or_404(managed_dataset_id_for(artifact))


def get_dataset_summary_for_artifact_or_404(artifact_id: str) -> DatasetRecord:
    """Return the managed dataset summary corresponding to an internal artifact id."""
    artifact = get_artifact_or_404(artifact_id)
    dataset_id = managed_dataset_id_for(artifact)
    artifacts = group_datasets().get(dataset_id)
    if artifacts is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    return _build_dataset_record(dataset_id, artifacts)


def get_ingestion_or_404(artifact_id: str) -> IngestionResponse:
    """Return an ingestion run record resolved to the managed dataset summary."""
    return _build_ingestion_response(get_artifact_or_404(artifact_id))


def list_datasets() -> DatasetListResponse:
    """Return managed datasets grouped by stable dataset id."""
    grouped = group_datasets()
    items = [_build_dataset_record(dataset_id, artifacts) for dataset_id, artifacts in sorted(grouped.items())]
    return DatasetListResponse(items=items)


def get_dataset_or_404(dataset_id: str) -> DatasetDetailRecord:
    """Return one managed dataset or raise 404."""
    grouped = group_datasets()
    artifacts = grouped.get(dataset_id)
    if artifacts is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    return _build_dataset_detail_record(dataset_id, artifacts)


def get_latest_artifact_for_dataset_or_404(dataset_id: str) -> ArtifactRecord:
    """Return the latest artifact backing a managed dataset."""
    grouped = group_datasets()
    artifacts = grouped.get(dataset_id)
    if artifacts is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    return max(artifacts, key=lambda artifact: artifact.created_at)


def create_artifact(
    *,
    dataset: dict[str, object],
    start: str,
    end: str | None,
    bbox: list[float] | None,
    country_code: str | None,
    overwrite: bool,
    publish: bool,
    download_start: str | None = None,
    download_end: str | None = None,
    on_progress: Callable[[int | None, int | None, str | None], None] | None = None,
    is_cancel_requested: Callable[[], bool] | None = None,
    save_cursor: Callable[[dict[str, object]], None] | None = None,
) -> ArtifactRecord:
    """Materialize one managed dataset artifact and persist its metadata.

    Source dataset materialization is plugin-backed and always writes an
    Icechunk store. Sync requests may still pass `download_start` and
    `download_end`, but those only describe the requested append window. The
    streaming engine remains store-authoritative and appends only periods that
    are actually missing from the committed store.
    """
    period_type = str(dataset["period_type"])
    start = _normalize_request_period(start, period_type=period_type, field_name="start")
    end = _normalize_optional_request_period(end, period_type=period_type, field_name="end")
    download_start = _normalize_optional_request_period(
        download_start, period_type=period_type, field_name="download_start"
    )
    download_end = _normalize_optional_request_period(download_end, period_type=period_type, field_name="download_end")
    _validate_download_scope(
        start=start,
        end=end,
        download_start=download_start,
        download_end=download_end,
    )
    resolved_download_end = download_end if download_end is not None else end
    if resolved_download_end is None:
        resolved_download_end = _default_request_end(period_type)
    request_scope = ArtifactRequestScope(
        start=start,
        end=end,
        bbox=(bbox[0], bbox[1], bbox[2], bbox[3]) if bbox is not None else None,
    )
    ingestion = dataset.get("ingestion")
    plugin_path = ingestion.get("plugin") if isinstance(ingestion, dict) else None
    if isinstance(plugin_path, str) and plugin_path:
        return _create_streaming_artifact(
            dataset=dataset,
            plugin_path=plugin_path,
            start=start,
            end=resolved_download_end,
            bbox=bbox,
            country_code=country_code,
            overwrite=overwrite,
            publish=publish,
            request_scope=request_scope,
            on_progress=on_progress,
            is_cancel_requested=is_cancel_requested,
            save_cursor=save_cursor,
        )
    raise HTTPException(status_code=500, detail=f"Dataset '{dataset['id']}' does not define ingestion.plugin")


def _create_streaming_artifact(
    *,
    dataset: dict[str, object],
    plugin_path: str,
    start: str,
    end: str,
    bbox: list[float] | None,
    country_code: str | None,
    overwrite: bool,
    publish: bool,
    request_scope: ArtifactRequestScope,
    on_progress: Callable[[int | None, int | None, str | None], None] | None = None,
    is_cancel_requested: Callable[[], bool] | None = None,
    save_cursor: Callable[[dict[str, object]], None] | None = None,
) -> ArtifactRecord:
    """Create or update one plugin-backed Icechunk artifact.

    The same helper is used for both initial ingest and store-based sync. The
    streaming orchestrator probes the plugin for the full requested range, then
    appends only periods that are not already committed in the target
    Icechunk-backed store.
    """
    if bbox is None:
        raise HTTPException(status_code=400, detail="Streaming ingest requires a bounding box")

    existing = _find_existing_artifact(
        dataset_id=str(dataset["id"]),
        request_scope=request_scope,
    )
    if existing is not None and not overwrite:
        if publish and existing.publication.status != PublicationStatus.PUBLISHED:
            return publish_artifact_record(existing.artifact_id)
        return existing

    ingestion = dataset.get("ingestion")
    raw_params = ingestion.get("default_params") if isinstance(ingestion, dict) else None
    if raw_params is None:
        params: dict[str, object] = {}
    elif isinstance(raw_params, dict):
        params = dict(raw_params)
    else:
        raise HTTPException(status_code=500, detail="ingestion.default_params must be an object")
    if country_code is not None:
        params["country_code"] = country_code

    plugin = _load_streaming_plugin(plugin_path, params=params)
    store_path = downloader.get_icechunk_path(dataset)

    lock = _acquire_store_lock(store_path)
    if not lock.acquire(blocking=False):
        raise HTTPException(
            status_code=409,
            detail=f"An ingest or sync is already running for dataset '{dataset['id']}'. Wait for it to finish.",
        )
    try:
        if overwrite and store_path.exists():
            if store_path.is_dir():
                shutil.rmtree(store_path)
            else:
                store_path.unlink()

        result = run_streaming_ingest_sync(
            plugin=plugin,
            params=params,
            dataset=dataset,
            bbox=bbox,
            start=start,
            end=end,
            store_path=store_path,
            period_type=str(dataset["period_type"]),
            on_progress=on_progress,
            is_cancel_requested=is_cancel_requested,
            save_cursor=save_cursor,
        )
        if result.periods_written == 0 and not store_path.exists():
            raise HTTPException(status_code=409, detail="Source has no data for the requested temporal scope")

        coverage_data = get_data_coverage_for_paths(dataset, icechunk_path=str(store_path.resolve()))
        if not coverage_data.get("has_data", True):
            raise HTTPException(
                status_code=409,
                detail="Materialized artifact contains no data for the requested scope",
            )

        _spatial_wgs84_data = coverage_data["coverage"].get("spatial_wgs84")
        coverage = ArtifactCoverage(
            temporal=CoverageTemporal(**coverage_data["coverage"]["temporal"]),
            spatial=CoverageSpatial(**coverage_data["coverage"]["spatial"]),
            spatial_wgs84=CoverageSpatial(**_spatial_wgs84_data) if _spatial_wgs84_data else None,
        )
        if not _temporal_coverage_matches_streaming_request_scope(coverage.temporal, request_scope):
            raise HTTPException(
                status_code=409,
                detail=(
                    "Materialized artifact coverage does not match the requested scope: "
                    f"coverage={coverage.temporal.start}..{coverage.temporal.end}, "
                    f"request={request_scope.start}..{request_scope.end}"
                ),
            )

        request_scope = request_scope.model_copy(update={"end": coverage.temporal.end})

        record = ArtifactRecord(
            artifact_id=str(uuid4()),
            dataset_id=str(dataset["id"]),
            dataset_name=str(dataset["name"]),
            variable=str(dataset["variable"]),
            period_type=str(dataset.get("period_type")) if dataset.get("period_type") is not None else None,
            format=ArtifactFormat.ICECHUNK,
            path=str(store_path.resolve()),
            asset_paths=[str(store_path.resolve())],
            variables=[str(dataset["variable"])],
            request_scope=request_scope,
            coverage=coverage,
            created_at=datetime.now(UTC),
            publication=ArtifactPublication(),
        )
        stored_record = _upsert_artifact_record(
            record,
            publish=publish,
            overwrite=overwrite,
        )
        if publish and stored_record.publication.status != PublicationStatus.PUBLISHED:
            return publish_artifact_record(stored_record.artifact_id)
        return stored_record
    finally:
        lock.release()


def _load_streaming_plugin(plugin_path: str, *, params: dict[str, object]) -> IngestionPlugin:
    """Load and instantiate one streaming plugin class from a dotted import path.

    Template-defined `ingestion.default_params` are treated as plugin
    configuration and passed to the constructor here. The same params are also
    forwarded later to `probe(...)` and `fetch_period(...)` so plugins may keep
    configuration in constructor state, per-call kwargs, or both.
    """
    module_path, _, attr_name = plugin_path.rpartition(".")
    if not module_path or not attr_name:
        raise HTTPException(status_code=500, detail=f"Invalid ingestion.plugin path '{plugin_path}'")
    try:
        module = import_module(module_path)
        plugin_cls = getattr(module, attr_name)
        if not callable(plugin_cls):
            raise TypeError(f"{plugin_path} is not callable")
        constructor_kwargs = dict(params)
        signature = inspect.signature(plugin_cls)
        accepts_var_kwargs = any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in signature.parameters.values()
        )
        if not accepts_var_kwargs:
            constructor_kwargs = {name: value for name, value in params.items() if name in signature.parameters}
        plugin = plugin_cls(**constructor_kwargs)
        if not isinstance(plugin, IngestionPlugin):
            raise TypeError(
                f"{plugin_path} does not implement the required streaming plugin contract "
                "(probe, periods, fetch_period, max_concurrency, commit_batch_size)"
            )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to load ingestion.plugin '%s'", plugin_path, exc_info=exc)
        raise HTTPException(status_code=500, detail=f"Failed to load ingestion.plugin '{plugin_path}'") from exc
    return plugin


def publish_artifact_record(artifact_id: str) -> ArtifactRecord:
    """Publish an artifact via pygeoapi and persist publication metadata."""
    published = publish_artifact(get_artifact_or_404(artifact_id))

    def mutate(records: list[ArtifactRecord]) -> ArtifactRecord:
        for index, record in enumerate(records):
            if record.artifact_id != artifact_id:
                continue
            records[index] = published
            return published
        raise HTTPException(status_code=404, detail=f"Artifact '{artifact_id}' not found")

    return _mutate_records(mutate)


def store_materialized_zarr_artifact(
    *,
    dataset: dict[str, object],
    start: str,
    end: str | None,
    bbox: list[float] | None,
    zarr_path: Path,
    overwrite: bool,
    publish: bool,
) -> ArtifactRecord:
    """Store metadata for a locally materialized Zarr artifact."""
    period_type = str(dataset["period_type"])
    normalized_start = _normalize_request_period(start, period_type=period_type, field_name="start")
    normalized_end = _normalize_optional_request_period(end, period_type=period_type, field_name="end")
    request_scope = ArtifactRequestScope(
        start=normalized_start,
        end=normalized_end,
        bbox=(bbox[0], bbox[1], bbox[2], bbox[3]) if bbox is not None else None,
    )
    coverage_data = get_data_coverage_for_paths(dataset, zarr_path=str(zarr_path.resolve()))
    if not coverage_data.get("has_data", True):
        raise HTTPException(status_code=409, detail="Materialized artifact contains no data for the requested scope")
    _spatial_wgs84_data = coverage_data["coverage"].get("spatial_wgs84")
    coverage = ArtifactCoverage(
        temporal=CoverageTemporal(**coverage_data["coverage"]["temporal"]),
        spatial=CoverageSpatial(**coverage_data["coverage"]["spatial"]),
        spatial_wgs84=CoverageSpatial(**_spatial_wgs84_data) if _spatial_wgs84_data else None,
    )
    request_scope = request_scope.model_copy(update={"end": coverage.temporal.end})

    record = ArtifactRecord(
        artifact_id=str(uuid4()),
        dataset_id=str(dataset["id"]),
        source_dataset_id=(
            str(dataset.get("source_dataset_id")) if dataset.get("source_dataset_id") is not None else None
        ),
        dataset_name=str(dataset["name"]),
        variable=str(dataset["variable"]),
        period_type=str(dataset.get("period_type")) if dataset.get("period_type") is not None else None,
        format=ArtifactFormat.ZARR,
        path=str(zarr_path.resolve()),
        asset_paths=[str(zarr_path.resolve())],
        variables=[str(dataset["variable"])],
        request_scope=request_scope,
        coverage=coverage,
        created_at=datetime.now(UTC),
        publication=ArtifactPublication(),
    )
    stored_record = _upsert_artifact_record(record, publish=publish, overwrite=overwrite)
    if publish and stored_record.publication.status != PublicationStatus.PUBLISHED:
        return publish_artifact_record(stored_record.artifact_id)
    return stored_record


def sync_dataset(
    *,
    dataset_id: str,
    end: str | None,
    publish: bool,
    on_progress: Callable[[int | None, int | None, str | None], None] | None = None,
) -> SyncResponse:
    """Resolve sync inputs and delegate managed-dataset sync to the sync engine.

    The service layer stays thin on purpose: it validates that the requested
    public dataset id resolves to a managed dataset plus a source template, then
    hands execution to `sync_engine.run_sync(...)`.
    """
    latest_artifact = get_latest_artifact_for_dataset_or_404(dataset_id)
    source_dataset = registry_datasets.get_dataset(latest_artifact.dataset_id)
    if source_dataset is None:
        raise HTTPException(status_code=404, detail=f"Source dataset '{latest_artifact.dataset_id}' not found")
    extent = get_extent()
    resolved_country_code = extent.get("country_code") if extent else None
    try:
        return run_sync(
            latest_artifact=latest_artifact,
            source_dataset=source_dataset,
            requested_end=end,
            country_code=resolved_country_code,
            publish=publish,
            create_artifact_fn=create_artifact,
            get_dataset_fn=get_dataset_or_404,
            on_progress=on_progress,
        )
    except SyncConfigurationError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def plan_sync_dataset(
    *,
    dataset_id: str,
    end: str | None,
) -> SyncDetail:
    """Return the sync plan for a managed dataset without downloading or writing artifacts."""
    latest_artifact = get_latest_artifact_for_dataset_or_404(dataset_id)
    source_dataset = registry_datasets.get_dataset(latest_artifact.dataset_id)
    if source_dataset is None:
        raise HTTPException(status_code=404, detail=f"Source dataset '{latest_artifact.dataset_id}' not found")
    try:
        return plan_sync(
            latest_artifact=latest_artifact,
            source_dataset=source_dataset,
            requested_end=end,
        )
    except SyncConfigurationError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def get_dataset_zarr_store_info_or_404(dataset_id: str) -> dict[str, object]:
    """Return a public Zarr store listing for a managed dataset."""
    artifact = get_latest_artifact_for_dataset_or_404(dataset_id)
    if artifact.format == ArtifactFormat.ICECHUNK:
        store = _open_icechunk_store_or_404(artifact)
        entries = _icechunk_entries(dataset_id=dataset_id, store=store, prefix="")
        store_attrs = _read_icechunk_attrs(store)
        store_crs = store_attrs.get("proj:code") if store_attrs else None
        crs = store_crs if isinstance(store_crs, str) and store_crs else api_config.get_crs()
        return {
            "kind": "ZarrListing",
            "dataset_id": dataset_id,
            "format": artifact.format,
            "path": ".",
            "crs": crs,
            "proj4": _crs_to_proj4(crs),
            "bounds": _read_zarr_bounds(store_attrs),
            "entries": entries,
        }

    store_root = _get_zarr_root_or_409(artifact)

    entries = _zarr_entries(dataset_id=dataset_id, store_root=store_root, directory=store_root)
    store_attrs = _read_zarr_attrs(store_root)
    store_crs = store_attrs.get("proj:code") if store_attrs else None
    crs = store_crs if isinstance(store_crs, str) and store_crs else api_config.get_crs()
    return {
        "kind": "ZarrListing",
        "dataset_id": dataset_id,
        "format": artifact.format,
        "path": ".",
        "crs": crs,
        "proj4": _crs_to_proj4(crs),
        "bounds": _read_zarr_bounds(store_attrs),
        "entries": entries,
    }


def _crs_to_proj4(crs: str) -> str | None:
    """Convert an EPSG code or WKT string to a proj4 definition string, or None on failure."""
    import warnings

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            return pyproj.CRS.from_user_input(crs).to_proj4()
    except Exception:
        return None


def _read_zarr_attrs(store_root: Path) -> dict[str, object] | None:
    """Read the root attributes from a Zarr store, normalising v2/v3 layout differences."""
    for attrs_file in (store_root / "zarr.json", store_root / ".zattrs"):
        if attrs_file.exists():
            attrs: dict[str, object] = json.loads(attrs_file.read_text(encoding="utf-8"))
            if attrs_file.name == "zarr.json":
                attrs = attrs.get("attributes", attrs)  # type: ignore[assignment]
            return attrs
    return None


def _read_zarr_bounds(store_attrs: dict[str, object] | None) -> list[float] | None:
    """Extract the spatial:bbox from pre-read zarr store attributes, reprojected to WGS84.

    Map clients (zarr-layer) expect bounds in geographic coordinates regardless of the
    store's native CRS, so we reproject here when the store CRS is not WGS84/CRS84.
    """
    if store_attrs is None:
        return None
    bbox = store_attrs.get("spatial:bbox")
    if not (isinstance(bbox, list) and len(bbox) == 4):
        return None
    xmin, ymin, xmax, ymax = (float(v) for v in bbox)
    native_crs = store_attrs.get("proj:code")
    if isinstance(native_crs, str) and native_crs not in ("EPSG:4326", "CRS84", "OGC:CRS84"):
        try:
            transformer = pyproj.Transformer.from_crs(native_crs, "EPSG:4326", always_xy=True)
            xmin, ymin = transformer.transform(xmin, ymin)
            xmax, ymax = transformer.transform(xmax, ymax)
        except Exception:
            pass
    return [xmin, ymin, xmax, ymax]


def get_dataset_zarr_store_file_or_404(
    dataset_id: str, relative_path: str
) -> FileResponse | Response | dict[str, object]:
    """Serve a file, metadata document, or directory listing within a dataset Zarr store."""
    artifact = get_latest_artifact_for_dataset_or_404(dataset_id)
    if artifact.format == ArtifactFormat.ICECHUNK:
        store = _open_icechunk_store_or_404(artifact)
        return _get_icechunk_store_path_or_404(dataset_id=dataset_id, store=store, relative_path=relative_path)

    store_root = _get_zarr_root_or_409(artifact)
    target = _resolve_zarr_path(store_root, relative_path)
    if not target.exists():
        raise HTTPException(status_code=404, detail=f"Zarr path '{relative_path}' not found")
    if target.is_dir():
        return _zarr_directory_listing(dataset_id=dataset_id, store_root=store_root, directory=target)
    if target.name in {".zarray", ".zattrs", ".zgroup", "zarr.json"}:
        return JSONResponse(content=json.loads(target.read_text(encoding="utf-8")))

    media_type, _ = mimetypes.guess_type(target.name)
    if media_type is None:
        media_type = "application/octet-stream"
    return FileResponse(target, media_type=media_type, filename=target.name)


def _open_icechunk_store_or_404(artifact: ArtifactRecord) -> _IcechunkReadableStore:
    """Open the readonly Icechunk session store for a published artifact."""
    if artifact.format != ArtifactFormat.ICECHUNK:
        raise HTTPException(status_code=409, detail="Artifact is not an Icechunk-backed Zarr store")
    store_path = artifact.path or (artifact.asset_paths[0] if artifact.asset_paths else None)
    if store_path is None:
        raise HTTPException(status_code=409, detail="Artifact has no resolvable store path")
    path = Path(store_path).resolve()
    if not path.exists():
        raise HTTPException(status_code=404, detail="Icechunk store path does not exist on disk")

    import icechunk

    storage = icechunk.local_filesystem_storage(str(path))
    repo = icechunk.Repository.open(storage)
    return cast(_IcechunkReadableStore, repo.readonly_session("main").store)


def _run_async(awaitable: Any) -> Any:
    """Run an async Icechunk store operation from sync route/service code.

    Most calls arrive from sync FastAPI handlers with no running event loop.
    If a loop is already running in this thread, run the awaitable in a short-
    lived worker thread instead of nesting event loops in-process.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(awaitable)
        finally:
            loop.close()

    result: dict[str, Any] = {}
    error: dict[str, BaseException] = {}

    def runner() -> None:
        loop = asyncio.new_event_loop()
        try:
            result["value"] = loop.run_until_complete(awaitable)
        except BaseException as exc:  # pragma: no cover - re-raised in caller thread
            error["value"] = exc
        finally:
            loop.close()

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    thread.join()
    if "value" in error:
        raise error["value"]
    if "value" not in result:
        raise RuntimeError("async runner completed without a result or error")
    return result["value"]


def _icechunk_list_dir(store: _IcechunkReadableStore, prefix: str) -> list[str]:
    async def collect() -> list[str]:
        items: list[str] = []
        async for item in store.list_dir(prefix):
            items.append(item)
        return items

    return cast(list[str], _run_async(collect()))


def _icechunk_exists(store: _IcechunkReadableStore, key: str) -> bool:
    return cast(bool, _run_async(store.exists(key)))


def _icechunk_get(store: _IcechunkReadableStore, key: str) -> bytes | None:
    buffer: Any = _run_async(store.get(key, prototype=default_buffer_prototype()))
    if buffer is None:
        return None
    if hasattr(buffer, "to_bytes"):
        return cast(bytes, buffer.to_bytes())
    return cast(bytes, buffer)


def _icechunk_directory_listing(
    *,
    dataset_id: str,
    store: _IcechunkReadableStore,
    prefix: str,
    child_names: list[str] | None = None,
) -> dict[str, object]:
    entries = _icechunk_entries(dataset_id=dataset_id, store=store, prefix=prefix, child_names=child_names)
    return {
        "kind": "ZarrListing",
        "dataset_id": dataset_id,
        "path": "." if prefix == "" else prefix,
        "entries": entries,
    }


def _icechunk_entries(
    *,
    dataset_id: str,
    store: _IcechunkReadableStore,
    prefix: str,
    child_names: list[str] | None = None,
) -> list[dict[str, str]]:
    base = "" if prefix == "" else prefix.rstrip("/") + "/"
    names = child_names if child_names is not None else _icechunk_list_dir(store, prefix)

    async def collect_entries() -> list[dict[str, str]]:
        sem = asyncio.Semaphore(16)

        async def probe(name: str) -> dict[str, str]:
            async with sem:
                relative_path = f"{base}{name}" if base else name
                href = f"/zarr/{dataset_id}/{relative_path}" if relative_path else f"/zarr/{dataset_id}"
                children = [item async for item in store.list_dir(relative_path)]
                return {"name": name, "kind": "directory" if children else "file", "href": href}

        return list(await asyncio.gather(*[probe(name) for name in sorted(names)]))

    return cast(list[dict[str, str]], _run_async(collect_entries()))


def _read_icechunk_attrs(store: _IcechunkReadableStore) -> dict[str, object] | None:
    payload = _icechunk_get(store, "zarr.json")
    if payload is None:
        return None
    attrs: dict[str, object] = json.loads(payload.decode("utf-8"))
    return attrs.get("attributes", attrs)  # type: ignore[return-value]


def _normalize_icechunk_relative_path(relative_path: str) -> str:
    """Normalize a requested Icechunk key path and reject unsafe segments."""
    if "\\" in relative_path:
        raise HTTPException(status_code=400, detail="Zarr path must use '/' separators")
    target = relative_path.strip("/")
    if target == "":
        return ""
    segments = target.split("/")
    if any(segment in {"", ".", ".."} for segment in segments):
        raise HTTPException(status_code=400, detail="Zarr path contains invalid segments")
    return target


def _get_icechunk_store_path_or_404(
    dataset_id: str, store: _IcechunkReadableStore, relative_path: str
) -> Response | dict[str, object]:
    target = _normalize_icechunk_relative_path(relative_path)
    if target == "":
        return _icechunk_directory_listing(dataset_id=dataset_id, store=store, prefix="")

    if _icechunk_exists(store, target):
        payload = _icechunk_get(store, target)
        if payload is None:
            raise HTTPException(status_code=404, detail=f"Zarr path '{relative_path}' not found")
        if target.endswith("zarr.json"):
            return JSONResponse(content=json.loads(payload.decode("utf-8")))
        media_type, _ = mimetypes.guess_type(target)
        return Response(content=payload, media_type=media_type or "application/octet-stream")

    child_names = _icechunk_list_dir(store, target)
    if child_names:
        return _icechunk_directory_listing(dataset_id=dataset_id, store=store, prefix=target, child_names=child_names)

    raise HTTPException(status_code=404, detail=f"Zarr path '{relative_path}' not found")


def _load_records() -> list[ArtifactRecord]:
    ensure_store()
    raw = json.loads(ARTIFACTS_INDEX_PATH.read_text(encoding="utf-8"))
    return [ArtifactRecord.model_validate(_upgrade_legacy_record(item)) for item in raw]


def _save_records(records: list[ArtifactRecord]) -> None:
    ensure_store()
    payload = [record.model_dump(mode="json") for record in records]
    ARTIFACTS_INDEX_PATH.write_text(f"{json.dumps(payload, indent=2)}\n", encoding="utf-8")


def _store_artifact_record(
    record: ArtifactRecord,
    *,
    publish: bool,
) -> ArtifactRecord:
    """Persist a newly created artifact record while avoiding lost updates."""

    def mutate(records: list[ArtifactRecord]) -> ArtifactRecord:
        existing = _find_existing_artifact_in_records(
            records=records,
            dataset_id=record.dataset_id,
            request_scope=record.request_scope,
        )
        if existing is not None:
            if publish and existing.publication.status != PublicationStatus.PUBLISHED:
                return existing
            return existing

        records.append(record)
        return record

    return _mutate_records(mutate)


def _upsert_artifact_record(
    record: ArtifactRecord,
    *,
    publish: bool,
    overwrite: bool,
) -> ArtifactRecord:
    """Persist a new or replacement artifact record for the same logical request scope."""
    if not overwrite:
        return _store_artifact_record(record, publish=publish)

    def mutate(records: list[ArtifactRecord]) -> ArtifactRecord:
        existing = _find_existing_artifact_in_records(
            records=records,
            dataset_id=record.dataset_id,
            request_scope=record.request_scope,
        )
        if existing is None:
            records.append(record)
            return record

        replacement = record.model_copy(
            update={
                "artifact_id": existing.artifact_id,
                "publication": existing.publication,
            }
        )
        for index, current in enumerate(records):
            if current.artifact_id != existing.artifact_id:
                continue
            records[index] = replacement
            return replacement
        raise HTTPException(status_code=404, detail=f"Artifact '{existing.artifact_id}' not found")

    return _mutate_records(mutate)


def _mutate_records(mutation: Callable[[list[ArtifactRecord]], ArtifactRecord]) -> ArtifactRecord:
    """Apply a read-modify-write mutation under an exclusive file lock."""
    ensure_store()
    with ARTIFACTS_INDEX_PATH.open("a+", encoding="utf-8") as handle:
        portalocker.lock(handle, portalocker.LOCK_EX)
        handle.seek(0)
        raw = handle.read()
        records = [ArtifactRecord.model_validate(_upgrade_legacy_record(item)) for item in json.loads(raw or "[]")]
        result = mutation(records)
        payload = [record.model_dump(mode="json") for record in records]
        handle.seek(0)
        handle.truncate()
        handle.write(f"{json.dumps(payload, indent=2)}\n")
        handle.flush()
        os.fsync(handle.fileno())
        portalocker.unlock(handle)
        return result


def _get_zarr_root_or_409(artifact: ArtifactRecord) -> Path:
    """Return the Zarr root path for an artifact or raise a 409 if it is not Zarr-backed."""
    if artifact.format != ArtifactFormat.ZARR:
        raise HTTPException(status_code=409, detail="Artifact is not a Zarr store")

    store_root = Path(artifact.path or artifact.asset_paths[0]).resolve()
    if not store_root.exists() or not store_root.is_dir():
        raise HTTPException(status_code=404, detail="Zarr store path does not exist on disk")
    return store_root


def _resolve_zarr_path(store_root: Path, relative_path: str) -> Path:
    """Resolve a requested Zarr path without allowing traversal outside the store root."""
    candidate = (store_root / relative_path).resolve()
    try:
        candidate.relative_to(store_root)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Requested Zarr path is outside the artifact store") from exc
    return candidate


def _zarr_directory_listing(*, dataset_id: str, store_root: Path, directory: Path) -> dict[str, object]:
    """Return a browseable directory listing for a Zarr path."""
    relative_directory = "." if directory == store_root else directory.relative_to(store_root).as_posix()
    entries = _zarr_entries(dataset_id=dataset_id, store_root=store_root, directory=directory)
    return {
        "kind": "ZarrListing",
        "dataset_id": dataset_id,
        "path": relative_directory,
        "entries": entries,
    }


def _zarr_entries(*, dataset_id: str, store_root: Path, directory: Path) -> list[dict[str, str]]:
    """Build directory entries for a Zarr store namespace."""
    return [
        {
            "name": child.name,
            "kind": "directory" if child.is_dir() else "file",
            "href": f"/zarr/{dataset_id}/{child.relative_to(store_root).as_posix()}",
        }
        for child in sorted(directory.iterdir(), key=lambda child: child.name)
    ]


def _find_existing_artifact(
    *,
    dataset_id: str,
    request_scope: ArtifactRequestScope,
) -> ArtifactRecord | None:
    """Return an existing artifact for an identical logical request when possible."""
    return _find_existing_artifact_in_records(
        records=_load_records(),
        dataset_id=dataset_id,
        request_scope=request_scope,
    )


def _normalize_request_period(value: str, *, period_type: str, field_name: str) -> str:
    """Normalize a required request period or raise a clear client error."""
    try:
        return normalize_period_string(value, period_type)
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid {field_name} period '{value}': {exc}",
        ) from exc


def _normalize_optional_request_period(value: str | None, *, period_type: str, field_name: str) -> str | None:
    """Normalize an optional request period or raise a clear client error."""
    if value is None:
        return None
    return _normalize_request_period(value, period_type=period_type, field_name=field_name)


def _default_request_end(period_type: str) -> str:
    """Return the current dataset-native period string for omitted ingestion end values."""
    if period_type == "hourly":
        return datetime_to_period_string(utc_now(), period_type)
    if period_type == "daily":
        return utc_today().isoformat()
    if period_type == "weekly":
        return datetime_to_period_string(utc_now(), period_type)
    if period_type == "monthly":
        today = utc_today()
        return f"{today.year:04d}-{today.month:02d}"
    if period_type == "yearly":
        return str(utc_today().year)
    raise HTTPException(status_code=400, detail=f"Invalid period_type '{period_type}' for request end defaulting")


def _validate_download_scope(
    *,
    start: str,
    end: str | None,
    download_start: str | None,
    download_end: str | None,
) -> None:
    """Validate optional delta download scope against the normalized request scope."""
    if (download_start is None) != (download_end is None):
        raise HTTPException(
            status_code=400,
            detail="download_start and download_end must either both be provided or both be omitted",
        )
    if download_start is None or download_end is None:
        return
    if download_start < start:
        raise HTTPException(status_code=400, detail="download_start must be greater than or equal to start")
    if download_end < download_start:
        raise HTTPException(status_code=400, detail="download_end must be greater than or equal to download_start")
    if end is not None and download_end > end:
        raise HTTPException(status_code=400, detail="download_end must be less than or equal to end")


def _find_existing_artifact_in_records(
    *,
    records: list[ArtifactRecord],
    dataset_id: str,
    request_scope: ArtifactRequestScope,
) -> ArtifactRecord | None:
    """Return an existing artifact for an identical logical request from a provided record set."""
    for record in reversed(records):
        if not _artifact_storage_exists(record):
            logger.warning(
                "Ignoring stale artifact '%s' because backing storage is missing",
                record.artifact_id,
            )
            continue
        if record.dataset_id != dataset_id:
            continue
        if record.request_scope != request_scope:
            continue
        if not _artifact_coverage_matches_request_scope(record):
            logger.warning(
                "Ignoring existing artifact '%s' because coverage %s..%s does not match request scope %s..%s",
                record.artifact_id,
                record.coverage.temporal.start,
                record.coverage.temporal.end,
                record.request_scope.start,
                record.request_scope.end,
            )
            continue
        return record
    return None


def _artifact_coverage_matches_request_scope(record: ArtifactRecord) -> bool:
    """Return whether an existing artifact is safe to reuse for its request scope."""
    return _temporal_coverage_matches_request_scope(record.coverage.temporal, record.request_scope)


def _materialized_records(records: list[ArtifactRecord]) -> list[ArtifactRecord]:
    """Return only artifact records whose backing storage still exists."""
    materialized: list[ArtifactRecord] = []
    for record in records:
        if _artifact_storage_exists(record):
            materialized.append(record)
            continue
        logger.warning("Ignoring stale artifact '%s' because backing storage is missing", record.artifact_id)
    return materialized


def _artifact_storage_exists(record: ArtifactRecord) -> bool:
    """Return whether an artifact's on-disk backing files are still present."""
    paths: list[str] = []
    if record.path is not None:
        paths.append(record.path)
    if record.asset_paths:
        paths.extend(record.asset_paths)
    if not paths:
        return False
    return all(Path(path).exists() for path in paths)


def _temporal_coverage_matches_request_scope(
    temporal: CoverageTemporal,
    request_scope: ArtifactRequestScope,
) -> bool:
    """Return whether temporal coverage exactly matches the requested temporal scope."""
    if temporal.start != request_scope.start:
        return False
    # Open-ended requests intentionally reuse the latest artifact for the same
    # logical start/scope even though the realized end is time-dependent.
    if request_scope.end is not None and temporal.end != request_scope.end:
        return False
    return True


def _temporal_coverage_matches_streaming_request_scope(
    temporal: CoverageTemporal,
    request_scope: ArtifactRequestScope,
) -> bool:
    """Return whether streaming coverage is compatible with the requested scope.

    Plugin-backed streaming ingest may legitimately clamp the realized end of an
    artifact to the source's latest available period. That should still be
    treated as a successful ingest as long as the coverage starts where
    requested and does not extend beyond the requested temporal end.

    Start handling remains strict by design. A later-than-requested start
    indicates the realized dataset does not cover the requested opening period
    at all, while a shorter realized end can be a normal consequence of source
    availability clamping.
    """
    if temporal.start != request_scope.start:
        return False
    if request_scope.end is not None and temporal.end > request_scope.end:
        return False
    return True


def _build_dataset_record(dataset_id: str, artifacts: list[ArtifactRecord]) -> DatasetRecord:
    latest = max(artifacts, key=lambda artifact: artifact.created_at)
    source_dataset = registry_datasets.get_dataset(latest.dataset_id) or {}
    return DatasetRecord(
        dataset_id=dataset_id,
        source_dataset_id=latest.source_dataset_id or latest.dataset_id,
        dataset_name=latest.dataset_name,
        short_name=_as_optional_str(source_dataset.get("short_name")),
        variable=latest.variable,
        period_type=_as_optional_str(source_dataset.get("period_type")) or latest.period_type or "unknown",
        units=_as_optional_str(source_dataset.get("units")),
        resolution=_as_optional_str(source_dataset.get("resolution")),
        source=_as_optional_str(source_dataset.get("source")),
        source_url=_as_optional_str(source_dataset.get("source_url")),
        extent=latest.coverage,
        last_updated=latest.created_at,
        links=_dataset_links(dataset_id, latest),
        publication=DatasetPublication(
            status=latest.publication.status,
            published_at=latest.publication.published_at,
        ),
    )


def _build_ingestion_response(artifact: ArtifactRecord) -> IngestionResponse:
    """Build an operational ingestion response from one stored artifact record."""
    return IngestionResponse(
        ingestion_id=artifact.artifact_id,
        status="completed",
        dataset=get_dataset_summary_for_artifact_or_404(artifact.artifact_id),
    )


def _build_dataset_detail_record(dataset_id: str, artifacts: list[ArtifactRecord]) -> DatasetDetailRecord:
    base = _build_dataset_record(dataset_id, artifacts)
    ordered_artifacts = sorted(artifacts, key=lambda artifact: artifact.created_at, reverse=True)
    return DatasetDetailRecord(
        **base.model_dump(),
        versions=[
            DatasetVersionRecord(
                created_at=artifact.created_at,
                format=artifact.format,
                coverage=artifact.coverage,
                request_scope=artifact.request_scope,
            )
            for artifact in ordered_artifacts
        ],
    )


def _dataset_links(dataset_id: str, latest: ArtifactRecord) -> list[DatasetAccessLink]:
    links = [DatasetAccessLink(href=f"/datasets/{dataset_id}", rel="self", title="Dataset detail")]
    if latest.format in {ArtifactFormat.ZARR, ArtifactFormat.ICECHUNK}:
        links.append(DatasetAccessLink(href=f"/zarr/{dataset_id}", rel="zarr", title="Zarr store"))
    if latest.publication.status == PublicationStatus.PUBLISHED and latest.format in {
        ArtifactFormat.ZARR,
        ArtifactFormat.ICECHUNK,
    }:
        links.append(DatasetAccessLink(href=f"/stac/collections/{dataset_id}", rel="stac", title="STAC collection"))
    if latest.format == ArtifactFormat.NETCDF:
        links.append(
            DatasetAccessLink(href=f"/datasets/{dataset_id}/download", rel="download", title="Download NetCDF")
        )
    if latest.publication.pygeoapi_path is not None:
        links.append(
            DatasetAccessLink(href=latest.publication.pygeoapi_path, rel="ogc-collection", title="OGC collection")
        )
    return links


def _as_optional_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _upgrade_legacy_record(item: dict[str, object]) -> dict[str, object]:
    """Backfill newer schema fields for records created before migrations existed."""
    if "request_scope" not in item:
        coverage = item.get("coverage")
        if isinstance(coverage, dict):
            spatial = coverage.get("spatial")
            temporal = coverage.get("temporal")
            bbox: tuple[float, float, float, float] | None = None
            if isinstance(spatial, dict):
                xmin = spatial.get("xmin")
                ymin = spatial.get("ymin")
                xmax = spatial.get("xmax")
                ymax = spatial.get("ymax")
                if (
                    isinstance(xmin, int | float)
                    and isinstance(ymin, int | float)
                    and isinstance(xmax, int | float)
                    and isinstance(ymax, int | float)
                ):
                    bbox = (float(xmin), float(ymin), float(xmax), float(ymax))

            start = ""
            end: str | None = None
            if isinstance(temporal, dict):
                raw_start = temporal.get("start")
                raw_end = temporal.get("end")
                if isinstance(raw_start, str):
                    start = raw_start
                if isinstance(raw_end, str):
                    end = raw_end

            item["request_scope"] = {
                "start": start,
                "end": end,
                "bbox": bbox,
            }
    return item
