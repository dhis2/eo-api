"""Services for ingestion, dataset persistence, and publication metadata."""

from __future__ import annotations

import asyncio
import json
import logging
import mimetypes
import os
import shutil
import threading
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, cast
from uuid import uuid4

import portalocker
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

# Consolidated zarr metadata cache: (store_path, snapshot_id) → metadata dict.
# Building consolidated metadata requires reading every zarr.json from the store;
# the result is stable for the lifetime of a snapshot, so we cache it keyed to
# the Icechunk branch tip.  Capped at 512 entries; the oldest half is evicted when
# the limit is hit (one entry per ingest per dataset, so 512 entries ≈ 85 ingests
# across 6 datasets before any eviction occurs).
_consolidated_metadata_cache: dict[str, dict[str, object]] = {}
_MAX_CONSOLIDATED_CACHE_ENTRIES = 512

# Icechunk artifact path cache: dataset_id → (records_mtime, artifact).
# Avoids reading and deserializing the full artifact index on every chunk request
# from the /icechunk/ endpoint.  Invalidated when records.json changes on disk.
_icechunk_artifact_cache: dict[str, tuple[float, "ArtifactRecord"]] = {}


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


class _IcechunkSession:
    """Thin holder for a readonly Icechunk session and its branch-tip snapshot id."""

    __slots__ = ("store", "snapshot_id", "store_path")

    def __init__(self, store: _IcechunkReadableStore, snapshot_id: str, store_path: str) -> None:
        self.store = store
        self.snapshot_id = snapshot_id
        self.store_path = store_path

    @property
    def cache_key(self) -> str:
        return f"{self.store_path}:{self.snapshot_id}"


def _resolve_artifacts_dir() -> Path:

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
        if latest.format != ArtifactFormat.ICECHUNK:
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
    periods: list[str] | None = None,
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
            periods=periods,
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
    periods: list[str] | None = None,
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
    raw_params = ingestion.get("params") if isinstance(ingestion, dict) else None
    if raw_params is None:
        params: dict[str, object] = {}
    elif isinstance(raw_params, dict):
        params = dict(raw_params)
    else:
        raise HTTPException(status_code=500, detail="ingestion.params must be an object")
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
            periods=periods,
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

        _maybe_build_pyramid(store_path, dataset)

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


def _maybe_build_pyramid(store_path: Path, dataset: dict[str, object]) -> None:
    """Apply GeoZarr conventions and a multiscale pyramid to the committed Icechunk store.

    Streaming ingest writes a flat store with root GeoZarr attrs but no
    ``spatial_ref`` CRS coordinate or pyramid levels. This rewrites the store via
    ``write_to_icechunk_store`` to add them, so every Icechunk store follows the
    same GeoZarr layout regardless of size.

    The rewrite must materialise the whole store in memory first, because
    ``write_to_icechunk_store`` overwrites the very store it is reading from. That
    is unavoidable on the initial ingest and whenever a pyramid must be rebuilt,
    but a *temporal append* to an already-normalised flat store produces nothing
    new: ``spatial_ref`` is a scalar that survives the append and streaming
    refreshes the root attrs on every commit. We detect that case and skip the
    read-rewrite, avoiding both the full in-memory load and the write
    amplification of re-emitting the entire store on every sync.

    Errors are logged and swallowed so that the plain flat artifact is still
    registered.
    """
    from open_climate_service.data_accessor.services.accessor import open_icechunk_dataset

    try:
        ds = open_icechunk_dataset(store_path)
    except Exception:
        logger.warning("Could not open Icechunk store for GeoZarr write; skipping", exc_info=True)
        return

    try:
        already_normalized = "spatial_ref" in ds.coords or "spatial_ref" in ds.variables
        if already_normalized and "x" in ds.sizes and "y" in ds.sizes and not downloader.needs_pyramid(ds):
            logger.info(
                "Store '%s' is already GeoZarr-normalized and flat; skipping read-rewrite",
                store_path.name,
            )
            return
        downloader.write_to_icechunk_store(ds.load(), store_path, commit_message="Applied GeoZarr conventions")
    except Exception:
        logger.error(
            "GeoZarr/pyramid write failed for '%s'; flat Icechunk artifact will be used as-is",
            store_path.name,
            exc_info=True,
        )
    finally:
        ds.close()


def _load_streaming_plugin(plugin_path: str, *, params: dict[str, object]) -> IngestionPlugin:
    """Load and instantiate one streaming plugin class from a dotted import path.

    Template-defined `ingestion.params` are treated as plugin
    configuration and passed to the constructor here. The same params are also
    forwarded later to `probe(...)` and `fetch_period(...)` so plugins may keep
    configuration in constructor state, per-call kwargs, or both.
    """
    from open_climate_service.shared.plugin_loader import instantiate_plugin

    module_path, _, attr_name = plugin_path.rpartition(".")
    if not module_path or not attr_name:
        raise HTTPException(status_code=500, detail=f"Invalid ingestion.plugin path '{plugin_path}'")
    try:
        plugin = instantiate_plugin(plugin_path, dict(params))
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to load ingestion.plugin '%s'", plugin_path, exc_info=exc)
        raise HTTPException(status_code=500, detail=f"Failed to load ingestion.plugin '{plugin_path}'") from exc
    return plugin


def register_artifact_record(record: ArtifactRecord, *, publish: bool) -> ArtifactRecord:
    """Store a pre-built artifact record and optionally publish it to STAC."""
    stored = _store_artifact_record(record, publish=publish)
    if publish and stored.publication.status != PublicationStatus.PUBLISHED:
        return publish_artifact_record(stored.artifact_id)
    return stored


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


def get_dataset_zarr_store_file_or_404(
    dataset_id: str, relative_path: str, range_header: str | None = None
) -> Response | dict[str, object]:
    """Serve a file, metadata document, or directory listing within a dataset Zarr store."""
    artifact = get_latest_artifact_for_dataset_or_404(dataset_id)
    session = _open_icechunk_store_or_404(artifact)
    return _get_icechunk_store_path_or_404(
        dataset_id=dataset_id, session=session, relative_path=relative_path, range_header=range_header
    )


def serve_icechunk_file(dataset_id: str, file_path: str) -> FileResponse:
    """Serve a raw Icechunk store file for native SDK access via icechunk.http_storage()."""
    artifact = _get_latest_published_icechunk_artifact_cached(dataset_id)
    store_root = Path(artifact.path or artifact.asset_paths[0]).resolve()

    full_path = (store_root / file_path).resolve()
    if not full_path.is_relative_to(store_root):
        raise HTTPException(status_code=404, detail="Not found")
    if not full_path.is_file():
        raise HTTPException(status_code=404, detail=f"Icechunk store file not found: {file_path}")

    return FileResponse(full_path, media_type="application/octet-stream")


def _get_latest_published_icechunk_artifact_cached(dataset_id: str) -> ArtifactRecord:
    """Return the latest published Icechunk artifact, cached by records.json mtime."""
    try:
        mtime = ARTIFACTS_INDEX_PATH.stat().st_mtime if ARTIFACTS_INDEX_PATH.exists() else 0.0
    except OSError:
        mtime = 0.0
    cached = _icechunk_artifact_cache.get(dataset_id)
    if cached is not None and cached[0] == mtime:
        return cached[1]
    artifact = _get_latest_published_icechunk_artifact(dataset_id)
    _icechunk_artifact_cache[dataset_id] = (mtime, artifact)
    return artifact


def _get_latest_published_icechunk_artifact(dataset_id: str) -> ArtifactRecord:
    """Return the latest published Icechunk artifact for dataset_id, or raise 404/409."""
    artifacts = latest_published_zarr_artifacts_by_dataset()
    artifact = artifacts.get(dataset_id)
    if artifact is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    if artifact.format != ArtifactFormat.ICECHUNK:
        raise HTTPException(status_code=409, detail=f"Dataset '{dataset_id}' is not Icechunk-backed")
    return artifact


def _open_icechunk_store_or_404(artifact: ArtifactRecord) -> _IcechunkSession:
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
    snapshot_id = repo.lookup_branch("main")
    session_store = cast(_IcechunkReadableStore, repo.readonly_session("main").store)
    return _IcechunkSession(store=session_store, snapshot_id=snapshot_id, store_path=str(path))


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
    try:
        return cast(bool, _run_async(store.exists(key)))
    except KeyError:
        # Icechunk raises KeyError for Zarr v2 keys (e.g. .zgroup, .zarray)
        return False


def _icechunk_get(store: _IcechunkReadableStore, key: str) -> bytes | None:
    buffer: Any = _run_async(store.get(key, prototype=default_buffer_prototype()))
    if buffer is None:
        return None
    if hasattr(buffer, "to_bytes"):
        return cast(bytes, buffer.to_bytes())
    return cast(bytes, buffer)


def _build_icechunk_consolidated_metadata(session: _IcechunkSession) -> dict[str, object]:
    """Recursively build zarr v3 consolidated metadata for all groups and arrays in the store.

    Icechunk explicitly blocks ``zarr.consolidate_metadata()`` because its transactional
    snapshot system makes a static .zmetadata incompatible with multi-writer safety.
    We generate equivalent consolidated metadata dynamically at serve time so that HTTP
    clients can open the store with a single request (``consolidated=True``) without
    needing server-side directory enumeration.

    Results are cached per (store_path, snapshot_id) so repeated requests within the
    lifetime of a snapshot — which is the common case between ingests — read once and
    serve many times.
    """
    cached = _consolidated_metadata_cache.get(session.cache_key)
    if cached is not None:
        return cached

    if len(_consolidated_metadata_cache) >= _MAX_CONSOLIDATED_CACHE_ENTRIES:
        evict_count = _MAX_CONSOLIDATED_CACHE_ENTRIES // 2
        for key in list(_consolidated_metadata_cache)[:evict_count]:
            # pop(..., None) rather than del: this can run in a worker thread, so a
            # concurrent request may have already evicted the same key.
            _consolidated_metadata_cache.pop(key, None)

    node_metadata: dict[str, object] = {}

    def traverse(prefix: str) -> None:
        try:
            children = _icechunk_list_dir(session.store, prefix)
        except Exception:
            logger.warning("Failed to list Icechunk store prefix '%s' for consolidated metadata", prefix, exc_info=True)
            return
        for child in sorted(children):
            path = f"{prefix}/{child}" if prefix else child
            meta_bytes = _icechunk_get(session.store, f"{path}/zarr.json")
            if meta_bytes is not None:
                meta: dict[str, object] = json.loads(meta_bytes.decode("utf-8"))
                node_metadata[path] = meta
                if meta.get("node_type") == "group":
                    traverse(path)

    traverse("")
    result: dict[str, object] = {"kind": "inline", "must_understand": False, "metadata": node_metadata}
    _consolidated_metadata_cache[session.cache_key] = result
    return result


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


def _serve_bytes_ranged(content: bytes, media_type: str, range_header: str | None) -> Response:
    """Serve bytes with HTTP range request support (RFC 7233).

    Zarrita's sharding_indexed codec requires range requests to read the shard index
    suffix and individual inner chunks without downloading the full shard file.
    """
    total = len(content)
    base_headers: dict[str, str] = {"Accept-Ranges": "bytes", "Content-Length": str(total)}
    if not range_header or not range_header.startswith("bytes="):
        return Response(content=content, media_type=media_type, headers=base_headers)
    try:
        spec = range_header[len("bytes=") :].strip()
        if spec.startswith("-"):
            suffix_len = int(spec[1:])
            start = max(0, total - suffix_len)
            end = total - 1
        elif spec.endswith("-"):
            start = int(spec[:-1])
            end = total - 1
        else:
            parts = spec.split("-", 1)
            start, end = int(parts[0]), int(parts[1])
        if start > end or start >= total or end >= total:
            raise ValueError("unsatisfiable range")
    except (ValueError, IndexError):
        return Response(
            status_code=416,
            headers={"Content-Range": f"bytes */{total}"},
        )
    sliced = content[start : end + 1]
    return Response(
        content=sliced,
        status_code=206,
        media_type=media_type,
        headers={
            "Content-Range": f"bytes {start}-{end}/{total}",
            "Content-Length": str(len(sliced)),
            "Accept-Ranges": "bytes",
        },
    )


def _get_icechunk_store_path_or_404(
    dataset_id: str, session: _IcechunkSession, relative_path: str, range_header: str | None = None
) -> Response | dict[str, object]:
    store = session.store
    target = _normalize_icechunk_relative_path(relative_path)
    if target == "":
        raise HTTPException(status_code=404, detail="Not found")

    if _icechunk_exists(store, target):
        payload = _icechunk_get(store, target)
        if payload is None:
            raise HTTPException(status_code=404, detail=f"Zarr path '{relative_path}' not found")
        if target.endswith("zarr.json"):
            meta = json.loads(payload.decode("utf-8"))
            # Inject consolidated metadata into the root zarr.json so xarray can
            # enumerate variables when accessing the store over HTTP, without needing
            # directory listing or a separate consolidation step.  Result is cached
            # per snapshot so repeated requests within a snapshot lifetime are free.
            if target == "zarr.json" and meta.get("node_type") == "group":
                meta["consolidated_metadata"] = _build_icechunk_consolidated_metadata(session)
            return JSONResponse(content=meta)
        media_type, _ = mimetypes.guess_type(target)
        return _serve_bytes_ranged(payload, media_type or "application/octet-stream", range_header)

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
    if latest.format == ArtifactFormat.ICECHUNK:
        links.append(DatasetAccessLink(href=f"/zarr/{dataset_id}", rel="zarr", title="Zarr store"))
    if latest.publication.status == PublicationStatus.PUBLISHED and latest.format == ArtifactFormat.ICECHUNK:
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
