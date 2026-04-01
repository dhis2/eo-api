"""Services for ingestion, dataset persistence, and publication metadata."""

from __future__ import annotations

import fcntl
import json
import logging
import mimetypes
import os
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from fastapi import HTTPException
from fastapi.responses import FileResponse, JSONResponse
from starlette.responses import Response

from eo_api.data_accessor.services.accessor import get_data_coverage
from eo_api.data_manager.services import downloader
from eo_api.data_registry.services import datasets as registry_datasets
from eo_api.ingestions.schemas import (
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
    PublicationStatus,
    SyncResponse,
)
from eo_api.publications.services import managed_dataset_id_for, publish_artifact

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data"
ARTIFACTS_DIR = DATA_DIR / "artifacts"
ARTIFACTS_INDEX_PATH = ARTIFACTS_DIR / "records.json"


def ensure_store() -> None:
    """Create the artifact metadata store if it does not exist."""
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    if not ARTIFACTS_INDEX_PATH.exists():
        ARTIFACTS_INDEX_PATH.write_text("[]\n", encoding="utf-8")


def list_artifacts() -> ArtifactListResponse:
    """Return all stored artifacts."""
    return ArtifactListResponse(items=_load_records())


def get_artifact_or_404(artifact_id: str) -> ArtifactRecord:
    """Return a single artifact or raise 404."""
    for record in _load_records():
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
    artifacts = _group_datasets().get(dataset_id)
    if artifacts is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    return _build_dataset_record(dataset_id, artifacts)


def list_datasets() -> DatasetListResponse:
    """Return managed datasets grouped by stable dataset id."""
    grouped = _group_datasets()
    items = [_build_dataset_record(dataset_id, artifacts) for dataset_id, artifacts in sorted(grouped.items())]
    return DatasetListResponse(items=items)


def get_dataset_or_404(dataset_id: str) -> DatasetDetailRecord:
    """Return one managed dataset or raise 404."""
    grouped = _group_datasets()
    artifacts = grouped.get(dataset_id)
    if artifacts is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    return _build_dataset_detail_record(dataset_id, artifacts)


def get_latest_artifact_for_dataset_or_404(dataset_id: str) -> ArtifactRecord:
    """Return the latest artifact backing a managed dataset."""
    grouped = _group_datasets()
    artifacts = grouped.get(dataset_id)
    if artifacts is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    return max(artifacts, key=lambda artifact: artifact.created_at)


def create_artifact(
    *,
    dataset: dict[str, object],
    start: str,
    end: str | None,
    extent_id: str | None,
    bbox: list[float] | None,
    country_code: str | None,
    overwrite: bool,
    prefer_zarr: bool,
    publish: bool,
) -> ArtifactRecord:
    """Download a dataset, persist it locally, and store artifact metadata."""
    request_scope = ArtifactRequestScope(
        start=start,
        end=end,
        extent_id=extent_id,
        bbox=(bbox[0], bbox[1], bbox[2], bbox[3]) if bbox is not None else None,
    )
    existing = _find_existing_artifact(
        dataset_id=str(dataset["id"]),
        request_scope=request_scope,
        prefer_zarr=prefer_zarr,
    )
    if existing is not None and not overwrite:
        if publish and existing.publication.status != PublicationStatus.PUBLISHED:
            return publish_artifact_record(existing.artifact_id)
        return existing

    downloader.download_dataset(
        dataset,
        start=start,
        end=end,
        bbox=bbox,
        country_code=country_code,
        overwrite=overwrite,
        background_tasks=None,
    )

    if prefer_zarr:
        try:
            downloader.build_dataset_zarr(dataset)
        except Exception:
            # Fall back to NetCDF when Zarr materialization is not viable.
            logger.warning(
                "Zarr materialization failed for dataset '%s'; falling back to NetCDF",
                dataset["id"],
                exc_info=True,
            )

    zarr_path = downloader.get_zarr_path(dataset)
    cache_files = downloader.get_cache_files(dataset)
    primary_path: str | None

    if zarr_path is not None:
        artifact_format = ArtifactFormat.ZARR
        primary_path = str(zarr_path.resolve())
        asset_paths = [primary_path]
    elif cache_files:
        artifact_format = ArtifactFormat.NETCDF
        asset_paths = [str(path.resolve()) for path in cache_files]
        primary_path = asset_paths[0] if len(asset_paths) == 1 else None
    else:
        raise HTTPException(status_code=500, detail="Download finished without any saved artifact files")

    coverage_data = get_data_coverage(dataset)
    coverage = ArtifactCoverage(
        temporal=CoverageTemporal(**coverage_data["coverage"]["temporal"]),
        spatial=CoverageSpatial(**coverage_data["coverage"]["spatial"]),
    )

    record = ArtifactRecord(
        artifact_id=str(uuid4()),
        dataset_id=str(dataset["id"]),
        dataset_name=str(dataset["name"]),
        variable=str(dataset["variable"]),
        format=artifact_format,
        path=primary_path,
        asset_paths=asset_paths,
        variables=[str(dataset["variable"])],
        request_scope=request_scope,
        coverage=coverage,
        created_at=datetime.now(UTC),
        publication=ArtifactPublication(),
    )
    stored_record = _store_artifact_record(record, prefer_zarr=prefer_zarr, publish=publish)
    if publish and stored_record.publication.status != PublicationStatus.PUBLISHED:
        return publish_artifact_record(stored_record.artifact_id)
    return stored_record


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
    prefer_zarr: bool,
    publish: bool,
) -> SyncResponse:
    """Sync a managed dataset by fetching data after its latest covered period."""
    latest_artifact = get_latest_artifact_for_dataset_or_404(dataset_id)
    source_dataset = registry_datasets.get_dataset(latest_artifact.dataset_id)
    if source_dataset is None:
        raise HTTPException(status_code=404, detail=f"Source dataset '{latest_artifact.dataset_id}' not found")

    resolved_end = end or date.today().isoformat()
    next_start = _next_period_start(
        latest_artifact.coverage.temporal.end,
        period_type=str(source_dataset["period_type"]),
    )

    if next_start > resolved_end:
        return SyncResponse(sync_id=None, status="up_to_date", dataset=get_dataset_or_404(dataset_id))

    artifact = create_artifact(
        dataset=source_dataset,
        start=next_start,
        end=resolved_end,
        extent_id=latest_artifact.request_scope.extent_id,
        bbox=list(latest_artifact.request_scope.bbox) if latest_artifact.request_scope.bbox is not None else None,
        country_code=None,
        overwrite=False,
        prefer_zarr=prefer_zarr,
        publish=publish,
    )
    return SyncResponse(
        sync_id=artifact.artifact_id,
        status="completed",
        dataset=get_dataset_or_404(managed_dataset_id_for(artifact)),
    )


def get_dataset_zarr_store_info_or_404(dataset_id: str) -> dict[str, object]:
    """Return a public Zarr store listing for a managed dataset."""
    artifact = get_latest_artifact_for_dataset_or_404(dataset_id)
    store_root = _get_zarr_root_or_409(artifact)

    entries = _zarr_entries(dataset_id=dataset_id, store_root=store_root, directory=store_root)
    return {
        "kind": "ZarrListing",
        "dataset_id": dataset_id,
        "format": artifact.format,
        "path": ".",
        "entries": entries,
    }


def get_dataset_zarr_store_file_or_404(
    dataset_id: str, relative_path: str
) -> FileResponse | Response | dict[str, object]:
    """Serve a file, metadata document, or directory listing within a dataset Zarr store."""
    artifact = get_latest_artifact_for_dataset_or_404(dataset_id)
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
    prefer_zarr: bool,
    publish: bool,
) -> ArtifactRecord:
    """Persist a newly created artifact record while avoiding lost updates."""

    def mutate(records: list[ArtifactRecord]) -> ArtifactRecord:
        existing = _find_existing_artifact_in_records(
            records=records,
            dataset_id=record.dataset_id,
            request_scope=record.request_scope,
            prefer_zarr=prefer_zarr,
        )
        if existing is not None:
            if publish and existing.publication.status != PublicationStatus.PUBLISHED:
                return existing
            return existing

        records.append(record)
        return record

    return _mutate_records(mutate)


def _mutate_records(mutation: Callable[[list[ArtifactRecord]], ArtifactRecord]) -> ArtifactRecord:
    """Apply a read-modify-write mutation under an exclusive file lock."""
    ensure_store()
    with ARTIFACTS_INDEX_PATH.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
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
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
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
    prefer_zarr: bool,
) -> ArtifactRecord | None:
    """Return an existing artifact for an identical logical request when possible."""
    return _find_existing_artifact_in_records(
        records=_load_records(),
        dataset_id=dataset_id,
        request_scope=request_scope,
        prefer_zarr=prefer_zarr,
    )


def _find_existing_artifact_in_records(
    *,
    records: list[ArtifactRecord],
    dataset_id: str,
    request_scope: ArtifactRequestScope,
    prefer_zarr: bool,
) -> ArtifactRecord | None:
    """Return an existing artifact for an identical logical request from a provided record set."""
    for record in reversed(records):
        if record.dataset_id != dataset_id:
            continue
        if record.request_scope != request_scope:
            continue
        if prefer_zarr and record.format != ArtifactFormat.ZARR:
            continue
        return record
    return None


def _build_dataset_record(dataset_id: str, artifacts: list[ArtifactRecord]) -> DatasetRecord:
    latest = max(artifacts, key=lambda artifact: artifact.created_at)
    source_dataset = registry_datasets.get_dataset(latest.dataset_id) or {}
    return DatasetRecord(
        dataset_id=dataset_id,
        source_dataset_id=latest.dataset_id,
        dataset_name=latest.dataset_name,
        short_name=_as_optional_str(source_dataset.get("short_name")),
        variable=latest.variable,
        period_type=_as_optional_str(source_dataset.get("period_type")) or "unknown",
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


def _group_datasets() -> dict[str, list[ArtifactRecord]]:
    grouped: dict[str, list[ArtifactRecord]] = {}
    for record in _load_records():
        grouped.setdefault(managed_dataset_id_for(record), []).append(record)
    return grouped


def _next_period_start(latest_period_end: str, *, period_type: str) -> str:
    """Compute the next expected period start for supported source period types."""
    if period_type == "hourly":
        timestamp = datetime.fromisoformat(latest_period_end)
        return (timestamp + timedelta(hours=1)).isoformat()
    if period_type == "daily":
        current = date.fromisoformat(latest_period_end)
        return (current + timedelta(days=1)).isoformat()
    if period_type == "monthly":
        current = date.fromisoformat(f"{latest_period_end}-01")
        year = current.year + (1 if current.month == 12 else 0)
        month = 1 if current.month == 12 else current.month + 1
        return f"{year:04d}-{month:02d}"
    if period_type == "yearly":
        return f"{int(latest_period_end) + 1:04d}"
    raise HTTPException(status_code=400, detail=f"Sync is not implemented for period type '{period_type}'")


def _dataset_links(dataset_id: str, latest: ArtifactRecord) -> list[DatasetAccessLink]:
    links = [
        DatasetAccessLink(href=f"/datasets/{dataset_id}", rel="self", title="Dataset detail"),
        DatasetAccessLink(href=f"/zarr/{dataset_id}", rel="zarr", title="Zarr store"),
    ]
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
                "extent_id": None,
                "bbox": bbox,
            }
    return item
