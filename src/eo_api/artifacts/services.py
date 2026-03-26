"""Services for artifact creation, persistence, and publication metadata."""

from __future__ import annotations

import json
import mimetypes
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from fastapi import HTTPException
from fastapi.responses import FileResponse, JSONResponse
from starlette.responses import Response

from eo_api.artifacts.schemas import (
    ArtifactCoverage,
    ArtifactFormat,
    ArtifactListResponse,
    ArtifactPublication,
    ArtifactRecord,
    ArtifactRequestScope,
    CollectionArtifactRecord,
    CollectionDetailRecord,
    CollectionListResponse,
    CollectionRecord,
    CoverageSpatial,
    CoverageTemporal,
    PublicationStatus,
)
from eo_api.data_accessor.services.accessor import get_data_coverage
from eo_api.data_manager.services import downloader
from eo_api.publications.services import publish_artifact

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


def create_artifact(
    *,
    dataset: dict[str, object],
    start: str,
    end: str | None,
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
        bbox=(bbox[0], bbox[1], bbox[2], bbox[3]) if bbox is not None else None,
        country_code=country_code,
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
            pass

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
    records = _load_records()
    records.append(record)
    _save_records(records)
    if publish:
        return publish_artifact_record(record.artifact_id)
    return record


def publish_artifact_record(artifact_id: str) -> ArtifactRecord:
    """Publish an artifact via pygeoapi and persist publication metadata."""
    records = _load_records()
    for index, record in enumerate(records):
        if record.artifact_id != artifact_id:
            continue
        published = publish_artifact(record)
        records[index] = published
        _save_records(records)
        return published
    raise HTTPException(status_code=404, detail=f"Artifact '{artifact_id}' not found")


def list_collections() -> CollectionListResponse:
    """Return published collections as a native FastAPI registry view."""
    grouped = _group_published_collections()
    items = [_build_collection_record(collection_id, artifacts) for collection_id, artifacts in sorted(grouped.items())]
    return CollectionListResponse(items=items)


def get_collection_or_404(collection_id: str) -> CollectionDetailRecord:
    """Return a single native collection view or raise 404."""
    grouped = _group_published_collections()
    artifacts = grouped.get(collection_id)
    if artifacts is not None:
        return _build_collection_detail_record(collection_id, artifacts)
    raise HTTPException(status_code=404, detail=f"Collection '{collection_id}' not found")


def get_zarr_store_info_or_404(artifact_id: str) -> dict[str, object]:
    """Return metadata for a Zarr store artifact."""
    artifact = get_artifact_or_404(artifact_id)
    store_root = _get_zarr_root_or_409(artifact)

    entries = _zarr_entries(artifact_id=artifact_id, store_root=store_root, directory=store_root)
    return {
        "artifact_id": artifact.artifact_id,
        "dataset_id": artifact.dataset_id,
        "format": artifact.format,
        "store_root": str(store_root),
        "entries": entries,
    }


def get_zarr_store_file_or_404(artifact_id: str, relative_path: str) -> FileResponse | Response | dict[str, object]:
    """Serve a file, metadata document, or directory listing within a Zarr store."""
    artifact = get_artifact_or_404(artifact_id)
    store_root = _get_zarr_root_or_409(artifact)
    target = _resolve_zarr_path(store_root, relative_path)
    if not target.exists():
        raise HTTPException(status_code=404, detail=f"Zarr path '{relative_path}' not found")
    if target.is_dir():
        return _zarr_directory_listing(artifact_id=artifact_id, store_root=store_root, directory=target)
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


def _zarr_directory_listing(*, artifact_id: str, store_root: Path, directory: Path) -> dict[str, object]:
    """Return a browseable directory listing for a Zarr path."""
    relative_directory = "." if directory == store_root else directory.relative_to(store_root).as_posix()
    entries = _zarr_entries(artifact_id=artifact_id, store_root=store_root, directory=directory)
    return {
        "artifact_id": artifact_id,
        "store_root": str(store_root),
        "directory": relative_directory,
        "entries": entries,
    }


def _zarr_entries(*, artifact_id: str, store_root: Path, directory: Path) -> list[dict[str, str]]:
    """Build directory entries for a Zarr store namespace."""
    return [
        {
            "name": child.name,
            "kind": "directory" if child.is_dir() else "file",
            "href": f"/artifacts/{artifact_id}/zarr/{child.relative_to(store_root).as_posix()}",
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
    for record in reversed(_load_records()):
        if record.dataset_id != dataset_id:
            continue
        if record.request_scope != request_scope:
            continue
        if prefer_zarr and record.format != ArtifactFormat.ZARR:
            continue
        return record
    return None


def _build_collection_record(collection_id: str, artifacts: list[ArtifactRecord]) -> CollectionRecord:
    latest = max(artifacts, key=lambda artifact: artifact.created_at)
    assert latest.publication.pygeoapi_path is not None
    return CollectionRecord(
        collection_id=collection_id,
        dataset_id=latest.dataset_id,
        dataset_name=latest.dataset_name,
        variable=latest.variable,
        latest_artifact_id=latest.artifact_id,
        artifact_count=len(artifacts),
        coverage=latest.coverage,
        latest_created_at=latest.created_at,
        pygeoapi_path=latest.publication.pygeoapi_path,
    )


def _build_collection_detail_record(collection_id: str, artifacts: list[ArtifactRecord]) -> CollectionDetailRecord:
    base = _build_collection_record(collection_id, artifacts)
    ordered_artifacts = sorted(artifacts, key=lambda artifact: artifact.created_at, reverse=True)
    return CollectionDetailRecord(
        **base.model_dump(),
        artifacts=[
            CollectionArtifactRecord(
                artifact_id=artifact.artifact_id,
                created_at=artifact.created_at,
                format=artifact.format,
                request_scope=artifact.request_scope,
                coverage=artifact.coverage,
                artifact_path=artifact.path,
                artifact_api_path=f"/artifacts/{artifact.artifact_id}",
            )
            for artifact in ordered_artifacts
        ],
    )


def _group_published_collections() -> dict[str, list[ArtifactRecord]]:
    grouped: dict[str, list[ArtifactRecord]] = {}
    for record in _load_records():
        if record.publication.status != PublicationStatus.PUBLISHED:
            continue
        collection_id = record.publication.collection_id
        if collection_id is None:
            continue
        grouped.setdefault(collection_id, []).append(record)
    return grouped


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
                "country_code": None,
            }
    return item
