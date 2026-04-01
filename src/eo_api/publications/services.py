"""Generate pygeoapi configuration from published artifacts."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from importlib import import_module
from pathlib import Path
from typing import Any, cast
from zlib import adler32

import xarray as xr
import yaml

from eo_api.data_manager.services.utils import get_lon_lat_dims, get_time_dim
from eo_api.ingestions.schemas import ArtifactFormat, ArtifactRecord, PublicationStatus

DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data"
CONFIG_DIR = Path(__file__).resolve().parent.parent.parent.parent / "config" / "pygeoapi"
PYGEOAPI_BASE_CONFIG_PATH = CONFIG_DIR / "base.yml"
PYGEOAPI_DIR = DATA_DIR / "pygeoapi"
PYGEOAPI_CONFIG_PATH = PYGEOAPI_DIR / "pygeoapi-config.yml"
PYGEOAPI_OPENAPI_PATH = PYGEOAPI_DIR / "pygeoapi-openapi.yml"


def ensure_pygeoapi_base_config() -> Path:
    """Ensure the generated pygeoapi config exists and is discoverable."""
    PYGEOAPI_DIR.mkdir(parents=True, exist_ok=True)
    if not PYGEOAPI_CONFIG_PATH.exists():
        _sync_pygeoapi_documents(resources={})
    os.environ.setdefault("PYGEOAPI_CONFIG", str(PYGEOAPI_CONFIG_PATH))
    os.environ.setdefault("PYGEOAPI_OPENAPI", str(PYGEOAPI_OPENAPI_PATH))
    return PYGEOAPI_CONFIG_PATH


def publish_artifact(record: ArtifactRecord) -> ArtifactRecord:
    """Mark an artifact as published and regenerate the pygeoapi config."""
    from eo_api.ingestions.services import list_artifacts

    collection_id = managed_dataset_id_for(record)
    published_record = record.model_copy(
        update={
            "publication": record.publication.model_copy(
                update={
                    "status": PublicationStatus.PUBLISHED,
                    "collection_id": collection_id,
                    "published_at": datetime.now(UTC),
                    "pygeoapi_path": f"/ogcapi/collections/{collection_id}",
                }
            )
        }
    )

    resources: dict[str, Any] = {}
    for artifact in list_artifacts().items:
        active = published_record if artifact.artifact_id == record.artifact_id else artifact
        if active.publication.status != PublicationStatus.PUBLISHED:
            continue
        assert active.publication.collection_id is not None
        resources[active.publication.collection_id] = _build_collection_resource(active)

    _sync_pygeoapi_documents(resources=resources)
    _refresh_mounted_pygeoapi()
    return published_record


def _write_config(*, resources: dict[str, Any]) -> None:
    config = _load_base_config()
    config["resources"] = resources

    PYGEOAPI_DIR.mkdir(parents=True, exist_ok=True)
    PYGEOAPI_CONFIG_PATH.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")


def _sync_pygeoapi_documents(*, resources: dict[str, Any]) -> None:
    """Write the pygeoapi config and regenerate its OpenAPI document."""
    _write_config(resources=resources)
    openapi_module = import_module("pygeoapi.openapi")
    openapi_models = import_module("pygeoapi.models.openapi")
    payload = openapi_module.generate_openapi_document(
        PYGEOAPI_CONFIG_PATH,
        openapi_models.OAPIFormat(root="yaml"),
    )
    PYGEOAPI_OPENAPI_PATH.write_text(payload, encoding="utf-8")


def _refresh_mounted_pygeoapi() -> None:
    """Refresh the in-process pygeoapi mount if the wrapper has been initialized."""
    try:
        from eo_api.pygeoapi_app import refresh_pygeoapi
    except Exception:
        return
    refresh_pygeoapi()


def _build_collection_resource(record: ArtifactRecord) -> dict[str, Any]:
    bbox = record.coverage.spatial
    temporal = record.coverage.temporal
    x_field, y_field, time_field = _provider_axes(record)
    provider: dict[str, Any] = {
        "type": "coverage",
        "name": "xarray",
        "data": record.path or record.asset_paths[0],
        "x_field": x_field,
        "y_field": y_field,
        "time_field": time_field,
        "storage_crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
        "format": _provider_format(record.format),
    }
    if record.format == ArtifactFormat.ZARR:
        provider["options"] = {"zarr": {"consolidated": True}, "squeeze": True}

    return {
        "type": "collection",
        "title": record.dataset_name,
        "description": f"Published EO grid for dataset '{record.dataset_id}'",
        "keywords": [record.dataset_id, record.variable, record.format.value, "eo", "coverage"],
        "links": [
            {
                "type": "application/json",
                "rel": "alternate",
                "title": "Native dataset detail",
                "href": _native_dataset_href(managed_dataset_id_for(record)),
            },
        ],
        "extents": {
            "spatial": {
                "bbox": [bbox.xmin, bbox.ymin, bbox.xmax, bbox.ymax],
                "crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
            },
            "temporal": {"begin": temporal.start, "end": temporal.end},
        },
        "providers": [provider],
    }


def _provider_format(artifact_format: ArtifactFormat) -> dict[str, str]:
    if artifact_format == ArtifactFormat.ZARR:
        return {"name": "zarr", "mimetype": "application/zip"}
    return {"name": "netcdf", "mimetype": "application/x-netcdf"}


def _provider_axes(record: ArtifactRecord) -> tuple[str, str, str]:
    """Inspect an artifact and return provider axis field names."""
    data_path = record.path or record.asset_paths[0]
    if record.format == ArtifactFormat.ZARR:
        ds = xr.open_zarr(data_path, consolidated=True)
    else:
        ds = xr.open_dataset(data_path)

    try:
        x_field, y_field = get_lon_lat_dims(ds)
        time_field = get_time_dim(ds)
        return x_field, y_field, time_field
    finally:
        ds.close()


def _collection_id_for(record: ArtifactRecord) -> str:
    """Build a stable collection identifier for a logical dataset scope."""
    if record.request_scope.extent_id:
        scope_key = f"extent-{record.request_scope.extent_id}"
    elif record.request_scope.bbox:
        bbox = ",".join(f"{value:.6f}" for value in record.request_scope.bbox)
        scope_hash = f"{adler32(bbox.encode('utf-8')):08x}"
        scope_key = f"bbox-{scope_hash}"
    else:
        scope_key = "global"
    return f"{record.dataset_id}-{scope_key}"


def managed_dataset_id_for(record: ArtifactRecord) -> str:
    """Return the stable managed dataset id for a stored record."""
    return record.publication.collection_id or _collection_id_for(record)


def _native_dataset_href(dataset_id: str) -> str:
    """Return a dataset-detail link suitable for generated pygeoapi metadata."""
    path = f"/datasets/{dataset_id}"
    base_url = os.getenv("EO_API_BASE_URL")
    if base_url:
        return f"{base_url.rstrip('/')}{path}"

    ogcapi_base_url = os.getenv("OGCAPI_BASE_URL")
    if ogcapi_base_url:
        normalized = ogcapi_base_url.rstrip("/")
        if normalized.endswith("/ogcapi"):
            normalized = normalized[: -len("/ogcapi")]
        return f"{normalized}{path}"

    return path


def _load_base_config() -> dict[str, Any]:
    """Load the checked-in base pygeoapi config used for generated publication docs."""
    return cast(dict[str, Any], yaml.safe_load(PYGEOAPI_BASE_CONFIG_PATH.read_text(encoding="utf-8")))
