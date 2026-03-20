"""Disk-backed published resource registry."""

from __future__ import annotations

import datetime as dt
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from ..data_accessor.services.accessor import get_data_coverage
from ..data_manager.services.downloader import DOWNLOAD_DIR, get_zarr_path
from ..data_registry.services.datasets import list_datasets
from .capabilities import (
    PublicationServingCapability,
    default_asset_format_for_kind,
    evaluate_publication_serving,
)
from .schemas import PublishedResource, PublishedResourceClass, PublishedResourceExposure, PublishedResourceKind

if TYPE_CHECKING:
    from ..workflows.schemas import WorkflowExecuteResponse

logger = logging.getLogger(__name__)

_LEGACY_PYGEOAPI_PREFIX = "/ogcapi"
_PYGEOAPI_PREFIX = "/pygeoapi"


def ensure_source_dataset_publications() -> list[PublishedResource]:
    """Seed published source dataset resources from the dataset registry."""
    resources: list[PublishedResource] = []
    for dataset in list_datasets():
        resource_id = f"dataset-{dataset['id']}"
        existing = get_published_resource(resource_id)
        timestamp = _utc_now()
        coverage_metadata = _coverage_metadata_for_dataset(dataset)
        record = PublishedResource(
            resource_id=resource_id,
            resource_class=PublishedResourceClass.SOURCE,
            kind=PublishedResourceKind.COVERAGE,
            title=str(dataset.get("name") or dataset["id"]),
            description=(
                f"Source dataset from {dataset.get('source') or dataset['id']}"
                f" for {dataset.get('variable') or dataset['id']}"
                f" with {dataset.get('period_type') or 'native'} cadence."
            ),
            dataset_id=str(dataset["id"]),
            path=None,
            ogc_path=f"/pygeoapi/collections/{dataset['id']}",
            asset_format="zarr",
            exposure=PublishedResourceExposure.OGC,
            created_at=existing.created_at if existing is not None else timestamp,
            updated_at=timestamp,
            metadata={
                "dataset_id": dataset["id"],
                "variable": dataset.get("variable"),
                "period_type": dataset.get("period_type"),
                "source": dataset.get("source"),
                "source_url": dataset.get("source_url"),
                "resolution": dataset.get("resolution"),
                "units": dataset.get("units"),
                **coverage_metadata,
            },
            links=[
                {
                    "rel": "collection",
                    "href": f"/pygeoapi/collections/{dataset['id']}",
                },
                {
                    "rel": "raster-capabilities",
                    "href": f"/raster/{dataset['id']}/capabilities",
                },
            ],
        )
        _write_resource(record)
        resources.append(record)
    return resources


def _coverage_metadata_for_dataset(dataset: dict[str, object]) -> dict[str, object]:
    """Best-effort spatial/temporal metadata for one source dataset."""
    zarr_path = get_zarr_path(dataset)
    if zarr_path is None:
        logger.info(
            "Skipping coverage metadata for dataset '%s': no zarr archive available",
            dataset.get("id"),
        )
        return {}

    try:
        coverage = get_data_coverage(dataset).get("coverage")
    except (OSError, RuntimeError, ValueError) as exc:
        logger.warning(
            "Skipping coverage metadata for dataset '%s': %s",
            dataset.get("id"),
            exc,
        )
        return {}
    except Exception:
        logger.exception("Could not derive coverage metadata for dataset '%s'", dataset.get("id"))
        return {}

    if not isinstance(coverage, dict):
        return {}

    spatial = coverage.get("spatial")
    temporal = coverage.get("temporal")
    metadata: dict[str, object] = {}

    if isinstance(spatial, dict):
        xmin = spatial.get("xmin")
        ymin = spatial.get("ymin")
        xmax = spatial.get("xmax")
        ymax = spatial.get("ymax")
        if all(value is not None for value in (xmin, ymin, xmax, ymax)):
            metadata["bbox"] = [float(xmin), float(ymin), float(xmax), float(ymax)]

    if isinstance(temporal, dict):
        start = temporal.get("start")
        end = temporal.get("end")
        if start is not None:
            metadata["time_start"] = str(start)
        if end is not None:
            metadata["time_end"] = str(end)

    return metadata


def register_workflow_output_publication(
    *,
    response: WorkflowExecuteResponse,
    kind: PublishedResourceKind,
    exposure: PublishedResourceExposure,
    published_path: str | None = None,
    asset_format: str | None = None,
) -> PublishedResource:
    """Register a completed workflow output as a published derived resource."""
    resource_id = f"workflow-output-{response.run_id}"
    existing = get_published_resource(resource_id)
    timestamp = _utc_now()
    publication_path = published_path or response.output_file
    resolved_asset_format = asset_format or default_asset_format_for_kind(kind)
    capability = evaluate_publication_serving(
        kind=kind,
        exposure=exposure,
        asset_format=resolved_asset_format,
    )
    if not capability.supported:
        raise ValueError(capability.error or "Unsupported publication serving contract")
    ogc_path = _derived_resource_ogc_path(resource_id=resource_id, capability=capability)
    analytics_metadata = (
        _analytics_metadata_for_published_asset(publication_path)
        if kind == PublishedResourceKind.FEATURE_COLLECTION
        else {"eligible": False, "period_count": 0, "has_period_field": False}
    )
    links = [
        {"rel": "job", "href": f"/workflows/jobs/{response.run_id}"},
        {"rel": "job-result", "href": f"/workflows/jobs/{response.run_id}/result"},
    ]
    if ogc_path is not None:
        links.append({"rel": "collection", "href": ogc_path})
    if "raster" in capability.served_by:
        links.append({"rel": "raster-capabilities", "href": f"/raster/{resource_id}/capabilities"})
    if analytics_metadata["eligible"] and "analytics" in capability.served_by:
        links.append({"rel": "analytics", "href": f"/analytics/publications/{resource_id}/viewer"})
    record = PublishedResource(
        resource_id=resource_id,
        resource_class=PublishedResourceClass.DERIVED,
        kind=kind,
        title=f"{response.workflow_id} output for {response.dataset_id}",
        description=(
            f"Derived workflow output from {response.workflow_id} for {response.dataset_id} "
            f"({response.feature_count or 0} features, {response.value_count or 0} values)."
        ),
        dataset_id=response.dataset_id,
        workflow_id=response.workflow_id,
        job_id=response.run_id,
        run_id=response.run_id,
        path=publication_path,
        ogc_path=ogc_path,
        asset_format=resolved_asset_format,
        exposure=exposure,
        created_at=existing.created_at if existing is not None else timestamp,
        updated_at=timestamp,
        metadata={
            "workflow_id": response.workflow_id,
            "workflow_version": response.workflow_version,
            "dataset_id": response.dataset_id,
            "feature_count": response.feature_count,
            "value_count": response.value_count,
            "bbox": response.bbox,
            "native_output_file": response.output_file,
            "period_count": analytics_metadata["period_count"],
            "has_period_field": analytics_metadata["has_period_field"],
            "analytics_eligible": analytics_metadata["eligible"],
        },
        links=links,
    )
    _write_resource(record)
    return record


def _derived_resource_ogc_path(*, resource_id: str, capability: PublicationServingCapability) -> str | None:
    if capability.ogc_collection:
        return f"/pygeoapi/collections/{resource_id}"
    return None


def list_published_resources(
    *,
    resource_class: PublishedResourceClass | None = None,
    dataset_id: str | None = None,
    workflow_id: str | None = None,
    exposure: PublishedResourceExposure | None = None,
) -> list[PublishedResource]:
    """List persisted published resources."""
    resources: list[PublishedResource] = []
    for path in _resources_dir().glob("*.json"):
        resource = PublishedResource.model_validate_json(path.read_text(encoding="utf-8"))
        resources.append(_normalize_pygeoapi_resource_links(resource))
    resources.sort(key=lambda item: item.created_at, reverse=True)
    if resource_class is not None:
        resources = [item for item in resources if item.resource_class == resource_class]
    if dataset_id is not None:
        resources = [item for item in resources if item.dataset_id == dataset_id]
    if workflow_id is not None:
        resources = [item for item in resources if item.workflow_id == workflow_id]
    if exposure is not None:
        resources = [item for item in resources if item.exposure == exposure]
    return resources


def get_published_resource(resource_id: str) -> PublishedResource | None:
    """Fetch a single published resource."""
    path = _resource_path(resource_id)
    if not path.exists():
        return None
    resource = PublishedResource.model_validate_json(path.read_text(encoding="utf-8"))
    return _normalize_pygeoapi_resource_links(resource)


def delete_published_resource(resource_id: str) -> PublishedResource | None:
    """Delete one persisted published resource if it exists."""
    resource = get_published_resource(resource_id)
    if resource is None:
        return None
    path = _resource_path(resource_id)
    if path.exists():
        path.unlink()
    return resource


def get_published_resource_by_collection_id(collection_id: str) -> PublishedResource | None:
    """Resolve an OGC collection identifier to a published resource."""
    ensure_source_dataset_publications()
    for resource in list_published_resources(exposure=PublishedResourceExposure.OGC):
        if _collection_id_for_resource(resource) == collection_id:
            return resource
    return None


def collection_id_for_resource(resource: PublishedResource) -> str:
    """Return the OGC collection identifier for a published resource."""
    return _collection_id_for_resource(resource)


def _write_resource(resource: PublishedResource) -> None:
    _resources_dir().mkdir(parents=True, exist_ok=True)
    _resource_path(resource.resource_id).write_text(resource.model_dump_json(indent=2), encoding="utf-8")


def _resource_path(resource_id: str) -> Path:
    return _resources_dir() / f"{resource_id}.json"


def _resources_dir() -> Path:
    return DOWNLOAD_DIR / "published_resources"


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _collection_id_for_resource(resource: PublishedResource) -> str:
    if resource.resource_class == PublishedResourceClass.SOURCE and resource.dataset_id is not None:
        return resource.dataset_id
    return resource.resource_id


def _normalize_pygeoapi_resource_links(resource: PublishedResource) -> PublishedResource:
    updates: dict[str, object] = {}

    if resource.ogc_path and resource.ogc_path.startswith(_LEGACY_PYGEOAPI_PREFIX):
        updates["ogc_path"] = resource.ogc_path.replace(_LEGACY_PYGEOAPI_PREFIX, _PYGEOAPI_PREFIX, 1)

    normalized_links: list[dict[str, object]] = []
    links_changed = False
    for link in resource.links:
        normalized_link = dict(link)
        href = normalized_link.get("href")
        if isinstance(href, str) and href.startswith(_LEGACY_PYGEOAPI_PREFIX):
            normalized_link["href"] = href.replace(_LEGACY_PYGEOAPI_PREFIX, _PYGEOAPI_PREFIX, 1)
            links_changed = True
        normalized_links.append(normalized_link)

    if links_changed:
        updates["links"] = normalized_links

    if not updates:
        return resource

    return resource.model_copy(update=updates)


def _analytics_metadata_for_published_asset(path_value: str | None) -> dict[str, bool | int]:
    if path_value is None:
        return {"eligible": False, "period_count": 0, "has_period_field": False}

    path = Path(path_value)
    if path.suffix.lower() != ".geojson" or not path.exists():
        return {"eligible": False, "period_count": 0, "has_period_field": False}

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"eligible": False, "period_count": 0, "has_period_field": False}

    features = payload.get("features")
    if not isinstance(features, list):
        return {"eligible": False, "period_count": 0, "has_period_field": False}

    periods: set[str] = set()
    has_period_field = False
    for feature in features:
        if not isinstance(feature, dict):
            continue
        properties = feature.get("properties", {})
        if not isinstance(properties, dict):
            continue
        if "period" in properties:
            has_period_field = True
            value = properties.get("period")
            if value is not None:
                periods.add(str(value))

    return {
        "eligible": has_period_field and len(periods) > 1,
        "period_count": len(periods),
        "has_period_field": has_period_field,
    }
