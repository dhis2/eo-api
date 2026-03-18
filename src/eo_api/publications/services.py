"""Disk-backed published resource registry."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import TYPE_CHECKING

from ..data_manager.services.downloader import DOWNLOAD_DIR
from ..data_registry.services.datasets import list_datasets
from .schemas import PublishedResource, PublishedResourceClass, PublishedResourceExposure, PublishedResourceKind

if TYPE_CHECKING:
    from ..workflows.schemas import WorkflowExecuteResponse


def ensure_source_dataset_publications() -> list[PublishedResource]:
    """Seed published source dataset resources from the dataset registry."""
    resources: list[PublishedResource] = []
    for dataset in list_datasets():
        resource_id = f"dataset-{dataset['id']}"
        existing = get_published_resource(resource_id)
        timestamp = _utc_now()
        record = PublishedResource(
            resource_id=resource_id,
            resource_class=PublishedResourceClass.SOURCE,
            kind=PublishedResourceKind.COVERAGE,
            title=str(dataset.get("name") or dataset["id"]),
            description=f"Source dataset: {dataset.get('source') or dataset['id']}",
            dataset_id=str(dataset["id"]),
            path=None,
            ogc_path=f"/ogcapi/collections/{dataset['id']}",
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
            },
            links=[
                {
                    "rel": "collection",
                    "href": f"/ogcapi/collections/{dataset['id']}",
                }
            ],
        )
        _write_resource(record)
        resources.append(record)
    return resources


def register_workflow_output_publication(
    *,
    response: WorkflowExecuteResponse,
    exposure: PublishedResourceExposure,
    published_path: str | None = None,
    asset_format: str | None = None,
) -> PublishedResource:
    """Register a completed workflow output as a published derived resource."""
    resource_id = f"workflow-output-{response.run_id}"
    existing = get_published_resource(resource_id)
    timestamp = _utc_now()
    record = PublishedResource(
        resource_id=resource_id,
        resource_class=PublishedResourceClass.DERIVED,
        kind=PublishedResourceKind.FEATURE_COLLECTION,
        title=f"{response.workflow_id} output for {response.dataset_id}",
        description="Derived workflow output registered for OGC publication.",
        dataset_id=response.dataset_id,
        workflow_id=response.workflow_id,
        job_id=response.run_id,
        run_id=response.run_id,
        path=published_path or response.output_file,
        ogc_path=f"/ogcapi/collections/{resource_id}",
        asset_format=asset_format or "datavalueset-json",
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
        },
        links=[
            {"rel": "job", "href": f"/workflows/jobs/{response.run_id}"},
            {"rel": "job-result", "href": f"/workflows/jobs/{response.run_id}/result"},
            {"rel": "collection", "href": f"/ogcapi/collections/{resource_id}"},
            {"rel": "analytics", "href": f"/analytics/publications/{resource_id}/viewer"},
        ],
    )
    _write_resource(record)
    return record


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
        resources.append(PublishedResource.model_validate_json(path.read_text(encoding="utf-8")))
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
    return PublishedResource.model_validate_json(path.read_text(encoding="utf-8"))


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
