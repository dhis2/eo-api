"""Artifact publication helpers."""

from __future__ import annotations

import os
from datetime import UTC, datetime

from open_climate_service.ingestions.schemas import ArtifactRecord, PublicationStatus


def publish_artifact(record: ArtifactRecord) -> ArtifactRecord:
    """Mark an artifact as published."""
    collection_id = managed_dataset_id_for(record)
    return record.model_copy(
        update={
            "publication": record.publication.model_copy(
                update={
                    "status": PublicationStatus.PUBLISHED,
                    "collection_id": collection_id,
                    "published_at": datetime.now(UTC),
                }
            )
        }
    )


def managed_dataset_id_for_scope(dataset_id: str) -> str:
    """Return a stable managed dataset identifier for the single configured extent."""
    return dataset_id


def _collection_id_for(record: ArtifactRecord) -> str:
    return managed_dataset_id_for_scope(record.dataset_id)


def managed_dataset_id_for(record: ArtifactRecord) -> str:
    """Return the stable managed dataset id for a stored record."""
    return _collection_id_for(record)


def _native_dataset_href(dataset_id: str) -> str:
    """Return a dataset-detail link suitable for generated metadata."""
    path = f"/datasets/{dataset_id}"
    base_url = os.getenv("CLIMATE_SERVICE_BASE_URL")
    if base_url:
        return f"{base_url.rstrip('/')}{path}"

    ogcapi_base_url = os.getenv("OGCAPI_BASE_URL")
    if ogcapi_base_url:
        normalized = ogcapi_base_url.rstrip("/")
        if normalized.endswith("/ogcapi"):
            normalized = normalized[: -len("/ogcapi")]
        return f"{normalized}{path}"

    return path
