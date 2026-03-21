"""Schemas for backend-owned published resources."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class PublishedResourceClass(StrEnum):
    """High-level publication origin."""

    SOURCE = "source"
    DERIVED = "derived"


class PublishedResourceKind(StrEnum):
    """Supported OGC-facing resource kinds."""

    COLLECTION = "collection"
    COVERAGE = "coverage"
    FEATURE_COLLECTION = "feature_collection"
    TILESET = "tileset"


class PublishedResourceExposure(StrEnum):
    """Whether a registered resource should be surfaced via OGC."""

    REGISTRY_ONLY = "registry_only"
    OGC = "ogc"


class PublishedResource(BaseModel):
    """Backend-owned publication state for one discoverable resource."""

    resource_id: str
    resource_class: PublishedResourceClass
    kind: PublishedResourceKind
    title: str
    description: str
    dataset_id: str | None = None
    workflow_id: str | None = None
    job_id: str | None = None
    run_id: str | None = None
    path: str | None = None
    ogc_path: str | None = None
    asset_format: str | None = None
    exposure: PublishedResourceExposure = PublishedResourceExposure.REGISTRY_ONLY
    created_at: str
    updated_at: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    links: list[dict[str, Any]] = Field(default_factory=list)


class PublishedResourceListResponse(BaseModel):
    """List of published resources."""

    resources: list[PublishedResource]
