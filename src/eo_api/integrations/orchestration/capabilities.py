"""Dataset/service capability discovery for generic workflow orchestration."""

from __future__ import annotations

from functools import lru_cache
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field
from pygeoapi.process.base import ProcessorExecuteError

from eo_api.integrations.orchestration.definitions import WORKFLOW_DEFINITION_DIR, available_workflow_definitions
from eo_api.integrations.orchestration.registry import build_default_component_registry

_CAPABILITIES_FILE = WORKFLOW_DEFINITION_DIR / "catalog" / "dataset_capabilities.yaml"


class SupportCapabilities(BaseModel):
    """Boolean capability flags for one dataset."""

    temporal_downsample: bool = True
    temporal_disaggregate: bool = False


class ReducerCapabilities(BaseModel):
    """Supported reducers by aggregation stage."""

    temporal: list[str] = Field(default_factory=list)
    spatial: list[str] = Field(default_factory=list)


class StageBinding(BaseModel):
    """Adapter binding for one stage."""

    adapter: str


class CapabilityProfile(BaseModel):
    """Capability profile for either provider or current integration."""

    source_temporal_resolution: str
    supported_temporal_resolutions: list[str]
    supported_output_formats: list[str] = Field(default_factory=list)
    supports: SupportCapabilities
    reducers: ReducerCapabilities


class DatasetCapabilities(BaseModel):
    """Capabilities for one dataset, separated by provider vs integration."""

    title: str
    provider_capabilities: CapabilityProfile
    integration_capabilities: CapabilityProfile
    stages: dict[str, StageBinding]
    collections: "DatasetCollections" = Field(default_factory=lambda: DatasetCollections())


class DatasetCapabilityCatalog(BaseModel):
    """Top-level capability catalog loaded from YAML."""

    version: str
    datasets: dict[str, DatasetCapabilities]


class CollectionDescriptor(BaseModel):
    """OGC collection descriptor for source or workflow output datasets."""

    id: str
    title: str
    description: str | None = None
    rel: Literal["collection", "derived-from"] = "collection"
    type: str = "application/json"
    path: str | None = None
    exposed: bool = True

    def href(self) -> str:
        """Build the OGC collection URL path."""
        return self.path or f"/ogcapi/collections/{self.id}"


class DatasetCollections(BaseModel):
    """Collection mapping for one dataset."""

    source: list[CollectionDescriptor] = Field(default_factory=list)
    workflow_outputs: list[CollectionDescriptor] = Field(default_factory=list)


@lru_cache(maxsize=1)
def load_dataset_capabilities() -> DatasetCapabilityCatalog:
    """Load dataset capability catalog from YAML."""
    if not _CAPABILITIES_FILE.exists():
        raise ProcessorExecuteError(f"Dataset capability catalog not found: {_CAPABILITIES_FILE}")
    raw = _CAPABILITIES_FILE.read_text(encoding="utf-8")
    payload = yaml.safe_load(raw)
    if not isinstance(payload, dict):
        raise ProcessorExecuteError(f"Dataset capability catalog must be a YAML object: {_CAPABILITIES_FILE}")
    try:
        return DatasetCapabilityCatalog.model_validate(payload)
    except Exception as exc:
        raise ProcessorExecuteError(f"Invalid dataset capability catalog: {exc}") from exc


def list_supported_datasets() -> list[str]:
    """List dataset IDs available in capability catalog."""
    catalog = load_dataset_capabilities()
    return sorted(catalog.datasets.keys())


def build_generic_workflow_capabilities_document() -> dict[str, Any]:
    """Build a discoverable capability document for generic workflow."""
    catalog = load_dataset_capabilities()
    registry = build_default_component_registry()
    collection_index: dict[str, dict[str, Any]] = {}
    for item in catalog.datasets.values():
        for descriptor in [*item.collections.source, *item.collections.workflow_outputs]:
            collection_index.setdefault(
                descriptor.id,
                {
                    "id": descriptor.id,
                    "title": descriptor.title,
                    "description": descriptor.description,
                    "href": descriptor.href(),
                    "exposed": descriptor.exposed,
                },
            )
    return {
        "processId": "generic-dhis2-workflow",
        "catalogVersion": catalog.version,
        "datasets": {dataset: item.model_dump() for dataset, item in catalog.datasets.items()},
        "collections": sorted(collection_index.values(), key=lambda x: x["id"]),
        "components": [
            {"id": component_id, "description": registry.get(component_id).description}
            for component_id in registry.list_ids()
        ],
        "workflowDefinitions": available_workflow_definitions(),
    }


def build_collection_links_for_dataset(
    dataset_id: str, *, include_workflow_outputs: bool = False
) -> list[dict[str, str]]:
    """Build OGC links for collection resources related to a dataset workflow run."""
    catalog = load_dataset_capabilities()
    dataset = catalog.datasets.get(dataset_id)
    if dataset is None:
        return []

    descriptors = list(dataset.collections.source)
    if include_workflow_outputs:
        descriptors.extend(dataset.collections.workflow_outputs)

    links: list[dict[str, str]] = []
    for descriptor in descriptors:
        if not descriptor.exposed:
            continue
        links.append(
            {
                "rel": descriptor.rel,
                "type": descriptor.type,
                "title": descriptor.title,
                "href": descriptor.href(),
            }
        )
    return links
