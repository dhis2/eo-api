"""Component registry scaffold for dynamic workflow execution."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from eo_api.integrations.components import (
    run_dhis2_payload_builder_stage,
    run_download_stage,
    run_feature_stage,
    run_spatial_aggregation_stage,
    run_temporal_aggregation_stage,
)

ComponentCallable = Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]]


@dataclass(frozen=True)
class ComponentDescriptor:
    """Metadata and callable for one workflow component."""

    component_id: str
    fn: ComponentCallable
    description: str


class ComponentRegistry:
    """In-memory component registry used by workflow executor."""

    def __init__(self) -> None:
        self._components: dict[str, ComponentDescriptor] = {}

    def register(self, descriptor: ComponentDescriptor) -> None:
        self._components[descriptor.component_id] = descriptor

    def get(self, component_id: str) -> ComponentDescriptor:
        try:
            return self._components[component_id]
        except KeyError as exc:
            raise KeyError(f"Unknown workflow component '{component_id}'") from exc

    def list_ids(self) -> list[str]:
        return sorted(self._components.keys())


def build_default_component_registry() -> ComponentRegistry:
    """Create default registry for canonical workflow-chain components."""
    registry = ComponentRegistry()

    # Canonical chain ids used by generic workflow templates.
    registry.register(
        ComponentDescriptor(
            component_id="workflow.features",
            fn=run_feature_stage,
            description="Resolve feature scope from inline GeoJSON or DHIS2 selectors.",
        )
    )
    registry.register(
        ComponentDescriptor(
            component_id="workflow.download",
            fn=run_download_stage,
            description="Acquire dataset files using dataset-specific adapters.",
        )
    )
    registry.register(
        ComponentDescriptor(
            component_id="workflow.temporal_aggregation",
            fn=run_temporal_aggregation_stage,
            description="Apply, skip, or exit temporal aggregation by dataset capabilities.",
        )
    )
    registry.register(
        ComponentDescriptor(
            component_id="workflow.spatial_aggregation",
            fn=run_spatial_aggregation_stage,
            description="Apply or skip spatial aggregation and emit canonical intermediates.",
        )
    )
    registry.register(
        ComponentDescriptor(
            component_id="workflow.dhis2_payload_builder",
            fn=run_dhis2_payload_builder_stage,
            description="Build DHIS2 payload from workflow intermediates.",
        )
    )

    return registry
