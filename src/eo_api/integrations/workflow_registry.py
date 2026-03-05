"""Component registry scaffold for dynamic workflow execution."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from eo_api.integrations.chirps3_fetch import download_chirps3
from eo_api.integrations.data_aggregate import aggregate_chirps_rows
from eo_api.integrations.dhis2_datavalues import build_data_value_set
from eo_api.integrations.feature_fetch import resolve_features
from eo_api.integrations.worldpop_sync import sync_worldpop
from eo_api.integrations.worldpop_to_dhis2 import build_worldpop_datavalueset
from eo_api.routers.ogcapi.plugins.processes.schemas import FeatureFetchInput

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


def _component_feature_resolve(params: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    del context
    return resolve_features(FeatureFetchInput.model_validate(params))


def _component_chirps_fetch(params: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    download_dir = Path(str(context.get("download_dir", "/tmp/data")))
    payload = dict(params)
    payload["download_root"] = download_dir
    return download_chirps3(**payload)


def _component_chirps_aggregate(params: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    payload = dict(params)
    payload["cache_root"] = str(context.get("download_dir", "/tmp/data"))
    return aggregate_chirps_rows(**payload)


def _component_worldpop_sync(params: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    payload = dict(params)
    payload["root_dir"] = Path(str(context.get("download_dir", "/tmp/data"))) / "worldpop_cache"
    return sync_worldpop(**payload)


def _component_worldpop_datavalues(params: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    del context
    return build_worldpop_datavalueset(**params)


def _component_datavalues_build(params: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    del context
    return build_data_value_set(**params)


def build_default_component_registry() -> ComponentRegistry:
    """Create default registry with components used by CHIRPS and WorldPop workflows."""
    registry = ComponentRegistry()
    registry.register(
        ComponentDescriptor(
            component_id="feature.resolve",
            fn=_component_feature_resolve,
            description="Resolve features from inline GeoJSON or DHIS2 selectors.",
        )
    )
    registry.register(
        ComponentDescriptor(
            component_id="chirps.fetch",
            fn=_component_chirps_fetch,
            description="Download CHIRPS3 files by scope and date window.",
        )
    )
    registry.register(
        ComponentDescriptor(
            component_id="chirps.aggregate",
            fn=_component_chirps_aggregate,
            description="Aggregate CHIRPS data over workflow features.",
        )
    )
    registry.register(
        ComponentDescriptor(
            component_id="worldpop.sync",
            fn=_component_worldpop_sync,
            description="Sync WorldPop files by request scope.",
        )
    )
    registry.register(
        ComponentDescriptor(
            component_id="worldpop.aggregate_to_datavalues",
            fn=_component_worldpop_datavalues,
            description="Aggregate WorldPop raster to DHIS2 dataValueSet rows.",
        )
    )
    registry.register(
        ComponentDescriptor(
            component_id="dhis2.datavalues.build",
            fn=_component_datavalues_build,
            description="Build DHIS2 dataValueSet/table from normalized rows.",
        )
    )
    return registry
