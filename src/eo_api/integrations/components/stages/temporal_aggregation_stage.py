"""OGC Process stage wrapper for temporal aggregation."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pygeoapi.process.base import ProcessorExecuteError

from eo_api.integrations.components.adapters.temporal import chirps3, era5, worldpop

_TEMPORAL_ADAPTERS_WITH_DIR: dict[str, Callable[[dict[str, Any], str], dict[str, Any]]] = {"chirps3": chirps3.run}
_TEMPORAL_ADAPTERS_SIMPLE: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
    "worldpop": worldpop.run,
    "era5": era5.run,
}


def run_temporal_aggregation_stage(params: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    """Apply or skip temporal aggregation based on dataset + request."""
    dataset = str(params.get("dataset", "")).strip().lower()
    adapter_with_dir = _TEMPORAL_ADAPTERS_WITH_DIR.get(dataset)
    if adapter_with_dir is not None:
        return adapter_with_dir(params, str(context.get("download_dir", "/tmp/data")))
    adapter_simple = _TEMPORAL_ADAPTERS_SIMPLE.get(dataset)
    if adapter_simple is not None:
        return adapter_simple(params)
    raise ProcessorExecuteError(f"Unsupported dataset '{dataset}' in temporal_aggregation stage")
