"""OGC Process stage wrapper for spatial aggregation."""

from __future__ import annotations

from typing import Any

from pygeoapi.process.base import ProcessorExecuteError

from eo_api.integrations.components.adapters.spatial import chirps3, passthrough_files, worldpop

_SPATIAL_ADAPTERS = {
    "chirps3": chirps3.run,
    "worldpop": worldpop.run,
    "era5": passthrough_files.run,
}


def run_spatial_aggregation_stage(params: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    """Apply or skip spatial aggregation based on dataset + available intermediates."""
    del context
    dataset = str(params.get("dataset", "")).strip().lower()
    adapter = _SPATIAL_ADAPTERS.get(dataset)
    if adapter is None:
        raise ProcessorExecuteError(f"Unsupported dataset '{dataset}' in spatial_aggregation stage")
    return adapter(params)
