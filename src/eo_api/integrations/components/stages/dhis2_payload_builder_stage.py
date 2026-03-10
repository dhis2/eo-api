"""OGC Process stage wrapper for DHIS2 payload generation."""

from __future__ import annotations

from typing import Any

from pygeoapi.process.base import ProcessorExecuteError

from eo_api.integrations.components.adapters.payload import chirps3, era5, worldpop

_PAYLOAD_ADAPTERS = {
    "chirps3": chirps3.run,
    "worldpop": worldpop.run,
    "era5": era5.run,
}


def run_dhis2_payload_builder_stage(params: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    """Build DHIS2 payload from workflow intermediates."""
    del context
    dataset = str(params.get("dataset", "")).strip().lower()
    adapter = _PAYLOAD_ADAPTERS.get(dataset)
    if adapter is None:
        raise ProcessorExecuteError(f"Unsupported dataset '{dataset}' in dhis2_payload_builder stage")
    return adapter(params)
