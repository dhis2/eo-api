"""Prefect flows for climate data pipelines.

Adding a new pipeline requires only a new entry in the PIPELINES dict below.
"""

import logging
from typing import Any

from prefect import flow

from eo_api.prefect_flows.schemas import PipelineResult
from eo_api.prefect_flows.tasks import run_process

logger = logging.getLogger(__name__)

PIPELINES: dict[str, str] = {
    "era5-land-download": "Download and process ERA5-Land reanalysis data.",
    "chirps3-download": "Download and process CHIRPS3 precipitation data.",
}


def _make_flow(process_id: str, description: str) -> Any:
    """Create a single Prefect flow bound to a specific process ID."""

    @flow(name=f"{process_id}-pipeline", description=description)
    def pipeline(inputs: dict[str, Any]) -> PipelineResult:
        result = run_process(process_id, inputs)
        return PipelineResult(
            status=result.get("status", "completed"),
            files=result.get("files", []),
            message=result.get("message", f"{process_id} pipeline completed"),
        )

    return pipeline


def _build_flows() -> dict[str, Any]:
    """Create one Prefect flow per registered pipeline."""
    return {pid: _make_flow(pid, desc) for pid, desc in PIPELINES.items()}


PIPELINE_FLOWS = _build_flows()

ALL_FLOWS = list(PIPELINE_FLOWS.values())
