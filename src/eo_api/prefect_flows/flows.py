"""Prefect flows for climate data pipelines.

Adding a new pipeline requires only a new entry in the PIPELINES dict below.
"""

import logging
from typing import Any

from prefect import flow

from eo_api.prefect_flows.schemas import PipelineResult
from eo_api.prefect_flows.tasks import run_process, summarize_datasets

logger = logging.getLogger(__name__)

PIPELINES: dict[str, dict[str, Any]] = {
    "era5-land-download": {
        "description": "Download and process ERA5-Land reanalysis data.",
        "default_inputs": {
            "start": "2024-01",
            "end": "2024-01",
            "bbox": [32.0, -2.0, 35.0, 1.0],
            "variables": ["2m_temperature"],
            "dry_run": True,
        },
    },
    "chirps3-download": {
        "description": "Download and process CHIRPS3 precipitation data.",
        "default_inputs": {
            "start": "2024-01",
            "end": "2024-01",
            "bbox": [32.0, -2.0, 35.0, 1.0],
            "stage": "final",
            "dry_run": True,
        },
    },
}


def _make_flow(process_id: str, description: str, default_inputs: dict[str, Any]) -> Any:
    """Create a single Prefect flow bound to a specific process ID."""

    @flow(name=f"{process_id}-pipeline", description=description)
    def pipeline(inputs: dict[str, Any] = default_inputs) -> PipelineResult:
        result = run_process(process_id, inputs)
        files = result.get("files", [])

        summarize_datasets(process_id, files)

        return PipelineResult(
            status=result.get("status", "completed"),
            files=files,
            message=result.get("message", f"{process_id} pipeline completed"),
        )

    return pipeline


def _build_flows() -> dict[str, Any]:
    """Create one Prefect flow per registered pipeline."""
    return {pid: _make_flow(pid, cfg["description"], cfg["default_inputs"]) for pid, cfg in PIPELINES.items()}


PIPELINE_FLOWS = _build_flows()

ALL_FLOWS = list(PIPELINE_FLOWS.values())
