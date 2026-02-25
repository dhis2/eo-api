"""FastAPI router for pipeline endpoints."""

import logging
from typing import Any

from fastapi import APIRouter, HTTPException

from eo_api.prefect_flows.flows import PIPELINE_FLOWS, PIPELINES
from eo_api.prefect_flows.schemas import PipelineResult

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/{process_id}", response_model=PipelineResult)
def run_pipeline(process_id: str, inputs: dict[str, Any]) -> PipelineResult:
    """Run a pipeline for the given OGC process identifier."""
    if process_id not in PIPELINES:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown process '{process_id}'. Available: {list(PIPELINES.keys())}",
        )
    try:
        result: PipelineResult = PIPELINE_FLOWS[process_id](inputs)
        return result
    except Exception as exc:
        logger.exception("Pipeline %s failed", process_id)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
