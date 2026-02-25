"""FastAPI router for pipeline endpoints."""

import logging

from fastapi import APIRouter, HTTPException

from eo_api.pipelines.flows import chirps3_pipeline, era5_land_pipeline
from eo_api.pipelines.schemas import CHIRPS3PipelineInput, ERA5LandPipelineInput, PipelineResult

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/era5-land", response_model=PipelineResult)
def run_era5_land_pipeline(inputs: ERA5LandPipelineInput) -> PipelineResult:
    """Run the ERA5-Land download-and-aggregate pipeline."""
    try:
        return era5_land_pipeline(inputs)
    except Exception as exc:
        logger.exception("ERA5-Land pipeline failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/chirps3", response_model=PipelineResult)
def run_chirps3_pipeline(inputs: CHIRPS3PipelineInput) -> PipelineResult:
    """Run the CHIRPS3 download-and-aggregate pipeline."""
    try:
        return chirps3_pipeline(inputs)
    except Exception as exc:
        logger.exception("CHIRPS3 pipeline failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
