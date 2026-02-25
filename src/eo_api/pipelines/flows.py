"""Prefect flows for climate data pipelines."""

import logging

from prefect import flow

from eo_api.pipelines.schemas import CHIRPS3PipelineInput, ERA5LandPipelineInput, PipelineResult
from eo_api.pipelines.tasks import download_chirps3, download_era5_land

logger = logging.getLogger(__name__)


@flow(name="era5-land-pipeline")
def era5_land_pipeline(inputs: ERA5LandPipelineInput) -> PipelineResult:
    """Download ERA5-Land data via the OGC API process."""
    result = download_era5_land(
        start=inputs.start,
        end=inputs.end,
        bbox=inputs.bbox,
        variables=inputs.variables,
    )

    return PipelineResult(
        status=result.get("status", "completed"),
        files=result.get("files", []),
        message=result.get("message", "ERA5-Land pipeline completed"),
    )


@flow(name="chirps3-pipeline")
def chirps3_pipeline(inputs: CHIRPS3PipelineInput) -> PipelineResult:
    """Download CHIRPS3 data via the OGC API process."""
    result = download_chirps3(
        start=inputs.start,
        end=inputs.end,
        bbox=inputs.bbox,
        stage=inputs.stage,
    )

    return PipelineResult(
        status=result.get("status", "completed"),
        files=result.get("files", []),
        message=result.get("message", "CHIRPS3 pipeline completed"),
    )
