
from fastapi import APIRouter, HTTPException

from constants import ORG_UNITS_GEOJSON
from . import pipeline

router = APIRouter()

@router.get("/")
def get_aggregate(
    dataset_id: str,
    period_type: str,
    start: str,
    end: str,
    temporal_aggregation: str,
    spatial_aggregation: str,
    ):
    """
    Compute aggregate statistics for a dataset and geojson features.
    """

    aggregate = pipeline.get_aggregate(
        dataset_id,
        ORG_UNITS_GEOJSON, # taken from constants
        period_type,
        start,
        end,
        temporal_aggregation,
        spatial_aggregation,
    )
    return aggregate
