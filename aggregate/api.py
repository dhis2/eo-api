
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
    ):
    """
    Compute aggregate statistics for a dataset and geojson features.
    """

    aggregate = pipeline.get_aggregate(
        dataset_id,
        ORG_UNITS_GEOJSON,
        period_type,
        start,
        end,
    )
    return aggregate
