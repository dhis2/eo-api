from pydantic import BaseModel

from .temporal import AggregationMethod
from ...schemas import PeriodType


class TemporalAggregationRunRequest(BaseModel):
    """Execute temporal aggregation component from cached dataset."""

    dataset_id: str
    start: str
    end: str
    target_period_type: PeriodType
    method: AggregationMethod = AggregationMethod.SUM
    bbox: list[float] | None = None


class TemporalAggregationRunResponse(BaseModel):
    """Temporal aggregation result summary."""

    dataset_id: str
    sizes: dict[str, int]
    dims: list[str]