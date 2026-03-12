from enum import StrEnum

from pydantic import BaseModel, ConfigDict

from ...schemas import PeriodType


class AggregationMethod(StrEnum):
    """Supported numeric aggregation methods."""

    MEAN = "mean"
    SUM = "sum"
    MIN = "min"
    MAX = "max"


class TemporalAggregationConfig(BaseModel):
    """Temporal rollup config."""

    target_period_type: PeriodType
    method: AggregationMethod = AggregationMethod.SUM


class _TemporalAggregationStepConfig(BaseModel):
    # from workflows folder
    model_config = ConfigDict(extra="forbid")

    target_period_type: PeriodType | None = None
    method: AggregationMethod | None = None
