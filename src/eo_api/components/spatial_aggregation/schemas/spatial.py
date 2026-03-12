from enum import StrEnum

from pydantic import BaseModel, ConfigDict


class AggregationMethod(StrEnum):
    """Supported numeric aggregation methods."""

    MEAN = "mean"
    SUM = "sum"
    MIN = "min"
    MAX = "max"


class SpatialAggregationConfig(BaseModel):
    """Spatial aggregation config."""

    method: AggregationMethod = AggregationMethod.MEAN


class _SpatialAggregationStepConfig(BaseModel):
    # from workflows folder
    model_config = ConfigDict(extra="forbid")

    method: AggregationMethod | None = None
    feature_id_property: str | None = None
