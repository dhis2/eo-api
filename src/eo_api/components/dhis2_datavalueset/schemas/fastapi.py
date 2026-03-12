from typing import Any

from pydantic import BaseModel, Field

from .datavalueset import Dhis2DataValueSetConfig
from ...schemas import PeriodType


class BuildDataValueSetRunRequest(BaseModel):
    """Execute build_datavalueset component directly from records."""

    dataset_id: str
    period_type: PeriodType
    records: list[dict[str, Any]] = Field(default_factory=list)
    dhis2: Dhis2DataValueSetConfig


class BuildDataValueSetRunResponse(BaseModel):
    """Build_datavalueset component output."""

    value_count: int
    output_file: str
    data_value_set: dict[str, Any]
