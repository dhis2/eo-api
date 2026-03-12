from typing import Any

from pydantic import BaseModel

from .features import FeatureSourceConfig


class FeatureSourceRunRequest(BaseModel):
    """Execute feature source component."""

    feature_source: FeatureSourceConfig
    include_features: bool = False


class FeatureSourceRunResponse(BaseModel):
    """Feature source component result."""

    bbox: list[float]
    feature_count: int
    features: dict[str, Any] | None = None