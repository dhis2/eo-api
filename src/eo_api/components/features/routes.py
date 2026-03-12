from .schemas.fastapi import FeatureSourceRunRequest, FeatureSourceRunResponse
from .services.features import feature_source_component

from fastapi import APIRouter

router = APIRouter()


@router.post("/run", response_model=FeatureSourceRunResponse)
def run_feature_source(payload: FeatureSourceRunRequest) -> FeatureSourceRunResponse:
    """Resolve feature source to features and bbox."""
    features, bbox = feature_source_component(payload.feature_source)
    return FeatureSourceRunResponse(
        bbox=bbox,
        feature_count=len(features["features"]),
        features=features if payload.include_features else None,
    )
