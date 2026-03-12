from ..data_registry.routes import require_dataset
from ..features.services.features import feature_source_component
from .services.spatial import spatial_aggregation_component
from .schemas.fastapi import SpatialAggregationRunRequest, SpatialAggregationRunResponse

from fastapi import APIRouter

router = APIRouter()


@router.post("/run", response_model=SpatialAggregationRunResponse)
def run_spatial_aggregation(payload: SpatialAggregationRunRequest) -> SpatialAggregationRunResponse:
    """Aggregate a dataset spatially to features."""
    dataset = require_dataset(payload.dataset_id)
    features, bbox = feature_source_component(payload.feature_source)
    records = spatial_aggregation_component(
        dataset=dataset,
        start=payload.start,
        end=payload.end,
        bbox=payload.bbox or bbox,
        features=features,
        method=payload.method,
        feature_id_property=payload.feature_id_property,
    )
    return SpatialAggregationRunResponse(
        dataset_id=payload.dataset_id,
        record_count=len(records),
        preview=records[: payload.max_preview_rows],
    )