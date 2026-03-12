from ..data_registry.routes import require_dataset
from .schemas.fastapi import TemporalAggregationRunRequest, TemporalAggregationRunResponse
from .services.temporal import temporal_aggregation_component

from fastapi import APIRouter

router = APIRouter()


@router.post("/run", response_model=TemporalAggregationRunResponse)
def run_temporal_aggregation(payload: TemporalAggregationRunRequest) -> TemporalAggregationRunResponse:
    """Aggregate a dataset temporally."""
    dataset = require_dataset(payload.dataset_id)
    ds = temporal_aggregation_component(
        dataset=dataset,
        start=payload.start,
        end=payload.end,
        bbox=payload.bbox,
        target_period_type=payload.target_period_type,
        method=payload.method,
    )
    return TemporalAggregationRunResponse(
        dataset_id=payload.dataset_id,
        sizes={str(k): int(v) for k, v in ds.sizes.items()},
        dims=[str(d) for d in ds.dims],
    )