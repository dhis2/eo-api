from .schemas.fastapi import BuildDataValueSetRunRequest, BuildDataValueSetRunResponse
from .services.datavalueset import build_datavalueset_component

from fastapi import APIRouter

router = APIRouter()


@router.post("/components/build-datavalue-set", response_model=BuildDataValueSetRunResponse)
def run_build_datavalueset(payload: BuildDataValueSetRunRequest) -> BuildDataValueSetRunResponse:
    """Build and serialize a DHIS2 DataValueSet from records."""
    data_value_set, output_file = build_datavalueset_component(
        dataset_id=payload.dataset_id,
        period_type=payload.period_type,
        records=payload.records,
        dhis2=payload.dhis2,
    )
    return BuildDataValueSetRunResponse(
        value_count=len(data_value_set.get("dataValues", [])),
        output_file=output_file,
        data_value_set=data_value_set,
    )
