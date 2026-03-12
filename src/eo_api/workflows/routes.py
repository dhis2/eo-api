"""API routes for workflow discovery and execution."""

from fastapi import APIRouter, HTTPException

from .schemas import WorkflowCatalogItem, WorkflowCatalogResponse, WorkflowExecuteResponse, WorkflowRequest
from .services.definitions import list_workflow_definitions
from .services.engine import execute_workflow
from .services.simple_mapper import normalize_simple_request

router = APIRouter()


@router.get("", response_model=WorkflowCatalogResponse)
def list_workflows() -> WorkflowCatalogResponse:
    """List all allowlisted workflow definitions."""
    try:
        definitions = list_workflow_definitions()
    except ValueError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return WorkflowCatalogResponse(
        workflows=[
            WorkflowCatalogItem(
                workflow_id=definition.workflow_id,
                version=definition.version,
                step_count=len(definition.steps),
                components=[step.component for step in definition.steps],
            )
            for definition in definitions
        ]
    )


@router.post("/dhis2-datavalue-set", response_model=WorkflowExecuteResponse)
def run_dhis2_datavalue_set_workflow(payload: WorkflowRequest) -> WorkflowExecuteResponse:
    """Run workflow from a single flat request payload."""
    request, _warnings = normalize_simple_request(payload)
    return execute_workflow(
        request,
        workflow_id=payload.workflow_id,
        request_params=payload.model_dump(),
        include_component_run_details=payload.include_component_run_details,
    )
