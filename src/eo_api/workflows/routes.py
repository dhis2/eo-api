"""API routes for workflow discovery and execution."""

from fastapi import APIRouter, HTTPException

from .schemas import (
    WorkflowAssemblyExecuteRequest,
    WorkflowCatalogItem,
    WorkflowCatalogResponse,
    WorkflowExecuteEnvelopeRequest,
    WorkflowExecuteResponse,
    WorkflowValidateRequest,
    WorkflowValidateResponse,
    WorkflowValidateStep,
)
from .services.definitions import list_workflow_definitions, load_workflow_definition
from .services.engine import execute_workflow, validate_workflow_steps
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
def run_dhis2_datavalue_set_workflow(payload: WorkflowExecuteEnvelopeRequest) -> WorkflowExecuteResponse:
    """Run workflow from a single flat request payload."""
    request, _warnings = normalize_simple_request(payload.request)
    return execute_workflow(
        request,
        workflow_id=payload.request.workflow_id,
        request_params=payload.request.model_dump(),
        include_component_run_details=payload.request.include_component_run_details,
    )


@router.post("/execute", response_model=WorkflowExecuteResponse)
def run_inline_assembled_workflow(payload: WorkflowAssemblyExecuteRequest) -> WorkflowExecuteResponse:
    """Run an inline assembled workflow definition from one flat request payload."""
    request, _warnings = normalize_simple_request(payload.request)
    return execute_workflow(
        request,
        workflow_id=payload.workflow.workflow_id,
        workflow_definition=payload.workflow,
        request_params=payload.request.model_dump(exclude_none=True),
        include_component_run_details=payload.request.include_component_run_details,
    )


@router.post("/validate", response_model=WorkflowValidateResponse)
def validate_workflow_assembly(payload: WorkflowValidateRequest) -> WorkflowValidateResponse:
    """Validate workflow assembly without executing any component."""
    warnings: list[str] = []
    errors: list[str] = []

    try:
        if payload.workflow is not None:
            workflow = payload.workflow
        else:
            workflow = load_workflow_definition(payload.workflow_id or "")
    except ValueError as exc:
        return WorkflowValidateResponse(
            valid=False,
            workflow_id=payload.workflow_id or "unknown",
            workflow_version=0,
            step_count=0,
            components=[],
            warnings=warnings,
            errors=[str(exc)],
        )

    request_params: dict[str, object] = {}
    if payload.request is not None:
        _request, map_warnings = normalize_simple_request(payload.request)
        warnings.extend(map_warnings)
        request_params = payload.request.model_dump(exclude_none=True)

    try:
        resolved_steps = [
            WorkflowValidateStep.model_validate(step)
            for step in validate_workflow_steps(workflow=workflow, request_params=request_params)
        ]
    except ValueError as exc:
        errors.append(str(exc))
        resolved_steps = []

    return WorkflowValidateResponse(
        valid=not errors,
        workflow_id=workflow.workflow_id,
        workflow_version=workflow.version,
        step_count=len(workflow.steps),
        components=[step.component for step in workflow.steps],
        resolved_steps=resolved_steps,
        warnings=warnings,
        errors=errors,
    )
