"""API routes for workflow discovery, execution, and native job access."""

from typing import Any

from fastapi import APIRouter, HTTPException, Request

from ..publications.schemas import PublishedResourceExposure
from ..publications.services import collection_id_for_resource, get_published_resource
from .schemas import (
    WorkflowAssemblyExecuteRequest,
    WorkflowCatalogItem,
    WorkflowCatalogResponse,
    WorkflowExecuteEnvelopeRequest,
    WorkflowExecuteResponse,
    WorkflowJobListResponse,
    WorkflowJobRecord,
    WorkflowJobStatus,
    WorkflowValidateRequest,
    WorkflowValidateResponse,
    WorkflowValidateStep,
)
from .services.definitions import list_workflow_definitions, load_workflow_definition
from .services.engine import execute_workflow, validate_workflow_steps
from .services.job_store import delete_job, get_job, get_job_result, get_job_trace, list_jobs
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
                publication_publishable=definition.publication.publishable,
                publication_intent=(str(definition.publication.intent) if definition.publication.publishable else None),
                publication_exposure=(
                    str(definition.publication.exposure) if definition.publication.publishable else None
                ),
                step_count=len(definition.steps),
                components=[step.component for step in definition.steps],
            )
            for definition in definitions
        ]
    )


@router.get("/jobs", response_model=WorkflowJobListResponse)
def list_workflow_jobs(
    process_id: str | None = None,
    status: WorkflowJobStatus | None = None,
) -> WorkflowJobListResponse:
    """List persisted workflow jobs."""
    return WorkflowJobListResponse(jobs=list_jobs(process_id=process_id, status=status))


@router.get("/jobs/{job_id}", response_model=WorkflowJobRecord)
def get_workflow_job(job_id: str, request: Request) -> WorkflowJobRecord:
    """Fetch one persisted workflow job."""
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Unknown job_id '{job_id}'")
    links: list[dict[str, str]] = [
        {"rel": "self", "href": str(request.url_for("get_workflow_job", job_id=job_id))},
        {"rel": "result", "href": str(request.url_for("get_workflow_job_result", job_id=job_id))},
        {"rel": "trace", "href": str(request.url_for("get_workflow_job_trace", job_id=job_id))},
    ]
    publication = get_published_resource(f"workflow-output-{job_id}")
    if publication is not None and publication.exposure == PublishedResourceExposure.OGC:
        collection_id = collection_id_for_resource(publication)
        links.append(
            {
                "rel": "collection",
                "href": f"{str(request.base_url).rstrip('/')}/ogcapi/collections/{collection_id}",
            }
        )
    return job.model_copy(update={"links": links})


@router.get("/jobs/{job_id}/result")
def get_workflow_job_result(job_id: str) -> dict[str, Any]:
    """Fetch persisted workflow results for a completed job."""
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Unknown job_id '{job_id}'")
    result = get_job_result(job_id)
    if result is None:
        raise HTTPException(status_code=409, detail={"job_id": job_id, "status": job.status})
    return result


@router.get("/jobs/{job_id}/trace")
def get_workflow_job_trace(job_id: str) -> dict[str, Any]:
    """Fetch persisted workflow trace for a completed or failed job."""
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Unknown job_id '{job_id}'")
    trace = get_job_trace(job_id)
    if trace is None:
        raise HTTPException(status_code=409, detail={"job_id": job_id, "status": job.status})
    return trace


@router.delete("/jobs/{job_id}")
def delete_workflow_job(job_id: str) -> dict[str, Any]:
    """Delete one workflow job and cascade run-owned derived artifacts."""
    deleted = delete_job(job_id)
    if deleted is None:
        raise HTTPException(status_code=404, detail=f"Unknown job_id '{job_id}'")
    return deleted


@router.post("/dhis2-datavalue-set", response_model=WorkflowExecuteResponse)
def run_dhis2_datavalue_set_workflow(payload: WorkflowExecuteEnvelopeRequest) -> WorkflowExecuteResponse:
    """Run workflow from a single flat request payload."""
    request, _warnings = normalize_simple_request(payload.request)
    return execute_workflow(
        request,
        workflow_id=payload.request.workflow_id,
        request_params=payload.request.model_dump(),
        include_component_run_details=payload.request.include_component_run_details,
        workflow_definition_source="catalog",
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
        workflow_definition_source="inline",
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
