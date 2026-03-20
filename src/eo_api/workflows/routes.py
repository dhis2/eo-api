"""API routes for workflow discovery, execution, and native job access."""

from typing import Any

from fastapi import APIRouter, HTTPException, Request

from ..publications.capabilities import evaluate_publication_serving
from ..publications.schemas import PublishedResourceExposure
from ..publications.services import collection_id_for_resource, get_published_resource
from ..shared.api_errors import api_error
from .schemas import (
    WorkflowAssemblyExecuteRequest,
    WorkflowCatalogItem,
    WorkflowCatalogResponse,
    WorkflowExecuteEnvelopeRequest,
    WorkflowExecuteResponse,
    WorkflowJobCleanupResponse,
    WorkflowJobListResponse,
    WorkflowJobRecord,
    WorkflowJobStatus,
    WorkflowSchedule,
    WorkflowScheduleCreateRequest,
    WorkflowScheduleTriggerRequest,
    WorkflowScheduleTriggerResponse,
    WorkflowValidateRequest,
    WorkflowValidateResponse,
    WorkflowValidateStep,
)
from .services.definitions import list_workflow_definitions, load_workflow_definition
from .services.engine import execute_workflow, validate_workflow_steps
from .services.job_store import cleanup_jobs, delete_job, get_job, get_job_result, get_job_trace, list_jobs
from .services.schedules import create_schedule, delete_schedule, get_schedule, list_schedules, trigger_schedule
from .services.simple_mapper import normalize_simple_request

router = APIRouter()


def _workflow_publication_summary(workflow: Any) -> dict[str, Any]:
    publication = workflow.publication
    capability = evaluate_publication_serving(
        kind=publication.intent,
        exposure=publication.exposure,
        asset_format=publication.asset_format,
    )
    asset_binding = None
    if publication.asset is not None:
        asset_binding = {"from_step": publication.asset.from_step, "output": publication.asset.output}
    publication_inputs = {
        name: {"from_step": ref.from_step, "output": ref.output} for name, ref in publication.inputs.items()
    }
    return {
        "publication_publishable": publication.publishable,
        "publication_intent": str(publication.intent) if publication.publishable else None,
        "publication_exposure": str(publication.exposure) if publication.publishable else None,
        "publication_asset_format": publication.asset_format,
        "publication_asset_binding": asset_binding,
        "publication_inputs": publication_inputs,
        "serving_supported": capability.supported,
        "serving_asset_format": capability.asset_format,
        "serving_targets": list(capability.served_by),
        "serving_error": capability.error,
    }


@router.get("", response_model=WorkflowCatalogResponse)
def list_workflows() -> WorkflowCatalogResponse:
    """List all allowlisted workflow definitions."""
    try:
        definitions = list_workflow_definitions()
    except ValueError as exc:
        raise HTTPException(
            status_code=500,
            detail=api_error(
                error="workflow_catalog_unavailable",
                error_code="CATALOG_UNAVAILABLE",
                message=str(exc),
            ),
        ) from exc
    return WorkflowCatalogResponse(
        workflows=[
            WorkflowCatalogItem(
                workflow_id=definition.workflow_id,
                version=definition.version,
                step_count=len(definition.steps),
                components=[step.component for step in definition.steps],
                **_workflow_publication_summary(definition),
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
        raise HTTPException(
            status_code=404,
            detail=api_error(
                error="job_not_found",
                error_code="JOB_NOT_FOUND",
                message=f"Unknown job_id '{job_id}'",
                job_id=job_id,
            ),
        )
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
                "href": f"{str(request.base_url).rstrip('/')}/pygeoapi/collections/{collection_id}",
            }
        )
        analytics_link = next((link for link in publication.links if link.get("rel") == "analytics"), None)
        if analytics_link is not None:
            links.append(
                {
                    "rel": "analytics",
                    "href": f"{str(request.base_url).rstrip('/')}{analytics_link['href']}",
                }
            )
    return job.model_copy(update={"links": links})


@router.get("/jobs/{job_id}/result")
def get_workflow_job_result(job_id: str) -> dict[str, Any]:
    """Fetch persisted workflow results for a completed job."""
    job = get_job(job_id)
    if job is None:
        raise HTTPException(
            status_code=404,
            detail=api_error(
                error="job_not_found",
                error_code="JOB_NOT_FOUND",
                message=f"Unknown job_id '{job_id}'",
                job_id=job_id,
            ),
        )
    result = get_job_result(job_id)
    if result is None:
        raise HTTPException(
            status_code=409,
            detail=api_error(
                error="job_result_unavailable",
                error_code="JOB_RESULT_UNAVAILABLE",
                message=f"Result is not available for job '{job_id}'",
                job_id=job_id,
                status=str(job.status),
            ),
        )
    return result


@router.get("/jobs/{job_id}/trace")
def get_workflow_job_trace(job_id: str) -> dict[str, Any]:
    """Fetch persisted workflow trace for a completed or failed job."""
    job = get_job(job_id)
    if job is None:
        raise HTTPException(
            status_code=404,
            detail=api_error(
                error="job_not_found",
                error_code="JOB_NOT_FOUND",
                message=f"Unknown job_id '{job_id}'",
                job_id=job_id,
            ),
        )
    trace = get_job_trace(job_id)
    if trace is None:
        raise HTTPException(
            status_code=409,
            detail=api_error(
                error="job_trace_unavailable",
                error_code="JOB_TRACE_UNAVAILABLE",
                message=f"Trace is not available for job '{job_id}'",
                job_id=job_id,
                status=str(job.status),
            ),
        )
    return trace


@router.delete("/jobs/{job_id}")
def delete_workflow_job(job_id: str) -> dict[str, Any]:
    """Delete one workflow job and cascade run-owned derived artifacts."""
    deleted = delete_job(job_id)
    if deleted is None:
        raise HTTPException(
            status_code=404,
            detail=api_error(
                error="job_not_found",
                error_code="JOB_NOT_FOUND",
                message=f"Unknown job_id '{job_id}'",
                job_id=job_id,
            ),
        )
    return deleted


@router.post("/jobs/cleanup", response_model=WorkflowJobCleanupResponse)
def cleanup_workflow_jobs(
    dry_run: bool = True,
    keep_latest: int | None = None,
    older_than_hours: int | None = None,
) -> WorkflowJobCleanupResponse:
    """Apply retention policy to terminal jobs and derived artifacts."""
    try:
        result = cleanup_jobs(
            dry_run=dry_run,
            keep_latest=keep_latest,
            older_than_hours=older_than_hours,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail=api_error(
                error="cleanup_policy_invalid",
                error_code="CLEANUP_POLICY_INVALID",
                message=str(exc),
            ),
        ) from exc
    return WorkflowJobCleanupResponse.model_validate(result)


@router.post("/schedules", response_model=WorkflowSchedule)
def create_workflow_schedule(payload: WorkflowScheduleCreateRequest) -> WorkflowSchedule:
    """Create a recurring workflow schedule contract."""
    try:
        return create_schedule(payload)
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail=api_error(
                error="schedule_invalid",
                error_code="SCHEDULE_INVALID",
                message=str(exc),
            ),
        ) from exc


@router.get("/schedules", response_model=list[WorkflowSchedule])
def list_workflow_schedules(workflow_id: str | None = None) -> list[WorkflowSchedule]:
    """List persisted workflow schedules."""
    return list_schedules(workflow_id=workflow_id)


@router.get("/schedules/{schedule_id}", response_model=WorkflowSchedule)
def get_workflow_schedule(schedule_id: str) -> WorkflowSchedule:
    """Fetch one persisted workflow schedule."""
    schedule = get_schedule(schedule_id)
    if schedule is None:
        raise HTTPException(
            status_code=404,
            detail=api_error(
                error="schedule_not_found",
                error_code="SCHEDULE_NOT_FOUND",
                message=f"Unknown schedule_id '{schedule_id}'",
                schedule_id=schedule_id,
            ),
        )
    return schedule


@router.delete("/schedules/{schedule_id}", status_code=204)
def delete_workflow_schedule(schedule_id: str) -> None:
    """Delete one persisted workflow schedule."""
    deleted = delete_schedule(schedule_id)
    if deleted is None:
        raise HTTPException(
            status_code=404,
            detail=api_error(
                error="schedule_not_found",
                error_code="SCHEDULE_NOT_FOUND",
                message=f"Unknown schedule_id '{schedule_id}'",
                schedule_id=schedule_id,
            ),
        )


@router.post("/schedules/{schedule_id}/trigger", response_model=WorkflowScheduleTriggerResponse)
def trigger_workflow_schedule(
    schedule_id: str,
    payload: WorkflowScheduleTriggerRequest | None = None,
) -> WorkflowScheduleTriggerResponse:
    """Trigger one persisted schedule immediately."""
    try:
        trigger_response, _result = trigger_schedule(
            schedule_id=schedule_id,
            execution_time=(payload.execution_time if payload is not None else None),
        )
    except ValueError as exc:
        message = str(exc)
        error_code = "SCHEDULE_NOT_FOUND" if "Unknown schedule_id" in message else "SCHEDULE_TRIGGER_INVALID"
        status_code = 404 if error_code == "SCHEDULE_NOT_FOUND" else 422
        raise HTTPException(
            status_code=status_code,
            detail=api_error(
                error="schedule_trigger_failed" if status_code == 422 else "schedule_not_found",
                error_code=error_code,
                message=message,
                schedule_id=schedule_id,
            ),
        ) from exc
    return trigger_response


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
            publication_publishable=False,
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
        **_workflow_publication_summary(workflow),
        resolved_steps=resolved_steps,
        warnings=warnings,
        errors=errors,
    )
