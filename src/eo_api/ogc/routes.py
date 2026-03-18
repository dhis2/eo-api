"""Thin OGC API adapter routes over the native workflow engine."""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Header, HTTPException, Request, Response

from ..publications.schemas import PublishedResourceExposure
from ..publications.services import collection_id_for_resource, get_published_resource
from ..workflows.schemas import WorkflowExecuteEnvelopeRequest, WorkflowJobStatus
from ..workflows.services.definitions import load_workflow_definition
from ..workflows.services.engine import execute_workflow
from ..workflows.services.job_store import get_job, get_job_result, initialize_job, list_jobs
from ..workflows.services.simple_mapper import normalize_simple_request

router = APIRouter()

_PROCESS_ID = "generic-dhis2-workflow"
_PROCESS_TITLE = "Generic DHIS2 workflow"


@router.get("/processes")
def list_processes(request: Request) -> dict[str, Any]:
    """List exposed OGC processes."""
    return {
        "processes": [
            {
                "id": _PROCESS_ID,
                "title": _PROCESS_TITLE,
                "description": "Execute the generic DHIS2 EO workflow and persist a native job record.",
                "jobControlOptions": ["sync-execute", "async-execute"],
                "outputTransmission": ["value", "reference"],
                "links": [
                    {
                        "rel": "self",
                        "type": "application/json",
                        "href": str(request.url_for("describe_ogc_process", process_id=_PROCESS_ID)),
                    }
                ],
            }
        ]
    }


@router.get("/processes/{process_id}", name="describe_ogc_process")
def describe_process(process_id: str, request: Request) -> dict[str, Any]:
    """Describe the single exposed generic workflow process."""
    _require_process(process_id)
    return {
        "id": _PROCESS_ID,
        "title": _PROCESS_TITLE,
        "description": "OGC-facing adapter over the reusable native workflow engine.",
        "jobControlOptions": ["sync-execute", "async-execute"],
        "outputTransmission": ["value", "reference"],
        "links": [
            {
                "rel": "execute",
                "type": "application/json",
                "href": str(request.url_for("execute_ogc_process", process_id=_PROCESS_ID)),
            }
        ],
    }


@router.post("/processes/{process_id}/execution", name="execute_ogc_process")
def execute_process(
    process_id: str,
    payload: WorkflowExecuteEnvelopeRequest,
    request: Request,
    response: Response,
    background_tasks: BackgroundTasks,
    prefer: str | None = Header(default=None),
) -> dict[str, Any]:
    """Execute the generic workflow synchronously or submit it asynchronously."""
    _require_process(process_id)
    normalized, _warnings = normalize_simple_request(payload.request)

    if prefer is not None and "respond-async" in prefer.lower():
        job_id = str(uuid.uuid4())
        workflow = load_workflow_definition(payload.request.workflow_id)
        initialize_job(
            job_id=job_id,
            request=normalized,
            request_payload=payload.request.model_dump(exclude_none=True),
            workflow=workflow,
            workflow_definition_source="catalog",
            workflow_id=payload.request.workflow_id,
            workflow_version=workflow.version,
            status=WorkflowJobStatus.ACCEPTED,
            process_id=_PROCESS_ID,
        )
        background_tasks.add_task(
            _run_async_workflow_job,
            job_id,
            normalized,
            payload.request.workflow_id,
            payload.request.model_dump(exclude_none=True),
            payload.request.include_component_run_details,
        )
        job_url = str(request.url_for("get_ogc_job", job_id=job_id))
        results_url = str(request.url_for("get_ogc_job_results", job_id=job_id))
        response.status_code = 202
        response.headers["Location"] = job_url
        return {
            "jobID": job_id,
            "status": WorkflowJobStatus.ACCEPTED,
            "location": job_url,
            "jobUrl": job_url,
            "resultsUrl": results_url,
        }

    result = execute_workflow(
        normalized,
        workflow_id=payload.request.workflow_id,
        request_params=payload.request.model_dump(exclude_none=True),
        include_component_run_details=payload.request.include_component_run_details,
        workflow_definition_source="catalog",
    )
    job_url = str(request.url_for("get_ogc_job", job_id=result.run_id))
    results_url = str(request.url_for("get_ogc_job_results", job_id=result.run_id))
    publication = get_published_resource(f"workflow-output-{result.run_id}")
    links: list[dict[str, Any]] = [
        {"rel": "monitor", "type": "application/json", "href": job_url},
        {"rel": "results", "type": "application/json", "href": results_url},
    ]
    if publication is not None and publication.exposure == PublishedResourceExposure.OGC:
        collection_id = collection_id_for_resource(publication)
        links.append(
            {
                "rel": "collection",
                "type": "application/json",
                "href": _collection_href(request, collection_id),
            }
        )
    return {
        "jobID": result.run_id,
        "processID": _PROCESS_ID,
        "status": WorkflowJobStatus.SUCCESSFUL,
        "outputs": result.model_dump(mode="json"),
        "links": links,
    }


@router.get("/jobs")
def list_ogc_jobs(process_id: str | None = None) -> dict[str, Any]:
    """List OGC-visible jobs backed by the native job store."""
    jobs = list_jobs(process_id=process_id, status=None)
    return {"jobs": [job.model_dump(mode="json") for job in jobs]}


@router.get("/jobs/{job_id}", name="get_ogc_job")
def get_ogc_job(job_id: str, request: Request) -> dict[str, Any]:
    """Fetch one OGC job view from the native job store."""
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Unknown job_id '{job_id}'")
    publication = get_published_resource(f"workflow-output-{job.job_id}")
    links: list[dict[str, Any]] = [
        {
            "rel": "self",
            "type": "application/json",
            "href": str(request.url_for("get_ogc_job", job_id=job.job_id)),
        },
        {
            "rel": "results",
            "type": "application/json",
            "href": str(request.url_for("get_ogc_job_results", job_id=job.job_id)),
        },
    ]
    if publication is not None and publication.exposure == PublishedResourceExposure.OGC:
        links.append(
            {
                "rel": "collection",
                "type": "application/json",
                "href": _collection_href(request, collection_id_for_resource(publication)),
            }
        )
    return {
        "jobID": job.job_id,
        "processID": job.process_id,
        "status": job.status,
        "created": job.created_at,
        "updated": job.updated_at,
        "links": links,
    }


@router.get("/jobs/{job_id}/results", name="get_ogc_job_results")
def get_ogc_job_results(job_id: str) -> dict[str, Any]:
    """Return persisted results for a completed OGC job."""
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Unknown job_id '{job_id}'")
    result = get_job_result(job_id)
    if result is None:
        raise HTTPException(status_code=409, detail={"jobID": job_id, "status": job.status})
    return result


def _require_process(process_id: str) -> None:
    if process_id != _PROCESS_ID:
        raise HTTPException(status_code=404, detail=f"Unknown process_id '{process_id}'")


def _run_async_workflow_job(
    job_id: str,
    normalized_request: Any,
    workflow_id: str,
    request_params: dict[str, Any],
    include_component_run_details: bool,
) -> None:
    try:
        execute_workflow(
            normalized_request,
            workflow_id=workflow_id,
            request_params=request_params,
            include_component_run_details=include_component_run_details,
            run_id=job_id,
        )
    except HTTPException:
        return


def _collection_href(request: Request, collection_id: str) -> str:
    return str(request.base_url).rstrip("/") + f"/ogcapi/collections/{collection_id}"
