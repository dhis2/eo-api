from typing import Any

from fastapi import APIRouter, Body, Request
from pydantic import BaseModel, Field

try:
    from pygeoapi.api import FORMAT_TYPES, F_JSON
    from pygeoapi.util import url_join
except ImportError:
    F_JSON = "json"
    FORMAT_TYPES = {F_JSON: "application/json"}

    def url_join(base_url: str, *parts: str) -> str:
        segments = [base_url.rstrip("/"), *(part.strip("/") for part in parts if part)]
        return "/".join(segment for segment in segments if segment)

from eoapi.endpoints.errors import invalid_parameter, not_found
from eoapi.endpoints.processes import run_process
from eoapi.workflows import (
    create_workflow,
    delete_workflow,
    get_workflow,
    list_workflows,
    mark_workflow_run,
    update_workflow,
)

router = APIRouter(tags=["Workflows"])


class WorkflowStep(BaseModel):
    name: str | None = Field(default=None, min_length=1, description="Optional step label.")
    processId: str = Field(min_length=1, description="Process ID to execute in this step.")
    payload: dict[str, Any] = Field(description="Execution payload containing `inputs`.")


class WorkflowCreateRequest(BaseModel):
    name: str = Field(min_length=1, description="Workflow name.")
    steps: list[WorkflowStep] = Field(min_length=1, description="Ordered list of steps.")


class WorkflowUpdateRequest(BaseModel):
    name: str | None = Field(default=None, min_length=1)
    steps: list[WorkflowStep] | None = None


def _base_url(request: Request) -> str:
    return str(request.base_url).rstrip("/")


def _workflow_response(request: Request, workflow: dict[str, Any]) -> dict[str, Any]:
    base = _base_url(request)
    workflow_url = url_join(base, "workflows", workflow["workflowId"])
    return {
        **workflow,
        "links": [
            {"rel": "self", "type": FORMAT_TYPES[F_JSON], "href": workflow_url},
            {"rel": "run", "type": FORMAT_TYPES[F_JSON], "href": url_join(workflow_url, "run")},
        ],
    }


def run_workflow_by_id(workflow_id: str) -> dict[str, Any]:
    workflow = get_workflow(workflow_id)
    if workflow is None:
        raise not_found("Workflow", workflow_id)

    steps = workflow.get("steps") or []
    if not steps:
        raise invalid_parameter("Workflow has no steps")

    job_ids: list[str] = []
    step_results: list[dict[str, Any]] = []
    for index, step in enumerate(steps):
        process_id = step.get("processId")
        payload = step.get("payload") or {}
        if process_id is None:
            raise invalid_parameter(f"Workflow step {index + 1} is missing processId")

        inputs = payload.get("inputs")
        if not isinstance(inputs, dict):
            raise invalid_parameter(f"Workflow step {index + 1} payload must include object field 'inputs'")

        job = run_process(process_id, inputs)
        job_ids.append(job["jobId"])
        step_results.append(
            {
                "step": step.get("name") or f"step-{index + 1}",
                "processId": process_id,
                "jobId": job["jobId"],
                "status": job["status"],
            }
        )

    mark_workflow_run(workflow_id, job_ids)
    return {
        "workflowId": workflow_id,
        "status": "queued",
        "jobIds": job_ids,
        "steps": step_results,
    }


@router.get(
    "/workflows",
    summary="List workflows",
    description="Returns all workflow definitions.",
)
def get_workflows(request: Request) -> dict[str, Any]:
    base = _base_url(request)
    workflows = [_workflow_response(request, workflow) for workflow in list_workflows()]
    return {
        "workflows": workflows,
        "links": [
            {"rel": "self", "type": FORMAT_TYPES[F_JSON], "href": url_join(base, "workflows")},
            {"rel": "root", "type": FORMAT_TYPES[F_JSON], "href": url_join(base, "/")},
        ],
    }


@router.post(
    "/workflows",
    status_code=201,
    summary="Create workflow",
    description="Creates a workflow composed of one or more process execution steps.",
)
def post_workflow(
    request: Request,
    payload: WorkflowCreateRequest = Body(
        ...,
        openapi_examples={
            "two_step": {
                "summary": "Two-step climate workflow",
                "value": {
                    "name": "climate-workflow",
                    "steps": [
                        {
                            "name": "zonal",
                            "processId": "raster.zonal_stats",
                            "payload": {
                                "inputs": {
                                    "dataset_id": "chirps-daily",
                                    "params": ["precip"],
                                    "time": "2026-01-31",
                                    "aoi": [30.0, -10.0, 31.0, -9.0],
                                }
                            },
                        },
                        {
                            "name": "timeseries",
                            "processId": "raster.point_timeseries",
                            "payload": {
                                "inputs": {
                                    "dataset_id": "chirps-daily",
                                    "params": ["precip"],
                                    "time": "2026-01-31",
                                    "aoi": {"bbox": [30.0, -10.0, 32.0, -8.0]},
                                }
                            },
                        },
                    ],
                },
            }
        },
    ),
) -> dict[str, Any]:
    workflow = create_workflow(
        {
            "name": payload.name,
            "steps": [step.model_dump() for step in payload.steps],
        }
    )
    return _workflow_response(request, workflow)


@router.get(
    "/workflows/{workflowId}",
    summary="Get workflow",
    description="Returns one workflow definition by ID.",
)
def get_workflow_by_id(workflowId: str, request: Request) -> dict[str, Any]:
    workflow = get_workflow(workflowId)
    if workflow is None:
        raise not_found("Workflow", workflowId)
    return _workflow_response(request, workflow)


@router.patch(
    "/workflows/{workflowId}",
    summary="Update workflow",
    description="Updates workflow name and/or steps.",
)
def patch_workflow(
    workflowId: str,
    request: Request,
    payload: WorkflowUpdateRequest = Body(
        ...,
        openapi_examples={
            "rename": {"summary": "Rename workflow", "value": {"name": "updated-workflow-name"}},
        },
    ),
) -> dict[str, Any]:
    updates = payload.model_dump(exclude_unset=True)
    if "steps" in updates and updates["steps"] is not None:
        updates["steps"] = [WorkflowStep.model_validate(step).model_dump() for step in updates["steps"]]

    workflow = update_workflow(workflowId, updates)
    if workflow is None:
        raise not_found("Workflow", workflowId)

    return _workflow_response(request, workflow)


@router.delete(
    "/workflows/{workflowId}",
    status_code=204,
    summary="Delete workflow",
    description="Deletes a workflow by ID.",
)
def remove_workflow(workflowId: str) -> None:
    deleted = delete_workflow(workflowId)
    if not deleted:
        raise not_found("Workflow", workflowId)


@router.post(
    "/workflows/{workflowId}/run",
    status_code=202,
    summary="Run workflow",
    description="Runs all workflow steps in order and returns resulting job IDs.",
)
def run_workflow(workflowId: str, request: Request) -> dict[str, Any]:
    result = run_workflow_by_id(workflowId)
    base = _base_url(request)
    last_job_id = result["jobIds"][-1] if result["jobIds"] else None

    links = []
    if last_job_id:
        links.append({"rel": "monitor", "type": FORMAT_TYPES[F_JSON], "href": url_join(base, "jobs", last_job_id)})

    return {
        **result,
        "links": links,
    }
