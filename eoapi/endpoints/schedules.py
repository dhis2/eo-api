import os
from secrets import compare_digest
from typing import Any

from fastapi import APIRouter, Body, Header, HTTPException, Request, Response
from pydantic import BaseModel, Field, model_validator

try:
    from pygeoapi.api import FORMAT_TYPES, F_JSON
    from pygeoapi.util import url_join
except ImportError:
    F_JSON = "json"
    FORMAT_TYPES = {F_JSON: "application/json"}

    def url_join(base_url: str, *parts: str) -> str:
        segments = [base_url.rstrip("/"), *(part.strip("/") for part in parts if part)]
        return "/".join(segment for segment in segments if segment)

from eoapi.endpoints.errors import not_found
from eoapi.endpoints.processes import run_process
from eoapi.endpoints.workflows import run_workflow_by_id
from eoapi.schedules import (
    create_schedule,
    delete_schedule,
    get_schedule,
    list_schedules,
    mark_schedule_run,
    update_schedule,
)

router = APIRouter(tags=["Schedules"])


class ScheduleCreateRequest(BaseModel):
    name: str = Field(min_length=1, description="Human-readable schedule name.")
    cron: str = Field(min_length=1, description="Cron expression, e.g. 0 0 * * *")
    timezone: str = Field(default="UTC", min_length=1, description="IANA timezone for cron evaluation.")
    enabled: bool = Field(default=True, description="Whether this schedule is active.")
    processId: str | None = Field(default=None, min_length=1, description="Process ID for direct execution target.")
    inputs: dict[str, Any] | None = Field(default=None, description="Inputs passed to process execution.")
    workflowId: str | None = Field(default=None, min_length=1, description="Workflow ID as alternative target.")

    @model_validator(mode="after")
    def validate_target(self) -> "ScheduleCreateRequest":
        has_workflow = self.workflowId is not None
        has_process = self.processId is not None
        has_inputs = self.inputs is not None

        if has_workflow and (has_process or has_inputs):
            raise ValueError("Provide either workflowId or processId+inputs, not both")
        if not has_workflow and not (has_process and has_inputs):
            raise ValueError("Provide processId and inputs, or workflowId")
        return self


class ScheduleUpdateRequest(BaseModel):
    name: str | None = Field(default=None, min_length=1)
    cron: str | None = Field(default=None, min_length=1)
    timezone: str | None = Field(default=None, min_length=1)
    enabled: bool | None = None
    processId: str | None = Field(default=None, min_length=1)
    inputs: dict[str, Any] | None = None
    workflowId: str | None = Field(default=None, min_length=1)


def _base_url(request: Request) -> str:
    return str(request.base_url).rstrip("/")


def _validate_schedule_target(schedule: dict[str, Any]) -> None:
    workflow_id = schedule.get("workflowId")
    process_id = schedule.get("processId")
    inputs = schedule.get("inputs")

    has_workflow = isinstance(workflow_id, str) and bool(workflow_id.strip())
    has_process = isinstance(process_id, str) and bool(process_id.strip())
    has_inputs = isinstance(inputs, dict)

    if has_workflow and (has_process or has_inputs):
        raise HTTPException(
            status_code=400,
            detail={
                "code": "InvalidParameterValue",
                "description": "Schedule target must be either workflowId or processId+inputs",
            },
        )
    if not has_workflow and not (has_process and has_inputs):
        raise HTTPException(
            status_code=400,
            detail={
                "code": "InvalidParameterValue",
                "description": "Schedule target requires workflowId or processId+inputs",
            },
        )


def _schedule_response(request: Request, schedule: dict[str, Any]) -> dict[str, Any]:
    base = _base_url(request)
    schedule_url = url_join(base, "schedules", schedule["scheduleId"])
    links = [
        {"rel": "self", "type": FORMAT_TYPES[F_JSON], "href": schedule_url},
        {"rel": "run", "type": FORMAT_TYPES[F_JSON], "href": url_join(schedule_url, "run")},
        {"rel": "run", "type": FORMAT_TYPES[F_JSON], "href": url_join(schedule_url, "callback")},
    ]

    if schedule.get("workflowId"):
        links.append(
            {
                "rel": "process",
                "type": FORMAT_TYPES[F_JSON],
                "href": url_join(base, "workflows", schedule["workflowId"]),
            }
        )
    elif schedule.get("processId"):
        links.append(
            {
                "rel": "process",
                "type": FORMAT_TYPES[F_JSON],
                "href": url_join(base, "processes", schedule["processId"]),
            }
        )

    return {
        **schedule,
        "links": links,
    }


def execute_schedule_target(schedule_id: str, trigger: str) -> dict[str, Any]:
    schedule = get_schedule(schedule_id)
    if schedule is None:
        raise not_found("Schedule", schedule_id)

    _validate_schedule_target(schedule)

    workflow_id = schedule.get("workflowId")
    inputs_payload = schedule.get("inputs")
    process_id = schedule.get("processId")

    if workflow_id:
        workflow_result = run_workflow_by_id(workflow_id)
        job_ids = workflow_result["jobIds"]
        if not job_ids:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "InvalidParameterValue",
                    "description": "Workflow did not produce any job runs",
                },
            )
        selected_job_id = job_ids[-1]
        mark_schedule_run(schedule_id, selected_job_id)
        return {
            "scheduleId": schedule_id,
            "workflowId": workflow_id,
            "jobId": selected_job_id,
            "jobIds": job_ids,
            "status": "queued",
            "trigger": trigger,
            "execution": {
                "source": "workflow",
                "flowRunId": None,
            },
        }

    if not isinstance(process_id, str) or not isinstance(inputs_payload, dict):
        raise HTTPException(
            status_code=400,
            detail={
                "code": "InvalidParameterValue",
                "description": "Schedule target requires processId and inputs",
            },
        )

    job = run_process(process_id, inputs_payload)
    mark_schedule_run(schedule_id, job["jobId"])
    return {
        "scheduleId": schedule_id,
        "processId": process_id,
        "jobId": job["jobId"],
        "status": "queued",
        "trigger": trigger,
        "execution": {
            "source": "local",
            "flowRunId": None,
        },
    }


def _run_schedule_now(schedule_id: str, request: Request, trigger: str) -> dict[str, Any]:
    payload = execute_schedule_target(schedule_id, trigger)
    selected_job_id = payload["jobId"]
    base = _base_url(request)
    return {
        **payload,
        "links": [
            {"rel": "monitor", "type": FORMAT_TYPES[F_JSON], "href": url_join(base, "jobs", selected_job_id)},
            {"rel": "results", "type": FORMAT_TYPES[F_JSON], "href": url_join(base, "jobs", selected_job_id)},
        ],
    }


def _assert_scheduler_callback_authorized(token: str | None) -> None:
    expected = os.getenv("EOAPI_SCHEDULER_TOKEN")
    if not expected:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "ServiceUnavailable",
                "description": "Scheduler callback is not configured; set EOAPI_SCHEDULER_TOKEN",
            },
        )

    if token is None or not compare_digest(token, expected):
        raise HTTPException(
            status_code=403,
            detail={
                "code": "Forbidden",
                "description": "Invalid scheduler callback token",
            },
        )


@router.get(
    "/schedules",
    summary="List schedules",
    description="Returns all configured schedules and execution targets.",
)
def get_schedules(request: Request) -> dict[str, Any]:
    base = _base_url(request)
    schedules = [_schedule_response(request, schedule) for schedule in list_schedules()]
    return {
        "schedules": schedules,
        "links": [
            {"rel": "self", "type": FORMAT_TYPES[F_JSON], "href": url_join(base, "schedules")},
            {"rel": "root", "type": FORMAT_TYPES[F_JSON], "href": url_join(base, "/")},
        ],
    }


@router.post(
    "/schedules",
    status_code=201,
    summary="Create schedule",
    description="Creates a new schedule targeting either a process or a workflow.",
)
def post_schedule(
    request: Request,
    payload: ScheduleCreateRequest = Body(
        ...,
        openapi_examples={
            "process_target": {
                "summary": "Schedule a process",
                "value": {
                    "name": "nightly-zonal-stats",
                    "cron": "0 0 * * *",
                    "timezone": "UTC",
                    "enabled": True,
                    "processId": "raster.zonal_stats",
                    "inputs": {
                        "dataset_id": "chirps-daily",
                        "params": ["precip"],
                        "time": "2026-01-31",
                        "aoi": [30.0, -10.0, 31.0, -9.0],
                    },
                },
            },
            "workflow_target": {
                "summary": "Schedule a workflow",
                "value": {
                    "name": "nightly-workflow",
                    "cron": "0 0 * * *",
                    "timezone": "UTC",
                    "enabled": True,
                    "workflowId": "wf_123",
                },
            },
        },
    ),
) -> dict[str, Any]:
    schedule = create_schedule(
        {
            "processId": payload.processId,
            "workflowId": payload.workflowId,
            "name": payload.name,
            "cron": payload.cron,
            "timezone": payload.timezone,
            "enabled": payload.enabled,
            "inputs": payload.inputs,
        }
    )
    return _schedule_response(request, schedule)


@router.get(
    "/schedules/{scheduleId}",
    summary="Get schedule",
    description="Returns one schedule by ID.",
)
def get_schedule_by_id(scheduleId: str, request: Request) -> dict[str, Any]:
    schedule = get_schedule(scheduleId)
    if schedule is None:
        raise not_found("Schedule", scheduleId)
    return _schedule_response(request, schedule)


@router.patch(
    "/schedules/{scheduleId}",
    summary="Update schedule",
    description="Updates mutable schedule fields including target configuration.",
)
def patch_schedule(
    scheduleId: str,
    request: Request,
    payload: ScheduleUpdateRequest = Body(
        ...,
        openapi_examples={
            "disable": {"summary": "Disable schedule", "value": {"enabled": False}},
            "switch_to_workflow": {"summary": "Switch target", "value": {"workflowId": "wf_123"}},
        },
    ),
) -> dict[str, Any]:
    current = get_schedule(scheduleId)
    if current is None:
        raise not_found("Schedule", scheduleId)

    updates = payload.model_dump(exclude_unset=True)
    if updates.get("workflowId") is not None:
        updates["processId"] = None
        updates["inputs"] = None
    if updates.get("processId") is not None or updates.get("inputs") is not None:
        updates["workflowId"] = None

    candidate = {**current, **updates}
    _validate_schedule_target(candidate)

    schedule = update_schedule(scheduleId, updates)
    if schedule is None:
        raise not_found("Schedule", scheduleId)

    return _schedule_response(request, schedule)


@router.delete(
    "/schedules/{scheduleId}",
    status_code=204,
    summary="Delete schedule",
    description="Deletes a schedule by ID.",
)
def remove_schedule(scheduleId: str) -> Response:
    deleted = delete_schedule(scheduleId)
    if not deleted:
        raise not_found("Schedule", scheduleId)
    return Response(status_code=204)


@router.post(
    "/schedules/{scheduleId}/run",
    status_code=202,
    summary="Run schedule now",
    description="Triggers immediate execution for a schedule target.",
)
def run_schedule(scheduleId: str, request: Request) -> dict[str, Any]:
    return _run_schedule_now(scheduleId, request, trigger="manual")


@router.post(
    "/schedules/{scheduleId}/callback",
    status_code=202,
    summary="Scheduler callback trigger",
    description="Triggers schedule execution through token-protected callback endpoint.",
)
def callback_schedule(
    scheduleId: str,
    request: Request,
    x_scheduler_token: str | None = Header(default=None, alias="X-Scheduler-Token"),
) -> dict[str, Any]:
    _assert_scheduler_callback_authorized(x_scheduler_token)
    return _run_schedule_now(scheduleId, request, trigger="scheduler-callback")
