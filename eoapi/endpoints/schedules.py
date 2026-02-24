import os
from secrets import compare_digest
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request, Response
from pydantic import BaseModel, Field, model_validator

from pygeoapi.api import FORMAT_TYPES, F_JSON
from pygeoapi.util import url_join

from eoapi.endpoints.errors import not_found
from eoapi.endpoints.processes import AGGREGATE_PROCESS_ID, AggregateImportInputs, run_aggregate_import
from eoapi.endpoints.workflows import run_workflow_by_id
from eoapi.jobs import create_pending_job, update_job
from eoapi.orchestration.prefect import prefect_enabled, submit_aggregate_import_run
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
    name: str = Field(min_length=1)
    cron: str = Field(min_length=1, description="Cron expression, e.g. 0 0 * * *")
    timezone: str = Field(default="UTC", min_length=1)
    enabled: bool = True
    inputs: AggregateImportInputs | None = None
    workflowId: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def validate_target(self) -> "ScheduleCreateRequest":
        if self.inputs is None and self.workflowId is None:
            raise ValueError("Either 'inputs' or 'workflowId' is required")
        if self.inputs is not None and self.workflowId is not None:
            raise ValueError("Provide only one of 'inputs' or 'workflowId'")
        return self


class ScheduleUpdateRequest(BaseModel):
    name: str | None = Field(default=None, min_length=1)
    cron: str | None = Field(default=None, min_length=1)
    timezone: str | None = Field(default=None, min_length=1)
    enabled: bool | None = None
    inputs: AggregateImportInputs | None = None
    workflowId: str | None = Field(default=None, min_length=1)


def _base_url(request: Request) -> str:
    return str(request.base_url).rstrip("/")


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
    else:
        links.append(
            {
                "rel": "process",
                "type": FORMAT_TYPES[F_JSON],
                "href": url_join(base, "processes", AGGREGATE_PROCESS_ID),
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

    workflow_id = schedule.get("workflowId")
    inputs_payload = schedule.get("inputs")

    execution_source = "local"
    flow_run_id: str | None = None
    job_ids: list[str] = []

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

    if inputs_payload is None:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "InvalidParameterValue",
                "description": "Schedule target is missing; provide inputs or workflowId",
            },
        )

    inputs = AggregateImportInputs.model_validate(inputs_payload)

    if prefect_enabled():
        pending_job = create_pending_job(
            AGGREGATE_PROCESS_ID,
            inputs.model_dump(),
            source="prefect",
        )
        try:
            flow_run = submit_aggregate_import_run(
                schedule_id=schedule_id,
                payload_inputs=inputs.model_dump(mode="json"),
                trigger=trigger,
                eoapi_job_id=pending_job["jobId"],
            )
            flow_run_id = flow_run.get("id")
            update_job(
                pending_job["jobId"],
                {
                    "execution": {
                        "source": "prefect",
                        "flowRunId": flow_run_id,
                    },
                    "status": "queued",
                    "progress": 0,
                },
            )
            job = pending_job
            execution_source = "prefect"
        except RuntimeError:
            # fall back to local execution to keep schedule runs operational in case Prefect is unavailable
            job = run_aggregate_import(inputs)
            update_job(
                pending_job["jobId"],
                {
                    "status": "failed",
                    "progress": 100,
                },
            )
    else:
        job = run_aggregate_import(inputs)

    mark_schedule_run(schedule_id, job["jobId"])
    return {
        "scheduleId": schedule_id,
        "jobId": job["jobId"],
        "status": "queued",
        "trigger": trigger,
        "execution": {
            "source": execution_source,
            "flowRunId": flow_run_id,
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
            {
                "rel": "results",
                "type": FORMAT_TYPES[F_JSON],
                "href": f"{url_join(base, 'features', 'aggregated-results', 'items')}?jobId={selected_job_id}",
            },
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


@router.get("/schedules")
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


@router.post("/schedules", status_code=201)
def post_schedule(payload: ScheduleCreateRequest, request: Request) -> dict[str, Any]:
    process_id = "workflow" if payload.workflowId else AGGREGATE_PROCESS_ID
    schedule = create_schedule(
        {
            "processId": process_id,
            "workflowId": payload.workflowId,
            "name": payload.name,
            "cron": payload.cron,
            "timezone": payload.timezone,
            "enabled": payload.enabled,
            "inputs": payload.inputs.model_dump() if payload.inputs else None,
        }
    )
    return _schedule_response(request, schedule)


@router.get("/schedules/{scheduleId}")
def get_schedule_by_id(scheduleId: str, request: Request) -> dict[str, Any]:
    schedule = get_schedule(scheduleId)
    if schedule is None:
        raise not_found("Schedule", scheduleId)
    return _schedule_response(request, schedule)


@router.patch("/schedules/{scheduleId}")
def patch_schedule(scheduleId: str, payload: ScheduleUpdateRequest, request: Request) -> dict[str, Any]:
    updates = payload.model_dump(exclude_unset=True)
    if "inputs" in updates and updates["inputs"] is not None:
        updates["inputs"] = updates["inputs"].model_dump()

    if updates.get("workflowId") is not None:
        updates["processId"] = "workflow"
        if "inputs" not in updates:
            updates["inputs"] = None
    elif updates.get("inputs") is not None:
        updates["processId"] = AGGREGATE_PROCESS_ID
        if "workflowId" not in updates:
            updates["workflowId"] = None

    schedule = update_schedule(scheduleId, updates)
    if schedule is None:
        raise not_found("Schedule", scheduleId)

    return _schedule_response(request, schedule)


@router.delete("/schedules/{scheduleId}", status_code=204)
def remove_schedule(scheduleId: str) -> Response:
    deleted = delete_schedule(scheduleId)
    if not deleted:
        raise not_found("Schedule", scheduleId)
    return Response(status_code=204)


@router.post("/schedules/{scheduleId}/run", status_code=202)
def run_schedule(scheduleId: str, request: Request) -> dict[str, Any]:
    return _run_schedule_now(scheduleId, request, trigger="manual")


@router.post("/schedules/{scheduleId}/callback", status_code=202)
def callback_schedule(
    scheduleId: str,
    request: Request,
    x_scheduler_token: str | None = Header(default=None, alias="X-Scheduler-Token"),
) -> dict[str, Any]:
    _assert_scheduler_callback_authorized(x_scheduler_token)
    return _run_schedule_now(scheduleId, request, trigger="scheduler-callback")
