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

from eoapi.endpoints.errors import not_found
from eoapi.jobs import get_job, update_job
from eoapi.orchestration.prefect import get_flow_run, prefect_enabled, prefect_state_to_job_status
from eoapi.processing.process_catalog import PROCESS_IDS, get_process_definition
from eoapi.processing.runtime import ProcessHandler, ProcessRuntime
from eoapi.processing.service import execute_skeleton_process

router = APIRouter(tags=["Processes"])


class ExecuteRequest(BaseModel):
    inputs: dict[str, Any] = Field(description="Process-specific input object.")


def _base_url(request: Request) -> str:
    return str(request.base_url).rstrip("/")


def _build_process_runtime() -> ProcessRuntime:
    handlers: list[ProcessHandler] = []
    for process_id in PROCESS_IDS:
        handlers.append(
            ProcessHandler(
                process_id=process_id,
                definition=lambda base_url, pid=process_id: get_process_definition(pid, base_url),
                execute=lambda inputs, pid=process_id: execute_skeleton_process(pid, inputs),
            )
        )
    return ProcessRuntime(handlers)


PROCESS_RUNTIME = _build_process_runtime()


def run_process(process_id: str, inputs: dict[str, Any]) -> dict[str, Any]:
    return PROCESS_RUNTIME.execute(process_id, inputs)


def _sync_prefect_job(job: dict[str, Any]) -> dict[str, Any]:
    execution = job.get("execution") or {}
    if execution.get("source") != "prefect":
        return job

    flow_run_id = execution.get("flowRunId")
    if not flow_run_id or not prefect_enabled():
        return job

    try:
        flow_run = get_flow_run(flow_run_id)
    except RuntimeError:
        return job

    state = flow_run.get("state") or {}
    mapped_status = prefect_state_to_job_status(state.get("type"))
    progress = 0
    if mapped_status == "running":
        progress = 50
    elif mapped_status in {"succeeded", "failed"}:
        progress = 100

    updated = update_job(
        job["jobId"],
        {
            "status": mapped_status,
            "progress": progress,
            "execution": {
                **execution,
                "state": state,
            },
        },
    )
    return updated or job


@router.get(
    "/processes",
    summary="List processes",
    description="Returns all registered OGC process definitions available in this API.",
)
def get_processes(request: Request) -> dict[str, Any]:
    base = _base_url(request)
    return {
        "processes": PROCESS_RUNTIME.list_summaries(base),
        "links": [
            {"rel": "self", "type": FORMAT_TYPES[F_JSON], "href": url_join(base, "processes")},
            {"rel": "root", "type": FORMAT_TYPES[F_JSON], "href": url_join(base, "/")},
        ],
    }


@router.get(
    "/processes/{processId}",
    summary="Get process definition",
    description="Returns a full OGC process definition for the supplied process ID.",
)
def get_process(processId: str, request: Request) -> dict[str, Any]:
    base = _base_url(request)
    return PROCESS_RUNTIME.get_definition(processId, base)


@router.post(
    "/processes/{processId}/execution",
    status_code=202,
    summary="Execute process",
    description="Queues process execution and returns a job reference for monitoring.",
    responses={
        202: {"description": "Execution accepted and job created."},
        404: {"description": "Process not found."},
    },
)
def execute_process(
    processId: str,
    request: Request,
    payload: ExecuteRequest = Body(
        ...,
        openapi_examples={
            "raster_zonal_stats": {
                "summary": "Zonal stats skeleton execution",
                "value": {
                    "inputs": {
                        "dataset_id": "chirps-daily",
                        "params": ["precip"],
                        "time": "2026-01-15",
                        "aoi": [30.0, -10.0, 31.0, -9.0],
                    }
                },
            },
            "raster_point_timeseries": {
                "summary": "Point timeseries skeleton execution",
                "value": {
                    "inputs": {
                        "dataset_id": "chirps-daily",
                        "params": ["precip"],
                        "time": "2026-01-16",
                        "aoi": {"bbox": [30.0, -10.0, 32.0, -8.0]},
                    }
                },
            },
            "data_temporal_aggregate": {
                "summary": "Temporal harmonization execution",
                "value": {
                    "inputs": {
                        "dataset_id": "chirps-daily",
                        "params": ["precip"],
                        "time": "2026-01-31",
                        "frequency": "P1M",
                        "aggregation": "sum"
                    }
                },
            },
        },
    ),
) -> dict[str, Any]:
    job = run_process(processId, payload.inputs)

    base = _base_url(request)
    return {
        "jobId": job["jobId"],
        "processId": processId,
        "status": "queued",
        "links": [
            {"rel": "monitor", "type": FORMAT_TYPES[F_JSON], "href": url_join(base, "jobs", job["jobId"])},
            {"rel": "results", "type": FORMAT_TYPES[F_JSON], "href": url_join(base, "jobs", job["jobId"])},
        ],
    }


@router.get(
    "/jobs/{jobId}",
    summary="Get job status",
    description="Returns execution status and outputs for a submitted process job.",
    responses={
        200: {"description": "Job status payload."},
        404: {"description": "Job not found."},
    },
)
def get_job_status(jobId: str, request: Request) -> dict[str, Any]:
    job = get_job(jobId)
    if job is None:
        raise not_found("Job", jobId)

    job = _sync_prefect_job(job)
    base = _base_url(request)
    return {
        "jobId": job["jobId"],
        "processId": job["processId"],
        "status": job["status"],
        "progress": job["progress"],
        "created": job["created"],
        "updated": job["updated"],
        "outputs": job.get("outputs", {}),
        "execution": job.get("execution"),
        "links": [
            {"rel": "self", "type": FORMAT_TYPES[F_JSON], "href": url_join(base, "jobs", jobId)},
        ],
    }
