from typing import Any

from fastapi import APIRouter, Body, Request
from pydantic import BaseModel, Field

from pygeoapi.api import FORMAT_TYPES, F_JSON
from pygeoapi.util import url_join

from eoapi.endpoints.errors import not_found
from eoapi.jobs import get_job, list_jobs, update_job
from eoapi.orchestration.prefect import get_flow_run, prefect_enabled, prefect_state_to_job_status
from eoapi.processing.pipeline import execute_dhis2_pipeline, get_pipeline_definition
from eoapi.processing.process_catalog import DHIS2_PIPELINE_PROCESS_ID, PROCESS_IDS, get_process_definition
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
        if process_id == DHIS2_PIPELINE_PROCESS_ID:
            handlers.append(
                ProcessHandler(
                    process_id=process_id,
                    definition=get_pipeline_definition,
                    execute=execute_dhis2_pipeline,
                )
            )
            continue

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
    status_code=200,
    summary="Execute process",
    description="Executes a process synchronously and returns the completed job with outputs inline.",
    responses={
        200: {"description": "Execution complete. Job record and outputs returned inline."},
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
                "summary": "Zonal stats execution",
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
                "summary": "Point timeseries execution",
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
                        "aggregation": "sum",
                    }
                },
            },
            "dhis2_pipeline": {
                "summary": "DHIS2 org unit GeoJSON to dataValueSet",
                "value": {
                    "inputs": {
                        "features": {
                            "type": "FeatureCollection",
                            "features": [
                                {
                                    "type": "Feature",
                                    "id": "O6uvpzGd5pu",
                                    "geometry": {
                                        "type": "Polygon",
                                        "coordinates": [[[30.0, -10.0], [31.0, -10.0], [31.0, -9.0], [30.0, -9.0], [30.0, -10.0]]],
                                    },
                                    "properties": {"name": "Bo"},
                                }
                            ],
                        },
                        "dataset_id": "chirps-daily",
                        "params": ["precip"],
                        "time": "2026-01-31",
                        "aggregation": "mean",
                        "data_element": "abc123def45",
                    }
                },
            },
        },
    ),
) -> dict[str, Any]:
    job = run_process(processId, payload.inputs)

    base = _base_url(request)
    job_id = job["jobId"]
    return {
        **job,
        "links": [
            {"rel": "self", "type": FORMAT_TYPES[F_JSON], "href": url_join(base, "jobs", job_id)},
        ],
    }


@router.get(
    "/jobs",
    summary="List jobs",
    description="Returns all recorded jobs with their current status.",
)
def get_jobs(request: Request) -> dict[str, Any]:
    base = _base_url(request)
    jobs = list_jobs()
    return {
        "jobs": [
            {
                "jobId": job["jobId"],
                "processId": job["processId"],
                "status": job["status"],
                "progress": job["progress"],
                "created": job["created"],
                "updated": job["updated"],
                "links": [
                    {"rel": "self", "type": FORMAT_TYPES[F_JSON], "href": url_join(base, "jobs", job["jobId"])},
                ],
            }
            for job in jobs
        ],
        "links": [
            {"rel": "self", "type": FORMAT_TYPES[F_JSON], "href": url_join(base, "jobs")},
            {"rel": "root", "type": FORMAT_TYPES[F_JSON], "href": url_join(base, "/")},
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
            {"rel": "collection", "type": FORMAT_TYPES[F_JSON], "href": url_join(base, "jobs")},
        ],
    }
