"""Routes for derived processing operations."""

from typing import Any

from fastapi import APIRouter, Body, Header, HTTPException, Response

from open_climate_service.data_registry.services import processes as process_registry
from open_climate_service.jobs.service import get_job_service
from open_climate_service.processing import services as processing_services
from open_climate_service.processing.schemas import (
    ProcessDetail,
    ProcessField,
    ProcessLink,
    ProcessListResponse,
    ProcessSummary,
)

router = APIRouter()


def _prefer_respond_async(prefer: str | None) -> bool:
    """Return True when Prefer contains a respond-async directive."""
    if prefer is None:
        return False
    directives = [item.strip().split(";", 1)[0].strip().lower() for item in prefer.split(",")]
    return "respond-async" in directives


def _process_links(process_id: str) -> list[ProcessLink]:
    return [
        ProcessLink(href=f"/processes/{process_id}", rel="self", title="Process detail"),
        ProcessLink(href=f"/processes/{process_id}/execution", rel="execute", title="Execute process"),
    ]


def _catalog_links() -> list[ProcessLink]:
    return [ProcessLink(href="/processes", rel="self", title="Processes")]


def _field_map(raw: object) -> dict[str, ProcessField]:
    if not isinstance(raw, dict):
        return {}
    result: dict[str, ProcessField] = {}
    for key, value in raw.items():
        if not isinstance(key, str):
            continue
        if isinstance(value, dict):
            raw_enum = value.get("enum")
            result[key] = ProcessField(
                type=value.get("type") if isinstance(value.get("type"), str) else None,
                required=value.get("required") if isinstance(value.get("required"), bool) else None,
                description=value.get("description") if isinstance(value.get("description"), str) else None,
                enum=[item for item in raw_enum if isinstance(item, str)] if isinstance(raw_enum, list) else None,
                default=value.get("default"),
            )
        else:
            result[key] = ProcessField()
    return result


def _public_process_summary(process: dict[str, Any]) -> ProcessSummary:
    process_id = process["id"]
    keywords = process.get("keywords")
    job_control_options = process.get("jobControlOptions")
    return ProcessSummary(
        id=process_id,
        title=process["title"],
        description=process.get("description") if isinstance(process.get("description"), str) else None,
        version=process.get("version") if isinstance(process.get("version"), str) else None,
        keywords=[k for k in keywords if isinstance(k, str)] if isinstance(keywords, list) else [],
        job_control_options=[value for value in job_control_options if isinstance(value, str)]
        if isinstance(job_control_options, list)
        else [],
        links=_process_links(process_id),
    )


def _public_process_detail(process: dict[str, Any]) -> ProcessDetail:
    summary = _public_process_summary(process)
    return ProcessDetail(
        **summary.model_dump(),
        inputs=_field_map(process.get("inputs")),
        outputs=_field_map(process.get("outputs")),
    )


def _get_public_process_or_404(process_id: str) -> dict[str, Any]:
    process = process_registry.get_process(process_id)
    if process is None or not process["expose"]:
        raise HTTPException(status_code=404, detail=f"Unknown process '{process_id}'")
    return process


def _validate_required_process_inputs(process: dict[str, Any], request: dict[str, Any]) -> None:
    raw_inputs = process.get("inputs")
    if not isinstance(raw_inputs, dict):
        return
    missing = [
        key
        for key, value in raw_inputs.items()
        if isinstance(key, str) and isinstance(value, dict) and value.get("required") is True and key not in request
    ]
    if missing:
        joined = ", ".join(sorted(missing))
        raise HTTPException(status_code=400, detail=f"Missing required process inputs: {joined}")


def _validate_process_request(process: dict[str, Any], request: dict[str, Any]) -> None:
    _validate_required_process_inputs(process, request)
    if process.get("id") == "resample":
        processing_services.validate_resample_request(**request)


def _supports_async_execution(process: dict[str, Any]) -> bool:
    job_control_options = process.get("jobControlOptions")
    return isinstance(job_control_options, list) and "async-execute" in job_control_options


@router.get("", response_model=ProcessListResponse)
def get_processes_catalog() -> ProcessListResponse:
    """Return the registered native process catalog."""
    visible = [process for process in process_registry.list_processes() if process["expose"]]
    return ProcessListResponse(
        processes=[_public_process_summary(process) for process in visible],
        links=_catalog_links(),
    )


@router.get("/{process_id}", response_model=ProcessDetail)
def get_process(process_id: str) -> ProcessDetail:
    """Return one registered process description."""
    process = _get_public_process_or_404(process_id)
    return _public_process_detail(process)


@router.post("/{process_id}/execution")
def run_process_execution(
    process_id: str,
    response: Response,
    request: dict[str, Any] = Body(...),
    prefer: str | None = Header(default=None),
) -> Any:
    """Dispatch to a registered process execution function by process id."""
    process = _get_public_process_or_404(process_id)
    _validate_process_request(process, request)
    if _prefer_respond_async(prefer):
        if not _supports_async_execution(process):
            raise HTTPException(
                status_code=409,
                detail=f"Process '{process_id}' does not support async execution",
            )
        job = get_job_service().submit_process_job(process_id=process_id, request=request)
        response.status_code = 202
        response.headers["Location"] = f"/internal/jobs/{job.job_id}"
        return job

    try:
        func = process_registry.get_process_function(process_id)
    except (ImportError, AttributeError, ValueError) as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load execution.function for process '{process_id}': {exc}",
        ) from exc

    try:
        return func(**request)
    except TypeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
