"""Disk-backed workflow job persistence."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any, cast

from ...data_manager.services.downloader import DOWNLOAD_DIR
from ...publications.pygeoapi import write_generated_pygeoapi_documents
from ...publications.services import delete_published_resource
from ..schemas import (
    WorkflowExecuteRequest,
    WorkflowExecuteResponse,
    WorkflowJobOrchestration,
    WorkflowJobOrchestrationStep,
    WorkflowJobRecord,
    WorkflowJobStatus,
    WorkflowJobStoredRecord,
)
from .definitions import WorkflowDefinition

_DEFAULT_PROCESS_ID = "generic-dhis2-workflow"


def initialize_job(
    *,
    job_id: str,
    request: WorkflowExecuteRequest,
    request_payload: dict[str, Any] | None,
    workflow: WorkflowDefinition,
    workflow_definition_source: str,
    workflow_id: str,
    workflow_version: int,
    status: WorkflowJobStatus = WorkflowJobStatus.RUNNING,
    process_id: str = _DEFAULT_PROCESS_ID,
) -> WorkflowJobRecord:
    """Create or replace a persisted job record."""
    existing = get_stored_job(job_id)
    timestamp = _utc_now()
    record = WorkflowJobStoredRecord(
        job_id=job_id,
        run_id=job_id,
        process_id=process_id,
        workflow_id=workflow_id,
        workflow_version=workflow_version,
        dataset_id=request.dataset_id,
        status=status,
        created_at=existing.created_at if existing is not None else timestamp,
        updated_at=timestamp,
        request=request_payload if request_payload is not None else request.model_dump(mode="json"),
        orchestration=_build_orchestration_summary(
            workflow=workflow,
            workflow_definition_source=workflow_definition_source,
        ),
        run_log_file=existing.run_log_file if existing is not None else None,
        output_file=existing.output_file if existing is not None else None,
        result=existing.result if existing is not None else None,
        error=existing.error if existing is not None else None,
        error_code=existing.error_code if existing is not None else None,
        failed_component=existing.failed_component if existing is not None else None,
        failed_component_version=existing.failed_component_version if existing is not None else None,
    )
    _write_job(record)
    return record


def mark_job_running(job_id: str) -> WorkflowJobRecord:
    """Transition an existing job to running."""
    record = _require_job(job_id)
    updated = record.model_copy(update={"status": WorkflowJobStatus.RUNNING, "updated_at": _utc_now()})
    _write_job(updated)
    return updated


def mark_job_success(
    *,
    job_id: str,
    response: WorkflowExecuteResponse,
) -> WorkflowJobRecord:
    """Persist successful job completion details."""
    record = _require_job(job_id)
    updated = record.model_copy(
        update={
            "status": WorkflowJobStatus.SUCCESSFUL,
            "updated_at": _utc_now(),
            "run_log_file": response.run_log_file,
            "output_file": response.output_file,
            "result": response.model_dump(mode="json"),
            "error": None,
            "error_code": None,
            "failed_component": None,
            "failed_component_version": None,
        }
    )
    _write_job(updated)
    return updated


def mark_job_failed(
    *,
    job_id: str,
    error: str,
    error_code: str | None = None,
    failed_component: str | None = None,
    failed_component_version: str | None = None,
    run_log_file: str | None = None,
) -> WorkflowJobRecord:
    """Persist failed job details."""
    record = _require_job(job_id)
    updated = record.model_copy(
        update={
            "status": WorkflowJobStatus.FAILED,
            "updated_at": _utc_now(),
            "run_log_file": run_log_file or record.run_log_file,
            "error": error,
            "error_code": error_code,
            "failed_component": failed_component,
            "failed_component_version": failed_component_version,
            "result": None,
        }
    )
    _write_job(updated)
    return updated


def get_job(job_id: str) -> WorkflowJobRecord | None:
    """Load one persisted job if it exists."""
    record = get_stored_job(job_id)
    if record is None:
        return None
    return _to_public_job_record(record)


def get_stored_job(job_id: str) -> WorkflowJobStoredRecord | None:
    """Load one persisted job including internal result payload if it exists."""
    path = _job_path(job_id)
    if not path.exists():
        return None
    return WorkflowJobStoredRecord.model_validate_json(path.read_text(encoding="utf-8"))


def list_jobs(*, process_id: str | None = None, status: WorkflowJobStatus | None = None) -> list[WorkflowJobRecord]:
    """List persisted jobs ordered by newest first."""
    jobs: list[WorkflowJobRecord] = []
    for path in _jobs_dir().glob("*.json"):
        jobs.append(
            _to_public_job_record(WorkflowJobStoredRecord.model_validate_json(path.read_text(encoding="utf-8")))
        )
    jobs.sort(key=lambda item: item.created_at, reverse=True)
    if process_id is not None:
        jobs = [job for job in jobs if job.process_id == process_id]
    if status is not None:
        jobs = [job for job in jobs if job.status == status]
    return jobs


def get_job_result(job_id: str) -> dict[str, Any] | None:
    """Return persisted workflow result payload for a completed job."""
    record = get_stored_job(job_id)
    if record is None:
        return None
    return record.result


def get_job_trace(job_id: str) -> dict[str, Any] | None:
    """Return persisted run-trace payload for a workflow job if available."""
    record = get_stored_job(job_id)
    if record is None or record.run_log_file is None:
        return None
    path = Path(record.run_log_file)
    if not path.exists():
        return None
    return cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))


def delete_job(job_id: str) -> dict[str, Any] | None:
    """Delete a job and cascade removal of run-owned derived artifacts."""
    record = get_stored_job(job_id)
    if record is None:
        return None

    deleted_paths: list[str] = []
    publication = delete_published_resource(f"workflow-output-{job_id}")
    if publication is not None:
        for candidate in (publication.path, publication.metadata.get("native_output_file")):
            deleted = _delete_owned_path(candidate)
            if deleted is not None:
                deleted_paths.append(deleted)

    for candidate in (record.run_log_file, record.output_file):
        deleted = _delete_owned_path(candidate)
        if deleted is not None:
            deleted_paths.append(deleted)

    job_path = _job_path(job_id)
    if job_path.exists():
        job_path.unlink()
        deleted_paths.append(str(job_path))

    # Keep generated documents on disk aligned with current publication truth.
    config_path, openapi_path = write_generated_pygeoapi_documents()
    return {
        "job_id": job_id,
        "deleted": True,
        "deleted_paths": deleted_paths,
        "deleted_publication": publication.resource_id if publication is not None else None,
        "materialized_config_path": str(config_path),
        "materialized_openapi_path": str(openapi_path),
        "pygeoapi_runtime_reload_required": True,
    }


def cleanup_jobs(
    *,
    dry_run: bool,
    keep_latest: int | None = None,
    older_than_hours: int | None = None,
) -> dict[str, Any]:
    """Apply retention policy to terminal jobs and their run-owned artifacts."""
    if keep_latest is not None and keep_latest < 0:
        raise ValueError("keep_latest must be >= 0")
    if older_than_hours is not None and older_than_hours < 0:
        raise ValueError("older_than_hours must be >= 0")

    terminal_statuses = {
        WorkflowJobStatus.SUCCESSFUL,
        WorkflowJobStatus.FAILED,
        WorkflowJobStatus.DISMISSED,
    }
    terminal_jobs = [job for job in list_jobs() if job.status in terminal_statuses]
    candidates = terminal_jobs

    if older_than_hours is not None:
        cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=older_than_hours)
        candidates = [job for job in candidates if _parse_iso8601(job.created_at) <= cutoff]

    if keep_latest is not None:
        protected_ids = {job.job_id for job in terminal_jobs[:keep_latest]}
        candidates = [job for job in candidates if job.job_id not in protected_ids]

    deleted_job_ids: list[str] = []
    if not dry_run:
        for job in candidates:
            deleted = delete_job(job.job_id)
            if deleted is not None:
                deleted_job_ids.append(job.job_id)

    return {
        "dry_run": dry_run,
        "keep_latest": keep_latest,
        "older_than_hours": older_than_hours,
        "candidate_count": len(candidates),
        "deleted_count": len(deleted_job_ids),
        "candidates": [
            {
                "job_id": job.job_id,
                "status": job.status,
                "created_at": job.created_at,
                "workflow_id": job.workflow_id,
                "dataset_id": job.dataset_id,
            }
            for job in candidates
        ],
        "deleted_job_ids": deleted_job_ids,
    }


def _require_job(job_id: str) -> WorkflowJobStoredRecord:
    record = get_stored_job(job_id)
    if record is None:
        raise ValueError(f"Unknown job_id '{job_id}'")
    return record


def _write_job(record: WorkflowJobRecord) -> None:
    _jobs_dir().mkdir(parents=True, exist_ok=True)
    _job_path(record.job_id).write_text(record.model_dump_json(indent=2), encoding="utf-8")


def _job_path(job_id: str) -> Path:
    return _jobs_dir() / f"{job_id}.json"


def _jobs_dir() -> Path:
    return DOWNLOAD_DIR / "workflow_jobs"


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _parse_iso8601(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value)


def _to_public_job_record(record: WorkflowJobStoredRecord) -> WorkflowJobRecord:
    data = record.model_dump(mode="json")
    data.pop("run_id", None)
    data.pop("result", None)
    return WorkflowJobRecord.model_validate(data)


def _build_orchestration_summary(
    *,
    workflow: WorkflowDefinition,
    workflow_definition_source: str,
) -> WorkflowJobOrchestration:
    return WorkflowJobOrchestration(
        definition_source=workflow_definition_source,
        step_count=len(workflow.steps),
        components=[step.component for step in workflow.steps],
        steps=[
            WorkflowJobOrchestrationStep(
                component=step.component,
                version=step.version,
                execution_mode=cast(str | None, step.config.get("execution_mode")),
            )
            for step in workflow.steps
        ],
    )


def _delete_owned_path(path_value: Any) -> str | None:
    if not isinstance(path_value, str) or path_value == "":
        return None
    path = Path(path_value)
    if not path.exists() or not path.is_file():
        return None
    try:
        resolved = path.resolve()
        downloads_root = DOWNLOAD_DIR.resolve()
    except OSError:
        return None
    if downloads_root not in resolved.parents:
        return None
    path.unlink()
    return str(path)
