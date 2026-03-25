"""Disk-backed workflow schedule persistence and execution helpers."""

from __future__ import annotations

import datetime as dt
import uuid
from pathlib import Path

from ...data_manager.services.downloader import DOWNLOAD_DIR
from ..schemas import (
    WorkflowExecuteResponse,
    WorkflowJobStatus,
    WorkflowSchedule,
    WorkflowScheduleCreateRequest,
    WorkflowScheduleTriggerResponse,
)
from .definitions import load_workflow_definition
from .engine import execute_workflow
from .job_store import find_job_by_schedule_key
from .simple_mapper import normalize_simple_request


def create_schedule(payload: WorkflowScheduleCreateRequest) -> WorkflowSchedule:
    """Persist one workflow schedule."""
    timestamp = _utc_now()
    workflow_id = payload.workflow_id or payload.request.workflow_id
    if payload.workflow_id is not None and payload.request.workflow_id != payload.workflow_id:
        raise ValueError("workflow_id must match request.workflow_id when both are provided")
    schedule = WorkflowSchedule(
        schedule_id=str(uuid.uuid4()),
        workflow_id=workflow_id,
        cron_expression=payload.cron_expression,
        request=payload.request.model_copy(update={"workflow_id": workflow_id}),
        enabled=payload.enabled,
        idempotency_key_template=payload.idempotency_key_template,
        retention_policy=payload.retention_policy,
        created_at=timestamp,
        updated_at=timestamp,
        last_triggered_at=None,
    )
    _validate_cron(schedule.cron_expression)
    load_workflow_definition(schedule.workflow_id)
    _write_schedule(schedule)
    return schedule


def list_schedules(*, workflow_id: str | None = None) -> list[WorkflowSchedule]:
    """List persisted schedules ordered by newest first."""
    schedules: list[WorkflowSchedule] = []
    for path in _schedules_dir().glob("*.json"):
        schedules.append(WorkflowSchedule.model_validate_json(path.read_text(encoding="utf-8")))
    schedules.sort(key=lambda item: item.created_at, reverse=True)
    if workflow_id is not None:
        schedules = [item for item in schedules if item.workflow_id == workflow_id]
    return schedules


def get_schedule(schedule_id: str) -> WorkflowSchedule | None:
    """Fetch one persisted schedule."""
    path = _schedule_path(schedule_id)
    if not path.exists():
        return None
    return WorkflowSchedule.model_validate_json(path.read_text(encoding="utf-8"))


def delete_schedule(schedule_id: str) -> WorkflowSchedule | None:
    """Delete one persisted schedule."""
    schedule = get_schedule(schedule_id)
    if schedule is None:
        return None
    path = _schedule_path(schedule_id)
    if path.exists():
        path.unlink()
    return schedule


def trigger_schedule(
    *,
    schedule_id: str,
    execution_time: str | None = None,
) -> tuple[WorkflowScheduleTriggerResponse, WorkflowExecuteResponse | None]:
    """Execute one schedule immediately with idempotency protection."""
    schedule = get_schedule(schedule_id)
    if schedule is None:
        raise ValueError(f"Unknown schedule_id '{schedule_id}'")
    if not schedule.enabled:
        raise ValueError(f"Schedule '{schedule_id}' is disabled")

    trigger_time = _parse_execution_time(execution_time)
    idempotency_key = _render_idempotency_key(
        template=schedule.idempotency_key_template,
        workflow_id=schedule.workflow_id,
        schedule_id=schedule.schedule_id,
        execution_time=trigger_time,
    )
    existing_job = find_job_by_schedule_key(schedule_id=schedule.schedule_id, idempotency_key=idempotency_key)
    if existing_job is not None:
        return (
            WorkflowScheduleTriggerResponse(
                schedule_id=schedule.schedule_id,
                workflow_id=schedule.workflow_id,
                job_id=existing_job.job_id,
                status=existing_job.status,
                idempotency_key=idempotency_key,
                reused_existing_job=True,
            ),
            None,
        )

    request, _warnings = normalize_simple_request(schedule.request)
    response = execute_workflow(
        request,
        workflow_id=schedule.workflow_id,
        request_params=schedule.request.model_dump(exclude_none=True),
        include_component_run_details=schedule.request.include_component_run_details,
        run_id=str(uuid.uuid4()),
        workflow_definition_source="catalog",
        trigger_type="scheduled",
        schedule_id=schedule.schedule_id,
        idempotency_key=idempotency_key,
    )
    updated_schedule = schedule.model_copy(update={"updated_at": _utc_now(), "last_triggered_at": _utc_now()})
    _write_schedule(updated_schedule)
    return (
        WorkflowScheduleTriggerResponse(
            schedule_id=schedule.schedule_id,
            workflow_id=schedule.workflow_id,
            job_id=response.run_id,
            status=WorkflowJobStatus.SUCCESSFUL,
            idempotency_key=idempotency_key,
            reused_existing_job=False,
        ),
        response,
    )


def _write_schedule(schedule: WorkflowSchedule) -> None:
    _schedules_dir().mkdir(parents=True, exist_ok=True)
    _schedule_path(schedule.schedule_id).write_text(schedule.model_dump_json(indent=2), encoding="utf-8")


def _schedules_dir() -> Path:
    return DOWNLOAD_DIR / "workflow_schedules"


def _schedule_path(schedule_id: str) -> Path:
    return _schedules_dir() / f"{schedule_id}.json"


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _validate_cron(value: str) -> None:
    parts = value.split()
    if len(parts) != 5:
        raise ValueError("cron_expression must have 5 space-separated fields")


def _parse_execution_time(value: str | None) -> dt.datetime:
    if value is None:
        return dt.datetime.now(dt.timezone.utc)
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _render_idempotency_key(
    *,
    template: str,
    workflow_id: str,
    schedule_id: str,
    execution_time: dt.datetime,
) -> str:
    values = {
        "workflow_id": workflow_id,
        "schedule_id": schedule_id,
        "date": execution_time.strftime("%Y-%m-%d"),
        "datetime": execution_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "hour": execution_time.strftime("%Y-%m-%dT%H"),
    }
    rendered = template
    for key, value in values.items():
        rendered = rendered.replace(f"{{{key}}}", value)
    return rendered
