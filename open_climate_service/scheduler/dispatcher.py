"""Plan and submit one scheduled dataset sync without owning any timing logic."""

from __future__ import annotations

from enum import StrEnum

from fastapi import HTTPException
from pydantic import BaseModel

from open_climate_service import config as api_config
from open_climate_service.ingestions import services
from open_climate_service.ingestions.job_submission import submit_sync_job
from open_climate_service.ingestions.schemas import SyncAction
from open_climate_service.jobs.models import JobStatus
from open_climate_service.jobs.service import get_job_service
from open_climate_service.scheduler.config import DatasetSyncSchedule

_ACTIVE_JOB_STATUSES = {JobStatus.ACCEPTED, JobStatus.RUNNING, JobStatus.RETRYING}
_SYNC_PROCESS_IDS = {"sync", "scheduled-sync"}


class CheckOutcome(StrEnum):
    """Outcome of one scheduled check."""

    SUBMITTED = "submitted"
    UP_TO_DATE = "up_to_date"
    NOT_MATERIALIZED = "not_materialized"
    NOT_SYNCABLE = "not_syncable"
    ALREADY_RUNNING = "already_running"
    READ_ONLY = "read_only"


class CheckResult(BaseModel):
    """Small operational record retained by the in-memory scheduler."""

    schedule_id: str
    dataset_id: str
    outcome: CheckOutcome
    message: str
    job_id: str | None = None


def _has_active_sync_job(dataset_id: str) -> bool:
    return any(
        record.process_id in _SYNC_PROCESS_IDS
        and record.status in _ACTIVE_JOB_STATUSES
        and record.request.get("dataset_id") == dataset_id
        for record in get_job_service().list_jobs().jobs
    )


def check_and_submit(schedule: DatasetSyncSchedule) -> CheckResult:
    """Plan one dataset sync and submit it only when actionable and not already active."""
    if api_config.is_read_only():
        return CheckResult(
            schedule_id=schedule.schedule_id,
            dataset_id=schedule.dataset_id,
            outcome=CheckOutcome.READ_ONLY,
            message="Scheduler is disabled for a read-only instance",
        )

    try:
        plan = services.plan_sync_dataset(dataset_id=schedule.dataset_id, end=None)
    except HTTPException as exc:
        if exc.status_code == 404:
            return CheckResult(
                schedule_id=schedule.schedule_id,
                dataset_id=schedule.dataset_id,
                outcome=CheckOutcome.NOT_MATERIALIZED,
                message=str(exc.detail),
            )
        raise

    if plan.action == SyncAction.NO_OP:
        return CheckResult(
            schedule_id=schedule.schedule_id,
            dataset_id=schedule.dataset_id,
            outcome=CheckOutcome.UP_TO_DATE,
            message=plan.message,
        )
    if plan.action == SyncAction.NOT_SYNCABLE:
        return CheckResult(
            schedule_id=schedule.schedule_id,
            dataset_id=schedule.dataset_id,
            outcome=CheckOutcome.NOT_SYNCABLE,
            message=plan.message,
        )
    if _has_active_sync_job(schedule.dataset_id):
        return CheckResult(
            schedule_id=schedule.schedule_id,
            dataset_id=schedule.dataset_id,
            outcome=CheckOutcome.ALREADY_RUNNING,
            message="An active sync job already exists for this dataset",
        )

    job = submit_sync_job(
        dataset_id=schedule.dataset_id,
        end=None,
        publish=schedule.publish,
        label="scheduled-sync",
    )
    return CheckResult(
        schedule_id=schedule.schedule_id,
        dataset_id=schedule.dataset_id,
        outcome=CheckOutcome.SUBMITTED,
        message="Scheduled sync submitted",
        job_id=job.job_id,
    )
