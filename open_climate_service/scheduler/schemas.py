"""Public status schemas for scheduled dataset synchronization."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from open_climate_service.scheduler.dispatcher import CheckOutcome


class ScheduleStatus(BaseModel):
    """Configuration and volatile runtime status for one dataset schedule."""

    schedule_id: str
    dataset_id: str
    cron: str
    timezone: str
    publish: bool
    max_attempts: int
    next_check: datetime | None = None
    last_check: datetime | None = None
    last_outcome: CheckOutcome | None = None
    last_message: str | None = None
    last_job_id: str | None = None


class ScheduleListResponse(BaseModel):
    """Status of the process-local scheduler and configured schedules."""

    enabled: bool
    running: bool
    timezone: str
    schedules: list[ScheduleStatus] = Field(default_factory=list)
