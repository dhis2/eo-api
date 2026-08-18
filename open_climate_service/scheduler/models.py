"""Persisted and request models for per-dataset schedules."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from apscheduler.triggers.cron import CronTrigger
from pydantic import BaseModel, ConfigDict, Field, model_validator


class ScheduleValues(BaseModel):
    """User-controlled values shared by schedule requests and records."""

    model_config = ConfigDict(extra="forbid")

    cron: str = Field(min_length=1)
    timezone: str = Field(min_length=1)
    enabled: bool = True
    publish: bool = True
    max_attempts: int = Field(default=3, ge=1)

    @model_validator(mode="after")
    def validate_timing(self) -> "ScheduleValues":
        try:
            timezone = ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown schedule timezone {self.timezone!r}") from exc
        try:
            CronTrigger.from_crontab(self.cron, timezone=timezone)
        except ValueError as exc:
            raise ValueError(f"invalid five-field cron expression {self.cron!r}: {exc}") from exc
        return self


class SchedulePut(BaseModel):
    """Create or replace one dataset schedule."""

    model_config = ConfigDict(extra="forbid")

    cron: str = Field(min_length=1)
    timezone: str | None = None
    enabled: bool = True
    publish: bool = True
    max_attempts: int = Field(default=3, ge=1)


class SchedulePatch(BaseModel):
    """Mutable fields for one existing schedule."""

    model_config = ConfigDict(extra="forbid")

    cron: str | None = Field(default=None, min_length=1)
    timezone: str | None = Field(default=None, min_length=1)
    enabled: bool | None = None
    publish: bool | None = None
    max_attempts: int | None = Field(default=None, ge=1)


class DatasetSyncSchedule(ScheduleValues):
    """One persisted schedule keyed by its managed dataset ID."""

    dataset_id: str = Field(min_length=1)
    created_at: datetime
    updated_at: datetime

    @property
    def schedule_id(self) -> str:
        """Return the APScheduler identifier for this one-per-dataset resource."""
        return f"dataset-sync:{self.dataset_id}"

    @property
    def timezone_info(self) -> ZoneInfo:
        """Return the validated IANA timezone."""
        return ZoneInfo(self.timezone)
