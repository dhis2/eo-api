"""Validated instance configuration for scheduled dataset synchronization."""

from __future__ import annotations

from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from apscheduler.triggers.cron import CronTrigger
from pydantic import BaseModel, ConfigDict, Field, model_validator

from open_climate_service import config as api_config


class DatasetSyncSchedule(BaseModel):
    """One cron-driven check of an existing managed dataset."""

    model_config = ConfigDict(extra="forbid")

    dataset_id: str = Field(min_length=1)
    cron: str = Field(min_length=1)
    publish: bool = True
    max_attempts: int = Field(default=3, ge=1)

    @model_validator(mode="after")
    def validate_cron(self) -> "DatasetSyncSchedule":
        try:
            CronTrigger.from_crontab(self.cron)
        except ValueError as exc:
            raise ValueError(f"invalid five-field cron expression {self.cron!r}: {exc}") from exc
        return self

    @property
    def schedule_id(self) -> str:
        """Return the stable identifier derived from the one-schedule-per-dataset rule."""
        return f"dataset-sync:{self.dataset_id}"


class SchedulerConfig(BaseModel):
    """Scheduler configuration loaded from ``climate-service.yaml``."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    timezone: str = "UTC"
    dataset_sync: list[DatasetSyncSchedule] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_configuration(self) -> "SchedulerConfig":
        try:
            ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown scheduler timezone {self.timezone!r}") from exc
        dataset_ids = [schedule.dataset_id for schedule in self.dataset_sync]
        duplicates = sorted({dataset_id for dataset_id in dataset_ids if dataset_ids.count(dataset_id) > 1})
        if duplicates:
            raise ValueError(f"only one scheduler entry is allowed per dataset: {duplicates}")
        return self

    @property
    def timezone_info(self) -> ZoneInfo:
        """Return the validated IANA timezone."""
        return ZoneInfo(self.timezone)


def get_scheduler_config() -> SchedulerConfig:
    """Load scheduler configuration from the instance configuration."""
    raw = api_config.get_config().get("scheduler", {})
    if not isinstance(raw, dict):
        raise ValueError("scheduler in CLIMATE_SERVICE_CONFIG must be a mapping")
    return SchedulerConfig.model_validate(raw)
