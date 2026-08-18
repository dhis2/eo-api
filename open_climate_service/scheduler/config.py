"""Validated instance configuration for scheduled dataset synchronization."""

from __future__ import annotations

from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, model_validator

from open_climate_service import config as api_config


class SchedulerConfig(BaseModel):
    """Scheduler configuration loaded from ``climate-service.yaml``."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    timezone: str = "UTC"
    max_concurrent_syncs: int = Field(default=1, ge=1)

    @model_validator(mode="after")
    def validate_configuration(self) -> "SchedulerConfig":
        try:
            ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown scheduler timezone {self.timezone!r}") from exc
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
