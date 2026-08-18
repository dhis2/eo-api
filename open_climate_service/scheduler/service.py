"""APScheduler adapter and application service for dataset schedules."""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import HTTPException

from open_climate_service import config as api_config
from open_climate_service.data_registry.services import datasets as registry_datasets
from open_climate_service.ingestions import services as ingestion_services
from open_climate_service.scheduler import store
from open_climate_service.scheduler.config import SchedulerConfig, get_scheduler_config
from open_climate_service.scheduler.dispatcher import CheckOutcome, CheckResult, enqueue_sync
from open_climate_service.scheduler.models import DatasetSyncSchedule, SchedulePatch, SchedulePut, ScheduleValues
from open_climate_service.scheduler.schemas import ScheduleListResponse, ScheduleStatus
from open_climate_service.shared.time import utc_now

logger = logging.getLogger(__name__)


class SchedulerService:
    """Manage durable per-dataset schedules and the process-local clock."""

    def __init__(
        self,
        *,
        config_loader: Callable[[], SchedulerConfig] = get_scheduler_config,
        dispatcher: Callable[[DatasetSyncSchedule], CheckResult] = enqueue_sync,
        template_loader: Callable[[str], dict[str, Any] | None] = registry_datasets.get_dataset,
    ) -> None:
        self._config_loader = config_loader
        self._dispatcher = dispatcher
        self._template_loader = template_loader
        self._scheduler: AsyncIOScheduler | None = None
        self._config: SchedulerConfig | None = None
        self._last_results: dict[str, CheckResult] = {}

    def start(self) -> None:
        """Start the clock and reconstruct enabled jobs from the durable store."""
        config = self._config_loader()
        self._config = config
        if not config.enabled:
            logger.info("Dataset scheduler is disabled")
            return
        if api_config.is_read_only():
            logger.info("Dataset scheduler will not start on a read-only instance")
            return

        scheduler = AsyncIOScheduler(timezone=config.timezone_info)
        self._scheduler = scheduler
        schedules = store.list_schedules()
        for schedule in schedules:
            try:
                self._validate_target(schedule.dataset_id)
                self._register(schedule)
            except Exception:
                logger.exception("Stored schedule for %s could not be registered", schedule.dataset_id)
        scheduler.start()
        logger.warning(
            "Dataset scheduler enabled in process %d with %d persisted schedule(s); "
            "exactly one writable OCS process may enable scheduling",
            os.getpid(),
            len(schedules),
        )

    def _validate_target(self, dataset_id: str) -> None:
        """Require an existing, syncable managed dataset."""
        artifact = ingestion_services.get_latest_artifact_for_dataset_or_404(dataset_id)
        template = self._template_loader(artifact.dataset_id)
        if template is None:
            raise HTTPException(status_code=404, detail=f"Source dataset '{artifact.dataset_id}' not found")
        if registry_datasets.is_future_facing(template):
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Dataset '{dataset_id}' is future-facing; forecast refresh requires "
                    "overlapping-window rematerialization and is not supported yet"
                ),
            )
        sync = template.get("sync")
        if not isinstance(sync, dict) or sync.get("kind") == "static":
            raise HTTPException(status_code=422, detail=f"Dataset '{dataset_id}' is not syncable")

    def _require_available(self) -> SchedulerConfig:
        config = self._config or self._config_loader()
        if not config.enabled:
            raise HTTPException(status_code=409, detail="Scheduling is disabled for this deployment")
        if api_config.is_read_only():
            raise HTTPException(status_code=403, detail="Scheduling is unavailable on a read-only instance")
        return config

    def _register(self, schedule: DatasetSyncSchedule) -> None:
        scheduler = self._scheduler
        if scheduler is None:
            return
        existing = scheduler.get_job(schedule.schedule_id)
        if not schedule.enabled:
            if existing is not None:
                scheduler.remove_job(schedule.schedule_id)
            return
        trigger = CronTrigger.from_crontab(schedule.cron, timezone=schedule.timezone_info)
        scheduler.add_job(
            self.run_now,
            trigger=trigger,
            args=[schedule.dataset_id],
            id=schedule.schedule_id,
            coalesce=True,
            max_instances=1,
            replace_existing=True,
        )

    def put(self, dataset_id: str, request: SchedulePut) -> ScheduleStatus:
        """Create or replace the only schedule for a managed dataset."""
        config = self._require_available()
        self._validate_target(dataset_id)
        previous = store.get_schedule(dataset_id)
        now = utc_now()
        values = ScheduleValues(
            cron=request.cron,
            timezone=request.timezone or config.timezone,
            enabled=request.enabled,
            publish=request.publish,
            max_attempts=request.max_attempts,
        )
        schedule = DatasetSyncSchedule(
            dataset_id=dataset_id,
            created_at=previous.created_at if previous else now,
            updated_at=now,
            **values.model_dump(),
        )
        store.upsert_schedule(schedule)
        self._register(schedule)
        return self._status_for(schedule)

    def patch(self, dataset_id: str, request: SchedulePatch) -> ScheduleStatus:
        """Update selected fields of an existing schedule."""
        self._require_available()
        current = self.get(dataset_id)
        changes = request.model_dump(exclude_none=True)
        current_values = {
            "cron": current.cron,
            "timezone": current.timezone,
            "enabled": current.enabled,
            "publish": current.publish,
            "max_attempts": current.max_attempts,
        }
        values = ScheduleValues.model_validate({**current_values, **changes})
        updated = current.model_copy(update={**values.model_dump(), "updated_at": utc_now()})
        store.upsert_schedule(updated)
        self._register(updated)
        return self._status_for(updated)

    def delete(self, dataset_id: str) -> None:
        """Remove one schedule without deleting its dataset or jobs."""
        self._require_available()
        removed = store.delete_schedule(dataset_id)
        if removed is None:
            raise HTTPException(status_code=404, detail=f"Schedule for dataset '{dataset_id}' not found")
        if self._scheduler is not None and self._scheduler.get_job(removed.schedule_id) is not None:
            self._scheduler.remove_job(removed.schedule_id)
        self._last_results.pop(removed.schedule_id, None)

    def get(self, dataset_id: str) -> DatasetSyncSchedule:
        """Return one persisted schedule or raise 404."""
        schedule = store.get_schedule(dataset_id)
        if schedule is None:
            raise HTTPException(status_code=404, detail=f"Schedule for dataset '{dataset_id}' not found")
        return schedule

    def get_status(self, dataset_id: str) -> ScheduleStatus:
        """Return persisted and runtime state for one dataset schedule."""
        return self._status_for(self.get(dataset_id))

    def run_now(self, dataset_id: str) -> CheckResult:
        """Enqueue a sync immediately without changing the recurring schedule."""
        self._require_available()
        schedule = self.get(dataset_id)
        try:
            result = self._dispatcher(schedule)
        except Exception as exc:
            result = CheckResult(
                schedule_id=schedule.schedule_id,
                dataset_id=schedule.dataset_id,
                outcome=CheckOutcome.ERROR,
                message=f"{type(exc).__name__}: {exc}",
            )
            logger.exception("Scheduled sync enqueue failed for %s", schedule.dataset_id)
        self._last_results[schedule.schedule_id] = result
        return result

    def shutdown(self) -> None:
        """Stop future callbacks without waiting for queued native jobs."""
        if self._scheduler is None:
            return
        self._scheduler.shutdown(wait=False)
        self._scheduler = None
        logger.info("Dataset scheduler stopped")

    def _status_for(self, schedule: DatasetSyncSchedule) -> ScheduleStatus:
        result = self._last_results.get(schedule.schedule_id)
        job = self._scheduler.get_job(schedule.schedule_id) if self._scheduler is not None else None
        return ScheduleStatus(
            schedule_id=schedule.schedule_id,
            dataset_id=schedule.dataset_id,
            cron=schedule.cron,
            timezone=schedule.timezone,
            enabled=schedule.enabled,
            publish=schedule.publish,
            max_attempts=schedule.max_attempts,
            created_at=schedule.created_at,
            updated_at=schedule.updated_at,
            next_check=getattr(job, "next_run_time", None),
            last_check=result.checked_at if result else None,
            last_outcome=result.outcome if result else None,
            last_message=result.message if result else None,
            last_job_id=result.job_id if result else None,
        )

    def status(self) -> ScheduleListResponse:
        """Return persisted schedules plus process-local runtime status."""
        config = self._config or self._config_loader()
        return ScheduleListResponse(
            enabled=config.enabled,
            running=self._scheduler is not None,
            timezone=config.timezone,
            max_concurrent_syncs=config.max_concurrent_syncs,
            schedules=[self._status_for(schedule) for schedule in store.list_schedules()],
        )


_service: SchedulerService | None = None


def get_scheduler_service() -> SchedulerService:
    """Return the process-local scheduler singleton."""
    global _service
    if _service is None:
        _service = SchedulerService()
    return _service
