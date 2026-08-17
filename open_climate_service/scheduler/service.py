"""APScheduler adapter for the CLIM-878 in-process scheduler proof of concept."""

from __future__ import annotations

import logging
from collections.abc import Callable

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from open_climate_service import config as api_config
from open_climate_service.scheduler.config import DatasetSyncSchedule, SchedulerConfig, get_scheduler_config
from open_climate_service.scheduler.dispatcher import CheckResult, check_and_submit

logger = logging.getLogger(__name__)


class SchedulerService:
    """Own the in-process clock while delegating sync decisions to the dispatcher."""

    def __init__(
        self,
        *,
        config_loader: Callable[[], SchedulerConfig] = get_scheduler_config,
        dispatcher: Callable[[DatasetSyncSchedule], CheckResult] = check_and_submit,
    ) -> None:
        self._config_loader = config_loader
        self._dispatcher = dispatcher
        self._scheduler: AsyncIOScheduler | None = None
        self._last_results: dict[str, CheckResult] = {}

    def start(self) -> None:
        """Validate configuration and start callbacks when this process is enabled."""
        config = self._config_loader()
        if not config.enabled:
            logger.info("Dataset scheduler is disabled")
            return
        if api_config.is_read_only():
            logger.info("Dataset scheduler will not start on a read-only instance")
            return

        scheduler = AsyncIOScheduler(timezone=config.timezone_info)
        for schedule in config.dataset_sync:
            trigger = CronTrigger.from_crontab(schedule.cron, timezone=config.timezone_info)
            scheduler.add_job(
                self.check_now,
                trigger=trigger,
                args=[schedule],
                id=schedule.schedule_id,
                coalesce=True,
                max_instances=1,
                replace_existing=True,
            )
        scheduler.start()
        self._scheduler = scheduler
        logger.info("Dataset scheduler started with %d schedule(s)", len(config.dataset_sync))

    def shutdown(self) -> None:
        """Stop future callbacks without waiting for submitted native jobs."""
        if self._scheduler is None:
            return
        self._scheduler.shutdown(wait=False)
        self._scheduler = None
        logger.info("Dataset scheduler stopped")

    def check_now(self, schedule: DatasetSyncSchedule) -> CheckResult:
        """Run one isolated check, retain its status, and let later ticks continue after errors."""
        try:
            result = self._dispatcher(schedule)
        except Exception:
            logger.exception("Scheduled sync check failed for %s", schedule.dataset_id)
            raise
        self._last_results[schedule.schedule_id] = result
        logger.info(
            "Scheduled sync check for %s: %s (%s)",
            schedule.dataset_id,
            result.outcome,
            result.message,
        )
        return result

    def last_result(self, schedule_id: str) -> CheckResult | None:
        """Return volatile status for demonstrations and operational inspection."""
        return self._last_results.get(schedule_id)


_service: SchedulerService | None = None


def get_scheduler_service() -> SchedulerService:
    """Return the process-local scheduler singleton."""
    global _service
    if _service is None:
        _service = SchedulerService()
    return _service
