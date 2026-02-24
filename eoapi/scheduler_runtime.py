import logging
import os
from datetime import UTC, datetime
from threading import Event, Thread

from eoapi.endpoints.schedules import execute_schedule_target
from eoapi.schedules import list_schedules

logger = logging.getLogger(__name__)

_THREAD: Thread | None = None
_STOP_EVENT = Event()


def _enabled() -> bool:
    raw = os.getenv("EOAPI_INTERNAL_SCHEDULER_ENABLED", "true").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _poll_seconds() -> float:
    raw = os.getenv("EOAPI_INTERNAL_SCHEDULER_POLL_SECONDS", "30").strip()
    try:
        value = float(raw)
    except ValueError:
        value = 30.0
    return value if value > 0 else 30.0


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except ValueError:
        return None


def _schedule_due(schedule: dict, now_utc: datetime) -> bool:
    if not schedule.get("enabled", True):
        return False

    cron = str(schedule.get("cron", "")).strip()
    if not cron:
        return False

    try:
        croniter_module = __import__("croniter", fromlist=["croniter"])
        croniter = getattr(croniter_module, "croniter")
        from zoneinfo import ZoneInfo
    except ImportError:
        return False

    timezone_name = str(schedule.get("timezone", "UTC") or "UTC")
    try:
        timezone = ZoneInfo(timezone_name)
    except Exception:
        timezone = ZoneInfo("UTC")

    anchor = _parse_iso(schedule.get("lastRunAt")) or _parse_iso(schedule.get("created"))
    if anchor is None:
        anchor = now_utc

    anchor_tz = anchor.astimezone(timezone)
    now_tz = now_utc.astimezone(timezone)

    try:
        next_run = croniter(cron, anchor_tz).get_next(datetime)
    except Exception:
        return False

    return next_run <= now_tz


def poll_scheduler_once(now_utc: datetime | None = None) -> None:
    if not _enabled():
        return

    current = now_utc or datetime.now(UTC)
    for schedule in list_schedules():
        if not _schedule_due(schedule, current):
            continue
        schedule_id = schedule.get("scheduleId")
        if not schedule_id:
            continue
        try:
            execute_schedule_target(str(schedule_id), trigger="internal-cron")
        except Exception as exc:
            logger.warning("Internal scheduler failed for schedule %s: %s", schedule_id, exc)


def _worker() -> None:
    while not _STOP_EVENT.is_set():
        poll_scheduler_once()
        _STOP_EVENT.wait(timeout=_poll_seconds())


def start_internal_scheduler() -> None:
    global _THREAD
    if _THREAD is not None and _THREAD.is_alive():
        return

    _STOP_EVENT.clear()
    _THREAD = Thread(target=_worker, daemon=True, name="eoapi-internal-scheduler")
    _THREAD.start()


def stop_internal_scheduler() -> None:
    _STOP_EVENT.set()
