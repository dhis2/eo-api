from datetime import UTC, datetime
from threading import Lock
from typing import Any
from uuid import uuid4

from eoapi.state_store import load_state_map, save_state_map


_SCHEDULES: dict[str, dict[str, Any]] = load_state_map("schedules")
_LOCK = Lock()


def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def create_schedule(payload: dict[str, Any]) -> dict[str, Any]:
    schedule_id = str(uuid4())
    timestamp = _now_iso()
    schedule = {
        "scheduleId": schedule_id,
        "processId": payload["processId"],
        "workflowId": payload.get("workflowId"),
        "name": payload["name"],
        "cron": payload["cron"],
        "timezone": payload["timezone"],
        "enabled": payload["enabled"],
        "inputs": payload["inputs"],
        "created": timestamp,
        "updated": timestamp,
        "lastRunAt": None,
        "lastRunJobId": None,
    }

    with _LOCK:
        _SCHEDULES[schedule_id] = schedule
        save_state_map("schedules", _SCHEDULES)

    return schedule


def list_schedules() -> list[dict[str, Any]]:
    with _LOCK:
        return list(_SCHEDULES.values())


def get_schedule(schedule_id: str) -> dict[str, Any] | None:
    with _LOCK:
        return _SCHEDULES.get(schedule_id)


def update_schedule(schedule_id: str, updates: dict[str, Any]) -> dict[str, Any] | None:
    with _LOCK:
        schedule = _SCHEDULES.get(schedule_id)
        if schedule is None:
            return None

        for key, value in updates.items():
            if value is not None:
                schedule[key] = value

        schedule["updated"] = _now_iso()
        save_state_map("schedules", _SCHEDULES)
        return schedule


def delete_schedule(schedule_id: str) -> bool:
    with _LOCK:
        if schedule_id not in _SCHEDULES:
            return False
        del _SCHEDULES[schedule_id]
        save_state_map("schedules", _SCHEDULES)
        return True


def mark_schedule_run(schedule_id: str, job_id: str) -> dict[str, Any] | None:
    with _LOCK:
        schedule = _SCHEDULES.get(schedule_id)
        if schedule is None:
            return None

        timestamp = _now_iso()
        schedule["lastRunAt"] = timestamp
        schedule["lastRunJobId"] = job_id
        schedule["updated"] = timestamp
        save_state_map("schedules", _SCHEDULES)
        return schedule
