"""JSON-backed persistence for user-managed dataset schedules."""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from pathlib import Path

import portalocker

from open_climate_service import config as api_config
from open_climate_service.scheduler.models import DatasetSyncSchedule


def _resolve_schedules_dir() -> Path:
    data_dir = api_config.get_data_dir()
    if data_dir is not None:
        return data_dir / "schedules"
    xdg_data = Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share"))
    return xdg_data / "climate-service" / "schedules"


SCHEDULES_DIR = _resolve_schedules_dir()
SCHEDULES_INDEX_PATH = SCHEDULES_DIR / "schedules.json"


def ensure_store() -> None:
    """Create the schedule store if it does not exist."""
    SCHEDULES_DIR.mkdir(parents=True, exist_ok=True)
    if not SCHEDULES_INDEX_PATH.exists():
        SCHEDULES_INDEX_PATH.write_text("[]\n", encoding="utf-8")


def list_schedules() -> list[DatasetSyncSchedule]:
    """Return all schedules ordered by dataset ID."""
    return sorted(
        (DatasetSyncSchedule.model_validate(raw) for raw in _load_records()),
        key=lambda schedule: schedule.dataset_id,
    )


def get_schedule(dataset_id: str) -> DatasetSyncSchedule | None:
    """Return the schedule for one managed dataset."""
    for raw in _load_records():
        if raw.get("dataset_id") == dataset_id:
            return DatasetSyncSchedule.model_validate(raw)
    return None


def upsert_schedule(schedule: DatasetSyncSchedule) -> DatasetSyncSchedule:
    """Create or replace the only schedule for a managed dataset."""

    def mutation(records: list[dict[str, object]]) -> DatasetSyncSchedule:
        payload = schedule.model_dump(mode="json")
        for index, existing in enumerate(records):
            if existing.get("dataset_id") == schedule.dataset_id:
                records[index] = payload
                return schedule
        records.append(payload)
        return schedule

    result = _mutate_records(mutation)
    if result is None:  # pragma: no cover - mutation always returns the supplied schedule
        raise RuntimeError("schedule upsert returned no record")
    return result


def delete_schedule(dataset_id: str) -> DatasetSyncSchedule | None:
    """Delete and return one schedule, or return None when absent."""

    def mutation(records: list[dict[str, object]]) -> DatasetSyncSchedule | None:
        for index, existing in enumerate(records):
            if existing.get("dataset_id") == dataset_id:
                removed = records.pop(index)
                return DatasetSyncSchedule.model_validate(removed)
        return None

    return _mutate_records(mutation)


def _load_records() -> list[dict[str, object]]:
    ensure_store()
    with open(SCHEDULES_INDEX_PATH, encoding="utf-8") as handle:
        portalocker.lock(handle, portalocker.LOCK_SH)
        try:
            payload = json.load(handle)
        finally:
            portalocker.unlock(handle)
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        raise ValueError("schedules.json must contain a list of objects")
    return payload


def _mutate_records(
    mutation: Callable[[list[dict[str, object]]], DatasetSyncSchedule | None],
) -> DatasetSyncSchedule | None:
    ensure_store()
    with open(SCHEDULES_INDEX_PATH, "r+", encoding="utf-8") as handle:
        portalocker.lock(handle, portalocker.LOCK_EX)
        try:
            payload = json.load(handle)
            if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
                raise ValueError("schedules.json must contain a list of objects")
            result = mutation(payload)
            handle.seek(0)
            json.dump(payload, handle, indent=2)
            handle.write("\n")
            handle.truncate()
            return result
        finally:
            portalocker.unlock(handle)
