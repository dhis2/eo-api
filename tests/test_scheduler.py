"""User-managed scheduled dataset synchronization tests (CLIM-849)."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from open_climate_service.jobs.models import JobRecord, JobStatus
from open_climate_service.scheduler import store
from open_climate_service.scheduler.config import SchedulerConfig
from open_climate_service.scheduler.dispatcher import CheckOutcome, enqueue_sync
from open_climate_service.scheduler.models import DatasetSyncSchedule, SchedulePatch, SchedulePut
from open_climate_service.scheduler.schemas import ScheduleStatus
from open_climate_service.scheduler.service import SchedulerService


def _schedule(**updates: object) -> DatasetSyncSchedule:
    now = datetime.now(timezone.utc)
    values: dict[str, object] = {
        "dataset_id": "chirps3_precipitation_daily",
        "cron": "0 6 * * *",
        "timezone": "UTC",
        "enabled": True,
        "publish": True,
        "max_attempts": 3,
        "created_at": now,
        "updated_at": now,
    }
    values.update(updates)
    return DatasetSyncSchedule.model_validate(values)


def _template(**updates: object) -> dict[str, object]:
    values: dict[str, object] = {
        "id": "chirps3_precipitation_daily",
        "sync": {"kind": "temporal", "execution": "append"},
    }
    values.update(updates)
    return values


def _job(*, process_id: str = "scheduled-sync", status: JobStatus = JobStatus.ACCEPTED) -> JobRecord:
    return JobRecord(
        job_id="job-123",
        process_id=process_id,
        status=status,
        created_at=datetime.now(timezone.utc),
        request={"dataset_id": "chirps3_precipitation_daily"},
    )


@pytest.fixture
def schedule_store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    schedules_dir = tmp_path / "schedules"
    monkeypatch.setattr(store, "SCHEDULES_DIR", schedules_dir)
    monkeypatch.setattr(store, "SCHEDULES_INDEX_PATH", schedules_dir / "schedules.json")


def test_scheduler_configuration_is_global_only() -> None:
    config = SchedulerConfig.model_validate({"enabled": True})

    assert config.timezone_info.key == "UTC"
    assert config.max_concurrent_syncs == 1


@pytest.mark.parametrize(
    "payload,match",
    [
        ({"timezone": "Not/A_Zone"}, "timezone"),
        ({"max_concurrent_syncs": 0}, "greater than"),
        ({"dataset_sync": []}, "extra"),
    ],
)
def test_scheduler_configuration_rejects_invalid_values(payload: dict[str, object], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        SchedulerConfig.model_validate(payload)


def test_schedule_validates_cron_and_timezone() -> None:
    with pytest.raises(ValueError, match="cron"):
        _schedule(cron="daily")
    with pytest.raises(ValueError, match="timezone"):
        _schedule(timezone="Not/A_Zone")


def test_store_enforces_one_schedule_per_dataset(schedule_store: None) -> None:
    first = _schedule(cron="0 6 * * *")
    replacement = first.model_copy(update={"cron": "0 7 * * *"})

    store.upsert_schedule(first)
    store.upsert_schedule(replacement)

    assert store.list_schedules() == [replacement]
    assert store.get_schedule(first.dataset_id) == replacement


def test_store_delete_is_durable(schedule_store: None) -> None:
    schedule = _schedule()
    store.upsert_schedule(schedule)

    assert store.delete_schedule(schedule.dataset_id) == schedule
    assert store.get_schedule(schedule.dataset_id) is None


def test_due_schedule_enqueues_without_planning(monkeypatch: pytest.MonkeyPatch) -> None:
    submit = MagicMock(return_value=_job())
    job_service = MagicMock()
    job_service.list_jobs.return_value.jobs = []
    monkeypatch.setattr("open_climate_service.scheduler.dispatcher.api_config.is_read_only", lambda: False)
    monkeypatch.setattr("open_climate_service.scheduler.dispatcher.get_job_service", lambda: job_service)
    monkeypatch.setattr("open_climate_service.scheduler.dispatcher.submit_sync_job", submit)

    result = enqueue_sync(_schedule(max_attempts=4))

    assert result.outcome == CheckOutcome.SUBMITTED
    assert result.job_id == "job-123"
    submit.assert_called_once_with(
        dataset_id="chirps3_precipitation_daily",
        end=None,
        publish=True,
        label="scheduled-sync",
        max_attempts=4,
    )


def test_active_sync_job_prevents_duplicate_enqueue(monkeypatch: pytest.MonkeyPatch) -> None:
    submit = MagicMock()
    job_service = MagicMock()
    job_service.list_jobs.return_value.jobs = [_job(process_id="sync", status=JobStatus.RUNNING)]
    monkeypatch.setattr("open_climate_service.scheduler.dispatcher.api_config.is_read_only", lambda: False)
    monkeypatch.setattr("open_climate_service.scheduler.dispatcher.get_job_service", lambda: job_service)
    monkeypatch.setattr("open_climate_service.scheduler.dispatcher.submit_sync_job", submit)

    result = enqueue_sync(_schedule())

    assert result.outcome == CheckOutcome.ALREADY_RUNNING
    submit.assert_not_called()


def test_service_reconstructs_enabled_schedules_on_start(monkeypatch: pytest.MonkeyPatch) -> None:
    scheduler = MagicMock()
    scheduler.get_job.return_value = None
    monkeypatch.setattr("open_climate_service.scheduler.service.AsyncIOScheduler", lambda **_: scheduler)
    monkeypatch.setattr("open_climate_service.scheduler.service.api_config.is_read_only", lambda: False)
    monkeypatch.setattr("open_climate_service.scheduler.service.store.list_schedules", lambda: [_schedule()])
    monkeypatch.setattr(
        "open_climate_service.scheduler.service.ingestion_services.get_latest_artifact_for_dataset_or_404",
        lambda _: MagicMock(dataset_id="chirps3_precipitation_daily"),
    )
    service = SchedulerService(
        config_loader=lambda: SchedulerConfig(enabled=True),
        template_loader=lambda _: _template(),
    )

    service.start()

    scheduler.add_job.assert_called_once()
    assert scheduler.add_job.call_args.kwargs["coalesce"] is True
    assert scheduler.add_job.call_args.kwargs["max_instances"] == 1
    scheduler.start.assert_called_once_with()


def test_put_persists_and_registers_schedule(monkeypatch: pytest.MonkeyPatch) -> None:
    scheduler = MagicMock()
    scheduler.get_job.return_value = None
    saved: list[DatasetSyncSchedule] = []
    monkeypatch.setattr("open_climate_service.scheduler.service.api_config.is_read_only", lambda: False)
    monkeypatch.setattr("open_climate_service.scheduler.service.store.get_schedule", lambda _: None)
    monkeypatch.setattr("open_climate_service.scheduler.service.store.upsert_schedule", saved.append)
    monkeypatch.setattr(
        "open_climate_service.scheduler.service.ingestion_services.get_latest_artifact_for_dataset_or_404",
        lambda _: MagicMock(dataset_id="chirps3_precipitation_daily"),
    )
    service = SchedulerService(
        config_loader=lambda: SchedulerConfig(enabled=True, timezone="Europe/Oslo"),
        template_loader=lambda _: _template(),
    )
    service._config = SchedulerConfig(enabled=True, timezone="Europe/Oslo")
    service._scheduler = scheduler

    status = service.put("chirps3_precipitation_daily", SchedulePut(cron="0 6 * * *"))

    assert len(saved) == 1
    assert saved[0].timezone == "Europe/Oslo"
    assert status.dataset_id == "chirps3_precipitation_daily"
    scheduler.add_job.assert_called_once()


def test_patch_can_disable_schedule(monkeypatch: pytest.MonkeyPatch) -> None:
    scheduler = MagicMock()
    scheduler.get_job.return_value = MagicMock()
    saved: list[DatasetSyncSchedule] = []
    monkeypatch.setattr("open_climate_service.scheduler.service.api_config.is_read_only", lambda: False)
    monkeypatch.setattr("open_climate_service.scheduler.service.store.get_schedule", lambda _: _schedule())
    monkeypatch.setattr("open_climate_service.scheduler.service.store.upsert_schedule", saved.append)
    service = SchedulerService(config_loader=lambda: SchedulerConfig(enabled=True))
    service._config = SchedulerConfig(enabled=True)
    service._scheduler = scheduler

    status = service.patch("chirps3_precipitation_daily", SchedulePatch(enabled=False))

    assert status.enabled is False
    assert saved[0].enabled is False
    scheduler.remove_job.assert_called_once_with(_schedule().schedule_id)


def test_run_now_enqueues_even_when_schedule_is_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    result = MagicMock(outcome=CheckOutcome.SUBMITTED)
    monkeypatch.setattr("open_climate_service.scheduler.service.api_config.is_read_only", lambda: False)
    monkeypatch.setattr("open_climate_service.scheduler.service.store.get_schedule", lambda _: _schedule(enabled=False))
    service = SchedulerService(config_loader=lambda: SchedulerConfig(enabled=True), dispatcher=lambda _: result)

    assert service.run_now("chirps3_precipitation_daily") is result


def test_create_schedule_endpoint_is_exposed(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    schedule = _schedule(timezone="Europe/Oslo")
    expected = ScheduleStatus(
        schedule_id=schedule.schedule_id,
        dataset_id=schedule.dataset_id,
        cron=schedule.cron,
        timezone=schedule.timezone,
        enabled=True,
        publish=True,
        max_attempts=3,
        created_at=schedule.created_at,
        updated_at=schedule.updated_at,
    )
    service = MagicMock()
    service.put.return_value = expected
    monkeypatch.setattr("open_climate_service.scheduler.routes.get_scheduler_service", lambda: service)

    response = client.put(
        "/schedules/chirps3_precipitation_daily",
        json={"cron": "0 6 * * *", "timezone": "Europe/Oslo"},
    )

    assert response.status_code == 200
    service.put.assert_called_once()


def test_disabled_deployment_rejects_schedule_mutation() -> None:
    service = SchedulerService(config_loader=lambda: SchedulerConfig(enabled=False))

    with pytest.raises(HTTPException) as exc_info:
        service.put("dataset", SchedulePut(cron="0 6 * * *"))

    assert exc_info.value.status_code == 409
