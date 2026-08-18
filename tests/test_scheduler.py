"""Scheduled dataset synchronization tests (CLIM-849)."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from open_climate_service.ingestions.schemas import SyncAction, SyncDetail, SyncKind
from open_climate_service.jobs.models import JobRecord, JobStatus
from open_climate_service.scheduler.config import DatasetSyncSchedule, SchedulerConfig
from open_climate_service.scheduler.dispatcher import CheckOutcome, CheckResult, check_and_submit
from open_climate_service.scheduler.service import SchedulerService


def _schedule(**updates: object) -> DatasetSyncSchedule:
    values: dict[str, object] = {
        "dataset_id": "chirps3_precipitation_daily",
        "cron": "0 6 * * *",
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


def _plan(action: SyncAction) -> SyncDetail:
    return SyncDetail(
        source_dataset_id="chirps3_precipitation_daily",
        sync_kind=SyncKind.TEMPORAL,
        action=action,
        reason=action.value,
        message=f"Planner selected {action.value}",
        target_end_source="current_coverage",
    )


def _job(*, process_id: str = "scheduled-sync", status: JobStatus = JobStatus.ACCEPTED) -> JobRecord:
    return JobRecord(
        job_id="job-123",
        process_id=process_id,
        status=status,
        created_at=datetime.now(timezone.utc),
        request={"dataset_id": "chirps3_precipitation_daily"},
    )


def test_scheduler_configuration_defaults_to_utc_and_three_attempts() -> None:
    config = SchedulerConfig.model_validate(
        {"enabled": True, "dataset_sync": [{"dataset_id": "chirps3_precipitation_daily", "cron": "0 6 * * *"}]}
    )

    assert config.timezone_info.key == "UTC"
    assert config.dataset_sync[0].max_attempts == 3
    assert config.dataset_sync[0].schedule_id == "dataset-sync:chirps3_precipitation_daily"


@pytest.mark.parametrize(
    "payload,match",
    [
        ({"timezone": "Not/A_Zone"}, "timezone"),
        ({"dataset_sync": [{"dataset_id": "x", "cron": "daily"}]}, "cron"),
        ({"dataset_sync": [{"dataset_id": "x", "cron": "0 6 * * *", "max_attempts": 0}]}, "greater than"),
        (
            {
                "dataset_sync": [
                    {"dataset_id": "x", "cron": "0 6 * * *"},
                    {"dataset_id": "x", "cron": "0 7 * * *"},
                ]
            },
            "one scheduler entry",
        ),
    ],
)
def test_scheduler_configuration_rejects_invalid_values(payload: dict[str, object], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        SchedulerConfig.model_validate(payload)


def test_up_to_date_check_does_not_submit(monkeypatch: pytest.MonkeyPatch) -> None:
    submit = MagicMock()
    monkeypatch.setattr("open_climate_service.scheduler.dispatcher.api_config.is_read_only", lambda: False)
    monkeypatch.setattr(
        "open_climate_service.scheduler.dispatcher.services.plan_sync_dataset", lambda **_: _plan(SyncAction.NO_OP)
    )
    monkeypatch.setattr("open_climate_service.scheduler.dispatcher.submit_sync_job", submit)

    result = check_and_submit(_schedule())

    assert result.outcome == CheckOutcome.UP_TO_DATE
    submit.assert_not_called()


def test_actionable_check_submits_retryable_native_job(monkeypatch: pytest.MonkeyPatch) -> None:
    submit = MagicMock(return_value=_job())
    job_service = MagicMock()
    job_service.list_jobs.return_value.jobs = []
    monkeypatch.setattr("open_climate_service.scheduler.dispatcher.api_config.is_read_only", lambda: False)
    monkeypatch.setattr(
        "open_climate_service.scheduler.dispatcher.services.plan_sync_dataset", lambda **_: _plan(SyncAction.APPEND)
    )
    monkeypatch.setattr("open_climate_service.scheduler.dispatcher.get_job_service", lambda: job_service)
    monkeypatch.setattr("open_climate_service.scheduler.dispatcher.submit_sync_job", submit)

    result = check_and_submit(_schedule(max_attempts=4))

    assert result.outcome == CheckOutcome.SUBMITTED
    assert result.job_id == "job-123"
    submit.assert_called_once_with(
        dataset_id="chirps3_precipitation_daily",
        end=None,
        publish=True,
        label="scheduled-sync",
        max_attempts=4,
    )


def test_active_sync_job_prevents_duplicate_submission(monkeypatch: pytest.MonkeyPatch) -> None:
    submit = MagicMock()
    job_service = MagicMock()
    job_service.list_jobs.return_value.jobs = [_job(process_id="sync", status=JobStatus.RUNNING)]
    monkeypatch.setattr("open_climate_service.scheduler.dispatcher.api_config.is_read_only", lambda: False)
    monkeypatch.setattr(
        "open_climate_service.scheduler.dispatcher.services.plan_sync_dataset", lambda **_: _plan(SyncAction.APPEND)
    )
    monkeypatch.setattr("open_climate_service.scheduler.dispatcher.get_job_service", lambda: job_service)
    monkeypatch.setattr("open_climate_service.scheduler.dispatcher.submit_sync_job", submit)

    result = check_and_submit(_schedule())

    assert result.outcome == CheckOutcome.ALREADY_RUNNING
    submit.assert_not_called()


def test_read_only_check_neither_plans_nor_submits(monkeypatch: pytest.MonkeyPatch) -> None:
    plan = MagicMock()
    submit = MagicMock()
    monkeypatch.setattr("open_climate_service.scheduler.dispatcher.api_config.is_read_only", lambda: True)
    monkeypatch.setattr("open_climate_service.scheduler.dispatcher.services.plan_sync_dataset", plan)
    monkeypatch.setattr("open_climate_service.scheduler.dispatcher.submit_sync_job", submit)

    result = check_and_submit(_schedule())

    assert result.outcome == CheckOutcome.READ_ONLY
    plan.assert_not_called()
    submit.assert_not_called()


def test_missing_managed_dataset_is_reported_without_submission(monkeypatch: pytest.MonkeyPatch) -> None:
    def missing(**_: object) -> None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    submit = MagicMock()
    monkeypatch.setattr("open_climate_service.scheduler.dispatcher.api_config.is_read_only", lambda: False)
    monkeypatch.setattr("open_climate_service.scheduler.dispatcher.services.plan_sync_dataset", missing)
    monkeypatch.setattr("open_climate_service.scheduler.dispatcher.submit_sync_job", submit)

    result = check_and_submit(_schedule())

    assert result.outcome == CheckOutcome.NOT_MATERIALIZED
    submit.assert_not_called()


def test_service_registers_coalesced_non_overlapping_cron_job(monkeypatch: pytest.MonkeyPatch) -> None:
    scheduler = MagicMock()
    monkeypatch.setattr("open_climate_service.scheduler.service.AsyncIOScheduler", lambda **_: scheduler)
    monkeypatch.setattr("open_climate_service.scheduler.service.api_config.is_read_only", lambda: False)
    service = SchedulerService(
        config_loader=lambda: SchedulerConfig(enabled=True, dataset_sync=[_schedule()]),
        template_loader=lambda _: _template(),
    )

    service.start()

    scheduler.add_job.assert_called_once()
    assert scheduler.add_job.call_args.kwargs["coalesce"] is True
    assert scheduler.add_job.call_args.kwargs["max_instances"] == 1
    scheduler.start.assert_called_once_with()
    service.shutdown()
    scheduler.shutdown.assert_called_once_with(wait=False)


def test_service_rejects_future_facing_schedule(monkeypatch: pytest.MonkeyPatch) -> None:
    scheduler_factory = MagicMock()
    monkeypatch.setattr("open_climate_service.scheduler.service.AsyncIOScheduler", scheduler_factory)
    monkeypatch.setattr("open_climate_service.scheduler.service.api_config.is_read_only", lambda: False)
    service = SchedulerService(
        config_loader=lambda: SchedulerConfig(enabled=True, dataset_sync=[_schedule()]),
        template_loader=lambda _: _template(temporal_direction="future"),
    )

    with pytest.raises(ValueError, match="future-facing|forecast"):
        service.start()

    scheduler_factory.assert_not_called()


def test_service_rejects_static_schedule(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("open_climate_service.scheduler.service.api_config.is_read_only", lambda: False)
    service = SchedulerService(
        config_loader=lambda: SchedulerConfig(enabled=True, dataset_sync=[_schedule()]),
        template_loader=lambda _: _template(sync={"kind": "static"}),
    )

    with pytest.raises(ValueError, match="not syncable"):
        service.start()


def test_service_does_not_start_on_read_only_instance(monkeypatch: pytest.MonkeyPatch) -> None:
    scheduler_factory = MagicMock()
    monkeypatch.setattr("open_climate_service.scheduler.service.AsyncIOScheduler", scheduler_factory)
    monkeypatch.setattr("open_climate_service.scheduler.service.api_config.is_read_only", lambda: True)
    service = SchedulerService(config_loader=lambda: SchedulerConfig(enabled=True, dataset_sync=[_schedule()]))

    service.start()

    scheduler_factory.assert_not_called()


def test_status_exposes_next_and_last_check(monkeypatch: pytest.MonkeyPatch) -> None:
    next_check = datetime(2026, 8, 19, 6, tzinfo=timezone.utc)
    scheduler = MagicMock()
    scheduler.get_jobs.return_value = [MagicMock(id=_schedule().schedule_id, next_run_time=next_check)]
    monkeypatch.setattr("open_climate_service.scheduler.service.AsyncIOScheduler", lambda **_: scheduler)
    monkeypatch.setattr("open_climate_service.scheduler.service.api_config.is_read_only", lambda: False)
    result = CheckResult(
        schedule_id=_schedule().schedule_id,
        dataset_id=_schedule().dataset_id,
        outcome=CheckOutcome.SUBMITTED,
        message="submitted",
        job_id="job-123",
    )
    service = SchedulerService(
        config_loader=lambda: SchedulerConfig(enabled=True, dataset_sync=[_schedule()]),
        dispatcher=lambda _: result,
        template_loader=lambda _: _template(),
    )
    service.start()
    service.check_now(_schedule())

    status = service.status()

    assert status.running is True
    assert status.schedules[0].next_check == next_check
    assert status.schedules[0].last_outcome == CheckOutcome.SUBMITTED
    assert status.schedules[0].last_job_id == "job-123"


def test_check_error_is_retained_without_escaping() -> None:
    def fail(_: DatasetSyncSchedule) -> CheckResult:
        raise RuntimeError("source unavailable")

    service = SchedulerService(
        config_loader=lambda: SchedulerConfig(enabled=True, dataset_sync=[_schedule()]),
        dispatcher=fail,
        template_loader=lambda _: _template(),
    )

    result = service.check_now(_schedule())

    assert result.outcome == CheckOutcome.ERROR
    assert "source unavailable" in result.message


def test_schedules_endpoint_is_read_only_status(client: TestClient) -> None:
    response = client.get("/schedules")

    assert response.status_code == 200
    assert response.json() == {
        "enabled": False,
        "running": False,
        "timezone": "UTC",
        "schedules": [],
    }
