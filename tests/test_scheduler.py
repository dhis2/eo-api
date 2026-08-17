"""CLIM-878 scheduler proof-of-concept tests."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from open_climate_service.ingestions.schemas import SyncAction, SyncDetail, SyncKind
from open_climate_service.jobs.models import JobRecord, JobStatus
from open_climate_service.scheduler.config import DatasetSyncSchedule, SchedulerConfig
from open_climate_service.scheduler.dispatcher import CheckOutcome, check_and_submit
from open_climate_service.scheduler.service import SchedulerService


def _schedule() -> DatasetSyncSchedule:
    return DatasetSyncSchedule(dataset_id="chirps3_precipitation_daily", cron="0 6 * * *")


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


def test_scheduler_configuration_accepts_cron_and_iana_timezone() -> None:
    config = SchedulerConfig.model_validate(
        {
            "enabled": True,
            "timezone": "Europe/Oslo",
            "dataset_sync": [{"dataset_id": "chirps3_precipitation_daily", "cron": "0 6 * * *"}],
        }
    )

    assert config.timezone_info.key == "Europe/Oslo"
    assert config.dataset_sync[0].schedule_id == "dataset-sync:chirps3_precipitation_daily"


@pytest.mark.parametrize(
    "payload,match",
    [
        ({"timezone": "Not/A_Zone"}, "timezone"),
        ({"dataset_sync": [{"dataset_id": "x", "cron": "daily"}]}, "cron"),
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


def test_actionable_check_submits_native_job(monkeypatch: pytest.MonkeyPatch) -> None:
    submit = MagicMock(return_value=_job())
    job_service = MagicMock()
    job_service.list_jobs.return_value.jobs = []
    monkeypatch.setattr("open_climate_service.scheduler.dispatcher.api_config.is_read_only", lambda: False)
    monkeypatch.setattr(
        "open_climate_service.scheduler.dispatcher.services.plan_sync_dataset", lambda **_: _plan(SyncAction.APPEND)
    )
    monkeypatch.setattr("open_climate_service.scheduler.dispatcher.get_job_service", lambda: job_service)
    monkeypatch.setattr("open_climate_service.scheduler.dispatcher.submit_sync_job", submit)

    result = check_and_submit(_schedule())

    assert result.outcome == CheckOutcome.SUBMITTED
    assert result.job_id == "job-123"
    submit.assert_called_once_with(
        dataset_id="chirps3_precipitation_daily",
        end=None,
        publish=True,
        label="scheduled-sync",
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
    monkeypatch.setattr("open_climate_service.scheduler.dispatcher.api_config.is_read_only", lambda: False)

    def missing(**_: object) -> None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    submit = MagicMock()
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
        config_loader=lambda: SchedulerConfig(
            enabled=True,
            timezone="Europe/Oslo",
            dataset_sync=[_schedule()],
        )
    )

    service.start()

    scheduler.add_job.assert_called_once()
    assert scheduler.add_job.call_args.kwargs["coalesce"] is True
    assert scheduler.add_job.call_args.kwargs["max_instances"] == 1
    scheduler.start.assert_called_once_with()
    service.shutdown()
    scheduler.shutdown.assert_called_once_with(wait=False)


def test_service_does_not_start_on_read_only_instance(monkeypatch: pytest.MonkeyPatch) -> None:
    scheduler_factory = MagicMock()
    monkeypatch.setattr("open_climate_service.scheduler.service.AsyncIOScheduler", scheduler_factory)
    monkeypatch.setattr("open_climate_service.scheduler.service.api_config.is_read_only", lambda: True)
    service = SchedulerService(config_loader=lambda: SchedulerConfig(enabled=True, dataset_sync=[_schedule()]))

    service.start()

    scheduler_factory.assert_not_called()
