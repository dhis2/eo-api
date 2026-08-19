"""Tests for durable dataset-update workflow automation."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from open_climate_service.automation.config import AutomationConfig, WorkflowTrigger
from open_climate_service.automation.service import WorkflowAutomationService, _resolve_event_values
from open_climate_service.jobs.models import JobEvent
from open_climate_service.openeo.jobs import OpenEOJobService
from open_climate_service.openeo.schemas import OpenEOJobCreate, OpenEOJobRecord, OpenEOJobStatus


def _event(dataset_id: str = "chirps") -> JobEvent:
    return JobEvent(
        event_id="native-job:0",
        time=datetime(2026, 8, 19, tzinfo=UTC),
        type="dataset.updated",
        source=f"/datasets/{dataset_id}",
        data={
            "dataset_id": dataset_id,
            "artifact_id": "artifact-2",
            "action": "append",
            "previous_end": "2026-08-17",
            "current_end": "2026-08-18",
        },
    )


def _config() -> AutomationConfig:
    return AutomationConfig(
        workflow_triggers=[
            WorkflowTrigger(
                id="chap-after-chirps",
                on_update_of="chirps",
                workflow_id="aggregate_to_chap_csv",
                arguments={
                    "dataset_id": "$event.dataset_id",
                    "temporal_extent": ["$event.previous_end", "$event.current_end"],
                    "method": "mean",
                },
            )
        ]
    )


def _openeo_record(status: OpenEOJobStatus = OpenEOJobStatus.CREATED) -> OpenEOJobRecord:
    return OpenEOJobRecord(
        id="triggered-job",
        process={"process_graph": {}},
        status=status,
        created=datetime(2026, 8, 19, tzinfo=UTC),
    )


def test_trigger_ids_must_be_unique() -> None:
    trigger = _config().workflow_triggers[0]
    with pytest.raises(ValidationError, match="must be unique"):
        AutomationConfig(workflow_triggers=[trigger, trigger])


def test_event_references_are_resolved_recursively() -> None:
    assert _resolve_event_values(
        {"source": "$event.dataset_id", "range": ["$event.previous_end", "$event.current_end"]},
        _event(),
    ) == {"source": "chirps", "range": ["2026-08-17", "2026-08-18"]}


def test_matching_update_submits_and_starts_workflow_once() -> None:
    openeo = MagicMock()
    openeo.create_triggered_job.return_value = (_openeo_record(), True)
    service = WorkflowAutomationService(config_loader=_config, openeo_service=openeo)

    service.consume([_event()])

    body = openeo.create_triggered_job.call_args.args[0]
    node = body.process["process_graph"]["workflow"]
    assert node == {
        "process_id": "aggregate_to_chap_csv",
        "arguments": {
            "dataset_id": "chirps",
            "temporal_extent": ["2026-08-17", "2026-08-18"],
            "method": "mean",
        },
        "result": True,
    }
    assert openeo.create_triggered_job.call_args.kwargs == {
        "source_event_id": "native-job:0",
        "trigger_id": "chap-after-chirps",
    }
    openeo.start_triggered_job.assert_called_once_with("triggered-job")


def test_nonmatching_and_non_update_events_are_ignored() -> None:
    openeo = MagicMock()
    service = WorkflowAutomationService(config_loader=_config, openeo_service=openeo)
    other = _event("era5")
    unrelated = other.model_copy(update={"type": "dataset.created"})

    service.consume([other, unrelated])

    openeo.create_triggered_job.assert_not_called()


def test_replay_starts_an_existing_created_job_after_interrupted_submission() -> None:
    openeo = MagicMock()
    openeo.create_triggered_job.return_value = (_openeo_record(), False)
    service = WorkflowAutomationService(config_loader=_config, openeo_service=openeo)

    service.consume([_event()])

    openeo.start_triggered_job.assert_called_once_with("triggered-job")


def test_replay_delegates_atomic_claim_for_an_existing_queued_job() -> None:
    openeo = MagicMock()
    openeo.create_triggered_job.return_value = (_openeo_record(OpenEOJobStatus.QUEUED), False)
    service = WorkflowAutomationService(config_loader=_config, openeo_service=openeo)

    service.consume([_event()])

    openeo.start_triggered_job.assert_called_once_with("triggered-job")


def test_triggered_job_creation_is_deterministic_and_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    records: list[dict[str, object]] = []

    def mutate(mutation: Callable[[list[dict[str, object]]], Any]) -> Any:
        return mutation(records)

    monkeypatch.setattr("open_climate_service.openeo.jobs._mutate_store", mutate)
    service = OpenEOJobService()
    body = OpenEOJobCreate(process={"process_graph": {"result": {"process_id": "constant"}}})
    try:
        first, first_created = service.create_triggered_job(
            body,
            source_event_id="native-job:0",
            trigger_id="chap-after-chirps",
        )
        second, second_created = service.create_triggered_job(
            body,
            source_event_id="native-job:0",
            trigger_id="chap-after-chirps",
        )
    finally:
        service.shutdown()

    assert first.id == second.id
    assert first_created is True
    assert second_created is False
    assert len(records) == 1


def test_triggered_job_start_is_an_atomic_one_time_claim(monkeypatch: pytest.MonkeyPatch) -> None:
    records: list[dict[str, object]] = []

    def mutate(mutation: Callable[[list[dict[str, object]]], Any]) -> Any:
        return mutation(records)

    monkeypatch.setattr("open_climate_service.openeo.jobs._mutate_store", mutate)
    service = OpenEOJobService()
    enqueue = MagicMock()
    monkeypatch.setattr(service, "_enqueue", enqueue)
    body = OpenEOJobCreate(process={"process_graph": {"result": {"process_id": "constant"}}})
    try:
        job, _ = service.create_triggered_job(
            body,
            source_event_id="native-job:0",
            trigger_id="chap-after-chirps",
        )

        assert service.start_triggered_job(job.id) is True
        assert service.start_triggered_job(job.id) is False
        enqueue.assert_called_once_with(job.id)
    finally:
        service.shutdown()
