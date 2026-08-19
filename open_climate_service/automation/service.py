"""Consume durable dataset updates and submit configured openEO workflows."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from open_climate_service import config as api_config
from open_climate_service.automation.config import AutomationConfig, get_automation_config
from open_climate_service.jobs import store as job_store
from open_climate_service.jobs.models import JobEvent
from open_climate_service.openeo import workflows
from open_climate_service.openeo.jobs import OpenEOJobService, get_openeo_job_service
from open_climate_service.openeo.schemas import OpenEOJobCreate

logger = logging.getLogger(__name__)

_EVENT_VALUES = {
    "$event.dataset_id": "dataset_id",
    "$event.artifact_id": "artifact_id",
    "$event.action": "action",
    "$event.previous_end": "previous_end",
    "$event.current_end": "current_end",
}


def _resolve_event_values(value: Any, event: JobEvent) -> Any:
    """Replace exact event references recursively while preserving literal values."""
    if isinstance(value, str) and value in _EVENT_VALUES:
        return event.data.get(_EVENT_VALUES[value])
    if isinstance(value, list):
        return [_resolve_event_values(item, event) for item in value]
    if isinstance(value, dict):
        return {key: _resolve_event_values(item, event) for key, item in value.items()}
    return value


class WorkflowAutomationService:
    """Dispatch configured workflows once for each matching durable event."""

    def __init__(
        self,
        *,
        config_loader: Callable[[], AutomationConfig] = get_automation_config,
        openeo_service: OpenEOJobService | None = None,
    ) -> None:
        self._config_loader = config_loader
        self._openeo_service = openeo_service
        self._config: AutomationConfig | None = None

    def start(self) -> None:
        """Load configuration and validate every workflow target."""
        self._config = self._config_loader()
        if not self._config.workflow_triggers or api_config.is_read_only():
            return
        for trigger in self._config.workflow_triggers:
            if workflows.get_workflow(trigger.workflow_id) is None:
                raise ValueError(f"Workflow trigger {trigger.id!r} references unknown workflow {trigger.workflow_id!r}")

    def replay(self) -> None:
        """Consume every persisted event; deterministic jobs make replay idempotent."""
        if api_config.is_read_only():
            return
        for record in job_store.list_job_records():
            self.consume(record.events)

    def consume(self, events: list[JobEvent]) -> None:
        """Submit workflows matching the supplied successful-job events."""
        config = self._config or self._config_loader()
        if api_config.is_read_only():
            return
        service = self._openeo_service or get_openeo_job_service()
        for event in events:
            if event.type != "dataset.updated":
                continue
            dataset_id = event.data.get("dataset_id")
            for trigger in config.workflow_triggers:
                if trigger.on_update_of != dataset_id:
                    continue
                arguments = _resolve_event_values(trigger.arguments, event)
                body = OpenEOJobCreate(
                    title=f"{trigger.workflow_id} after {dataset_id} update",
                    description=f"Triggered by {event.event_id} using automation rule {trigger.id}",
                    process={
                        "process_graph": {
                            "workflow": {
                                "process_id": trigger.workflow_id,
                                "arguments": arguments,
                                "result": True,
                            }
                        }
                    },
                )
                job, created = service.create_triggered_job(
                    body,
                    source_event_id=event.event_id,
                    trigger_id=trigger.id,
                )
                service.start_triggered_job(job.id)
                if created:
                    logger.info(
                        "Submitted workflow %s as job %s for event %s",
                        trigger.workflow_id,
                        job.id,
                        event.event_id,
                    )


_service: WorkflowAutomationService | None = None


def get_workflow_automation_service() -> WorkflowAutomationService:
    """Return the process-local automation service singleton."""
    global _service
    if _service is None:
        _service = WorkflowAutomationService()
    return _service
