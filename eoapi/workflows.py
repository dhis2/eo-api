from datetime import UTC, datetime
from threading import Lock
from typing import Any
from uuid import uuid4

from eoapi.state_store import load_state_map, save_state_map


_WORKFLOWS: dict[str, dict[str, Any]] = load_state_map("workflows")
_LOCK = Lock()


def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def create_workflow(payload: dict[str, Any]) -> dict[str, Any]:
    workflow_id = str(uuid4())
    timestamp = _now_iso()
    workflow = {
        "workflowId": workflow_id,
        "name": payload["name"],
        "steps": payload["steps"],
        "created": timestamp,
        "updated": timestamp,
        "lastRunAt": None,
        "lastRunJobIds": [],
    }

    with _LOCK:
        _WORKFLOWS[workflow_id] = workflow
        save_state_map("workflows", _WORKFLOWS)

    return workflow


def list_workflows() -> list[dict[str, Any]]:
    with _LOCK:
        return list(_WORKFLOWS.values())


def get_workflow(workflow_id: str) -> dict[str, Any] | None:
    with _LOCK:
        return _WORKFLOWS.get(workflow_id)


def update_workflow(workflow_id: str, updates: dict[str, Any]) -> dict[str, Any] | None:
    with _LOCK:
        workflow = _WORKFLOWS.get(workflow_id)
        if workflow is None:
            return None

        for key, value in updates.items():
            if value is not None:
                workflow[key] = value

        workflow["updated"] = _now_iso()
        save_state_map("workflows", _WORKFLOWS)
        return workflow


def delete_workflow(workflow_id: str) -> bool:
    with _LOCK:
        if workflow_id not in _WORKFLOWS:
            return False
        del _WORKFLOWS[workflow_id]
        save_state_map("workflows", _WORKFLOWS)
        return True


def mark_workflow_run(workflow_id: str, job_ids: list[str]) -> dict[str, Any] | None:
    with _LOCK:
        workflow = _WORKFLOWS.get(workflow_id)
        if workflow is None:
            return None

        timestamp = _now_iso()
        workflow["lastRunAt"] = timestamp
        workflow["lastRunJobIds"] = job_ids
        workflow["updated"] = timestamp
        save_state_map("workflows", _WORKFLOWS)
        return workflow
