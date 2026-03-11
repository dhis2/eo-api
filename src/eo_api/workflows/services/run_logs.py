"""Run-log persistence for workflow executions."""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

from ...data_manager.services.downloader import DOWNLOAD_DIR
from ..schemas import ComponentRun, WorkflowExecuteRequest


def persist_run_log(
    *,
    run_id: str,
    request: WorkflowExecuteRequest,
    component_runs: list[ComponentRun],
    status: str,
    output_file: str | None = None,
    error: str | None = None,
) -> str:
    """Write workflow run metadata to disk and return file path."""
    logs_dir = DOWNLOAD_DIR / "workflow_runs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = logs_dir / f"{timestamp}_{run_id}.json"

    payload: dict[str, Any] = {
        "run_id": run_id,
        "status": status,
        "request": request.model_dump(mode="json"),
        "component_runs": [run.model_dump(mode="json") for run in component_runs],
        "output_file": output_file,
        "error": error,
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return str(path)
