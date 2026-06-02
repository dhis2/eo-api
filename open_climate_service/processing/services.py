"""Services for derived processing workflows."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pandas as pd
from fastapi import HTTPException

from open_climate_service.ingestions import services as ingestion_services
from open_climate_service.ingestions.schemas import DatasetRecord
from open_climate_service.jobs.models import JobCancelledError
from open_climate_service.processing.resample import materialize_resampled_artifact

_SUPPORTED_RESAMPLE_METHODS: frozenset[str] = frozenset({"mean", "sum", "min", "max"})


def _noop_progress(_done: int | None = None, _total: int | None = None, _message: str | None = None) -> None:
    """Default no-op progress callback."""


def _never_cancel() -> bool:
    """Default cancellation probe for synchronous execution."""
    return False


def validate_resample_request(
    *,
    source_dataset_id: str,
    frequency: str | None,
    method: str | None,
    start: str | None,
    **_ignored: Any,
) -> None:
    """Validate resample request parameters before execution or async acceptance."""
    if not source_dataset_id:
        raise HTTPException(status_code=400, detail="source_dataset_id is required")
    if not frequency:
        raise HTTPException(status_code=400, detail="frequency is required")
    if not start:
        raise HTTPException(status_code=400, detail="start is required")
    if method not in _SUPPORTED_RESAMPLE_METHODS:
        supported = ", ".join(sorted(_SUPPORTED_RESAMPLE_METHODS))
        raise HTTPException(status_code=400, detail=f"Unsupported method '{method}'. Supported: {supported}")
    try:
        _offset = pd.tseries.frequencies.to_offset(frequency)
    except (TypeError, ValueError):
        _offset = None
    if _offset is None:
        raise HTTPException(status_code=400, detail=f"Invalid frequency '{frequency}'")


def execute_resample(
    *,
    source_dataset_id: str,
    frequency: str,
    method: str,
    start: str,
    end: str | None = None,
    overwrite: bool = False,
    publish: bool = True,
    on_progress: Callable[[int | None, int | None, str | None], None] | None = None,
    is_cancel_requested: Callable[[], bool] | None = None,
    load_cursor: Callable[[], dict[str, Any] | None] | None = None,
    save_cursor: Callable[[dict[str, Any]], None] | None = None,
    **_ignored: Any,
) -> dict[str, Any]:
    """Execute the resample process; return a JSON-serializable response dict.

    This is the execution_function registered in the process YAML. Public route handlers
    perform request validation before dispatching here.
    """
    progress = on_progress or _noop_progress
    cancel_requested = is_cancel_requested or _never_cancel
    _ = load_cursor, save_cursor

    progress(0, 2, "Validating request")
    if cancel_requested():
        raise JobCancelledError()

    progress(1, 2, "Materializing derived dataset")
    artifact_id, dataset_summary = run_resample_process(
        source_dataset_id=source_dataset_id,
        frequency=frequency,
        method=method,
        start=start,
        end=end,
        overwrite=overwrite,
        publish=publish,
    )
    if cancel_requested():
        raise JobCancelledError()
    progress(2, 2, "Completed")
    return {
        "artifact_id": artifact_id,
        "status": "completed",
        "dataset": dataset_summary.model_dump() if hasattr(dataset_summary, "model_dump") else dataset_summary,
    }


def run_resample_process(
    *,
    source_dataset_id: str,
    frequency: str,
    method: str,
    start: str,
    end: str | None,
    overwrite: bool,
    publish: bool,
) -> tuple[str, DatasetRecord]:
    """Materialize one derived resampled dataset and return its artifact id plus dataset summary."""
    artifact = materialize_resampled_artifact(
        source_dataset_id=source_dataset_id,
        frequency=frequency,
        method=method,
        start=start,
        end=end,
        overwrite=overwrite,
        publish=publish,
    )
    return artifact.artifact_id, ingestion_services.get_dataset_summary_for_artifact_or_404(artifact.artifact_id)
