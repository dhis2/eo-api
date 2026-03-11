"""Component runtime wrapper for workflow housekeeping metadata."""

from __future__ import annotations

import datetime as dt
import time
import uuid
from collections.abc import Callable
from typing import Any

from ..schemas import ComponentRun


class WorkflowRuntime:
    """Capture execution metadata for component orchestration."""

    def __init__(self) -> None:
        self.run_id = str(uuid.uuid4())
        self.component_runs: list[ComponentRun] = []

    def run(self, component: str, fn: Callable[..., Any], **kwargs: Any) -> Any:
        """Execute one component and record start/end/input/output metadata."""
        started = dt.datetime.now(dt.timezone.utc)
        started_perf = time.perf_counter()

        try:
            result = fn(**kwargs)
            ended = dt.datetime.now(dt.timezone.utc)
            self.component_runs.append(
                ComponentRun(
                    component=component,
                    status="completed",
                    started_at=started.isoformat(),
                    ended_at=ended.isoformat(),
                    duration_ms=int((time.perf_counter() - started_perf) * 1000),
                    inputs=_to_json_summary(kwargs),
                    outputs={"result": _to_json_summary(result)},
                )
            )
            return result
        except Exception as exc:
            ended = dt.datetime.now(dt.timezone.utc)
            self.component_runs.append(
                ComponentRun(
                    component=component,
                    status="failed",
                    started_at=started.isoformat(),
                    ended_at=ended.isoformat(),
                    duration_ms=int((time.perf_counter() - started_perf) * 1000),
                    inputs=_to_json_summary(kwargs),
                    outputs=None,
                    error=str(exc),
                )
            )
            raise


def _to_json_summary(value: Any, *, depth: int = 0, max_depth: int = 2) -> Any:
    """Convert arbitrary values into a compact JSON-safe summary."""
    if depth >= max_depth:
        return _fallback_summary(value)

    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, list):
        return [_to_json_summary(v, depth=depth + 1, max_depth=max_depth) for v in value[:20]]

    if isinstance(value, tuple):
        return [_to_json_summary(v, depth=depth + 1, max_depth=max_depth) for v in value[:20]]

    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for i, (k, v) in enumerate(value.items()):
            if i >= 30:
                out["..."] = "truncated"
                break
            out[str(k)] = _to_json_summary(v, depth=depth + 1, max_depth=max_depth)
        return out

    return _fallback_summary(value)


def _fallback_summary(value: Any) -> str:
    if hasattr(value, "shape"):
        return f"{type(value).__name__}(shape={getattr(value, 'shape')})"
    if hasattr(value, "sizes"):
        return f"{type(value).__name__}(sizes={getattr(value, 'sizes')})"
    return type(value).__name__
