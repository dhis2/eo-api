"""Minimal workflow runtime helpers for step entry/exit logging."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any, TypeVar

from pygeoapi.process.base import ProcessorExecuteError

LOGGER = logging.getLogger(__name__)
T = TypeVar("T")


def run_step(name: str, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Run a workflow step with consistent logging and error wrapping."""
    LOGGER.info("[chirps3-dhis2-workflow] step=%s start", name)
    try:
        result = fn(*args, **kwargs)
    except ProcessorExecuteError:
        raise
    except Exception as exc:
        raise ProcessorExecuteError(f"Step '{name}' failed: {exc}") from exc
    LOGGER.info("[chirps3-dhis2-workflow] step=%s done", name)
    return result


def run_process_with_trace(
    trace: list[dict[str, Any]],
    *,
    step_name: str,
    processor_cls: type[Any],
    process_name: str,
    data: dict[str, Any],
) -> dict[str, Any]:
    """Execute an OGC processor class as a step and capture workflow trace."""
    start = time.perf_counter()
    processor = processor_cls({"name": process_name})
    try:
        mimetype, output = run_step(step_name, processor.execute, data, None)
    except Exception as exc:
        trace.append(
            {
                "step": step_name,
                "status": "failed",
                "durationMs": round((time.perf_counter() - start) * 1000.0, 2),
                "error": str(exc),
            }
        )
        raise

    trace.append(
        {
            "step": step_name,
            "status": "completed",
            "durationMs": round((time.perf_counter() - start) * 1000.0, 2),
        }
    )
    if mimetype != "application/json":
        raise ProcessorExecuteError(f"Step '{step_name}' returned unsupported mimetype: {mimetype}")
    if not isinstance(output, dict):
        raise ProcessorExecuteError(f"Step '{step_name}' returned non-object output")
    return output
