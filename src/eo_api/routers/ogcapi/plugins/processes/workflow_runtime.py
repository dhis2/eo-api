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


def run_step_with_trace(
    trace: list[dict[str, Any]],
    name: str,
    fn: Callable[..., T],
    *args: Any,
    **kwargs: Any,
) -> T:
    """Run a step and append status/duration details to workflow trace."""
    start = time.perf_counter()
    try:
        result = run_step(name, fn, *args, **kwargs)
    except Exception as exc:
        duration_ms = round((time.perf_counter() - start) * 1000.0, 2)
        trace.append(
            {
                "step": name,
                "status": "failed",
                "durationMs": duration_ms,
                "error": str(exc),
            }
        )
        raise

    duration_ms = round((time.perf_counter() - start) * 1000.0, 2)
    trace.append(
        {
            "step": name,
            "status": "completed",
            "durationMs": duration_ms,
        }
    )
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
    processor = processor_cls({"name": process_name})
    mimetype, output = run_step_with_trace(trace, step_name, processor.execute, data, None)
    if mimetype != "application/json":
        raise ProcessorExecuteError(f"Step '{step_name}' returned unsupported mimetype: {mimetype}")
    if not isinstance(output, dict):
        raise ProcessorExecuteError(f"Step '{step_name}' returned non-object output")
    return output
