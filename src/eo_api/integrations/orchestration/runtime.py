"""Minimal workflow runtime helpers for step entry/exit logging."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any, TypeVar

from pygeoapi.process.base import ProcessorExecuteError

LOGGER = logging.getLogger(__name__)
T = TypeVar("T")
_STEP_CONTROL_KEY = "_step_control"
_WORKFLOW_EXIT_KEY = "_workflow_exit"
_WORKFLOW_EXIT_REASON_KEY = "_workflow_exit_reason"
ComponentFn = Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]]


def _pop_step_control(output: dict[str, Any]) -> tuple[str, str | None]:
    """Extract optional step control metadata from component output.

    Supported actions:
    - executed: normal behavior
    - pass_through: step intentionally did no transformation
    - exit: request graceful workflow exit after this step
    """
    raw = output.pop(_STEP_CONTROL_KEY, None)
    if raw is None:
        return "executed", None
    if not isinstance(raw, dict):
        raise ProcessorExecuteError(f"'{_STEP_CONTROL_KEY}' must be an object when provided")
    action = raw.get("action", "executed")
    if action not in {"executed", "pass_through", "exit"}:
        raise ProcessorExecuteError(
            f"Invalid {_STEP_CONTROL_KEY}.action '{action}'. Expected one of: executed, pass_through, exit."
        )
    reason = raw.get("reason")
    if reason is not None and not isinstance(reason, str):
        raise ProcessorExecuteError(f"'{_STEP_CONTROL_KEY}.reason' must be a string when provided")
    return str(action), reason


def should_exit_workflow(step_output: dict[str, Any]) -> tuple[bool, str | None]:
    """Return whether a step requested graceful workflow exit."""
    exit_requested = bool(step_output.get(_WORKFLOW_EXIT_KEY))
    reason = step_output.get(_WORKFLOW_EXIT_REASON_KEY)
    if reason is not None and not isinstance(reason, str):
        reason = str(reason)
    return exit_requested, reason


def run_step(name: str, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """Run a workflow step with consistent logging and error wrapping."""
    LOGGER.info("[workflow-runtime] step=%s start", name)
    try:
        result = fn(*args, **kwargs)
    except ProcessorExecuteError:
        raise
    except Exception as exc:
        raise ProcessorExecuteError(f"Step '{name}' failed: {exc}") from exc
    LOGGER.info("[workflow-runtime] step=%s done", name)
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


def run_planned_component_with_trace(
    trace: list[dict[str, Any]],
    *,
    step_name: str,
    fn: ComponentFn,
    params: dict[str, Any],
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute a component function as a step and capture workflow trace.

    Strict contract:
    - fn(params, context) -> dict
    """
    start = time.perf_counter()
    run_context = dict(context or {})
    try:
        output: Any = run_step(step_name, fn, params, run_context)
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

    if not isinstance(output, dict):
        raise ProcessorExecuteError(f"Step '{step_name}' returned non-object output")
    action, reason = _pop_step_control(output)
    if action == "exit":
        output.setdefault(_WORKFLOW_EXIT_KEY, True)
        if reason:
            output.setdefault(_WORKFLOW_EXIT_REASON_KEY, reason)

    trace_entry: dict[str, Any] = {
        "step": step_name,
        "status": "passed_through" if action == "pass_through" else "completed",
        "durationMs": round((time.perf_counter() - start) * 1000.0, 2),
        "action": action,
    }
    if reason:
        trace_entry["reason"] = reason
    trace.append(trace_entry)
    return output


def run_component_with_trace(
    trace: list[dict[str, Any]],
    *,
    step_name: str,
    fn: Callable[..., Any],
    **kwargs: Any,
) -> dict[str, Any]:
    """Execute legacy kwargs-style step and capture workflow trace."""
    start = time.perf_counter()
    try:
        output: Any = run_step(step_name, fn, **kwargs)
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

    if not isinstance(output, dict):
        raise ProcessorExecuteError(f"Step '{step_name}' returned non-object output")
    action, reason = _pop_step_control(output)
    if action == "exit":
        output.setdefault(_WORKFLOW_EXIT_KEY, True)
        if reason:
            output.setdefault(_WORKFLOW_EXIT_REASON_KEY, reason)

    trace_entry: dict[str, Any] = {
        "step": step_name,
        "status": "passed_through" if action == "pass_through" else "completed",
        "durationMs": round((time.perf_counter() - start) * 1000.0, 2),
        "action": action,
    }
    if reason:
        trace_entry["reason"] = reason
    trace.append(trace_entry)
    return output
