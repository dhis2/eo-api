"""Workflow orchestration engine for gridded-data pipelines."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

import httpx
from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, ValidationError

from ...components import services as component_services
from ...data_registry.services.datasets import get_dataset
from ..schemas import WorkflowExecuteRequest, WorkflowExecuteResponse
from .definitions import WorkflowDefinition, load_workflow_definition
from .run_logs import persist_run_log
from .runtime import WorkflowRuntime


class WorkflowComponentError(RuntimeError):
    """Typed component failure with stable error code and component context."""

    def __init__(
        self,
        *,
        error_code: str,
        message: str,
        component: str,
        component_version: str,
        status_code: int,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.component = component
        self.component_version = component_version
        self.status_code = status_code


def execute_workflow(
    request: WorkflowExecuteRequest,
    *,
    workflow_id: str = "dhis2_datavalue_set_v1",
    workflow_definition: WorkflowDefinition | None = None,
    request_params: dict[str, Any] | None = None,
    include_component_run_details: bool = False,
) -> WorkflowExecuteResponse:
    """Execute the feature->download->aggregate->DataValueSet workflow."""
    runtime = WorkflowRuntime()

    dataset = get_dataset(request.dataset_id)
    if dataset is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{request.dataset_id}' not found")

    context: dict[str, Any] = {}

    try:
        if workflow_definition is not None:
            workflow = workflow_definition
        else:
            try:
                workflow = load_workflow_definition(workflow_id)
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
        _execute_workflow_steps(
            workflow=workflow,
            runtime=runtime,
            request=request,
            request_params=request_params,
            dataset=dataset,
            context=context,
        )
        features = _require_context(context, "features")
        bbox = _require_context(context, "bbox")
        data_value_set = _require_context(context, "data_value_set")
        output_file = _require_context(context, "output_file")
        run_log_file = persist_run_log(
            run_id=runtime.run_id,
            request=request,
            component_runs=runtime.component_runs,
            status="completed",
            output_file=output_file,
        )

        return WorkflowExecuteResponse(
            status="completed",
            run_id=runtime.run_id,
            workflow_id=workflow.workflow_id,
            workflow_version=workflow.version,
            dataset_id=request.dataset_id,
            bbox=bbox,
            feature_count=len(features["features"]),
            value_count=len(data_value_set["dataValues"]),
            output_file=output_file,
            run_log_file=run_log_file,
            data_value_set=data_value_set,
            component_runs=runtime.component_runs if include_component_run_details else [],
            component_run_details_included=include_component_run_details,
            component_run_details_available=True,
        )
    except WorkflowComponentError as exc:
        persist_run_log(
            run_id=runtime.run_id,
            request=request,
            component_runs=runtime.component_runs,
            status="failed",
            error=str(exc),
            error_code=exc.error_code,
            failed_component=exc.component,
            failed_component_version=exc.component_version,
        )
        error = "upstream_unreachable" if exc.error_code == "UPSTREAM_UNREACHABLE" else "workflow_execution_failed"
        raise HTTPException(
            status_code=exc.status_code,
            detail={
                "error": error,
                "error_code": exc.error_code,
                "message": str(exc),
                "failed_component": exc.component,
                "failed_component_version": exc.component_version,
                "run_id": runtime.run_id,
            },
        ) from exc
    except HTTPException:
        persist_run_log(
            run_id=runtime.run_id,
            request=request,
            component_runs=runtime.component_runs,
            status="failed",
            error="http_exception",
        )
        raise
    except Exception as exc:
        persist_run_log(
            run_id=runtime.run_id,
            request=request,
            component_runs=runtime.component_runs,
            status="failed",
            error=str(exc),
            error_code="EXECUTION_FAILED",
        )
        last_component = runtime.component_runs[-1].component if runtime.component_runs else "unknown"
        raise HTTPException(
            status_code=500,
            detail={
                "error": "workflow_execution_failed",
                "error_code": "EXECUTION_FAILED",
                "message": str(exc),
                "failed_component": last_component,
                "failed_component_version": "unknown",
                "run_id": runtime.run_id,
            },
        ) from exc


def _is_upstream_connectivity_error(exc: Exception) -> bool:
    message = str(exc).lower()
    patterns = (
        "could not connect to server",
        "failed to connect",
        "connection refused",
        "name or service not known",
        "temporary failure in name resolution",
        "timed out",
        "curl error",
    )
    return any(pattern in message for pattern in patterns)


def _execute_workflow_steps(
    *,
    workflow: WorkflowDefinition,
    runtime: WorkflowRuntime,
    request: WorkflowExecuteRequest,
    request_params: dict[str, Any] | None,
    dataset: dict[str, Any],
    context: dict[str, Any],
) -> None:
    """Execute workflow components using declarative YAML step order."""
    executors: dict[str, StepExecutor] = {
        "feature_source": _run_feature_source,
        "download_dataset": _run_download_dataset,
        "temporal_aggregation": _run_temporal_aggregation,
        "spatial_aggregation": _run_spatial_aggregation,
        "build_datavalueset": _run_build_datavalueset,
    }

    for step in workflow.steps:
        executor = executors.get(step.component)
        if executor is None:
            raise WorkflowComponentError(
                error_code="INPUT_VALIDATION_FAILED",
                message=f"Unsupported workflow component '{step.component}'",
                component=step.component,
                component_version=step.version,
                status_code=422,
            )
        try:
            step_config = _resolve_step_config(step.config, request_params or {})
            _validate_step_config(step.component, step.version, step_config)
        except ValueError as exc:
            raise WorkflowComponentError(
                error_code="CONFIG_VALIDATION_FAILED",
                message=str(exc),
                component=step.component,
                component_version=step.version,
                status_code=422,
            ) from exc

        try:
            updates = executor(
                runtime=runtime,
                request=request,
                dataset=dataset,
                context=context,
                step_config=step_config,
            )
        except Exception as exc:
            if _is_upstream_connectivity_error(exc):
                raise WorkflowComponentError(
                    error_code="UPSTREAM_UNREACHABLE",
                    message="Could not reach upstream data source. Check network/proxy and retry.",
                    component=step.component,
                    component_version=step.version,
                    status_code=503,
                ) from exc
            raise WorkflowComponentError(
                error_code="EXECUTION_FAILED",
                message=str(exc),
                component=step.component,
                component_version=step.version,
                status_code=500,
            ) from exc

        context.update(updates)


def validate_workflow_steps(
    *,
    workflow: WorkflowDefinition,
    request_params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Resolve and validate step configs without executing components."""
    resolved_steps: list[dict[str, Any]] = []
    params = request_params or {}
    for index, step in enumerate(workflow.steps):
        try:
            resolved_config = _resolve_step_config(step.config, params)
            _validate_step_config(step.component, step.version, resolved_config)
        except ValueError as exc:
            raise ValueError(f"Step {index + 1} ({step.component}@{step.version}) validation failed: {exc}") from exc
        resolved_steps.append(
            {
                "index": index + 1,
                "component": step.component,
                "version": step.version,
                "resolved_config": resolved_config,
            }
        )
    return resolved_steps


type StepExecutor = Callable[..., dict[str, Any]]


def _run_feature_source(
    *,
    runtime: WorkflowRuntime,
    request: WorkflowExecuteRequest,
    dataset: dict[str, Any],
    context: dict[str, Any],
    step_config: dict[str, Any],
) -> dict[str, Any]:
    del dataset, context
    execution_mode = str(step_config.get("execution_mode", "local")).lower()
    if execution_mode == "remote":
        features, bbox = runtime.run(
            "feature_source",
            _invoke_remote_feature_source_component,
            remote_url=str(step_config["remote_url"]),
            feature_source=request.feature_source.model_dump(mode="json"),
            timeout_sec=float(step_config.get("remote_timeout_sec", 30.0)),
            retries=int(step_config.get("remote_retries", 1)),
            retry_delay_sec=float(step_config.get("remote_retry_delay_sec", 1.0)),
        )
    else:
        features, bbox = runtime.run(
            "feature_source",
            component_services.feature_source_component,
            config=request.feature_source,
        )
    return {"features": features, "bbox": bbox}


def _run_download_dataset(
    *,
    runtime: WorkflowRuntime,
    request: WorkflowExecuteRequest,
    dataset: dict[str, Any],
    context: dict[str, Any],
    step_config: dict[str, Any],
) -> dict[str, Any]:
    execution_mode = str(step_config.get("execution_mode", "local")).lower()
    if execution_mode not in {"local", "remote"}:
        raise ValueError("download_dataset.execution_mode must be 'local' or 'remote'")

    overwrite = request.overwrite
    country_code = request.country_code
    bbox = _require_context(context, "bbox")
    if execution_mode == "remote":
        remote_url = step_config.get("remote_url")
        if not isinstance(remote_url, str) or not remote_url:
            raise ValueError("download_dataset remote mode requires non-empty 'remote_url'")
        remote_timeout = float(step_config.get("remote_timeout_sec", 30.0))
        remote_retries = int(step_config.get("remote_retries", 1))
        remote_retry_delay_sec = float(step_config.get("remote_retry_delay_sec", 1.0))
        runtime.run(
            "download_dataset",
            _invoke_remote_download_component,
            remote_url=remote_url,
            dataset_id=request.dataset_id,
            start=request.start,
            end=request.end,
            overwrite=overwrite,
            country_code=country_code,
            bbox=bbox,
            timeout_sec=remote_timeout,
            retries=remote_retries,
            retry_delay_sec=remote_retry_delay_sec,
        )
    else:
        runtime.run(
            "download_dataset",
            component_services.download_dataset_component,
            dataset=dataset,
            start=request.start,
            end=request.end,
            overwrite=overwrite,
            country_code=country_code,
            bbox=bbox,
        )
    return {}


def _run_temporal_aggregation(
    *,
    runtime: WorkflowRuntime,
    request: WorkflowExecuteRequest,
    dataset: dict[str, Any],
    context: dict[str, Any],
    step_config: dict[str, Any],
) -> dict[str, Any]:
    target_period_type = request.temporal_aggregation.target_period_type
    method = request.temporal_aggregation.method
    execution_mode = str(step_config.get("execution_mode", "local")).lower()
    if execution_mode == "remote":
        temporal_ds = runtime.run(
            "temporal_aggregation",
            _invoke_remote_temporal_aggregation_component,
            remote_url=str(step_config["remote_url"]),
            dataset_id=request.dataset_id,
            start=request.start,
            end=request.end,
            bbox=_require_context(context, "bbox"),
            target_period_type=target_period_type.value,
            method=method.value,
            timeout_sec=float(step_config.get("remote_timeout_sec", 30.0)),
            retries=int(step_config.get("remote_retries", 1)),
            retry_delay_sec=float(step_config.get("remote_retry_delay_sec", 1.0)),
        )
    else:
        temporal_ds = runtime.run(
            "temporal_aggregation",
            component_services.temporal_aggregation_component,
            dataset=dataset,
            start=request.start,
            end=request.end,
            bbox=_require_context(context, "bbox"),
            target_period_type=target_period_type,
            method=method,
        )
    return {"temporal_dataset": temporal_ds}


def _run_spatial_aggregation(
    *,
    runtime: WorkflowRuntime,
    request: WorkflowExecuteRequest,
    dataset: dict[str, Any],
    context: dict[str, Any],
    step_config: dict[str, Any],
) -> dict[str, Any]:
    method = request.spatial_aggregation.method
    feature_id_property = request.dhis2.org_unit_property
    execution_mode = str(step_config.get("execution_mode", "local")).lower()
    if execution_mode == "remote":
        records = runtime.run(
            "spatial_aggregation",
            _invoke_remote_spatial_aggregation_component,
            remote_url=str(step_config["remote_url"]),
            dataset_id=request.dataset_id,
            start=request.start,
            end=request.end,
            bbox=_require_context(context, "bbox"),
            feature_source=request.feature_source.model_dump(mode="json"),
            method=method.value,
            feature_id_property=feature_id_property,
            timeout_sec=float(step_config.get("remote_timeout_sec", 30.0)),
            retries=int(step_config.get("remote_retries", 1)),
            retry_delay_sec=float(step_config.get("remote_retry_delay_sec", 1.0)),
        )
    else:
        records = runtime.run(
            "spatial_aggregation",
            component_services.spatial_aggregation_component,
            dataset=dataset,
            start=request.start,
            end=request.end,
            bbox=_require_context(context, "bbox"),
            features=_require_context(context, "features"),
            method=method,
            feature_id_property=feature_id_property,
        )
    return {"records": records}


def _run_build_datavalueset(
    *,
    runtime: WorkflowRuntime,
    request: WorkflowExecuteRequest,
    dataset: dict[str, Any],
    context: dict[str, Any],
    step_config: dict[str, Any],
) -> dict[str, Any]:
    del dataset
    period_type = request.temporal_aggregation.target_period_type
    execution_mode = str(step_config.get("execution_mode", "local")).lower()
    if execution_mode == "remote":
        data_value_set, output_file = runtime.run(
            "build_datavalueset",
            _invoke_remote_build_datavalueset_component,
            remote_url=str(step_config["remote_url"]),
            dataset_id=request.dataset_id,
            period_type=period_type.value,
            records=_require_context(context, "records"),
            dhis2=request.dhis2.model_dump(mode="json"),
            timeout_sec=float(step_config.get("remote_timeout_sec", 30.0)),
            retries=int(step_config.get("remote_retries", 1)),
            retry_delay_sec=float(step_config.get("remote_retry_delay_sec", 1.0)),
        )
    else:
        data_value_set, output_file = runtime.run(
            "build_datavalueset",
            component_services.build_datavalueset_component,
            records=_require_context(context, "records"),
            dataset_id=request.dataset_id,
            period_type=period_type,
            dhis2=request.dhis2,
        )
    return {"data_value_set": data_value_set, "output_file": output_file}


def _require_context(context: dict[str, Any], key: str) -> Any:
    """Return required context value or raise a clear orchestration error."""
    if key not in context:
        raise RuntimeError(f"Workflow definition missing prerequisite for '{key}'")
    return context[key]


def _resolve_step_config(config: dict[str, Any], request_params: dict[str, Any]) -> dict[str, Any]:
    """Resolve $request.<field> tokens in step config."""
    resolved: dict[str, Any] = {}
    for key, value in config.items():
        resolved[key] = _resolve_value(value, request_params)
    return resolved


def _resolve_value(value: Any, request_params: dict[str, Any]) -> Any:
    """Resolve a config value recursively."""
    if isinstance(value, str) and value.startswith("$request."):
        field = value.removeprefix("$request.")
        if field not in request_params:
            raise ValueError(f"Unknown request field in config token: {value}")
        return request_params[field]
    if isinstance(value, dict):
        return {k: _resolve_value(v, request_params) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_value(v, request_params) for v in value]
    return value


class _FeatureSourceStepConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    execution_mode: str = "local"
    remote_url: str | None = None
    remote_timeout_sec: float = 30.0
    remote_retries: int = 1
    remote_retry_delay_sec: float = 1.0


class _DownloadDatasetStepConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    execution_mode: str = "local"
    remote_url: str | None = None
    remote_timeout_sec: float = 30.0
    remote_retries: int = 1
    remote_retry_delay_sec: float = 1.0


class _TemporalAggregationStepConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    execution_mode: str = "local"
    remote_url: str | None = None
    remote_timeout_sec: float = 30.0
    remote_retries: int = 1
    remote_retry_delay_sec: float = 1.0


class _SpatialAggregationStepConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    execution_mode: str = "local"
    remote_url: str | None = None
    remote_timeout_sec: float = 30.0
    remote_retries: int = 1
    remote_retry_delay_sec: float = 1.0


class _BuildDataValueSetStepConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    execution_mode: str = "local"
    remote_url: str | None = None
    remote_timeout_sec: float = 30.0
    remote_retries: int = 1
    remote_retry_delay_sec: float = 1.0


_STEP_CONFIG_MODELS: dict[str, type[BaseModel]] = {
    "feature_source": _FeatureSourceStepConfig,
    "download_dataset": _DownloadDatasetStepConfig,
    "temporal_aggregation": _TemporalAggregationStepConfig,
    "spatial_aggregation": _SpatialAggregationStepConfig,
    "build_datavalueset": _BuildDataValueSetStepConfig,
}


def _validate_step_config(component: str, version: str, config: dict[str, Any]) -> None:
    """Validate step config with strict Pydantic models."""
    if version != "v1":
        raise ValueError(f"Unsupported component version for config validation: {component}@{version}")
    model = _STEP_CONFIG_MODELS.get(component)
    if model is None:
        raise ValueError(f"No config schema registered for component '{component}'")
    try:
        validated = model.model_validate(config)
    except ValidationError as exc:
        raise ValueError(f"Invalid config for component '{component}@{version}': {exc}") from exc
    mode = str(getattr(validated, "execution_mode", "local")).lower()
    if mode not in {"local", "remote"}:
        raise ValueError(
            f"Invalid config for component '{component}@{version}': execution_mode must be local or remote"
        )
    remote_url = getattr(validated, "remote_url", None)
    remote_timeout_sec = getattr(validated, "remote_timeout_sec", 30.0)
    remote_retries = getattr(validated, "remote_retries", 1)
    remote_retry_delay_sec = getattr(validated, "remote_retry_delay_sec", 1.0)

    has_remote_config = bool(
        (isinstance(remote_url, str) and remote_url.strip())
        or float(remote_timeout_sec) != 30.0
        or int(remote_retries) != 1
        or float(remote_retry_delay_sec) != 1.0
    )

    if mode == "local" and has_remote_config:
        raise ValueError(
            f"Invalid config for component '{component}@{version}': "
            "remote_url/remote_timeout_sec/remote_retries/remote_retry_delay_sec are only allowed in remote mode"
        )
    if mode == "remote":
        if not isinstance(remote_url, str) or not remote_url.strip():
            raise ValueError(
                f"Invalid config for component '{component}@{version}': remote_url is required for remote mode"
            )


def _invoke_remote_download_component(
    *,
    remote_url: str,
    dataset_id: str,
    start: str,
    end: str,
    overwrite: bool,
    country_code: str | None,
    bbox: list[float],
    timeout_sec: float,
    retries: int,
    retry_delay_sec: float,
) -> None:
    """Invoke remote download component endpoint with retry/timeout."""
    payload = {
        "dataset_id": dataset_id,
        "start": start,
        "end": end,
        "overwrite": overwrite,
        "country_code": country_code,
        "bbox": bbox,
    }
    attempts = max(1, retries)
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            with httpx.Client(timeout=timeout_sec) as client:
                response = client.post(remote_url, json=payload)
                response.raise_for_status()
            return
        except Exception as exc:
            last_exc = exc
            if attempt < attempts:
                time.sleep(max(0.0, retry_delay_sec))
    if last_exc is None:
        raise RuntimeError("Remote download invocation failed without exception context")
    raise last_exc


def _invoke_remote_feature_source_component(
    *,
    remote_url: str,
    feature_source: dict[str, Any],
    timeout_sec: float,
    retries: int,
    retry_delay_sec: float,
) -> tuple[dict[str, Any], list[float]]:
    """Invoke remote feature-source component endpoint."""
    payload = {
        "feature_source": feature_source,
        "include_features": True,
    }
    result = _post_remote_json(
        remote_url=remote_url,
        payload=payload,
        timeout_sec=timeout_sec,
        retries=retries,
        retry_delay_sec=retry_delay_sec,
    )
    features = result.get("features")
    bbox = result.get("bbox")
    if not isinstance(features, dict) or not isinstance(bbox, list):
        raise RuntimeError("Remote feature_source response missing features/bbox")
    return features, [float(x) for x in bbox]


def _invoke_remote_temporal_aggregation_component(
    *,
    remote_url: str,
    dataset_id: str,
    start: str,
    end: str,
    bbox: list[float],
    target_period_type: str,
    method: str,
    timeout_sec: float,
    retries: int,
    retry_delay_sec: float,
) -> dict[str, Any]:
    """Invoke remote temporal-aggregation component endpoint."""
    payload = {
        "dataset_id": dataset_id,
        "start": start,
        "end": end,
        "bbox": bbox,
        "target_period_type": target_period_type,
        "method": method,
    }
    return _post_remote_json(
        remote_url=remote_url,
        payload=payload,
        timeout_sec=timeout_sec,
        retries=retries,
        retry_delay_sec=retry_delay_sec,
    )


def _invoke_remote_spatial_aggregation_component(
    *,
    remote_url: str,
    dataset_id: str,
    start: str,
    end: str,
    bbox: list[float],
    feature_source: dict[str, Any],
    method: str,
    feature_id_property: str,
    timeout_sec: float,
    retries: int,
    retry_delay_sec: float,
) -> list[dict[str, Any]]:
    """Invoke remote spatial-aggregation component endpoint."""
    payload = {
        "dataset_id": dataset_id,
        "start": start,
        "end": end,
        "feature_source": feature_source,
        "method": method,
        "bbox": bbox,
        "feature_id_property": feature_id_property,
        "include_records": True,
    }
    result = _post_remote_json(
        remote_url=remote_url,
        payload=payload,
        timeout_sec=timeout_sec,
        retries=retries,
        retry_delay_sec=retry_delay_sec,
    )
    records = result.get("records")
    if not isinstance(records, list):
        raise RuntimeError("Remote spatial_aggregation response missing records")
    return records


def _invoke_remote_build_datavalueset_component(
    *,
    remote_url: str,
    dataset_id: str,
    period_type: str,
    records: list[dict[str, Any]],
    dhis2: dict[str, Any],
    timeout_sec: float,
    retries: int,
    retry_delay_sec: float,
) -> tuple[dict[str, Any], str]:
    """Invoke remote build-datavalue-set component endpoint."""
    payload = {
        "dataset_id": dataset_id,
        "period_type": period_type,
        "records": records,
        "dhis2": dhis2,
    }
    result = _post_remote_json(
        remote_url=remote_url,
        payload=payload,
        timeout_sec=timeout_sec,
        retries=retries,
        retry_delay_sec=retry_delay_sec,
    )
    data_value_set = result.get("data_value_set")
    output_file = result.get("output_file")
    if not isinstance(data_value_set, dict) or not isinstance(output_file, str):
        raise RuntimeError("Remote build_datavalueset response missing data_value_set/output_file")
    return data_value_set, output_file


def _post_remote_json(
    *,
    remote_url: str,
    payload: dict[str, Any],
    timeout_sec: float,
    retries: int,
    retry_delay_sec: float,
) -> dict[str, Any]:
    """POST JSON to remote component endpoint with retry and return JSON body."""
    attempts = max(1, retries)
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            with httpx.Client(timeout=timeout_sec) as client:
                response = client.post(remote_url, json=payload)
                response.raise_for_status()
                body = response.json()
                if not isinstance(body, dict):
                    raise RuntimeError("Remote component returned non-object JSON response")
                return body
        except Exception as exc:
            last_exc = exc
            if attempt < attempts:
                time.sleep(max(0.0, retry_delay_sec))
    if last_exc is None:
        raise RuntimeError("Remote component invocation failed without exception context")
    raise last_exc
