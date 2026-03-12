"""Workflow orchestration engine for gridded-data pipelines."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, ValidationError

from ...components import services as component_services
from ...data_registry.services.datasets import get_dataset
from ..schemas import AggregationMethod, PeriodType, WorkflowExecuteRequest, WorkflowExecuteResponse
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


type StepExecutor = Callable[..., dict[str, Any]]


def _run_feature_source(
    *,
    runtime: WorkflowRuntime,
    request: WorkflowExecuteRequest,
    dataset: dict[str, Any],
    context: dict[str, Any],
    step_config: dict[str, Any],
) -> dict[str, Any]:
    del dataset, context, step_config
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
    overwrite = bool(step_config.get("overwrite", request.overwrite))
    country_code = step_config.get("country_code", request.country_code)
    runtime.run(
        "download_dataset",
        component_services.download_dataset_component,
        dataset=dataset,
        start=request.start,
        end=request.end,
        overwrite=overwrite,
        country_code=country_code,
        bbox=_require_context(context, "bbox"),
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
    target_period_type = PeriodType(
        str(step_config.get("target_period_type", request.temporal_aggregation.target_period_type))
    )
    method = AggregationMethod(str(step_config.get("method", request.temporal_aggregation.method)))
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
    method = AggregationMethod(str(step_config.get("method", request.spatial_aggregation.method)))
    feature_id_property = str(step_config.get("feature_id_property", request.dhis2.org_unit_property))
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
    period_type = PeriodType(str(step_config.get("period_type", request.temporal_aggregation.target_period_type)))
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


class _DownloadDatasetStepConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    overwrite: bool | None = None
    country_code: str | None = None


class _TemporalAggregationStepConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_period_type: PeriodType | None = None
    method: AggregationMethod | None = None


class _SpatialAggregationStepConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    method: AggregationMethod | None = None
    feature_id_property: str | None = None


class _BuildDataValueSetStepConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    period_type: PeriodType | None = None


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
        model.model_validate(config)
    except ValidationError as exc:
        raise ValueError(f"Invalid config for component '{component}@{version}': {exc}") from exc
