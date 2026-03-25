"""Workflow orchestration engine for gridded-data pipelines."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from fastapi import HTTPException

from ...components import services as component_services
from ...data_registry.services.datasets import get_dataset
from ...publications.services import register_workflow_output_publication
from ...shared.api_errors import api_error
from ..schemas import WorkflowExecuteRequest, WorkflowExecuteResponse, WorkflowJobStatus
from .definitions import (
    WorkflowDefinition,
    WorkflowOutputBinding,
    WorkflowPublicationPolicy,
    WorkflowStep,
    load_workflow_definition,
)
from .job_store import initialize_job, mark_job_failed, mark_job_running, mark_job_success
from .publication_assets import build_feature_collection_asset, write_feature_collection_asset, write_json_asset
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


@dataclass
class WorkflowExecutionContext:
    """Step-scoped workflow outputs and compatibility lookup helpers."""

    step_outputs: dict[str, dict[str, Any]] = field(default_factory=dict)
    latest_outputs: dict[str, Any] = field(default_factory=dict)

    def set_step_outputs(self, step_id: str, outputs: dict[str, Any]) -> None:
        self.step_outputs[step_id] = outputs
        self.latest_outputs.update(outputs)

    def get_step_output(self, *, step_id: str, output_name: str) -> Any:
        outputs = self.step_outputs.get(step_id)
        if outputs is None or output_name not in outputs:
            raise RuntimeError(f"Workflow definition missing prerequisite for '{step_id}.{output_name}'")
        return outputs[output_name]

    def require_output(self, output_name: str) -> Any:
        if output_name not in self.latest_outputs:
            raise RuntimeError(f"Workflow definition missing prerequisite for '{output_name}'")
        return self.latest_outputs[output_name]


def execute_workflow(
    request: WorkflowExecuteRequest,
    *,
    workflow_id: str = "dhis2_datavalue_set_v1",
    workflow_definition: WorkflowDefinition | None = None,
    request_params: dict[str, Any] | None = None,
    include_component_run_details: bool = False,
    run_id: str | None = None,
    workflow_definition_source: Literal["catalog", "inline"] = "catalog",
    trigger_type: str = "on_demand",
    schedule_id: str | None = None,
    idempotency_key: str | None = None,
) -> WorkflowExecuteResponse:
    """Execute the feature->download->aggregate->DataValueSet workflow."""
    runtime = WorkflowRuntime(run_id=run_id)
    workflow: WorkflowDefinition | None = None

    dataset = get_dataset(request.dataset_id)
    if dataset is None:
        raise HTTPException(
            status_code=404,
            detail=api_error(
                error="dataset_not_found",
                error_code="DATASET_NOT_FOUND",
                message=f"Dataset '{request.dataset_id}' not found",
                resource_id=request.dataset_id,
            ),
        )

    context = WorkflowExecutionContext()

    try:
        if workflow_definition is not None:
            workflow = workflow_definition
        else:
            try:
                workflow = load_workflow_definition(workflow_id)
            except ValueError as exc:
                raise HTTPException(
                    status_code=422,
                    detail=api_error(
                        error="workflow_definition_invalid",
                        error_code="WORKFLOW_DEFINITION_INVALID",
                        message=str(exc),
                    ),
                ) from exc

        initialize_job(
            job_id=runtime.run_id,
            request=request,
            request_payload=request_params,
            workflow=workflow,
            workflow_definition_source=workflow_definition_source,
            workflow_id=workflow.workflow_id,
            workflow_version=workflow.version,
            status=WorkflowJobStatus.RUNNING,
            trigger_type=trigger_type,
            schedule_id=schedule_id,
            idempotency_key=idempotency_key,
        )
        mark_job_running(runtime.run_id)
        _execute_workflow_steps(
            workflow=workflow,
            runtime=runtime,
            request=request,
            request_params=request_params,
            dataset=dataset,
            context=context,
        )
        exported_outputs = _resolve_workflow_outputs(workflow.outputs, context)
        output_summary = _summarize_workflow_outputs(exported_outputs)
        run_log_file = persist_run_log(
            run_id=runtime.run_id,
            request=request,
            component_runs=runtime.component_runs,
            status="completed",
            output_file=output_summary["output_file"],
        )

        response = WorkflowExecuteResponse(
            status="completed",
            run_id=runtime.run_id,
            workflow_id=workflow.workflow_id,
            workflow_version=workflow.version,
            dataset_id=request.dataset_id,
            outputs=exported_outputs,
            primary_output_name=next(iter(workflow.outputs), None),
            bbox=output_summary["bbox"],
            feature_count=output_summary["feature_count"],
            value_count=output_summary["value_count"],
            output_file=output_summary["output_file"],
            run_log_file=run_log_file,
            data_value_set=output_summary["data_value_set"],
            component_runs=runtime.component_runs if include_component_run_details else [],
            component_run_details_included=include_component_run_details,
            component_run_details_available=True,
        )
        mark_job_success(job_id=runtime.run_id, response=response)
        if _should_publish_workflow_output(
            request=request,
            response=response,
            publication=workflow.publication,
            workflow_definition_source=workflow_definition_source,
        ):
            publication_path, publication_asset_format = _build_publication_artifact(
                response=response,
                request=request,
                publication=workflow.publication,
                context=context,
                exported_outputs=exported_outputs,
            )
            register_workflow_output_publication(
                response=response,
                kind=workflow.publication.intent,
                exposure=workflow.publication.exposure,
                published_path=publication_path,
                asset_format=publication_asset_format,
            )
        return response
    except WorkflowComponentError as exc:
        run_log_file = persist_run_log(
            run_id=runtime.run_id,
            request=request,
            component_runs=runtime.component_runs,
            status="failed",
            error=str(exc),
            error_code=exc.error_code,
            failed_component=exc.component,
            failed_component_version=exc.component_version,
        )
        if workflow is not None:
            mark_job_failed(
                job_id=runtime.run_id,
                error=str(exc),
                error_code=exc.error_code,
                failed_component=exc.component,
                failed_component_version=exc.component_version,
                run_log_file=run_log_file,
            )
        error = "upstream_unreachable" if exc.error_code == "UPSTREAM_UNREACHABLE" else "workflow_execution_failed"
        raise HTTPException(
            status_code=exc.status_code,
            detail=api_error(
                error=error,
                error_code=exc.error_code,
                message=str(exc),
                run_id=runtime.run_id,
                failed_component=exc.component,
                failed_component_version=exc.component_version,
            ),
        ) from exc
    except HTTPException:
        run_log_file = persist_run_log(
            run_id=runtime.run_id,
            request=request,
            component_runs=runtime.component_runs,
            status="failed",
            error="http_exception",
        )
        if workflow is not None:
            mark_job_failed(job_id=runtime.run_id, error="http_exception", run_log_file=run_log_file)
        raise
    except Exception as exc:
        run_log_file = persist_run_log(
            run_id=runtime.run_id,
            request=request,
            component_runs=runtime.component_runs,
            status="failed",
            error=str(exc),
            error_code="EXECUTION_FAILED",
        )
        if workflow is not None:
            mark_job_failed(
                job_id=runtime.run_id,
                error=str(exc),
                error_code="EXECUTION_FAILED",
                run_log_file=run_log_file,
            )
        last_component = runtime.component_runs[-1].component if runtime.component_runs else "unknown"
        raise HTTPException(
            status_code=500,
            detail=api_error(
                error="workflow_execution_failed",
                error_code="EXECUTION_FAILED",
                message=str(exc),
                run_id=runtime.run_id,
                failed_component=last_component,
                failed_component_version="unknown",
            ),
        ) from exc


def _should_publish_workflow_output(
    *,
    request: WorkflowExecuteRequest,
    response: WorkflowExecuteResponse,
    publication: WorkflowPublicationPolicy,
    workflow_definition_source: Literal["catalog", "inline"],
) -> bool:
    """Apply workflow-level publication policy to a concrete workflow output."""
    if not request.publish:
        return False
    if not publication.publishable:
        return False
    if publication.strategy != "on_success":
        return False
    if not _server_allows_workflow_publication(workflow_definition_source=workflow_definition_source):
        return False
    if publication.required_output_file_suffixes:
        if response.output_file is None:
            return False
        suffix = Path(response.output_file).suffix.lower()
        return suffix in publication.required_output_file_suffixes
    return True


def _server_allows_workflow_publication(*, workflow_definition_source: Literal["catalog", "inline"]) -> bool:
    """Apply server-side guardrails to workflow-driven publication."""
    if workflow_definition_source == "catalog":
        return True
    return os.environ.get("EO_API_ALLOW_INLINE_WORKFLOW_PUBLICATION", "").lower() in {"1", "true", "yes"}


def _build_publication_artifact(
    *,
    response: WorkflowExecuteResponse,
    request: WorkflowExecuteRequest,
    publication: WorkflowPublicationPolicy,
    context: WorkflowExecutionContext,
    exported_outputs: dict[str, Any],
) -> tuple[str, str]:
    """Build the publication-facing artifact for a publishable workflow output."""
    if publication.asset is not None:
        asset_value = context.get_step_output(
            step_id=publication.asset.from_step,
            output_name=publication.asset.output,
        )
        return _materialize_publication_asset(
            asset_value=asset_value,
            dataset_id=response.dataset_id,
            publication=publication,
        )

    if publication.intent.value == "feature_collection":
        features_ref = publication.inputs.get("features")
        records_ref = publication.inputs.get("records")
        if features_ref is None or records_ref is None:
            raise ValueError("Feature collection publication requires declared publication inputs: features, records")
        features = context.get_step_output(step_id=features_ref.from_step, output_name=features_ref.output)
        records = context.get_step_output(step_id=records_ref.from_step, output_name=records_ref.output)
        path = build_feature_collection_asset(
            dataset_id=response.dataset_id,
            features=features,
            records=records,
            period_type=request.temporal_aggregation.target_period_type,
            feature_id_property=request.feature_source.feature_id_property,
        )
        return path, "geojson"
    output_file_ref = publication.inputs.get("output_file")
    if output_file_ref is not None:
        path_value = context.get_step_output(step_id=output_file_ref.from_step, output_name=output_file_ref.output)
        if not isinstance(path_value, str):
            raise ValueError("Publication input 'output_file' must resolve to a filesystem path string")
        return path_value, Path(path_value).suffix.lstrip(".") or "file"
    if response.output_file is not None:
        return response.output_file, _asset_format_for_path(response.output_file)
    primary_output_name = response.primary_output_name
    if primary_output_name is not None:
        primary_output = exported_outputs.get(primary_output_name)
        if isinstance(primary_output, str):
            return primary_output, _asset_format_for_path(primary_output)
    raise ValueError("Workflow publication could not resolve a publication artifact")


def _materialize_publication_asset(
    *,
    asset_value: Any,
    dataset_id: str,
    publication: WorkflowPublicationPolicy,
) -> tuple[str, str]:
    """Resolve a declared publication asset to a persisted asset path and format."""
    if isinstance(asset_value, str):
        return asset_value, publication.asset_format or _asset_format_for_path(asset_value)
    if publication.intent.value == "feature_collection" and isinstance(asset_value, dict):
        if asset_value.get("type") == "FeatureCollection":
            return write_feature_collection_asset(collection=asset_value, dataset_id=dataset_id), "geojson"
    if isinstance(asset_value, (dict, list)):
        asset_format = publication.asset_format or "json"
        return write_json_asset(payload=asset_value, dataset_id=dataset_id, suffix=asset_format), asset_format
    raise ValueError("Declared publication asset must resolve to a file path or JSON-serializable value")


def _resolve_workflow_outputs(
    bindings: dict[str, WorkflowOutputBinding],
    context: WorkflowExecutionContext,
) -> dict[str, Any]:
    """Resolve exported workflow outputs from step-scoped execution context."""
    resolved: dict[str, Any] = {}
    for name, binding in bindings.items():
        if not binding.include_in_response:
            continue
        resolved[name] = context.get_step_output(step_id=binding.from_step, output_name=binding.output)
    return resolved


def _summarize_workflow_outputs(outputs: dict[str, Any]) -> dict[str, Any]:
    """Derive compatibility summary fields from declared workflow outputs."""
    features = outputs.get("features")
    bbox = outputs.get("bbox")
    records = outputs.get("records")
    data_value_set = outputs.get("data_value_set")
    output_file = outputs.get("output_file")

    if not isinstance(bbox, list):
        bbox = None
    if not isinstance(output_file, str):
        output_file = None

    feature_count: int | None = None
    if isinstance(features, dict):
        feature_items = features.get("features")
        if isinstance(feature_items, list):
            feature_count = len(feature_items)

    value_count: int | None = None
    if isinstance(data_value_set, dict):
        data_values = data_value_set.get("dataValues")
        if isinstance(data_values, list):
            value_count = len(data_values)
    elif isinstance(records, list):
        value_count = len(records)

    return {
        "bbox": bbox,
        "feature_count": feature_count,
        "value_count": value_count,
        "output_file": output_file,
        "data_value_set": data_value_set if isinstance(data_value_set, dict) else None,
    }


def _asset_format_for_path(path_value: str) -> str:
    suffix = Path(path_value).suffix.lower()
    if suffix.startswith("."):
        suffix = suffix[1:]
    return suffix or "file"


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
    context: WorkflowExecutionContext,
) -> None:
    """Execute workflow components using declarative YAML step order."""
    for step in workflow.steps:
        if step.id is None:
            raise WorkflowComponentError(
                error_code="INPUT_VALIDATION_FAILED",
                message=f"Workflow step '{step.component}' is missing an id",
                component=step.component,
                component_version=step.version,
                status_code=422,
            )
        runtime_definition = component_services.workflow_runtime_registry().get(f"{step.component}@{step.version}")
        if runtime_definition is None:
            raise WorkflowComponentError(
                error_code="INPUT_VALIDATION_FAILED",
                message=f"Unsupported workflow component '{step.component}@{step.version}'",
                component=step.component,
                component_version=step.version,
                status_code=422,
            )
        try:
            step_config = _resolve_step_config(step.config, request_params or {})
            component_services.validate_component_runtime_config(step.component, step.version, step_config)
        except ValueError as exc:
            raise WorkflowComponentError(
                error_code="CONFIG_VALIDATION_FAILED",
                message=str(exc),
                component=step.component,
                component_version=step.version,
                status_code=422,
            ) from exc

        try:
            resolved_inputs = _resolve_step_inputs(step=step, context=context)
            updates = runtime_definition.executor(
                step=step,
                runtime=runtime,
                request=request,
                dataset=dataset,
                resolved_inputs=resolved_inputs,
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

        _validate_step_outputs(step=step, outputs=updates)
        context.set_step_outputs(step.id, updates)


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
            component_services.validate_component_runtime_config(step.component, step.version, resolved_config)
        except ValueError as exc:
            raise ValueError(f"Step {index + 1} ({step.component}@{step.version}) validation failed: {exc}") from exc
        resolved_steps.append(
            {
                "index": index + 1,
                "id": step.id,
                "component": step.component,
                "version": step.version,
                "resolved_config": resolved_config,
                "resolved_inputs": {
                    input_name: {"from_step": ref.from_step, "output": ref.output}
                    for input_name, ref in step.inputs.items()
                },
            }
        )
    return resolved_steps


def _resolve_step_inputs(step: WorkflowStep, context: WorkflowExecutionContext) -> dict[str, Any]:
    """Resolve one step's declared upstream references into concrete values."""
    resolved: dict[str, Any] = {}
    for input_name, ref in step.inputs.items():
        resolved[input_name] = context.get_step_output(step_id=ref.from_step, output_name=ref.output)
    return resolved


def _validate_step_outputs(*, step: WorkflowStep, outputs: dict[str, Any]) -> None:
    """Ensure a step only emits its declared outputs and required outputs are present."""
    declared_outputs = set(component_services.component_registry()[f"{step.component}@{step.version}"].outputs)
    internal_outputs = set(outputs)
    unexpected_outputs = internal_outputs - declared_outputs
    if unexpected_outputs:
        unexpected = ", ".join(sorted(unexpected_outputs))
        raise RuntimeError(f"Component '{step.component}' emitted undeclared outputs: {unexpected}")
    missing_outputs = declared_outputs - internal_outputs
    if missing_outputs:
        missing = ", ".join(sorted(missing_outputs))
        raise RuntimeError(f"Component '{step.component}' did not emit declared outputs: {missing}")


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
