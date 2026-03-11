"""Workflow orchestration engine for gridded-data pipelines."""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from ...components import services as component_services
from ...data_registry.services.datasets import get_dataset
from ..schemas import WorkflowExecuteRequest, WorkflowExecuteResponse
from .definitions import WorkflowDefinition, load_workflow_definition
from .run_logs import persist_run_log
from .runtime import WorkflowRuntime


def execute_workflow(
    request: WorkflowExecuteRequest,
    *,
    workflow_id: str = "dhis2_datavalue_set_v1",
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
        )
        last_component = runtime.component_runs[-1].component if runtime.component_runs else "unknown"
        if _is_upstream_connectivity_error(exc):
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "upstream_unreachable",
                    "message": "Could not reach upstream data source. Check network/proxy and retry.",
                    "failed_component": last_component,
                    "run_id": runtime.run_id,
                },
            ) from exc
        raise HTTPException(
            status_code=500,
            detail={
                "error": "workflow_execution_failed",
                "message": str(exc),
                "failed_component": last_component,
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
    dataset: dict[str, Any],
    context: dict[str, Any],
) -> None:
    """Execute workflow components using declarative YAML step order."""
    for step in workflow.steps:
        if step.component == "feature_source":
            features, bbox = runtime.run(
                "feature_source",
                component_services.feature_source_component,
                config=request.feature_source,
            )
            context["features"] = features
            context["bbox"] = bbox
            continue

        if step.component == "download_dataset":
            runtime.run(
                "download_dataset",
                component_services.download_dataset_component,
                dataset=dataset,
                start=request.start,
                end=request.end,
                overwrite=request.overwrite,
                country_code=request.country_code,
                bbox=_require_context(context, "bbox"),
            )
            continue

        if step.component == "temporal_aggregation":
            temporal_ds = runtime.run(
                "temporal_aggregation",
                component_services.temporal_aggregation_component,
                dataset=dataset,
                start=request.start,
                end=request.end,
                bbox=_require_context(context, "bbox"),
                target_period_type=request.temporal_aggregation.target_period_type,
                method=request.temporal_aggregation.method,
            )
            context["temporal_dataset"] = temporal_ds
            continue

        if step.component == "spatial_aggregation":
            records = runtime.run(
                "spatial_aggregation",
                component_services.spatial_aggregation_component,
                dataset=dataset,
                start=request.start,
                end=request.end,
                bbox=_require_context(context, "bbox"),
                features=_require_context(context, "features"),
                method=request.spatial_aggregation.method,
                feature_id_property=request.dhis2.org_unit_property,
            )
            context["records"] = records
            continue

        if step.component == "build_datavalueset":
            data_value_set, output_file = runtime.run(
                "build_datavalueset",
                component_services.build_datavalueset_component,
                records=_require_context(context, "records"),
                dataset_id=request.dataset_id,
                period_type=request.temporal_aggregation.target_period_type,
                dhis2=request.dhis2,
            )
            context["data_value_set"] = data_value_set
            context["output_file"] = output_file
            continue

        raise RuntimeError(f"Unsupported workflow component '{step.component}'")


def _require_context(context: dict[str, Any], key: str) -> Any:
    """Return required context value or raise a clear orchestration error."""
    if key not in context:
        raise RuntimeError(f"Workflow definition missing prerequisite for '{key}'")
    return context[key]
