"""Component service implementations and discovery metadata."""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

import httpx
import xarray as xr
from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, ValidationError

from ..data_accessor.services.accessor import get_data
from ..data_manager.services import downloader
from ..data_registry.services.datasets import get_dataset
from ..workflows.schemas import (
    AggregationMethod,
    Dhis2DataValueSetConfig,
    FeatureSourceConfig,
    PeriodType,
)
from ..workflows.services.datavalueset import build_data_value_set
from ..workflows.services.features import resolve_features
from ..workflows.services.preflight import check_upstream_connectivity
from ..workflows.services.spatial import aggregate_to_features
from ..workflows.services.temporal import aggregate_temporal
from .schemas import ComponentDefinition, ComponentEndpoint, ComponentManifest, ComponentRuntimeManifest

type WorkflowStepExecutor = Any


@dataclass(frozen=True)
class ComponentRuntimeDefinition:
    """Runtime binding for one workflow-executable component version."""

    component: str
    version: str
    executor: WorkflowStepExecutor
    config_model: type[BaseModel]

_ERROR_CODES_V1: Final[list[str]] = [
    "INPUT_VALIDATION_FAILED",
    "CONFIG_VALIDATION_FAILED",
    "OUTPUT_VALIDATION_FAILED",
    "UPSTREAM_UNREACHABLE",
    "EXECUTION_FAILED",
]

_COMPONENT_REGISTRY: Final[dict[str, ComponentManifest]] = {
    "feature_source@v1": ComponentManifest(
        name="feature_source",
        version="v1",
        description="Resolve feature source and compute bbox.",
        inputs=["feature_source"],
        outputs=["features", "bbox"],
        workflow_inputs_required=[],
        workflow_inputs_optional=[],
        input_schema={
            "type": "object",
            "properties": {"feature_source": {"type": "object"}},
            "required": ["feature_source"],
        },
        config_schema={
            "type": "object",
            "properties": {
                "execution_mode": {"type": "string", "enum": ["local", "remote"]},
                "remote_url": {"type": ["string", "null"]},
                "remote_timeout_sec": {"type": "number"},
                "remote_retries": {"type": "integer"},
                "remote_retry_delay_sec": {"type": "number"},
            },
            "additionalProperties": False,
        },
        output_schema={
            "type": "object",
            "properties": {
                "features": {"type": "object"},
                "bbox": {"type": "array", "items": {"type": "number"}, "minItems": 4, "maxItems": 4},
            },
            "required": ["features", "bbox"],
        },
        error_codes=_ERROR_CODES_V1,
        endpoint=ComponentEndpoint(path="/components/feature-source", method="POST"),
        runtime=ComponentRuntimeManifest(
            supported_execution_modes=["local", "remote"],
            local_handler="workflow.feature_source",
            remote_handler="workflow.feature_source",
            remote_request_bindings={
                "feature_source": "$request.feature_source",
                "include_features": True,
            },
            remote_response_bindings={
                "features": "features",
                "bbox": "bbox",
            },
        ),
    ),
    "download_dataset@v1": ComponentManifest(
        name="download_dataset",
        version="v1",
        description="Download dataset files for period and bbox.",
        inputs=["dataset_id", "start", "end", "overwrite", "country_code", "bbox"],
        outputs=["status"],
        workflow_inputs_required=["bbox"],
        workflow_inputs_optional=[],
        input_schema={
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string"},
                "start": {"type": "string"},
                "end": {"type": "string"},
                "overwrite": {"type": "boolean"},
                "country_code": {"type": ["string", "null"]},
                "bbox": {"type": "array", "items": {"type": "number"}, "minItems": 4, "maxItems": 4},
            },
            "required": ["dataset_id", "start", "end", "overwrite", "bbox"],
        },
        config_schema={
            "type": "object",
            "properties": {
                "execution_mode": {"type": "string", "enum": ["local", "remote"]},
                "remote_url": {"type": ["string", "null"]},
                "remote_timeout_sec": {"type": "number"},
                "remote_retries": {"type": "integer"},
                "remote_retry_delay_sec": {"type": "number"},
            },
            "additionalProperties": False,
        },
        output_schema={"type": "object", "properties": {"status": {"type": "string"}}},
        error_codes=_ERROR_CODES_V1,
        endpoint=ComponentEndpoint(path="/components/download-dataset", method="POST"),
        runtime=ComponentRuntimeManifest(
            supported_execution_modes=["local", "remote"],
            local_handler="workflow.download_dataset",
            remote_handler="workflow.download_dataset",
            remote_request_bindings={
                "dataset_id": "$request.dataset_id",
                "start": "$request.start",
                "end": "$request.end",
                "overwrite": "$request.overwrite",
                "country_code": "$request.country_code",
                "bbox": "$resolved.bbox",
            },
            remote_response_bindings={"status": "status"},
        ),
    ),
    "temporal_aggregation@v1": ComponentManifest(
        name="temporal_aggregation",
        version="v1",
        description="Aggregate dataset over time dimension.",
        inputs=["dataset_id", "start", "end", "target_period_type", "method", "bbox"],
        outputs=["temporal_dataset"],
        workflow_inputs_required=["bbox"],
        workflow_inputs_optional=[],
        input_schema={
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string"},
                "start": {"type": "string"},
                "end": {"type": "string"},
                "target_period_type": {"type": "string"},
                "method": {"type": "string"},
                "bbox": {"type": ["array", "null"], "items": {"type": "number"}},
            },
            "required": ["dataset_id", "start", "end", "target_period_type", "method"],
        },
        config_schema={
            "type": "object",
            "properties": {
                "execution_mode": {"type": "string", "enum": ["local", "remote"]},
                "remote_url": {"type": ["string", "null"]},
                "remote_timeout_sec": {"type": "number"},
                "remote_retries": {"type": "integer"},
                "remote_retry_delay_sec": {"type": "number"},
            },
            "additionalProperties": False,
        },
        output_schema={"type": "object", "properties": {"temporal_dataset": {"type": "object"}}},
        error_codes=_ERROR_CODES_V1,
        endpoint=ComponentEndpoint(path="/components/temporal-aggregation", method="POST"),
        runtime=ComponentRuntimeManifest(
            supported_execution_modes=["local"],
            local_handler="workflow.temporal_aggregation",
            remote_handler=None,
        ),
    ),
    "spatial_aggregation@v1": ComponentManifest(
        name="spatial_aggregation",
        version="v1",
        description="Aggregate gridded dataset to features.",
        inputs=["dataset_id", "start", "end", "feature_source", "method"],
        outputs=["records"],
        workflow_inputs_required=["bbox", "features"],
        workflow_inputs_optional=["temporal_dataset"],
        input_schema={
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string"},
                "start": {"type": "string"},
                "end": {"type": "string"},
                "feature_source": {"type": "object"},
                "method": {"type": "string"},
            },
            "required": ["dataset_id", "start", "end", "feature_source", "method"],
        },
        config_schema={
            "type": "object",
            "properties": {
                "execution_mode": {"type": "string", "enum": ["local", "remote"]},
                "remote_url": {"type": ["string", "null"]},
                "remote_timeout_sec": {"type": "number"},
                "remote_retries": {"type": "integer"},
                "remote_retry_delay_sec": {"type": "number"},
            },
            "additionalProperties": False,
        },
        output_schema={"type": "object", "properties": {"records": {"type": "array"}}},
        error_codes=_ERROR_CODES_V1,
        endpoint=ComponentEndpoint(path="/components/spatial-aggregation", method="POST"),
        runtime=ComponentRuntimeManifest(
            supported_execution_modes=["local", "remote"],
            local_handler="workflow.spatial_aggregation",
            remote_handler="workflow.spatial_aggregation",
            remote_request_bindings={
                "dataset_id": "$request.dataset_id",
                "start": "$request.start",
                "end": "$request.end",
                "feature_source": "$request.feature_source",
                "method": "$request.spatial_aggregation.method",
                "bbox": "$resolved.bbox",
                "feature_id_property": "$request.dhis2.org_unit_property",
                "include_records": True,
            },
            remote_response_bindings={"records": "records"},
        ),
    ),
    "build_datavalueset@v1": ComponentManifest(
        name="build_datavalueset",
        version="v1",
        description="Build and serialize DHIS2 DataValueSet JSON.",
        inputs=["dataset_id", "period_type", "records", "dhis2"],
        outputs=["data_value_set", "output_file"],
        workflow_inputs_required=["records"],
        workflow_inputs_optional=[],
        input_schema={
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string"},
                "period_type": {"type": "string"},
                "records": {"type": "array"},
                "dhis2": {"type": "object"},
            },
            "required": ["dataset_id", "period_type", "records", "dhis2"],
        },
        config_schema={
            "type": "object",
            "properties": {
                "execution_mode": {"type": "string", "enum": ["local", "remote"]},
                "remote_url": {"type": ["string", "null"]},
                "remote_timeout_sec": {"type": "number"},
                "remote_retries": {"type": "integer"},
                "remote_retry_delay_sec": {"type": "number"},
            },
            "additionalProperties": False,
        },
        output_schema={
            "type": "object",
            "properties": {"data_value_set": {"type": "object"}, "output_file": {"type": "string"}},
            "required": ["data_value_set", "output_file"],
        },
        error_codes=_ERROR_CODES_V1,
        endpoint=ComponentEndpoint(path="/components/build-datavalue-set", method="POST"),
        runtime=ComponentRuntimeManifest(
            supported_execution_modes=["local", "remote"],
            local_handler="workflow.build_datavalueset",
            remote_handler="workflow.build_datavalueset",
            remote_request_bindings={
                "dataset_id": "$request.dataset_id",
                "period_type": "$request.temporal_aggregation.target_period_type",
                "records": "$resolved.records",
                "dhis2": "$request.dhis2",
            },
            remote_response_bindings={
                "data_value_set": "data_value_set",
                "output_file": "output_file",
            },
        ),
    ),
}


def component_catalog(*, include_internal: bool = False) -> list[ComponentDefinition]:
    """Return discoverable component definitions.

    By default, internal orchestration-only metadata (config_schema) is hidden.
    """
    components = [manifest.to_definition() for manifest in _COMPONENT_REGISTRY.values()]
    if include_internal:
        return components
    return [component.model_copy(update={"config_schema": None}) for component in components]


def component_registry() -> dict[str, ComponentManifest]:
    """Return manifest registry entries keyed by component@version."""
    return dict(_COMPONENT_REGISTRY)


class _RemoteCapableStepConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    execution_mode: str = "local"
    remote_url: str | None = None
    remote_timeout_sec: float = 30.0
    remote_retries: int = 1
    remote_retry_delay_sec: float = 1.0


def feature_source_component(config: FeatureSourceConfig) -> tuple[dict[str, Any], list[float]]:
    """Run feature source component."""
    return resolve_features(config)


def download_dataset_component(
    *,
    dataset: dict[str, Any],
    start: str,
    end: str,
    overwrite: bool,
    country_code: str | None,
    bbox: list[float],
) -> None:
    """Run connectivity preflight and download dataset files."""
    check_upstream_connectivity(dataset)
    downloader.download_dataset(
        dataset=dataset,
        start=start,
        end=end,
        overwrite=overwrite,
        background_tasks=None,
        country_code=country_code,
        bbox=bbox,
    )


def temporal_aggregation_component(
    *,
    dataset: dict[str, Any],
    start: str,
    end: str,
    bbox: list[float] | None,
    target_period_type: PeriodType,
    method: AggregationMethod,
) -> xr.Dataset:
    """Load dataset and aggregate over time."""
    ds = get_data(dataset=dataset, start=start, end=end, bbox=bbox)
    source_period_type = _dataset_period_type(dataset)
    if source_period_type == target_period_type:
        return ds
    return aggregate_temporal(ds=ds, period_type=target_period_type, method=method)


def spatial_aggregation_component(
    *,
    dataset: dict[str, Any],
    start: str,
    end: str,
    bbox: list[float] | None,
    features: dict[str, Any],
    method: AggregationMethod,
    feature_id_property: str,
    aggregated_dataset: xr.Dataset | None = None,
) -> list[dict[str, Any]]:
    """Load dataset and aggregate spatially to provided features."""
    ds = (
        aggregated_dataset
        if aggregated_dataset is not None
        else get_data(dataset=dataset, start=start, end=end, bbox=bbox)
    )
    return aggregate_to_features(
        ds=ds,
        variable=dataset["variable"],
        features=features,
        method=method.value,
        feature_id_property=feature_id_property,
    )


def build_datavalueset_component(
    *,
    dataset_id: str,
    period_type: PeriodType,
    records: list[dict[str, Any]],
    dhis2: Dhis2DataValueSetConfig,
) -> tuple[dict[str, Any], str]:
    """Build and serialize DHIS2 DataValueSet from records."""
    return build_data_value_set(records=records, dataset_id=dataset_id, period_type=period_type, config=dhis2)


def run_feature_source_step(
    *,
    step: Any,
    runtime: Any,
    request: Any,
    dataset: dict[str, Any],
    resolved_inputs: dict[str, Any],
    step_config: dict[str, Any],
) -> dict[str, Any]:
    """Workflow runtime adapter for feature_source."""
    del dataset, step
    execution_mode = str(step_config.get("execution_mode", "local")).lower()
    if execution_mode == "remote":
        outputs = runtime.run(
            "feature_source",
            _invoke_registered_remote_component,
            component_key="feature_source@v1",
            remote_url=str(step_config["remote_url"]),
            request=request,
            resolved_inputs=resolved_inputs,
            timeout_sec=float(step_config.get("remote_timeout_sec", 30.0)),
            retries=int(step_config.get("remote_retries", 1)),
            retry_delay_sec=float(step_config.get("remote_retry_delay_sec", 1.0)),
        )
        features = outputs["features"]
        bbox = outputs["bbox"]
    else:
        features, bbox = runtime.run(
            "feature_source",
            feature_source_component,
            config=request.feature_source,
        )
    return {"features": features, "bbox": bbox}


def run_download_dataset_step(
    *,
    step: Any,
    runtime: Any,
    request: Any,
    dataset: dict[str, Any],
    resolved_inputs: dict[str, Any],
    step_config: dict[str, Any],
) -> dict[str, Any]:
    """Workflow runtime adapter for download_dataset."""
    del step
    execution_mode = str(step_config.get("execution_mode", "local")).lower()
    if execution_mode not in {"local", "remote"}:
        raise ValueError("download_dataset.execution_mode must be 'local' or 'remote'")
    bbox = resolved_inputs["bbox"]
    if execution_mode == "remote":
        remote_url = step_config.get("remote_url")
        if not isinstance(remote_url, str) or not remote_url:
            raise ValueError("download_dataset remote mode requires non-empty 'remote_url'")
        outputs = runtime.run(
            "download_dataset",
            _invoke_registered_remote_component,
            component_key="download_dataset@v1",
            remote_url=remote_url,
            request=request,
            resolved_inputs=resolved_inputs,
            timeout_sec=float(step_config.get("remote_timeout_sec", 30.0)),
            retries=int(step_config.get("remote_retries", 1)),
            retry_delay_sec=float(step_config.get("remote_retry_delay_sec", 1.0)),
        )
        return outputs
    else:
        runtime.run(
            "download_dataset",
            download_dataset_component,
            dataset=dataset,
            start=request.start,
            end=request.end,
            overwrite=request.overwrite,
            country_code=request.country_code,
            bbox=bbox,
        )
    return {"status": "downloaded"}


def run_temporal_aggregation_step(
    *,
    step: Any,
    runtime: Any,
    request: Any,
    dataset: dict[str, Any],
    resolved_inputs: dict[str, Any],
    step_config: dict[str, Any],
) -> dict[str, Any]:
    """Workflow runtime adapter for temporal_aggregation."""
    del step
    target_period_type = request.temporal_aggregation.target_period_type
    method = request.temporal_aggregation.method
    execution_mode = str(step_config.get("execution_mode", "local")).lower()
    if execution_mode == "remote":
        raise ValueError("temporal_aggregation does not declare a remote HTTP contract")
    else:
        temporal_ds = runtime.run(
            "temporal_aggregation",
            temporal_aggregation_component,
            dataset=dataset,
            start=request.start,
            end=request.end,
            bbox=resolved_inputs["bbox"],
            target_period_type=target_period_type,
            method=method,
        )
    return {"temporal_dataset": temporal_ds}


def run_spatial_aggregation_step(
    *,
    step: Any,
    runtime: Any,
    request: Any,
    dataset: dict[str, Any],
    resolved_inputs: dict[str, Any],
    step_config: dict[str, Any],
) -> dict[str, Any]:
    """Workflow runtime adapter for spatial_aggregation."""
    del step
    method = request.spatial_aggregation.method
    feature_id_property = request.dhis2.org_unit_property
    execution_mode = str(step_config.get("execution_mode", "local")).lower()
    temporal_dataset = resolved_inputs.get("temporal_dataset")
    if execution_mode == "remote":
        if temporal_dataset is not None:
            raise ValueError(
                "remote spatial_aggregation does not yet support workflow temporal_aggregation output; "
                "use local spatial_aggregation for temporally aggregated workflows"
            )
        outputs = runtime.run(
            "spatial_aggregation",
            _invoke_registered_remote_component,
            component_key="spatial_aggregation@v1",
            remote_url=str(step_config["remote_url"]),
            request=request,
            resolved_inputs=resolved_inputs,
            timeout_sec=float(step_config.get("remote_timeout_sec", 30.0)),
            retries=int(step_config.get("remote_retries", 1)),
            retry_delay_sec=float(step_config.get("remote_retry_delay_sec", 1.0)),
        )
        records = outputs["records"]
    else:
        records = runtime.run(
            "spatial_aggregation",
            spatial_aggregation_component,
            dataset=dataset,
            start=request.start,
            end=request.end,
            bbox=resolved_inputs["bbox"],
            features=resolved_inputs["features"],
            method=method,
            feature_id_property=feature_id_property,
            aggregated_dataset=temporal_dataset,
        )
    return {"records": records}


def run_build_datavalueset_step(
    *,
    step: Any,
    runtime: Any,
    request: Any,
    dataset: dict[str, Any],
    resolved_inputs: dict[str, Any],
    step_config: dict[str, Any],
) -> dict[str, Any]:
    """Workflow runtime adapter for build_datavalueset."""
    del dataset, step
    period_type = request.temporal_aggregation.target_period_type
    execution_mode = str(step_config.get("execution_mode", "local")).lower()
    if execution_mode == "remote":
        outputs = runtime.run(
            "build_datavalueset",
            _invoke_registered_remote_component,
            component_key="build_datavalueset@v1",
            remote_url=str(step_config["remote_url"]),
            request=request,
            resolved_inputs=resolved_inputs,
            timeout_sec=float(step_config.get("remote_timeout_sec", 30.0)),
            retries=int(step_config.get("remote_retries", 1)),
            retry_delay_sec=float(step_config.get("remote_retry_delay_sec", 1.0)),
        )
        data_value_set = outputs["data_value_set"]
        output_file = outputs["output_file"]
    else:
        data_value_set, output_file = runtime.run(
            "build_datavalueset",
            build_datavalueset_component,
            records=resolved_inputs["records"],
            dataset_id=request.dataset_id,
            period_type=period_type,
            dhis2=request.dhis2,
        )
    return {"data_value_set": data_value_set, "output_file": output_file}


def require_dataset(dataset_id: str) -> dict[str, Any]:
    """Resolve dataset or raise 404."""
    dataset = get_dataset(dataset_id)
    if dataset is None:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    return dataset


def workflow_runtime_registry() -> dict[str, ComponentRuntimeDefinition]:
    """Workflow runtime bindings keyed by component@version."""
    handler_registry = _workflow_runtime_handler_registry()
    runtime_bindings: dict[str, ComponentRuntimeDefinition] = {}
    for key, manifest in _COMPONENT_REGISTRY.items():
        local_handler = manifest.runtime.local_handler
        if local_handler is None:
            continue
        executor = handler_registry.get(local_handler)
        if executor is None:
            raise RuntimeError(f"Unknown local runtime handler '{local_handler}' for component '{key}'")
        runtime_bindings[key] = ComponentRuntimeDefinition(
            component=manifest.name,
            version=manifest.version,
            executor=executor,
            config_model=_RemoteCapableStepConfig,
        )
    return runtime_bindings


def _workflow_runtime_handler_registry() -> dict[str, WorkflowStepExecutor]:
    """Resolve local workflow runtime handlers from manifest identifiers."""
    return {
        "workflow.feature_source": run_feature_source_step,
        "workflow.download_dataset": run_download_dataset_step,
        "workflow.temporal_aggregation": run_temporal_aggregation_step,
        "workflow.spatial_aggregation": run_spatial_aggregation_step,
        "workflow.build_datavalueset": run_build_datavalueset_step,
    }


def validate_component_runtime_config(component: str, version: str, config: dict[str, Any]) -> None:
    """Validate runtime config for one workflow-executable component."""
    manifest = _COMPONENT_REGISTRY.get(f"{component}@{version}")
    if manifest is None:
        raise ValueError(f"No component manifest registered for '{component}@{version}'")
    runtime_definition = workflow_runtime_registry().get(f"{component}@{version}")
    if runtime_definition is None:
        raise ValueError(f"No runtime config schema registered for component '{component}@{version}'")
    try:
        validated = runtime_definition.config_model.model_validate(config)
    except ValidationError as exc:
        raise ValueError(f"Invalid config for component '{component}@{version}': {exc}") from exc
    mode = str(getattr(validated, "execution_mode", "local")).lower()
    if mode not in {"local", "remote"}:
        raise ValueError(
            f"Invalid config for component '{component}@{version}': execution_mode must be local or remote"
        )
    if mode not in set(manifest.runtime.supported_execution_modes):
        allowed = ", ".join(manifest.runtime.supported_execution_modes)
        raise ValueError(
            f"Invalid config for component '{component}@{version}': execution_mode '{mode}' not supported; "
            f"allowed values: {allowed}"
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
    if mode == "remote" and (not isinstance(remote_url, str) or not remote_url.strip()):
        raise ValueError(
            f"Invalid config for component '{component}@{version}': remote_url is required for remote mode"
        )


def _dataset_period_type(dataset: Mapping[str, Any]) -> PeriodType | None:
    raw_value = dataset.get("period_type")
    if not isinstance(raw_value, str):
        return None
    normalized = raw_value.strip().lower()
    try:
        return PeriodType(normalized)
    except ValueError:
        return None


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


def _invoke_registered_remote_component(
    *,
    component_key: str,
    remote_url: str,
    request: Any,
    resolved_inputs: dict[str, Any],
    timeout_sec: float,
    retries: int,
    retry_delay_sec: float,
) -> dict[str, Any]:
    """Invoke a manifest-registered HTTP component as a black box."""
    manifest = _COMPONENT_REGISTRY.get(component_key)
    if manifest is None:
        raise RuntimeError(f"Unknown component manifest '{component_key}'")
    payload = _resolve_runtime_bindings(manifest.runtime.remote_request_bindings, request, resolved_inputs)
    response = _post_remote_json(
        remote_url=remote_url,
        payload=payload,
        timeout_sec=timeout_sec,
        retries=retries,
        retry_delay_sec=retry_delay_sec,
    )
    return _extract_remote_outputs(manifest=manifest, response=response)


def _invoke_remote_feature_source_component(
    *,
    remote_url: str,
    feature_source: dict[str, Any],
    timeout_sec: float,
    retries: int,
    retry_delay_sec: float,
) -> tuple[dict[str, Any], list[float]]:
    result = _post_remote_json(
        remote_url=remote_url,
        payload={"feature_source": feature_source, "include_features": True},
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
    return _post_remote_json(
        remote_url=remote_url,
        payload={
            "dataset_id": dataset_id,
            "start": start,
            "end": end,
            "bbox": bbox,
            "target_period_type": target_period_type,
            "method": method,
        },
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
    result = _post_remote_json(
        remote_url=remote_url,
        payload={
            "dataset_id": dataset_id,
            "start": start,
            "end": end,
            "feature_source": feature_source,
            "method": method,
            "bbox": bbox,
            "feature_id_property": feature_id_property,
            "include_records": True,
        },
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
    result = _post_remote_json(
        remote_url=remote_url,
        payload={
            "dataset_id": dataset_id,
            "period_type": period_type,
            "records": records,
            "dhis2": dhis2,
        },
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


def _resolve_runtime_bindings(
    bindings: dict[str, Any],
    request: Any,
    resolved_inputs: dict[str, Any],
) -> dict[str, Any]:
    """Resolve manifest-declared HTTP payload bindings."""
    return {
        key: _resolve_runtime_value(value, request=request, resolved_inputs=resolved_inputs)
        for key, value in bindings.items()
    }


def _resolve_runtime_value(value: Any, *, request: Any, resolved_inputs: dict[str, Any]) -> Any:
    """Resolve one runtime binding value."""
    if isinstance(value, str) and value.startswith("$request."):
        return _dump_runtime_value(_lookup_object_path(request, value.removeprefix("$request.")))
    if isinstance(value, str) and value.startswith("$resolved."):
        return _dump_runtime_value(_lookup_mapping_path(resolved_inputs, value.removeprefix("$resolved.")))
    if isinstance(value, dict):
        return {
            key: _resolve_runtime_value(item, request=request, resolved_inputs=resolved_inputs)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_resolve_runtime_value(item, request=request, resolved_inputs=resolved_inputs) for item in value]
    return value


def _lookup_object_path(obj: Any, path: str) -> Any:
    """Resolve dotted attribute path from object or mapping."""
    current = obj
    for part in path.split("."):
        if isinstance(current, Mapping):
            current = current[part]
        else:
            current = getattr(current, part)
    return current


def _lookup_mapping_path(mapping: Mapping[str, Any], path: str) -> Any:
    """Resolve dotted path from mapping."""
    current: Any = mapping
    for part in path.split("."):
        if not isinstance(current, Mapping):
            raise KeyError(path)
        current = current[part]
    return current


def _dump_runtime_value(value: Any) -> Any:
    """Convert pydantic/enums to JSON-friendly values for HTTP payloads."""
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if hasattr(value, "value"):
        return value.value
    return value


def _extract_remote_outputs(*, manifest: ComponentManifest, response: dict[str, Any]) -> dict[str, Any]:
    """Project HTTP response into declared workflow outputs."""
    bindings = manifest.runtime.remote_response_bindings
    if not bindings:
        return {output_name: response[output_name] for output_name in manifest.outputs}
    extracted: dict[str, Any] = {}
    for output_name, response_key in bindings.items():
        extracted[output_name] = response.get(response_key)
    return extracted
