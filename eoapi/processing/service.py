"""Execution service for architecture skeleton OGC processes."""

from datetime import date
from typing import Any

from pydantic import AliasChoices, BaseModel, Field, ValidationError

from eoapi.endpoints.errors import invalid_parameter, not_found
from eoapi.jobs import create_job
from eoapi.processing.formatters import rows_to_csv, rows_to_dhis2_stub
from eoapi.processing.process_catalog import (
    DATA_TEMPORAL_AGGREGATE_PROCESS_ID,
    RASTER_POINT_TIMESERIES_PROCESS_ID,
    RASTER_ZONAL_STATS_PROCESS_ID,
    is_process_supported,
)
from eoapi.processing.providers import RasterFetchRequest, build_provider
from eoapi.processing.raster_ops import point_timeseries_stub, temporal_aggregate_stub, zonal_stats_stub
from eoapi.processing.registry import DatasetRegistryEntry, load_dataset_registry


class SkeletonProcessInputs(BaseModel):
    """Normalized skeleton process inputs."""

    dataset_id: str = Field(min_length=1, validation_alias=AliasChoices("dataset_id", "datasetId"))
    params: list[str] | None = Field(default=None, validation_alias=AliasChoices("params", "parameters"))
    time: str | None = Field(default=None, validation_alias=AliasChoices("time", "datetime"))
    aoi: dict[str, Any] | list[float] | None = None
    frequency: str | None = Field(default=None, validation_alias=AliasChoices("frequency", "target_frequency"))
    aggregation: str | None = None


def _parse_date(raw_value: str, field_name: str) -> date:
    """Parse YYYY-MM-DD (or ISO datetime) and raise API-friendly errors."""

    try:
        return date.fromisoformat(raw_value[:10])
    except ValueError as exc:
        raise invalid_parameter(f"Invalid {field_name} value '{raw_value}'") from exc


def _bbox_from_aoi(value: dict[str, Any] | list[float] | None) -> tuple[float, float, float, float]:
    """Resolve AOI input into a bbox tuple; default is global extent."""

    if value is None:
        return (-180.0, -90.0, 180.0, 90.0)

    if isinstance(value, list) and len(value) == 4:
        return (float(value[0]), float(value[1]), float(value[2]), float(value[3]))

    if isinstance(value, dict):
        bbox = value.get("bbox")
        if isinstance(bbox, list) and len(bbox) == 4:
            return (float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3]))

    raise invalid_parameter("Input 'aoi' must be a bbox array or an object with a bbox array")


def _resolve_requested_params(dataset: DatasetRegistryEntry, requested: list[str] | None) -> list[str]:
    """Validate requested params against registry dataset parameters."""

    if not requested:
        return dataset.parameters

    unknown = [parameter for parameter in requested if parameter not in dataset.parameters]
    if unknown:
        raise invalid_parameter(
            f"Unknown parameter(s) for dataset '{dataset.id}': {', '.join(sorted(set(unknown)))}"
        )

    return requested


def _run_raster_operation(
    *,
    process_id: str,
    dataset_id: str,
    time_value: str,
    bbox: tuple[float, float, float, float],
    assets: dict[str, list[str]],
    frequency: str,
    aggregation: str,
) -> list[dict[str, Any]]:
    """Dispatch to process-specific raster-op stub implementation."""

    if process_id == RASTER_ZONAL_STATS_PROCESS_ID:
        return zonal_stats_stub(
            dataset_id=dataset_id,
            time_value=time_value,
            bbox=bbox,
            assets=assets,
            aggregation=aggregation,
        )
    if process_id == RASTER_POINT_TIMESERIES_PROCESS_ID:
        return point_timeseries_stub(dataset_id=dataset_id, time_value=time_value, bbox=bbox, assets=assets)
    if process_id == DATA_TEMPORAL_AGGREGATE_PROCESS_ID:
        return temporal_aggregate_stub(
            dataset_id=dataset_id,
            time_value=time_value,
            assets=assets,
            frequency=frequency,
            aggregation=aggregation,
        )
    raise not_found("Process", process_id)


def _build_implementation_metadata(process_id: str, provider: Any) -> dict[str, Any]:
    """Expose the concrete libraries used for provider, compute and formatting stages."""

    provider_details: dict[str, str] = {}
    details_fn = getattr(provider, "implementation_details", None)
    if callable(details_fn):
        provider_details = details_fn()
    provider_libs = ["dhis2eo"] if provider_details else ["custom_provider"]

    if process_id == RASTER_ZONAL_STATS_PROCESS_ID:
        compute = {
            "operation": "zonal statistics over AOI bbox",
            "libs": ["xarray"],
            "notes": "Dataset loading and bbox slicing are handled with xarray in eoapi.processing.raster_ops.",
        }
    elif process_id == RASTER_POINT_TIMESERIES_PROCESS_ID:
        compute = {
            "operation": "point time-series placeholder at AOI centroid",
            "libs": ["python stdlib"],
            "notes": "Current implementation is a skeleton placeholder pending sampled raster extraction.",
        }
    else:
        compute = {
            "operation": "temporal harmonization placeholder",
            "libs": ["python stdlib"],
            "notes": "Current implementation is a skeleton placeholder pending xarray resample/groupby logic.",
        }

    return {
        "provider": {
            "id": getattr(provider, "provider_id", "unknown"),
            "libs": provider_libs,
            "details": provider_details,
        },
        "compute": compute,
        "formatting": {
            "csv": {
                "libs": ["python csv", "python json"],
                "notes": "Canonical rows are serialized to CSV in eoapi.processing.formatters.rows_to_csv.",
            },
            "dhis2": {
                "libs": ["eoapi stub", "dhis2eo.integrations.pandas (planned)"],
                "notes": "DHIS2 payload formatting currently returns a stub envelope.",
            },
        },
    }


def execute_skeleton_process(process_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Run skeleton process flow: validate -> fetch -> op -> format -> create job."""

    if not is_process_supported(process_id):
        raise not_found("Process", process_id)

    try:
        inputs = SkeletonProcessInputs.model_validate(payload)
    except ValidationError as exc:
        raise invalid_parameter(f"Invalid process inputs: {exc.errors()}") from exc

    datasets = load_dataset_registry()
    dataset = datasets.get(inputs.dataset_id)
    if dataset is None:
        raise not_found("Dataset", inputs.dataset_id)

    requested_params = _resolve_requested_params(dataset, inputs.params)
    if inputs.time:
        start = _parse_date(inputs.time, "time")
    elif dataset.temporal_start:
        start = _parse_date(dataset.temporal_start, "temporal_start")
    else:
        start = date.today()
    end = start
    bbox = _bbox_from_aoi(inputs.aoi)
    frequency = (inputs.frequency or "P1M").strip() or "P1M"
    aggregation = (inputs.aggregation or "mean").strip() or "mean"

    provider = build_provider(dataset)
    assets: dict[str, list[str]] = {}
    from_cache = True
    provider_id = provider.provider_id

    for parameter in requested_params:
        result = provider.fetch(
            RasterFetchRequest(
                dataset_id=dataset.id,
                parameter=parameter,
                start=start,
                end=end,
                bbox=bbox,
            )
        )
        assets[parameter] = result.asset_paths
        from_cache = from_cache and result.from_cache
        provider_id = result.provider

    rows = _run_raster_operation(
        process_id=process_id,
        dataset_id=dataset.id,
        time_value=start.isoformat(),
        bbox=bbox,
        assets=assets,
        frequency=frequency,
        aggregation=aggregation,
    )
    csv_payload = rows_to_csv(rows)
    dhis2_payload = rows_to_dhis2_stub(rows)

    outputs = {
        "status": "accepted",
        "message": "Execution complete using provider fetch + process operation pipeline.",
        "dataset_id": dataset.id,
        "requested_params": requested_params,
        "time": start.isoformat(),
        "bbox": list(bbox),
        "provider": provider_id,
        "assets": assets,
        "from_cache": from_cache,
        "options": {
            "frequency": frequency,
            "aggregation": aggregation,
        },
        "rows": rows,
        "csv": csv_payload,
        "dhis2": dhis2_payload,
        "implementation": _build_implementation_metadata(process_id, provider),
    }
    return create_job(process_id, inputs.model_dump(), outputs)
