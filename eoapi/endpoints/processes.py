from datetime import date
import logging
import os
from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field, ValidationError

from pygeoapi.api import FORMAT_TYPES, F_JSON
from pygeoapi.util import url_join

from eoapi.datasets import load_datasets
from eoapi.dhis2_integration import import_data_values_to_dhis2, iso_to_dhis2_period
from eoapi.endpoints.errors import invalid_parameter, not_found
from eoapi.endpoints.features import org_unit_items
from eoapi.jobs import create_job, get_job, update_job
from eoapi.orchestration.prefect import get_flow_run, prefect_enabled, prefect_state_to_job_status

router = APIRouter(tags=["Processes"])
logger = logging.getLogger(__name__)

AGGREGATE_PROCESS_ID = "eo-aggregate-import"
XCLIM_CDD_PROCESS_ID = "xclim-cdd"
XCLIM_CWD_PROCESS_ID = "xclim-cwd"
XCLIM_WARM_DAYS_PROCESS_ID = "xclim-warm-days"

XCLIM_PROCESS_IDS = {
    XCLIM_CDD_PROCESS_ID,
    XCLIM_CWD_PROCESS_ID,
    XCLIM_WARM_DAYS_PROCESS_ID,
}

TIME_DIM_CANDIDATES = ("time", "valid_time")
X_DIM_CANDIDATES = ("x", "lon", "longitude")
Y_DIM_CANDIDATES = ("y", "lat", "latitude")


class DHIS2ImportOptions(BaseModel):
    dataElementId: str = Field(min_length=1)
    dataElementMap: dict[str, str] | None = None
    dryRun: bool = True


class AggregateImportInputs(BaseModel):
    datasetId: str = Field(min_length=1)
    parameters: list[str] | None = None
    datetime: str | None = None
    start: str | None = None
    end: str | None = None
    orgUnitLevel: int = Field(default=2, ge=1)
    aggregation: Literal["mean", "sum", "min", "max"] = "mean"
    dhis2: DHIS2ImportOptions


class ThresholdInput(BaseModel):
    value: float
    unit: str = Field(min_length=1)


class XclimIndicatorInputs(BaseModel):
    datasetId: str = Field(min_length=1)
    parameter: str = Field(min_length=1)
    start: date
    end: date
    orgUnitLevel: int = Field(default=2, ge=1)
    threshold: ThresholdInput
    dhis2: DHIS2ImportOptions


class ExecuteRequest(BaseModel):
    inputs: dict[str, Any]


def _base_url(request: Request) -> str:
    return str(request.base_url).rstrip("/")


def _select_parameters(dataset_id: str, requested: list[str] | None) -> list[str]:
    dataset = load_datasets().get(dataset_id)
    if dataset is None:
        raise not_found("Collection", dataset_id)

    available = list(dataset.parameters.keys())
    if not available:
        raise invalid_parameter(f"No parameters configured for collection '{dataset.id}'")

    if not requested:
        return available

    unknown = [parameter for parameter in requested if parameter not in dataset.parameters]
    if unknown:
        raise invalid_parameter(f"Unknown parameter(s): {', '.join(unknown)}")

    return requested


def _value_for(index: int, parameter: str, aggregation: str) -> float:
    base = (index + 1) * (len(parameter) + 1)
    if aggregation == "sum":
        return round(base * 1.5, 4)
    if aggregation == "min":
        return round(base * 0.5, 4)
    if aggregation == "max":
        return round(base * 2.0, 4)
    return round(base * 1.0, 4)


def _build_aggregate_data_values(
    features: list[dict[str, Any]],
    *,
    default_data_element_id: str,
    data_element_map: dict[str, str] | None,
    period: str,
) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for feature in features:
        org_unit = feature["properties"].get("orgUnit")
        values = feature["properties"].get("values", {})
        if not isinstance(org_unit, str) or not isinstance(values, dict):
            continue

        for parameter_id, value in values.items():
            mapped_data_element = (data_element_map or {}).get(parameter_id, default_data_element_id)
            if not mapped_data_element:
                continue
            payload.append(
                {
                    "dataElement": mapped_data_element,
                    "orgUnit": org_unit,
                    "period": period,
                    "value": value,
                }
            )

    return payload


def _build_indicator_data_values(
    features: list[dict[str, Any]],
    *,
    data_element_id: str,
    period: str,
) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for feature in features:
        properties = feature.get("properties", {})
        if not isinstance(properties, dict):
            continue

        org_unit = properties.get("orgUnit")
        value = properties.get("value")
        if not isinstance(org_unit, str) or value is None:
            continue

        payload.append(
            {
                "dataElement": data_element_id,
                "orgUnit": org_unit,
                "period": period,
                "value": value,
            }
        )

    return payload


def _coerce_date(value: str) -> date:
    try:
        return date.fromisoformat(value[:10])
    except ValueError as exc:
        raise invalid_parameter(f"Invalid date value '{value}'") from exc


def _aggregate_series_values(values: list[float], aggregation: str) -> float:
    if not values:
        return 0.0

    if aggregation == "sum":
        return round(sum(values), 4)
    if aggregation == "min":
        return round(min(values), 4)
    if aggregation == "max":
        return round(max(values), 4)
    return round(sum(values) / len(values), 4)


def _extract_aggregate_values_from_dhis2eo(
    *,
    dataset_id: str,
    parameters: list[str],
    start: date,
    end: date,
    aggregation: str,
    org_units: list[dict[str, Any]],
) -> dict[str, dict[str, float]]:
    try:
        import numpy as np
        import pandas as pd
        import xarray as xr
    except ImportError as exc:
        raise RuntimeError("Missing xarray/pandas/numpy dependencies for aggregate extraction") from exc

    values_by_org: dict[str, dict[str, float]] = {feature["id"]: {} for feature in org_units}

    for parameter in parameters:
        series_by_org = _extract_org_unit_series(
            dataset_id=dataset_id,
            parameter=parameter,
            start=start,
            end=end,
            org_units=org_units,
            np=np,
            pd=pd,
            xr=xr,
        )

        for org_id, series in series_by_org.items():
            loaded = series.load()
            raw = loaded.values
            flat = [float(value) for value in raw.flatten().tolist() if value is not None]
            values_by_org.setdefault(org_id, {})[parameter] = _aggregate_series_values(flat, aggregation)

    return values_by_org


def _parse_process_inputs(process_id: str, payload: ExecuteRequest) -> AggregateImportInputs | XclimIndicatorInputs:
    try:
        if process_id == AGGREGATE_PROCESS_ID:
            return AggregateImportInputs.model_validate(payload.inputs)
        if process_id in XCLIM_PROCESS_IDS:
            return XclimIndicatorInputs.model_validate(payload.inputs)
    except ValidationError as exc:
        raise invalid_parameter(f"Invalid process inputs: {exc.errors()}") from exc

    raise not_found("Process", process_id)


def _process_definition(process_id: str, base: str) -> dict[str, Any]:
    descriptions = {
        AGGREGATE_PROCESS_ID: {
            "title": "EO aggregate and DHIS2 import",
            "description": "Aggregate EO dataset values to DHIS2 org units and import as dataValueSets.",
        },
        XCLIM_CDD_PROCESS_ID: {
            "title": "xclim consecutive dry days (CDD)",
            "description": "Compute xclim CDD over org units using daily precipitation and import-ready outputs.",
        },
        XCLIM_CWD_PROCESS_ID: {
            "title": "xclim consecutive wet days (CWD)",
            "description": "Compute xclim CWD over org units using daily precipitation and import-ready outputs.",
        },
        XCLIM_WARM_DAYS_PROCESS_ID: {
            "title": "xclim warm days above threshold",
            "description": "Compute number of warm days above threshold over org units and import-ready outputs.",
        },
    }
    meta = descriptions[process_id]

    return {
        "id": process_id,
        "title": meta["title"],
        "description": meta["description"],
        "jobControlOptions": ["async-execute"],
        "links": [
            {"rel": "self", "type": FORMAT_TYPES[F_JSON], "href": url_join(base, "processes", process_id)},
            {
                "rel": "execute",
                "type": FORMAT_TYPES[F_JSON],
                "href": url_join(base, "processes", process_id, "execution"),
            },
        ],
    }


def _validate_xclim_inputs(process_id: str, inputs: XclimIndicatorInputs) -> None:
    if inputs.start > inputs.end:
        raise invalid_parameter("Input 'start' must be before or equal to 'end'")

    datasets = load_datasets()
    dataset = datasets.get(inputs.datasetId)
    if dataset is None:
        raise not_found("Collection", inputs.datasetId)

    if inputs.parameter not in dataset.parameters:
        raise invalid_parameter(f"Unknown parameter '{inputs.parameter}' for collection '{inputs.datasetId}'")

    threshold_unit = inputs.threshold.unit.lower()
    if process_id in {XCLIM_CDD_PROCESS_ID, XCLIM_CWD_PROCESS_ID} and "mm" not in threshold_unit:
        raise invalid_parameter("Precipitation threshold unit must include 'mm' (for example 'mm/day')")
    if process_id == XCLIM_WARM_DAYS_PROCESS_ID and ("degc" not in threshold_unit and "c" != threshold_unit):
        raise invalid_parameter("Warm-days threshold unit must be Celsius (for example 'degC')")


def _run_xclim(process_id: str, inputs: XclimIndicatorInputs) -> dict[str, Any]:
    _validate_xclim_inputs(process_id, inputs)

    try:
        import numpy as np
        import pandas as pd
        import xarray as xr
        from xclim import indices as xci
    except ImportError as exc:
        raise invalid_parameter(
            "xclim execution dependencies are missing. Install optional runtime dependencies for xclim integration."
        ) from exc

    org_units = org_unit_items(level=inputs.orgUnitLevel)
    if not org_units:
        raise invalid_parameter(f"No org unit features found for level {inputs.orgUnitLevel}")

    threshold = f"{inputs.threshold.value} {inputs.threshold.unit}"
    uses_dhis2eo_source = True
    try:
        org_unit_series = _extract_org_unit_series(
            dataset_id=inputs.datasetId,
            parameter=inputs.parameter,
            start=inputs.start,
            end=inputs.end,
            org_units=org_units,
            np=np,
            pd=pd,
            xr=xr,
        )
    except Exception as exc:
        logger.warning("Falling back to synthetic xclim source after extraction failure: %s", exc)
        uses_dhis2eo_source = False
        org_unit_series = _synthetic_org_unit_series(
            process_id=process_id,
            start=inputs.start,
            end=inputs.end,
            org_units=org_units,
            np=np,
            pd=pd,
            xr=xr,
        )

    result_features: list[dict[str, Any]] = []

    for feature in org_units:
        series = org_unit_series.get(feature["id"])
        if series is None:
            continue

        if process_id in {XCLIM_CDD_PROCESS_ID, XCLIM_CWD_PROCESS_ID}:
            if process_id == XCLIM_CDD_PROCESS_ID:
                indicator = xci.maximum_consecutive_dry_days(series, thresh=threshold, freq="YS")
            else:
                indicator = xci.maximum_consecutive_wet_days(series, thresh=threshold, freq="YS")
        else:
            indicator = xci.tx_days_above(series, thresh=threshold, freq="YS")

        if "time" in indicator.dims:
            scalar = indicator.isel(time=-1).load()
        else:
            scalar = indicator.load()
        value = float(scalar.item())

        result_features.append(
            {
                "type": "Feature",
                "id": feature["id"],
                "geometry": feature["geometry"],
                "properties": {
                    "orgUnit": feature["id"],
                    "datasetId": inputs.datasetId,
                    "indicator": process_id,
                    "parameter": inputs.parameter,
                    "start": inputs.start.isoformat(),
                    "end": inputs.end.isoformat(),
                    "source": "dhis2eo" if uses_dhis2eo_source else "synthetic-fallback",
                    "threshold": inputs.threshold.model_dump(),
                    "value": round(value, 4),
                },
            }
        )

    period = inputs.end.strftime("%Y%m%d")
    data_values = _build_indicator_data_values(
        result_features,
        data_element_id=inputs.dhis2.dataElementId,
        period=period,
    )
    import_summary = import_data_values_to_dhis2(data_values, dry_run=inputs.dhis2.dryRun)
    outputs = {
        "importSummary": import_summary,
        "features": result_features,
    }
    return create_job(process_id, inputs.model_dump(mode="json"), outputs)


def _synthetic_org_unit_series(
    process_id: str,
    start: date,
    end: date,
    org_units: list[dict[str, Any]],
    np: Any,
    pd: Any,
    xr: Any,
) -> dict[str, Any]:
    times = pd.date_range(start.isoformat(), end.isoformat(), freq="D")
    if len(times) == 0:
        raise invalid_parameter("The selected period did not produce any daily timesteps")

    values_by_org_unit: dict[str, Any] = {}
    for index, feature in enumerate(org_units):
        if process_id in {XCLIM_CDD_PROCESS_ID, XCLIM_CWD_PROCESS_ID}:
            base = 0.5 + (index % 4)
            signal = base + (np.arange(len(times)) % 6) * 0.35
            series = xr.DataArray(
                signal,
                coords={"time": times},
                dims=["time"],
                attrs={"units": "mm/day"},
            )
        else:
            base = 22.0 + (index % 5)
            signal = base + (np.arange(len(times)) % 9) * 1.4
            series = xr.DataArray(
                signal,
                coords={"time": times},
                dims=["time"],
                attrs={"units": "degC"},
            )

        values_by_org_unit[feature["id"]] = series

    return values_by_org_unit


def _extract_org_unit_series(
    dataset_id: str,
    parameter: str,
    start: date,
    end: date,
    org_units: list[dict[str, Any]],
    np: Any,
    pd: Any,
    xr: Any,
) -> dict[str, Any]:
    da = _extract_dataarray_with_dhis2eo(dataset_id, parameter, start, end, org_units, xr)
    da = _prepare_dataarray_for_parameter(da, parameter, pd)

    x_dim, y_dim = _spatial_dims(da)
    values_by_org_unit: dict[str, Any] = {}

    for feature in org_units:
        minx, miny, maxx, maxy = _feature_bounds(feature)
        clipped = da
        if x_dim:
            clipped = _slice_dim(clipped, x_dim, minx, maxx)
        if y_dim:
            clipped = _slice_dim(clipped, y_dim, miny, maxy)

        if x_dim and y_dim:
            series = clipped.mean(dim=[y_dim, x_dim], skipna=True)
        elif x_dim:
            series = clipped.mean(dim=[x_dim], skipna=True)
        elif y_dim:
            series = clipped.mean(dim=[y_dim], skipna=True)
        else:
            series = clipped

        if "time" not in series.dims:
            time_values = pd.date_range(start.isoformat(), end.isoformat(), freq="D")
            expanded = xr.DataArray(
                np.repeat(float(series.item()), len(time_values)),
                coords={"time": time_values},
                dims=["time"],
                attrs=series.attrs,
            )
            series = expanded

        values_by_org_unit[feature["id"]] = series

    return values_by_org_unit


def _extract_dataarray_with_dhis2eo(
    dataset_id: str,
    parameter: str,
    start: date,
    end: date,
    org_units: list[dict[str, Any]],
    xr: Any,
) -> Any:
    bbox = _org_units_bbox(org_units)
    cache_dir = _xclim_cache_dir(dataset_id, parameter)
    prefix = f"{parameter}_{start.isoformat()}_{end.isoformat()}".replace("-", "")

    if dataset_id == "chirps-daily":
        from dhis2eo.data.chc.chirps3 import daily as chirps_daily

        files = chirps_daily.download(
            start=start.isoformat(),
            end=end.isoformat(),
            bbox=bbox,
            dirname=str(cache_dir),
            prefix=prefix,
            var_name="precip",
            overwrite=False,
        )
        paths = [str(path) for path in files if Path(path).exists()]
        if not paths:
            raise RuntimeError("No CHIRPS files were downloaded")

        dataset = xr.open_mfdataset(paths, combine="by_coords")
        return _dataset_dataarray(dataset, preferred=[parameter, "precip"])

    if dataset_id == "era5-land-daily":
        from dhis2eo.data.cds.era5_land import hourly as era5_hourly

        variable = "total_precipitation" if parameter == "precip" else parameter
        files = era5_hourly.download(
            start=start.isoformat(),
            end=end.isoformat(),
            bbox=bbox,
            dirname=str(cache_dir),
            prefix=prefix,
            variables=[variable],
            overwrite=False,
        )
        paths = [str(path) for path in files if Path(path).exists()]
        if not paths:
            raise RuntimeError("No ERA5-Land files were downloaded")

        dataset = xr.open_mfdataset(paths, combine="by_coords")
        return _dataset_dataarray(dataset, preferred=[parameter, variable, "t2m", "tp"])

    raise RuntimeError(f"No dhis2eo extractor configured for dataset '{dataset_id}'")


def _dataset_dataarray(dataset: Any, preferred: list[str]) -> Any:
    for name in preferred:
        if name in dataset.data_vars:
            da = dataset[name]
            da.name = name
            return da

    first_name = next(iter(dataset.data_vars.keys()), None)
    if first_name is None:
        raise RuntimeError("Downloaded dataset has no data variables")
    da = dataset[first_name]
    da.name = first_name
    return da


def _prepare_dataarray_for_parameter(da: Any, parameter: str, pd: Any) -> Any:
    time_dim = _time_dim(da)
    if time_dim != "time":
        da = da.rename({time_dim: "time"})

    da = da.sortby("time")
    units = str(da.attrs.get("units", "")).lower()

    if parameter in {"precip", "total_precipitation"}:
        da = da.resample(time="1D").sum(skipna=True)
        if units == "m":
            da = da * 1000.0
            da.attrs["units"] = "mm/day"
        elif "mm" in units:
            da.attrs["units"] = "mm/day"
        else:
            da.attrs["units"] = da.attrs.get("units", "mm/day")
    else:
        da = da.resample(time="1D").mean(skipna=True)
        if units in {"k", "kelvin"}:
            da = da - 273.15
            da.attrs["units"] = "degC"
        elif units in {"c", "degc", "°c"}:
            da.attrs["units"] = "degC"

    return da


def _org_units_bbox(org_units: list[dict[str, Any]]) -> tuple[float, float, float, float]:
    bounds = [_feature_bounds(feature) for feature in org_units]
    minx = min(bound[0] for bound in bounds)
    miny = min(bound[1] for bound in bounds)
    maxx = max(bound[2] for bound in bounds)
    maxy = max(bound[3] for bound in bounds)
    return (minx, miny, maxx, maxy)


def _feature_bounds(feature: dict[str, Any]) -> tuple[float, float, float, float]:
    coordinates = feature.get("geometry", {}).get("coordinates", [])
    if not coordinates:
        return (-180.0, -90.0, 180.0, 90.0)
    ring = coordinates[0]
    xs = [point[0] for point in ring]
    ys = [point[1] for point in ring]
    return (min(xs), min(ys), max(xs), max(ys))


def _xclim_cache_dir(dataset_id: str, parameter: str) -> Path:
    root = Path(os.getenv("EOAPI_XCLIM_CACHE_DIR", ".cache/xclim"))
    path = root / dataset_id / parameter
    path.mkdir(parents=True, exist_ok=True)
    return path


def _time_dim(da: Any) -> str:
    for dim in TIME_DIM_CANDIDATES:
        if dim in da.dims:
            return dim
    raise RuntimeError(f"No time dimension found in data array dims={da.dims}")


def _spatial_dims(da: Any) -> tuple[str | None, str | None]:
    x_dim = next((dim for dim in X_DIM_CANDIDATES if dim in da.dims), None)
    y_dim = next((dim for dim in Y_DIM_CANDIDATES if dim in da.dims), None)
    return x_dim, y_dim


def _slice_dim(da: Any, dim: str, lower: float, upper: float) -> Any:
    coords = da[dim].values
    if len(coords) == 0:
        return da
    if coords[0] <= coords[-1]:
        return da.sel({dim: slice(lower, upper)})
    return da.sel({dim: slice(upper, lower)})


def _sync_prefect_job(job: dict[str, Any]) -> dict[str, Any]:
    execution = job.get("execution") or {}
    if execution.get("source") != "prefect":
        return job

    flow_run_id = execution.get("flowRunId")
    if not flow_run_id or not prefect_enabled():
        return job

    try:
        flow_run = get_flow_run(flow_run_id)
    except RuntimeError:
        return job

    state = flow_run.get("state") or {}
    mapped_status = prefect_state_to_job_status(state.get("type"))
    progress = 0
    if mapped_status == "running":
        progress = 50
    elif mapped_status in {"succeeded", "failed"}:
        progress = 100

    updated = update_job(
        job["jobId"],
        {
            "status": mapped_status,
            "progress": progress,
            "execution": {
                **execution,
                "state": state,
            },
        },
    )
    return updated or job


def run_aggregate_import(inputs: AggregateImportInputs) -> dict[str, Any]:
    parameters = _select_parameters(inputs.datasetId, inputs.parameters)
    org_units = org_unit_items(level=inputs.orgUnitLevel)
    if not org_units:
        raise invalid_parameter(f"No org unit features found for level {inputs.orgUnitLevel}")

    dataset = load_datasets()[inputs.datasetId]
    effective_time = inputs.datetime or inputs.start or dataset.temporal_interval[0]

    aggregate_start = inputs.start or inputs.datetime or dataset.temporal_interval[0]
    aggregate_end = inputs.end or inputs.datetime or aggregate_start

    values_by_org: dict[str, dict[str, float]] = {}
    value_source = "dhis2eo"
    try:
        values_by_org = _extract_aggregate_values_from_dhis2eo(
            dataset_id=inputs.datasetId,
            parameters=parameters,
            start=_coerce_date(aggregate_start),
            end=_coerce_date(aggregate_end),
            aggregation=inputs.aggregation,
            org_units=org_units,
        )
    except Exception as exc:
        logger.warning("Falling back to synthetic aggregate source after extraction failure: %s", exc)
        value_source = "synthetic-fallback"

    result_features: list[dict[str, Any]] = []
    for index, feature in enumerate(org_units):
        values = values_by_org.get(feature["id"])
        if values is None:
            values = {
                parameter: _value_for(index, parameter, inputs.aggregation)
                for parameter in parameters
            }

        result_features.append(
            {
                "type": "Feature",
                "id": feature["id"],
                "geometry": feature["geometry"],
                "properties": {
                    "orgUnit": feature["id"],
                    "datasetId": inputs.datasetId,
                    "datetime": effective_time,
                    "aggregation": inputs.aggregation,
                    "source": value_source,
                    "values": values,
                },
            }
        )

    period = iso_to_dhis2_period(effective_time)
    data_values = _build_aggregate_data_values(
        result_features,
        default_data_element_id=inputs.dhis2.dataElementId,
        data_element_map=inputs.dhis2.dataElementMap,
        period=period,
    )
    import_summary = import_data_values_to_dhis2(data_values, dry_run=inputs.dhis2.dryRun)
    outputs = {
        "importSummary": import_summary,
        "features": result_features,
    }
    return create_job(AGGREGATE_PROCESS_ID, inputs.model_dump(), outputs)


def run_process(process_id: str, inputs: dict[str, Any]) -> dict[str, Any]:
    parsed_inputs = _parse_process_inputs(process_id, ExecuteRequest(inputs=inputs))
    if isinstance(parsed_inputs, AggregateImportInputs):
        return run_aggregate_import(parsed_inputs)
    return _run_xclim(process_id, parsed_inputs)


@router.get("/processes")
def get_processes(request: Request) -> dict[str, Any]:
    base = _base_url(request)
    process_ids = [
        AGGREGATE_PROCESS_ID,
        XCLIM_CDD_PROCESS_ID,
        XCLIM_CWD_PROCESS_ID,
        XCLIM_WARM_DAYS_PROCESS_ID,
    ]

    return {
        "processes": [
            {
                "id": process_id,
                "title": _process_definition(process_id, base)["title"],
                "description": _process_definition(process_id, base)["description"],
                "links": [
                    {
                        "rel": "process",
                        "type": FORMAT_TYPES[F_JSON],
                        "href": url_join(base, "processes", process_id),
                    }
                ],
            }
            for process_id in process_ids
        ],
        "links": [
            {"rel": "self", "type": FORMAT_TYPES[F_JSON], "href": url_join(base, "processes")},
            {"rel": "root", "type": FORMAT_TYPES[F_JSON], "href": url_join(base, "/")},
        ],
    }


@router.get("/processes/{processId}")
def get_process(processId: str, request: Request) -> dict[str, Any]:
    if processId not in {
        AGGREGATE_PROCESS_ID,
        XCLIM_CDD_PROCESS_ID,
        XCLIM_CWD_PROCESS_ID,
        XCLIM_WARM_DAYS_PROCESS_ID,
    }:
        raise not_found("Process", processId)

    base = _base_url(request)
    return _process_definition(processId, base)


@router.post("/processes/{processId}/execution", status_code=202)
def execute_process(processId: str, payload: ExecuteRequest, request: Request) -> dict[str, Any]:
    job = run_process(processId, payload.inputs)

    base = _base_url(request)
    return {
        "jobId": job["jobId"],
        "processId": processId,
        "status": "queued",
        "links": [
            {"rel": "monitor", "type": FORMAT_TYPES[F_JSON], "href": url_join(base, "jobs", job["jobId"])},
            {
                "rel": "results",
                "type": FORMAT_TYPES[F_JSON],
                "href": f"{url_join(base, 'features', 'aggregated-results', 'items')}?jobId={job['jobId']}",
            },
        ],
    }


@router.get("/jobs/{jobId}")
def get_job_status(jobId: str, request: Request) -> dict[str, Any]:
    job = get_job(jobId)
    if job is None:
        raise not_found("Job", jobId)

    job = _sync_prefect_job(job)
    import_summary = (job.get("outputs") or {}).get(
        "importSummary",
        {
            "imported": 0,
            "updated": 0,
            "ignored": 0,
            "deleted": 0,
            "dryRun": True,
        },
    )

    base = _base_url(request)
    return {
        "jobId": job["jobId"],
        "processId": job["processId"],
        "status": job["status"],
        "progress": job["progress"],
        "created": job["created"],
        "updated": job["updated"],
        "importSummary": import_summary,
        "execution": job.get("execution"),
        "links": [
            {"rel": "self", "type": FORMAT_TYPES[F_JSON], "href": url_join(base, "jobs", jobId)},
            {
                "rel": "results",
                "type": FORMAT_TYPES[F_JSON],
                "href": f"{url_join(base, 'features', 'aggregated-results', 'items')}?jobId={jobId}",
            },
        ],
    }
