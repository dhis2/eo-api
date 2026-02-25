"""DHIS2 pipeline process: GeoJSON org units → zonal stats → dataValueSet.

OGC process ID: dhis2.pipeline

Accepts a GeoJSON FeatureCollection (or Feature array) of DHIS2 org units,
runs raster.zonal_stats against the requested dataset for each feature's
bounding box, and returns a DHIS2-conformant dataValueSet ready for import.
"""

from datetime import date
from typing import Any

from pydantic import AliasChoices, BaseModel, Field, ValidationError

from pygeoapi.api import FORMAT_TYPES, F_JSON
from pygeoapi.util import url_join

from eoapi.endpoints.errors import invalid_parameter, not_found
from eoapi.jobs import create_job
from eoapi.processing.process_catalog import DHIS2_PIPELINE_PROCESS_ID
from eoapi.processing.providers import RasterFetchRequest, build_provider
from eoapi.processing.raster_ops import zonal_stats_stub
from eoapi.processing.registry import load_dataset_registry
from eoapi.processing.service import _parse_date, _resolve_requested_params

JSON_MEDIA_TYPE = FORMAT_TYPES[F_JSON]


# ---------------------------------------------------------------------------
# OGC process definition
# ---------------------------------------------------------------------------


def get_pipeline_definition(base_url: str) -> dict[str, Any]:
    """Return the OGC Process Description for dhis2.pipeline."""

    return {
        "id": DHIS2_PIPELINE_PROCESS_ID,
        "title": "DHIS2 Org Units → dataValueSet Pipeline",
        "description": (
            "Accepts DHIS2 GeoJSON org unit features, runs zonal statistics "
            "against a dataset collection for each org unit's bounding box, "
            "and returns a DHIS2-conformant dataValueSet ready for import."
        ),
        "version": "1.0.0",
        "jobControlOptions": ["sync-execute"],
        "inputs": {
            "features": {
                "description": (
                    "GeoJSON FeatureCollection of org units, or an array of "
                    "GeoJSON Features. Each feature must carry polygon geometry "
                    "and a DHIS2 UID in its 'id' field or properties."
                ),
                "schema": {
                    "oneOf": [
                        {
                            "type": "object",
                            "required": ["type", "features"],
                            "properties": {
                                "type": {"const": "FeatureCollection"},
                                "features": {"type": "array"},
                            },
                        },
                        {"type": "array", "items": {"type": "object"}},
                    ]
                },
            },
            "dataset_id": {
                "schema": {"type": "string"},
                "description": "Collection ID to process (e.g. 'chirps-daily').",
            },
            "params": {
                "schema": {"type": "array", "items": {"type": "string"}},
                "description": "Parameter names (e.g. ['precip']). Defaults to all available.",
            },
            "time": {
                "schema": {"type": "string"},
                "description": "Target date in YYYY-MM-DD format.",
            },
            "aggregation": {
                "schema": {
                    "type": "string",
                    "enum": ["mean", "sum", "min", "max"],
                    "default": "mean",
                },
                "description": "Spatial aggregation function applied over each org unit bbox.",
            },
            "data_element": {
                "schema": {"type": "string"},
                "description": "DHIS2 dataElement UID for the output dataValueSet.",
            },
            "category_option_combo": {
                "schema": {"type": "string"},
                "description": "DHIS2 categoryOptionCombo UID (optional; omit to use the server default).",
            },
            "dataset_name": {
                "schema": {"type": "string"},
                "description": "DHIS2 dataSet name/UID for the output envelope (optional; defaults to dataset_id).",
            },
        },
        "outputs": {
            "dataValueSet": {
                "schema": {"type": "object"},
                "description": "DHIS2-conformant dataValueSet payload ready for POST to /api/dataValueSets.",
            },
            "rows": {
                "schema": {"type": "array"},
                "description": "Per-org-unit raw stats rows (one per parameter per feature).",
            },
            "summary": {
                "schema": {"type": "object"},
                "description": "Execution summary: total features, computed values, errors.",
            },
        },
        "links": [
            {
                "rel": "self",
                "type": JSON_MEDIA_TYPE,
                "href": url_join(base_url, "processes", DHIS2_PIPELINE_PROCESS_ID),
            },
            {
                "rel": "http://www.opengis.net/def/rel/ogc/1.0/execute",
                "type": JSON_MEDIA_TYPE,
                "title": "Execute this process",
                "href": url_join(base_url, "processes", DHIS2_PIPELINE_PROCESS_ID, "execution"),
            },
            {
                "rel": "collection",
                "type": JSON_MEDIA_TYPE,
                "title": "Browse datasets usable via dataset_id",
                "href": url_join(base_url, "collections"),
            },
            {
                "rel": "related",
                "type": JSON_MEDIA_TYPE,
                "title": "DHIS2 org unit features source",
                "href": url_join(base_url, "features", "dhis2-org-units", "items"),
            },
        ],
    }


# ---------------------------------------------------------------------------
# Input model
# ---------------------------------------------------------------------------


class PipelineInputs(BaseModel):
    features: dict[str, Any] | list[dict[str, Any]] = Field(
        description="GeoJSON FeatureCollection or Feature array."
    )
    dataset_id: str = Field(
        min_length=1,
        validation_alias=AliasChoices("dataset_id", "datasetId"),
    )
    params: list[str] | None = Field(
        default=None,
        validation_alias=AliasChoices("params", "parameters"),
    )
    time: str | None = Field(
        default=None,
        validation_alias=AliasChoices("time", "datetime"),
    )
    aggregation: str | None = None
    data_element: str | None = Field(
        default=None,
        validation_alias=AliasChoices("data_element", "dataElement"),
    )
    category_option_combo: str | None = Field(
        default=None,
        validation_alias=AliasChoices("category_option_combo", "categoryOptionCombo"),
    )
    dataset_name: str | None = Field(
        default=None,
        validation_alias=AliasChoices("dataset_name", "dataSet", "dataset"),
    )

    model_config = {"populate_by_name": True}


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------


def _flatten_coords(geometry: dict[str, Any]) -> list[list[float]]:
    """Return all [x, y] coordinate pairs from any GeoJSON geometry."""

    geom_type = geometry.get("type", "")
    coords = geometry.get("coordinates", [])

    if geom_type == "Point":
        return [coords]
    if geom_type in ("LineString", "MultiPoint"):
        return list(coords)
    if geom_type in ("Polygon", "MultiLineString"):
        return [c for ring in coords for c in ring]
    if geom_type == "MultiPolygon":
        return [c for poly in coords for ring in poly for c in ring]
    return []


def _bbox_from_geometry(geometry: dict[str, Any]) -> tuple[float, float, float, float] | None:
    """Compute [minx, miny, maxx, maxy] from a GeoJSON geometry dict."""

    pairs = _flatten_coords(geometry)
    if not pairs:
        return None
    xs = [p[0] for p in pairs]
    ys = [p[1] for p in pairs]
    return (min(xs), min(ys), max(xs), max(ys))


def _extract_ou_id(feature: dict[str, Any]) -> str:
    """Return the DHIS2 org unit UID from a GeoJSON feature.

    Priority: feature.id → properties.uid → properties.id → properties.orgUnit
    Falls back to 'unknown' if nothing is found.
    """
    if fid := feature.get("id"):
        return str(fid)
    props = feature.get("properties") or {}
    for key in ("uid", "id", "orgUnit", "orgUnitId"):
        if val := props.get(key):
            return str(val)
    return "unknown"


def _extract_feature_list(raw: dict[str, Any] | list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalise the 'features' input to a plain Python list of Feature dicts."""

    if isinstance(raw, dict):
        if raw.get("type") == "FeatureCollection":
            return raw.get("features") or []
        raise invalid_parameter("'features' object must be a GeoJSON FeatureCollection")
    if isinstance(raw, list):
        return raw
    raise invalid_parameter("'features' must be a GeoJSON FeatureCollection or an array of Features")


# ---------------------------------------------------------------------------
# Pipeline execution
# ---------------------------------------------------------------------------


def execute_dhis2_pipeline(payload: dict[str, Any]) -> dict[str, Any]:
    """Run the dhis2.pipeline process.

    For each GeoJSON feature:
      1. Extract bbox from geometry.
      2. Fetch raster assets via the dataset provider.
      3. Compute zonal statistics (same logic as raster.zonal_stats).
      4. Accumulate one dataValue per parameter per org unit.

    Returns a single job record whose outputs contain the complete
    DHIS2 dataValueSet envelope.
    """

    try:
        inputs = PipelineInputs.model_validate(payload)
    except ValidationError as exc:
        raise invalid_parameter(f"Invalid pipeline inputs: {exc.errors()}") from exc

    datasets = load_dataset_registry()
    dataset = datasets.get(inputs.dataset_id)
    if dataset is None:
        raise not_found("Dataset", inputs.dataset_id)

    requested_params = _resolve_requested_params(dataset, inputs.params)

    if inputs.time:
        start: date = _parse_date(inputs.time, "time")
    elif dataset.temporal_start:
        start = _parse_date(dataset.temporal_start, "temporal_start")
    else:
        start = date.today()

    aggregation = (inputs.aggregation or "mean").strip() or "mean"

    # DHIS2 period string (YYYYMMDD for daily data)
    period_str = start.strftime("%Y%m%d")

    feature_list = _extract_feature_list(inputs.features)
    provider = build_provider(dataset)

    all_rows: list[dict[str, Any]] = []
    data_values: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for feature in feature_list:
        ou_id = _extract_ou_id(feature)
        geometry = feature.get("geometry") or {}
        bbox = _bbox_from_geometry(geometry)

        if bbox is None:
            errors.append({"orgUnit": ou_id, "error": "no geometry or unsupported geometry type"})
            continue

        # Fetch raster assets for this org unit's bbox
        assets: dict[str, list[str]] = {}
        fetch_error: str | None = None
        for parameter in requested_params:
            try:
                result = provider.fetch(
                    RasterFetchRequest(
                        dataset_id=dataset.id,
                        parameter=parameter,
                        start=start,
                        end=start,
                        bbox=bbox,
                    )
                )
                assets[parameter] = result.asset_paths
            except Exception as exc:  # noqa: BLE001
                fetch_error = str(exc)
                break

        if fetch_error:
            errors.append({"orgUnit": ou_id, "error": f"provider fetch failed: {fetch_error}"})
            continue

        # Compute zonal stats
        feature_rows = zonal_stats_stub(
            dataset_id=dataset.id,
            time_value=start.isoformat(),
            bbox=bbox,
            assets=assets,
            aggregation=aggregation,
        )

        # Attach org unit context to each raw row
        for row in feature_rows:
            all_rows.append({**row, "orgUnit": ou_id})

        # Build dataValue entries (only for rows with a computed value)
        for row in feature_rows:
            value = row.get("value")
            if value is None:
                continue
            dv: dict[str, Any] = {
                "dataElement": inputs.data_element or "<dataElement UID>",
                "orgUnit": ou_id,
                "period": period_str,
                "value": str(value),
            }
            if inputs.category_option_combo:
                dv["categoryOptionCombo"] = inputs.category_option_combo
            parameter = row.get("parameter", "")
            stat = row.get("stat", "")
            comment = f"{parameter} {stat}".strip()
            if comment:
                dv["comment"] = comment
            data_values.append(dv)

    data_value_set: dict[str, Any] = {
        "dataSet": inputs.dataset_name or inputs.dataset_id,
        "period": period_str,
        "dataValues": data_values,
    }

    outputs: dict[str, Any] = {
        "dataValueSet": data_value_set,
        "rows": all_rows,
        "summary": {
            "features": len(feature_list),
            "computed": len(data_values),
            "errors": len(errors),
        },
        "errors": errors,
    }

    return create_job(DHIS2_PIPELINE_PROCESS_ID, payload, outputs)
