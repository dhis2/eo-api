"""CHIRPS3 to DHIS2 data value pipeline process."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, cast

import httpx
import pandas as pd
import xarray as xr
from dhis2_client.client import DHIS2Client
from dhis2eo.integrations.pandas import format_value_for_dhis2
from pydantic import ValidationError
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from rioxarray.exceptions import NoDataInBounds
from shapely.geometry import shape

from eo_api.integrations.dhis2_adapter import (
    create_client,
    get_org_unit_geojson,
    get_org_unit_subtree_geojson,
    get_org_units_geojson,
)
from eo_api.routers.ogcapi.plugins.processes.chirps3 import download_chirps3_daily
from eo_api.routers.ogcapi.plugins.processes.schemas import CHIRPS3DHIS2PipelineInput

DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/tmp/data")

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    "version": "0.1.0",
    "id": "chirps3-dhis2-pipeline",
    "title": "CHIRPS3 to DHIS2 Data Value Pipeline",
    "description": "Fetch features, download CHIRPS3, aggregate values, and build a DHIS2 dataValueSet.",
    "jobControlOptions": ["sync-execute"],
    "keywords": ["climate", "CHIRPS3", "DHIS2", "pipeline", "dataValueSet"],
    "inputs": {
        "start_date": {
            "title": "Start date",
            "description": "Inclusive date",
            "schema": {"type": "string"},
            "minOccurs": 1,
            "maxOccurs": 1,
        },
        "end_date": {
            "title": "End date",
            "description": "Inclusive date",
            "schema": {"type": "string"},
            "minOccurs": 1,
            "maxOccurs": 1,
        },
        "features_geojson": {
            "title": "Features GeoJSON",
            "description": "Optional FeatureCollection",
            "schema": {"type": "object"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "org_unit_level": {
            "title": "Org unit level",
            "description": "DHIS2 org unit level",
            "schema": {"type": "integer", "minimum": 1},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "parent_org_unit": {
            "title": "Parent org unit",
            "description": "Optional subtree scope",
            "schema": {"type": "string"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "org_unit_ids": {
            "title": "Org unit IDs",
            "description": "Optional explicit org unit UIDs",
            "schema": {"type": "array", "items": {"type": "string"}},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "org_unit_id_property": {
            "title": "Org unit ID property",
            "description": "Property name for org unit UID",
            "schema": {"type": "string", "default": "id"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "data_element": {
            "title": "Data element UID",
            "description": "DHIS2 data element UID",
            "schema": {"type": "string"},
            "minOccurs": 1,
            "maxOccurs": 1,
        },
        "category_option_combo": {
            "title": "Category option combo UID",
            "description": "Optional category option combo UID",
            "schema": {"type": "string"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "attribute_option_combo": {
            "title": "Attribute option combo UID",
            "description": "Optional attribute option combo UID",
            "schema": {"type": "string"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "data_set": {
            "title": "Dataset UID",
            "description": "Optional dataset UID",
            "schema": {"type": "string"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "stage": {
            "title": "CHIRPS3 stage",
            "description": "final or prelim",
            "schema": {"type": "string", "enum": ["final", "prelim"], "default": "final"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "spatial_reducer": {
            "title": "Spatial reducer",
            "description": "mean or sum",
            "schema": {"type": "string", "enum": ["mean", "sum"], "default": "mean"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "temporal_resolution": {
            "title": "Temporal resolution",
            "description": "daily, weekly, or monthly",
            "schema": {"type": "string", "enum": ["daily", "weekly", "monthly"], "default": "monthly"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "temporal_reducer": {
            "title": "Temporal reducer",
            "description": "sum or mean",
            "schema": {"type": "string", "enum": ["sum", "mean"], "default": "sum"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "value_rounding": {
            "title": "Value rounding",
            "description": "Decimal places",
            "schema": {"type": "integer", "minimum": 0, "maximum": 10, "default": 3},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "auto_import": {
            "title": "Auto import",
            "description": "Import payload into DHIS2",
            "schema": {"type": "boolean", "default": False},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "import_strategy": {
            "title": "Import strategy",
            "description": "DHIS2 importStrategy value",
            "schema": {"type": "string", "default": "CREATE_AND_UPDATE"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "dry_run": {
            "title": "Dry run",
            "description": "Skip DHIS2 import",
            "schema": {"type": "boolean", "default": True},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
    },
    "outputs": {
        "result": {
            "title": "Pipeline result",
            "schema": {"type": "object", "contentMediaType": "application/json"},
        }
    },
}


def _dhis2_client_from_env() -> DHIS2Client:
    """Build DHIS2 client from environment settings."""
    try:
        return create_client()
    except ValueError as err:
        raise ProcessorExecuteError(str(err)) from None


def _ensure_feature_collection(maybe_fc: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract feature list from GeoJSON FeatureCollection."""
    if maybe_fc.get("type") != "FeatureCollection":
        raise ProcessorExecuteError("features_geojson must be a GeoJSON FeatureCollection")
    features = maybe_fc.get("features", [])
    if not isinstance(features, list):
        raise ProcessorExecuteError("features_geojson.features must be an array")
    return features


def _feature_org_unit_id(feature: dict[str, Any], id_property: str) -> str | None:
    """Resolve orgUnit id from feature.id or a property key."""
    if feature.get("id"):
        return str(feature["id"])
    props = feature.get("properties") or {}
    if id_property in props and props[id_property] is not None:
        return str(props[id_property])
    return None


def _fetch_features_from_dhis2(client: DHIS2Client, inputs: CHIRPS3DHIS2PipelineInput) -> list[dict[str, Any]]:
    """Fetch features from DHIS2 using client selectors."""
    if inputs.org_unit_ids:
        features: list[dict[str, Any]] = []
        for uid in inputs.org_unit_ids:
            geo = get_org_unit_geojson(client, uid)
            if geo.get("type") == "Feature":
                features.append(geo)
            elif geo.get("type") == "FeatureCollection":
                features.extend(geo.get("features", []))
        return features

    if inputs.parent_org_unit:
        if inputs.org_unit_level:
            fc = get_org_units_geojson(client, level=inputs.org_unit_level, parent=inputs.parent_org_unit)
            return _ensure_feature_collection(fc)
        fc = get_org_unit_subtree_geojson(client, inputs.parent_org_unit)
        return _ensure_feature_collection(fc)

    if inputs.org_unit_level:
        LOGGER.warning(
            "[chirps3-dhis2-pipeline] unscoped org_unit_level=%s fetch requested; "
            "this may be slow on large DHIS2 instances",
            inputs.org_unit_level,
        )
        fc = get_org_units_geojson(client, level=inputs.org_unit_level)
        return _ensure_feature_collection(fc)

    raise ProcessorExecuteError("Provide one of: features_geojson, org_unit_ids, parent_org_unit, org_unit_level")


def _features_union_bbox(features: list[dict[str, Any]]) -> tuple[float, float, float, float]:
    """Compute union bbox from GeoJSON feature geometries."""
    bounds: list[tuple[float, float, float, float]] = []
    for feature in features:
        geometry = feature.get("geometry")
        if not geometry:
            continue
        geom = shape(geometry)
        if geom.is_empty:
            continue
        bounds.append(geom.bounds)
    if not bounds:
        raise ProcessorExecuteError("No valid geometries found in input features")

    minx = min(b[0] for b in bounds)
    miny = min(b[1] for b in bounds)
    maxx = max(b[2] for b in bounds)
    maxy = max(b[3] for b in bounds)
    return (minx, miny, maxx, maxy)


def _resolve_value_var(dataset: xr.Dataset) -> str:
    """Resolve precipitation variable name from xarray dataset."""
    if "precip" in dataset.data_vars:
        return "precip"
    keys = list(dataset.data_vars.keys())
    if not keys:
        raise ProcessorExecuteError("Downloaded CHIRPS3 dataset has no data variables")
    return str(keys[0])


def _clip_spatial_series(
    data_array: xr.DataArray,
    geometry: dict[str, Any],
    spatial_reducer: str,
) -> pd.Series:
    """Clip a data array by geometry and reduce over spatial dimensions."""
    try:
        clipped = data_array.rio.clip([geometry], crs="EPSG:4326", drop=True)
    except NoDataInBounds:
        return pd.Series(dtype=float)
    spatial_dims = [dim for dim in clipped.dims if dim != "time"]
    if not spatial_dims:
        raise ProcessorExecuteError("Unable to resolve spatial dimensions in CHIRPS3 dataset")
    reduced = (
        clipped.mean(dim=spatial_dims, skipna=True)
        if spatial_reducer == "mean"
        else clipped.sum(dim=spatial_dims, skipna=True)
    )
    series = reduced.to_series().dropna()
    if series.empty:
        return cast(pd.Series, series)
    series.index = pd.to_datetime(series.index)
    return cast(pd.Series, series)


def _format_period_code(timestamp: pd.Timestamp, temporal_resolution: str) -> str:
    """Format timestamp into DHIS2 period code."""
    if temporal_resolution == "daily":
        return str(timestamp.strftime("%Y%m%d"))
    if temporal_resolution == "monthly":
        return str(timestamp.strftime("%Y%m"))
    iso = timestamp.isocalendar()
    iso_year, iso_week, _ = iso
    return f"{int(iso_year):04d}W{int(iso_week):02d}"


def _as_timestamp(value: Any) -> pd.Timestamp:
    """Convert a value into a valid pandas Timestamp."""
    ts = pd.Timestamp(value)
    if bool(pd.isna(ts)):
        raise ProcessorExecuteError("Encountered invalid timestamp while formatting periods")
    return ts


def _apply_temporal_aggregation(
    series: pd.Series,
    temporal_resolution: str,
    temporal_reducer: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> list[tuple[str, float]]:
    """Apply optional temporal aggregation over the time index."""
    windowed = cast(Any, series[(series.index >= start_date) & (series.index <= end_date)])
    if windowed.empty:
        return []

    if temporal_resolution == "daily":
        return [
            (_format_period_code(_as_timestamp(ts), temporal_resolution), float(val)) for ts, val in windowed.items()
        ]

    if temporal_resolution == "monthly":
        aggregated = (
            windowed.resample("MS").mean().dropna()
            if temporal_reducer == "mean"
            else windowed.resample("MS").sum().dropna()
        )
        return [
            (_format_period_code(_as_timestamp(ts), temporal_resolution), float(val)) for ts, val in aggregated.items()
        ]

    # Weekly aggregation uses ISO year/week to generate DHIS2 weekly periods (YYYYWww).
    iso = windowed.index.isocalendar()
    grouped = windowed.groupby([iso["year"], iso["week"]])
    weekly = grouped.mean() if temporal_reducer == "mean" else grouped.sum()
    return [(f"{int(year):04d}W{int(week):02d}", float(value)) for (year, week), value in weekly.items()]


def _chirps_cache_key(
    *,
    stage: str,
    start_date: Any,
    end_date: Any,
    bbox: tuple[float, float, float, float],
) -> str:
    """Build a deterministic cache key to avoid bbox/date collisions in downloads."""
    bbox_part = "_".join(f"{coord:.4f}".replace("-", "m").replace(".", "p") for coord in bbox)
    return f"{stage}_{start_date}_{end_date}_{bbox_part}"


class CHIRPS3DHIS2PipelineProcessor(BaseProcessor):
    """One-go CHIRPS3 processing pipeline for DHIS2 data values."""

    def __init__(self, processor_def: dict[str, Any]) -> None:
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data: dict[str, Any], outputs: Any = None) -> tuple[str, dict[str, Any]]:
        try:
            inputs = CHIRPS3DHIS2PipelineInput.model_validate(data)
        except ValidationError as e:
            raise ProcessorExecuteError(str(e)) from e

        LOGGER.info(
            "[chirps3-dhis2-pipeline] start start_date=%s end_date=%s auto_import=%s dry_run=%s",
            inputs.start_date,
            inputs.end_date,
            inputs.auto_import,
            inputs.dry_run,
        )
        client = _dhis2_client_from_env()
        try:
            LOGGER.info("[chirps3-dhis2-pipeline] step=1 fetch_features")
            if inputs.features_geojson:
                features = _ensure_feature_collection(inputs.features_geojson)
            else:
                features = _fetch_features_from_dhis2(client, inputs)

            valid_features: list[tuple[str, dict[str, Any]]] = []
            for feature in features:
                geometry = feature.get("geometry")
                if not geometry:
                    continue
                org_unit_id = _feature_org_unit_id(feature, inputs.org_unit_id_property)
                if not org_unit_id:
                    continue
                valid_features.append((org_unit_id, geometry))

            if not valid_features:
                raise ProcessorExecuteError("No valid features with geometry and org unit identifiers were found")

            LOGGER.info("[chirps3-dhis2-pipeline] features=%s", len(valid_features))
            union_bbox = _features_union_bbox([{"geometry": g} for _, g in valid_features])
            if inputs.bbox:
                effective_bbox: tuple[float, float, float, float] = (
                    float(inputs.bbox[0]),
                    float(inputs.bbox[1]),
                    float(inputs.bbox[2]),
                    float(inputs.bbox[3]),
                )
            else:
                effective_bbox = union_bbox
            LOGGER.info("[chirps3-dhis2-pipeline] step=2 download_chirps3 bbox=%s", effective_bbox)
            cache_key = _chirps_cache_key(
                stage=inputs.stage,
                start_date=inputs.start_date,
                end_date=inputs.end_date,
                bbox=effective_bbox,
            )
            download_dir = Path(DOWNLOAD_DIR) / "chirps3_dhis2_pipeline" / cache_key
            download_dir.mkdir(parents=True, exist_ok=True)

            files = download_chirps3_daily(
                start=str(inputs.start_date),
                end=str(inputs.end_date),
                bbox=effective_bbox,
                dirname=str(download_dir),
                prefix=f"chirps3_pipeline_{cache_key}",
                stage=inputs.stage,
            )
            if not files:
                raise ProcessorExecuteError("No CHIRPS3 files were downloaded for the requested date range")

            LOGGER.info("[chirps3-dhis2-pipeline] downloaded_files=%s", len(files))
            LOGGER.info("[chirps3-dhis2-pipeline] step=3 aggregate")
            dataset = xr.open_mfdataset([str(path) for path in files], combine="by_coords")
            data_var = _resolve_value_var(dataset)
            data_array = dataset[data_var]
            start_dt = pd.Timestamp(inputs.start_date)
            end_dt = pd.Timestamp(inputs.end_date)

            rows: list[dict[str, Any]] = []
            for org_unit_id, geometry in valid_features:
                series = _clip_spatial_series(data_array, geometry, inputs.spatial_reducer)
                if series.empty:
                    continue
                period_values = _apply_temporal_aggregation(
                    series,
                    inputs.temporal_resolution,
                    inputs.temporal_reducer,
                    start_dt,
                    end_dt,
                )
                for period, value in period_values:
                    if pd.isna(value):
                        continue
                    rows.append(
                        {
                            "orgUnit": org_unit_id,
                            "period": period,
                            "value": round(value, inputs.value_rounding),
                        }
                    )

            if not rows:
                raise ProcessorExecuteError("No non-empty aggregated values were produced for the selected features")

            LOGGER.info("[chirps3-dhis2-pipeline] aggregated_rows=%s", len(rows))
            LOGGER.info("[chirps3-dhis2-pipeline] step=4 build_datavalueset")
            data_values: list[dict[str, Any]] = []
            for row in rows:
                data_value = {
                    "dataElement": inputs.data_element,
                    "orgUnit": row["orgUnit"],
                    "period": row["period"],
                    "value": format_value_for_dhis2(row["value"]),
                }
                if inputs.category_option_combo:
                    data_value["categoryOptionCombo"] = inputs.category_option_combo
                if inputs.attribute_option_combo:
                    data_value["attributeOptionCombo"] = inputs.attribute_option_combo
                data_values.append(data_value)
            payload: dict[str, Any] = {"dataValues": data_values}
            if inputs.data_set:
                payload["dataSet"] = inputs.data_set

            import_response: dict[str, Any] | None = None
            should_import = inputs.auto_import and not inputs.dry_run
            if should_import:
                LOGGER.info("[chirps3-dhis2-pipeline] step=5 import_to_dhis2")
                import_response = client.post(
                    "/api/dataValueSets",
                    params={"importStrategy": inputs.import_strategy},
                    json=payload,
                )
                LOGGER.info("[chirps3-dhis2-pipeline] import_completed")

            LOGGER.info("[chirps3-dhis2-pipeline] completed")
            return "application/json", {
                "status": "completed",
                "files": [str(path) for path in files],
                "summary": {
                    "feature_count": len(valid_features),
                    "data_value_count": len(payload["dataValues"]),
                    "start_date": str(inputs.start_date),
                    "end_date": str(inputs.end_date),
                    "temporal_resolution": inputs.temporal_resolution,
                    "spatial_reducer": inputs.spatial_reducer,
                    "temporal_reducer": inputs.temporal_reducer,
                    "imported": bool(import_response),
                },
                "message": "Pipeline completed" + (" (dry run)" if inputs.dry_run else ""),
                "dataValueSet": payload,
                "importResponse": import_response,
            }
        except httpx.ReadTimeout:
            raise ProcessorExecuteError(
                "DHIS2 request timed out. Narrow feature scope with parent_org_unit/org_unit_ids "
                "or increase DHIS2_HTTP_TIMEOUT_SECONDS."
            ) from None
        except Exception as e:
            raise ProcessorExecuteError(str(e)) from None
        finally:
            client.close()

    def __repr__(self) -> str:
        return "<CHIRPS3DHIS2PipelineProcessor>"
