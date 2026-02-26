"""Zonal statistics process plugin for raster and GeoJSON inputs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.request import urlopen

import numpy as np
import rasterio
from pydantic import ValidationError
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from rasterio.mask import mask
from rasterio.warp import transform_geom

from eo_api.routers.ogcapi.plugins.processes.schemas import ZonalStatisticsInput

PROCESS_METADATA = {
    "version": "0.1.0",
    "id": "zonal-statistics",
    "title": "Zonal statistics",
    "description": "Calculate raster zonal statistics for GeoJSON features.",
    "jobControlOptions": ["sync-execute", "async-execute"],
    "keywords": ["raster", "geojson", "zonal", "statistics"],
    "inputs": {
        "geojson": {
            "title": "GeoJSON FeatureCollection",
            "description": "FeatureCollection object or URI/path to GeoJSON file.",
            "schema": {"oneOf": [{"type": "object"}, {"type": "string"}]},
            "minOccurs": 1,
            "maxOccurs": 1,
        },
        "raster": {
            "title": "Raster path or URI",
            "description": "Filesystem path or remote URI to raster data.",
            "schema": {"type": "string"},
            "minOccurs": 1,
            "maxOccurs": 1,
        },
        "band": {
            "title": "Raster band",
            "description": "1-based raster band index.",
            "schema": {"type": "integer", "minimum": 1, "default": 1},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "stats": {
            "title": "Statistics",
            "description": "Statistics to compute for each zone.",
            "schema": {
                "type": "array",
                "items": {
                    "type": "string",
                    "enum": ["count", "sum", "mean", "min", "max", "median", "std"],
                },
                "default": ["mean"],
            },
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "feature_id_property": {
            "title": "Feature ID property",
            "description": "Property key to use as fallback feature ID.",
            "schema": {"type": "string", "default": "id"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "output_property": {
            "title": "Output property",
            "description": "Property key where computed zonal stats are written.",
            "schema": {"type": "string", "default": "zonal_statistics"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "all_touched": {
            "title": "All touched",
            "description": "Include all raster cells touched by geometry.",
            "schema": {"type": "boolean", "default": False},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "include_nodata": {
            "title": "Include nodata",
            "description": "Include nodata values in computations.",
            "schema": {"type": "boolean", "default": False},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "nodata": {
            "title": "Override nodata",
            "description": "Optional nodata value override.",
            "schema": {"type": "number"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
    },
    "outputs": {
        "features": {
            "title": "FeatureCollection with zonal statistics",
            "schema": {"type": "object", "contentMediaType": "application/json"},
        }
    },
}


def _read_geojson_input(geojson_input: dict[str, Any] | str) -> dict[str, Any]:
    if isinstance(geojson_input, dict):
        return geojson_input

    parsed = urlparse(geojson_input)
    if parsed.scheme in {"http", "https"}:
        with urlopen(geojson_input) as response:
            payload = response.read().decode("utf-8")
            return json.loads(payload)

    path = Path(geojson_input)
    if not path.exists():
        raise ProcessorExecuteError(f"GeoJSON file not found: {geojson_input}")
    return json.loads(path.read_text(encoding="utf-8"))


def _ensure_feature_collection(geojson: dict[str, Any]) -> list[dict[str, Any]]:
    if geojson.get("type") != "FeatureCollection":
        raise ProcessorExecuteError("geojson must be a GeoJSON FeatureCollection")
    features = geojson.get("features")
    if not isinstance(features, list):
        raise ProcessorExecuteError("geojson.features must be an array")
    return features


def _to_float(value: np.floating[Any] | np.integer[Any] | float | int) -> float:
    return float(value)


def _compute_stats(values: np.ndarray[Any, np.dtype[np.float64]], stats: list[str]) -> dict[str, float | None]:
    if values.size == 0:
        return {name: None for name in stats}

    calculations: dict[str, float | None] = {}
    if "count" in stats:
        calculations["count"] = _to_float(values.size)
    if "sum" in stats:
        calculations["sum"] = _to_float(np.sum(values))
    if "mean" in stats:
        calculations["mean"] = _to_float(np.mean(values))
    if "min" in stats:
        calculations["min"] = _to_float(np.min(values))
    if "max" in stats:
        calculations["max"] = _to_float(np.max(values))
    if "median" in stats:
        calculations["median"] = _to_float(np.median(values))
    if "std" in stats:
        calculations["std"] = _to_float(np.std(values))
    return calculations


class ZonalStatisticsProcessor(BaseProcessor):
    """Processor that computes zonal statistics from a raster and GeoJSON zones."""

    def __init__(self, processor_def: dict[str, Any]) -> None:
        super().__init__(processor_def, PROCESS_METADATA)
        self.supports_outputs = True

    def execute(self, data: dict[str, Any], outputs: Any = None) -> tuple[str, dict[str, Any]]:
        try:
            inputs = ZonalStatisticsInput.model_validate(data)
        except ValidationError as err:
            raise ProcessorExecuteError(str(err)) from err

        geojson = _read_geojson_input(inputs.geojson)
        features = _ensure_feature_collection(geojson)

        output_features: list[dict[str, Any]] = []

        with rasterio.open(inputs.raster) as src:
            if inputs.band > src.count:
                raise ProcessorExecuteError(
                    f"Band index {inputs.band} out of range for raster with {src.count} band(s)"
                )

            raster_crs = src.crs
            raster_nodata = inputs.nodata if inputs.nodata is not None else src.nodata

            for index, feature in enumerate(features):
                geometry = feature.get("geometry")
                if not isinstance(geometry, dict):
                    raise ProcessorExecuteError(f"Feature at index {index} is missing geometry")

                projected_geometry = geometry
                if raster_crs:
                    projected_geometry = transform_geom("EPSG:4326", raster_crs, geometry, precision=12)

                try:
                    raster_data, _ = mask(
                        src,
                        [projected_geometry],
                        indexes=inputs.band,
                        crop=True,
                        all_touched=inputs.all_touched,
                        filled=False,
                    )
                except ValueError:
                    raster_data = np.ma.array([], dtype=np.float64)

                if isinstance(raster_data, np.ma.MaskedArray):
                    values = raster_data.compressed().astype(np.float64)
                else:
                    values = np.array(raster_data, dtype=np.float64).ravel()

                if not inputs.include_nodata and values.size and raster_nodata is not None:
                    values = values[~np.isclose(values, float(raster_nodata), equal_nan=True)]

                zonal = _compute_stats(values, inputs.stats)

                properties = dict(feature.get("properties") or {})
                properties[inputs.output_property] = zonal

                feature_id = feature.get("id")
                if feature_id is None:
                    feature_id = properties.get(inputs.feature_id_property, index)

                output_features.append(
                    {
                        "type": "Feature",
                        "id": feature_id,
                        "geometry": geometry,
                        "properties": properties,
                    }
                )

        value = {"type": "FeatureCollection", "features": output_features}
        result = {"id": "features", "value": value}

        if bool(outputs) and "features" not in outputs:
            return "application/json", {}
        return "application/json", result

    def __repr__(self) -> str:
        return "<ZonalStatisticsProcessor>"

