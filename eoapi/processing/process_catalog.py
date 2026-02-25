"""OGC process catalog for the architecture skeleton process set."""

from typing import Any

from pygeoapi.api import FORMAT_TYPES, F_JSON
from pygeoapi.util import url_join as _url_join

JSON_MEDIA_TYPE = FORMAT_TYPES[F_JSON]

RASTER_ZONAL_STATS_PROCESS_ID = "raster.zonal_stats"
RASTER_POINT_TIMESERIES_PROCESS_ID = "raster.point_timeseries"
DATA_TEMPORAL_AGGREGATE_PROCESS_ID = "data.temporal_aggregate"
DHIS2_PIPELINE_PROCESS_ID = "dhis2.pipeline"

PROCESS_IDS = (
    RASTER_ZONAL_STATS_PROCESS_ID,
    RASTER_POINT_TIMESERIES_PROCESS_ID,
    DATA_TEMPORAL_AGGREGATE_PROCESS_ID,
    DHIS2_PIPELINE_PROCESS_ID,
)

_COMMON_INPUTS: dict[str, Any] = {
    "dataset_id": {
        "type": "string",
        "description": "Collection ID to process (e.g. 'chirps-daily', 'era5-land-daily').",
    },
    "params": {
        "type": "array",
        "items": {"type": "string"},
        "description": "Parameter names to process. Defaults to all available parameters for the dataset.",
    },
    "time": {
        "type": "string",
        "description": "Target date in YYYY-MM-DD format.",
    },
}

_PROCESS_CATALOG: dict[str, dict[str, Any]] = {
    RASTER_ZONAL_STATS_PROCESS_ID: {
        "title": "Raster zonal statistics",
        "description": "Compute aggregate statistics of raster pixels over an input area of interest (AOI).",
        "inputs": {
            **_COMMON_INPUTS,
            "aoi": {
                "description": "Area of interest. Either a bbox array [minx, miny, maxx, maxy] or an object with a 'bbox' key.",
                "oneOf": [
                    {"type": "array", "items": {"type": "number"}, "minItems": 4, "maxItems": 4},
                    {"type": "object", "properties": {"bbox": {"type": "array"}}},
                ],
            },
            "aggregation": {
                "type": "string",
                "description": "Aggregation function to apply over the AOI pixels.",
                "enum": ["mean", "sum", "min", "max", "median"],
                "default": "mean",
            },
        },
    },
    RASTER_POINT_TIMESERIES_PROCESS_ID: {
        "title": "Raster point timeseries",
        "description": "Extract raster values at a point or AOI centroid for a given date.",
        "inputs": {
            **_COMMON_INPUTS,
            "aoi": {
                "description": "Point location or bbox (centroid used for point extraction). Either [minx, miny, maxx, maxy] or {'bbox': [...]}.",
                "oneOf": [
                    {"type": "array", "items": {"type": "number"}, "minItems": 4, "maxItems": 4},
                    {"type": "object", "properties": {"bbox": {"type": "array"}}},
                ],
            },
        },
    },
    DATA_TEMPORAL_AGGREGATE_PROCESS_ID: {
        "title": "Data temporal aggregate",
        "description": "Harmonize data by aggregating values into a target temporal frequency (e.g. daily → monthly).",
        "inputs": {
            **_COMMON_INPUTS,
            "frequency": {
                "type": "string",
                "description": "ISO 8601 duration for the target temporal frequency (e.g. 'P1M' for monthly, 'P1W' for weekly).",
                "default": "P1M",
            },
            "aggregation": {
                "type": "string",
                "description": "Temporal aggregation function.",
                "enum": ["mean", "sum", "min", "max"],
                "default": "mean",
            },
        },
    },
    DHIS2_PIPELINE_PROCESS_ID: {
        "title": "DHIS2 Org Units to dataValueSet pipeline",
        "description": (
            "Accept DHIS2 org unit GeoJSON features, compute zonal statistics for each "
            "feature, and return a DHIS2 dataValueSets-compatible payload."
        ),
        "inputs": {
            "features": {
                "description": "GeoJSON FeatureCollection (or Feature list) containing DHIS2 org unit geometries.",
                "type": "object",
            },
            **_COMMON_INPUTS,
            "aggregation": {"type": "string", "default": "mean"},
            "data_element": {"type": "string"},
            "category_option_combo": {"type": "string"},
            "dataset_name": {"type": "string"},
        },
    },
}


def is_process_supported(process_id: str) -> bool:
    """Return whether the process ID is part of the skeleton catalog."""

    return process_id in _PROCESS_CATALOG


def get_process_summary(process_id: str, base_url: str) -> dict[str, Any]:
    """Return list-view summary metadata for one process."""

    meta = _PROCESS_CATALOG[process_id]
    return {
        "id": process_id,
        "title": meta["title"],
        "description": meta["description"],
        "links": [
            {
                "rel": "self",
                "type": JSON_MEDIA_TYPE,
                "title": "Process definition",
                "href": _url_join(base_url, "processes", process_id),
            },
            {
                "rel": "execute",
                "type": JSON_MEDIA_TYPE,
                "title": "Execute this process",
                "href": _url_join(base_url, "processes", process_id, "execution"),
            },
        ],
    }


def get_process_definition(process_id: str, base_url: str) -> dict[str, Any]:
    """Return full process metadata including input/output schemas."""

    meta = _PROCESS_CATALOG[process_id]
    return {
        "id": process_id,
        "title": meta["title"],
        "description": meta["description"],
        "jobControlOptions": ["sync-execute"],
        "inputs": meta["inputs"],
        "outputs": {
            "rows": {"type": "array", "items": {"type": "object"}, "description": "Tabular result rows."},
            "csv": {"type": "string", "description": "Result serialized as CSV text."},
            "dhis2": {"type": "object", "description": "Result formatted as a DHIS2 dataValueSets payload."},
            "implementation": {
                "type": "object",
                "description": "Execution stack metadata (provider, compute, formatting) returned in job outputs.",
                "properties": {
                    "provider": {"type": "object"},
                    "compute": {"type": "object"},
                    "formatting": {"type": "object"},
                },
            },
        },
        "links": [
            {
                "rel": "self",
                "type": JSON_MEDIA_TYPE,
                "href": _url_join(base_url, "processes", process_id),
            },
            {
                "rel": "execute",
                "type": JSON_MEDIA_TYPE,
                "href": _url_join(base_url, "processes", process_id, "execution"),
            },
            {
                "rel": "collection",
                "type": JSON_MEDIA_TYPE,
                "title": "Browse collections usable with this process via dataset_id input",
                "href": _url_join(base_url, "collections"),
            },
        ],
    }


def list_process_summaries(base_url: str) -> list[dict[str, Any]]:
    """Return summaries for every process in declaration order."""

    return [get_process_summary(process_id, base_url) for process_id in PROCESS_IDS]
