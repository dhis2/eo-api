"""OGC process catalog for the architecture skeleton process set."""

from typing import Any

try:
    from pygeoapi.api import FORMAT_TYPES, F_JSON
    from pygeoapi.util import url_join as _url_join

    JSON_MEDIA_TYPE = FORMAT_TYPES[F_JSON]
except ImportError:
    JSON_MEDIA_TYPE = "application/json"

    def _url_join(base_url: str, *parts: str) -> str:
        segments = [base_url.rstrip("/"), *(part.strip("/") for part in parts if part)]
        return "/".join(segment for segment in segments if segment)

RASTER_ZONAL_STATS_PROCESS_ID = "raster.zonal_stats"
RASTER_POINT_TIMESERIES_PROCESS_ID = "raster.point_timeseries"
DATA_TEMPORAL_AGGREGATE_PROCESS_ID = "data.temporal_aggregate"

PROCESS_IDS = (
    RASTER_ZONAL_STATS_PROCESS_ID,
    RASTER_POINT_TIMESERIES_PROCESS_ID,
    DATA_TEMPORAL_AGGREGATE_PROCESS_ID,
)

_PROCESS_METADATA: dict[str, dict[str, str]] = {
    RASTER_ZONAL_STATS_PROCESS_ID: {
        "title": "Raster zonal statistics",
        "description": "Compute aggregate statistics of raster pixels over an input AOI.",
    },
    RASTER_POINT_TIMESERIES_PROCESS_ID: {
        "title": "Raster point timeseries",
        "description": "Extract time series values for one or more points from a raster dataset.",
    },
    DATA_TEMPORAL_AGGREGATE_PROCESS_ID: {
        "title": "Data temporal aggregate",
        "description": "Harmonize data by aggregating values into a target temporal frequency.",
    },
}


def is_process_supported(process_id: str) -> bool:
    """Return whether the process ID is part of the skeleton catalog."""

    return process_id in _PROCESS_METADATA


def get_process_summary(process_id: str, base_url: str) -> dict[str, Any]:
    """Return list-view summary metadata for one process."""

    meta = _PROCESS_METADATA[process_id]
    return {
        "id": process_id,
        "title": meta["title"],
        "description": meta["description"],
        "links": [
            {
                "rel": "process",
                "type": JSON_MEDIA_TYPE,
                "href": _url_join(base_url, "processes", process_id),
            }
        ],
    }


def get_process_definition(process_id: str, base_url: str) -> dict[str, Any]:
    """Return full process metadata including input/output schemas."""

    meta = _PROCESS_METADATA[process_id]
    return {
        "id": process_id,
        "title": meta["title"],
        "description": meta["description"],
        "jobControlOptions": ["async-execute"],
        "inputs": {
            "dataset_id": {"type": "string"},
            "params": {"type": "array", "items": {"type": "string"}},
            "time": {"type": "string"},
            "aoi": {"type": "object"},
            "frequency": {"type": "string"},
            "aggregation": {"type": "string"},
        },
        "outputs": {
            "rows": {"type": "array", "items": {"type": "object"}},
            "csv": {"type": "string"},
            "dhis2": {"type": "object"},
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
