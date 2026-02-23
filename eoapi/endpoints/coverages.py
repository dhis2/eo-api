from datetime import date

from fastapi import APIRouter, HTTPException, Query, Request

from dhis2eo.data.cds.era5_land import hourly as era5_land_hourly
from dhis2eo.data.cds.era5_land import monthly as era5_land_monthly
from dhis2eo.data.chc.chirps3 import daily as chirps3_daily
from pygeoapi.api import FORMAT_TYPES, F_JSON
from pygeoapi.util import url_join

from eoapi.datasets import load_datasets
from eoapi.endpoints.constants import CRS84
from eoapi.endpoints.errors import invalid_parameter, not_found

router = APIRouter(tags=["Coverages"])

COVERAGE_PARAMETERS: dict[str, dict[str, dict]] = {
    "chirps-daily": {
        "precip": {
            "type": "Parameter",
            "description": {"en": "Daily precipitation"},
            "unit": {"label": {"en": "mm/day"}},
            "observedProperty": {"label": {"en": "Precipitation"}},
        }
    },
    "era5-land-daily": {
        "2m_temperature": {
            "type": "Parameter",
            "description": {"en": "2m air temperature"},
            "unit": {"label": {"en": "K"}},
            "observedProperty": {"label": {"en": "2m temperature"}},
        },
        "total_precipitation": {
            "type": "Parameter",
            "description": {"en": "Total precipitation"},
            "unit": {"label": {"en": "m"}},
            "observedProperty": {"label": {"en": "Total precipitation"}},
        },
    },
}


def _base_url(request: Request) -> str:
    return str(request.base_url).rstrip("/")


def _parse_bbox(
    bbox: str | None,
    fallback_bbox: tuple[float, float, float, float],
) -> tuple[float, float, float, float]:
    if bbox is None:
        return fallback_bbox

    try:
        values = tuple(float(part.strip()) for part in bbox.split(","))
    except ValueError as exc:
        raise invalid_parameter("bbox must contain 4 comma-separated numbers") from exc

    if len(values) != 4:
        raise invalid_parameter("bbox must contain 4 comma-separated numbers")

    return values


def _parse_datetime(datetime_value: str | None, fallback_start: str) -> str:
    if not datetime_value:
        return fallback_start

    if "/" in datetime_value:
        start, _, _ = datetime_value.partition("/")
        if start in {"", ".."}:
            return fallback_start
        return start

    return datetime_value


def _select_parameters(collection_id: str, range_subset: str | None) -> dict[str, dict]:
    available = COVERAGE_PARAMETERS.get(collection_id, {})
    if not range_subset:
        return available

    requested = [parameter.strip() for parameter in range_subset.split(",") if parameter.strip()]
    unknown = [parameter for parameter in requested if parameter not in available]
    if unknown:
        raise invalid_parameter(f"Unknown range-subset parameter(s): {', '.join(unknown)}")

    return {parameter: available[parameter] for parameter in requested}


def _resolve_dhis2eo_source(
    collection_id: str,
    datetime_value: str,
    bbox: tuple[float, float, float, float],
) -> dict:
    if collection_id == "chirps-daily":
        try:
            sample_day = date.fromisoformat(datetime_value[:10])
        except ValueError as exc:
            raise invalid_parameter("datetime must be an ISO 8601 date or datetime") from exc

        return {
            "backend": "dhis2eo.data.chc.chirps3.daily",
            "resolver": "url_for_day",
            "source_url": chirps3_daily.url_for_day(sample_day),
            "bbox": list(bbox),
        }

    if collection_id == "era5-land-daily":
        return {
            "backend": "dhis2eo.data.cds.era5_land",
            "resolver": [
                era5_land_hourly.download.__name__,
                era5_land_monthly.download.__name__,
            ],
            "variables": list(COVERAGE_PARAMETERS[collection_id].keys()),
            "bbox": list(bbox),
            "note": "ERA5-Land source data is resolved via CDS download workflows in dhis2eo.",
        }

    return {
        "backend": "dhis2eo",
        "bbox": list(bbox),
    }


def _coverage_links(request: Request, collection_id: str) -> list[dict]:
    base = _base_url(request)
    collection_url = url_join(base, "collections", collection_id)
    coverage_url = url_join(collection_url, "coverage")
    return [
        {
            "rel": "self",
            "type": FORMAT_TYPES[F_JSON],
            "title": "Coverage as CoverageJSON",
            "href": coverage_url,
        },
        {
            "rel": "collection",
            "type": FORMAT_TYPES[F_JSON],
            "title": "Collection metadata",
            "href": collection_url,
        },
        {
            "rel": "root",
            "type": FORMAT_TYPES[F_JSON],
            "title": "API root",
            "href": url_join(base, "/"),
        },
    ]


@router.get("/collections/{collection_id}/coverage")
def get_collection_coverage(
    collection_id: str,
    request: Request,
    bbox: str | None = None,
    datetime_value: str | None = Query(default=None, alias="datetime"),
    range_subset: str | None = Query(default=None, alias="range-subset"),
    output_format: str = Query(default=F_JSON, alias="f"),
) -> dict:
    if output_format not in {F_JSON, "covjson"}:
        raise invalid_parameter("Only f=json and f=covjson are currently supported")

    dataset = load_datasets().get(collection_id)
    if dataset is None:
        raise not_found("Collection", collection_id)

    bbox_values = _parse_bbox(bbox, dataset.spatial_bbox)
    time_value = _parse_datetime(datetime_value, dataset.temporal_interval[0])
    parameters = _select_parameters(collection_id, range_subset)
    source = _resolve_dhis2eo_source(collection_id, time_value, bbox_values)

    ranges = {
        parameter_name: {
            "type": "NdArray",
            "dataType": "float",
            "axisNames": ["t", "y", "x"],
            "shape": [1, 1, 1],
            "values": [None],
        }
        for parameter_name in parameters
    }

    return {
        "type": "Coverage",
        "title": dataset.title,
        "description": dataset.description,
        "domain": {
            "type": "Domain",
            "domainType": "Grid",
            "axes": {
                "x": {"start": bbox_values[0], "stop": bbox_values[2], "num": 2},
                "y": {"start": bbox_values[1], "stop": bbox_values[3], "num": 2},
                "t": {"values": [time_value]},
            },
            "referencing": [
                {
                    "coordinates": ["x", "y"],
                    "system": {
                        "type": "GeographicCRS",
                        "id": CRS84,
                    },
                },
                {
                    "coordinates": ["t"],
                    "system": {
                        "type": "TemporalRS",
                        "calendar": "Gregorian",
                    },
                },
            ],
        },
        "parameters": parameters,
        "ranges": ranges,
        "links": _coverage_links(request, collection_id),
        "source": source,
    }
