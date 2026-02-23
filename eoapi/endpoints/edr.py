from datetime import date
import re

from fastapi import APIRouter, Query, Request

from dhis2eo.data.cds.era5_land import hourly as era5_land_hourly
from dhis2eo.data.cds.era5_land import monthly as era5_land_monthly
from dhis2eo.data.chc.chirps3 import daily as chirps3_daily
from pygeoapi.api import FORMAT_TYPES, F_JSON
from pygeoapi.util import url_join

from eoapi.datasets import DatasetDefinition, load_datasets
from eoapi.endpoints.constants import CRS84
from eoapi.endpoints.errors import invalid_parameter, not_found

router = APIRouter()

POINT_PATTERN = re.compile(
    r"^POINT\s*\(\s*(?P<x>-?\d+(?:\.\d+)?)\s+(?P<y>-?\d+(?:\.\d+)?)\s*\)$",
    re.IGNORECASE,
)

def _base_url(request: Request) -> str:
    return str(request.base_url).rstrip("/")


def _parse_point_coords(coords: str) -> tuple[float, float]:
    match = POINT_PATTERN.match(coords.strip())
    if not match:
        raise invalid_parameter("coords must be a WKT POINT like POINT(30 -1)")

    x = float(match.group("x"))
    y = float(match.group("y"))
    return (x, y)


def _parse_bbox(
    bbox: str,
) -> tuple[float, float, float, float]:
    try:
        values = tuple(float(part.strip()) for part in bbox.split(","))
    except ValueError as exc:
        raise invalid_parameter("bbox must contain 4 comma-separated numbers") from exc

    if len(values) != 4:
        raise invalid_parameter("bbox must contain 4 comma-separated numbers")

    minx, miny, maxx, maxy = values
    if minx >= maxx or miny >= maxy:
        raise invalid_parameter("bbox must follow minx,miny,maxx,maxy with min < max")

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


def _select_parameters(dataset: DatasetDefinition, parameter_name: str | None) -> dict[str, dict]:
    available = dataset.parameters
    if not available:
        raise invalid_parameter(f"No parameters configured for collection '{dataset.id}'")

    if not parameter_name:
        return available

    requested = [parameter.strip() for parameter in parameter_name.split(",") if parameter.strip()]
    unknown = [parameter for parameter in requested if parameter not in available]
    if unknown:
        raise invalid_parameter(f"Unknown parameter-name value(s): {', '.join(unknown)}")

    return {parameter: available[parameter] for parameter in requested}


def _resolve_dhis2eo_source(
    collection_id: str,
    parameters: dict[str, dict],
    datetime_value: str,
    coords: tuple[float, float],
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
            "point": [coords[0], coords[1]],
        }

    if collection_id == "era5-land-daily":
        return {
            "backend": "dhis2eo.data.cds.era5_land",
            "resolver": [
                era5_land_hourly.download.__name__,
                era5_land_monthly.download.__name__,
            ],
            "variables": list(parameters.keys()),
            "point": [coords[0], coords[1]],
            "note": "ERA5-Land point query is resolved through CDS workflows in dhis2eo.",
        }

    return {
        "backend": "dhis2eo",
        "point": [coords[0], coords[1]],
    }


def _resolve_dhis2eo_source_for_bbox(
    collection_id: str,
    parameters: dict[str, dict],
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
            "variables": list(parameters.keys()),
            "bbox": list(bbox),
            "note": "ERA5-Land area query is resolved through CDS workflows in dhis2eo.",
        }

    return {
        "backend": "dhis2eo",
        "bbox": list(bbox),
    }


def _bbox_polygon(bbox: tuple[float, float, float, float]) -> list[list[float]]:
    minx, miny, maxx, maxy = bbox
    return [
        [minx, miny],
        [maxx, miny],
        [maxx, maxy],
        [minx, maxy],
        [minx, miny],
    ]


def _position_links(request: Request, collection_id: str, coords: str) -> list[dict]:
    base = _base_url(request)
    collection_url = url_join(base, "collections", collection_id)
    position_url = url_join(collection_url, "position")
    return [
        {
            "rel": "self",
            "type": FORMAT_TYPES[F_JSON],
            "title": "EDR position query",
            "href": f"{position_url}?coords={coords}",
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


def _area_links(request: Request, collection_id: str, bbox: str) -> list[dict]:
    base = _base_url(request)
    collection_url = url_join(base, "collections", collection_id)
    area_url = url_join(collection_url, "area")
    return [
        {
            "rel": "self",
            "type": FORMAT_TYPES[F_JSON],
            "title": "EDR area query",
            "href": f"{area_url}?bbox={bbox}",
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


@router.get("/collections/{collectionId}/position")
def get_collection_position(
    collectionId: str,
    request: Request,
    coords: str = Query(..., description="WKT POINT, e.g. POINT(30 -1)"),
    datetime_value: str | None = Query(default=None, alias="datetime"),
    parameter_name: str | None = Query(
        default=None,
        alias="parameter-name",
        description="Comma-separated parameter IDs. Must match keys under datasets/<id>.yaml -> parameters",
    ),
    output_format: str = Query(default=F_JSON, alias="f"),
) -> dict:
    if output_format not in {F_JSON, "geojson"}:
        raise invalid_parameter("Only f=json and f=geojson are currently supported")

    dataset = load_datasets().get(collectionId)
    if dataset is None:
        raise not_found("Collection", collectionId)

    point = _parse_point_coords(coords)
    time_value = _parse_datetime(datetime_value, dataset.temporal_interval[0])
    parameters = _select_parameters(dataset, parameter_name)
    source = _resolve_dhis2eo_source(collectionId, parameters, time_value, point)

    return {
        "type": "FeatureCollection",
        "title": dataset.title,
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [point[0], point[1]]},
                "properties": {
                    "collection": collectionId,
                    "datetime": time_value,
                    "crs": CRS84,
                    "parameters": list(parameters.keys()),
                    "values": {parameter: None for parameter in parameters.keys()},
                },
            }
        ],
        "parameters": parameters,
        "links": _position_links(request, collectionId, coords),
        "source": source,
    }


@router.get("/collections/{collectionId}/area")
def get_collection_area(
    collectionId: str,
    request: Request,
    bbox: str = Query(..., description="Area bbox as minx,miny,maxx,maxy in CRS84"),
    datetime_value: str | None = Query(default=None, alias="datetime"),
    parameter_name: str | None = Query(
        default=None,
        alias="parameter-name",
        description="Comma-separated parameter IDs. Must match keys under datasets/<id>.yaml -> parameters",
    ),
    output_format: str = Query(default=F_JSON, alias="f"),
) -> dict:
    if output_format not in {F_JSON, "geojson"}:
        raise invalid_parameter("Only f=json and f=geojson are currently supported")

    dataset = load_datasets().get(collectionId)
    if dataset is None:
        raise not_found("Collection", collectionId)

    area_bbox = _parse_bbox(bbox)
    time_value = _parse_datetime(datetime_value, dataset.temporal_interval[0])
    parameters = _select_parameters(dataset, parameter_name)
    source = _resolve_dhis2eo_source_for_bbox(collectionId, parameters, time_value, area_bbox)

    return {
        "type": "FeatureCollection",
        "title": dataset.title,
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [_bbox_polygon(area_bbox)],
                },
                "properties": {
                    "collection": collectionId,
                    "datetime": time_value,
                    "crs": CRS84,
                    "parameters": list(parameters.keys()),
                    "aggregates": {parameter: None for parameter in parameters.keys()},
                },
            }
        ],
        "parameters": parameters,
        "links": _area_links(request, collectionId, bbox),
        "source": source,
    }
