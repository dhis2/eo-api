import re

from fastapi import APIRouter, Query, Request

from pygeoapi.api import FORMAT_TYPES, F_JSON
from pygeoapi.util import url_join

from eoapi.datasets import DatasetDefinition, load_datasets
from eoapi.datasets.base import AreaResolver, BBox, ParameterMap, Point, PositionResolver, SourcePayload
from eoapi.datasets.resolvers import area_resolvers, position_resolvers
from eoapi.endpoints.constants import CRS84
from eoapi.endpoints.errors import invalid_parameter, not_found
from eoapi.external_ogc import (
    is_external_operation_enabled,
    parse_federated_collection_id,
    proxy_external_collection_request,
)

router = APIRouter()

POSITION_RESOLVERS: dict[str, PositionResolver] = {
    **position_resolvers(),
}

AREA_RESOLVERS: dict[str, AreaResolver] = {
    **area_resolvers(),
}

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
    parameters: ParameterMap,
    datetime_value: str,
    coords: Point,
) -> SourcePayload:
    resolver = POSITION_RESOLVERS.get(collection_id)
    if resolver is not None:
        return resolver(datetime_value, parameters, coords)

    return {
        "backend": "dhis2eo",
        "point": [coords[0], coords[1]],
    }


def _resolve_dhis2eo_source_for_bbox(
    collection_id: str,
    parameters: ParameterMap,
    datetime_value: str,
    bbox: BBox,
) -> SourcePayload:
    resolver = AREA_RESOLVERS.get(collection_id)
    if resolver is not None:
        return resolver(datetime_value, parameters, bbox)

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
        description="Comma-separated parameter IDs. Must match keys under eoapi/datasets/<id>/<id>.yaml -> parameters",
    ),
    output_format: str = Query(default=F_JSON, alias="f"),
) -> dict:
    if parse_federated_collection_id(collectionId) is not None:
        operation_enabled = is_external_operation_enabled(collectionId, "position")
        if operation_enabled is False:
            raise invalid_parameter("position operation is disabled for this external provider")
        proxied = proxy_external_collection_request(
            collection_id=collectionId,
            operation="position",
            query_params=list(request.query_params.multi_items()),
        )
        if proxied is None:
            raise not_found("Collection", collectionId)
        return proxied

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
        description="Comma-separated parameter IDs. Must match keys under eoapi/datasets/<id>/<id>.yaml -> parameters",
    ),
    output_format: str = Query(default=F_JSON, alias="f"),
) -> dict:
    if parse_federated_collection_id(collectionId) is not None:
        operation_enabled = is_external_operation_enabled(collectionId, "area")
        if operation_enabled is False:
            raise invalid_parameter("area operation is disabled for this external provider")
        proxied = proxy_external_collection_request(
            collection_id=collectionId,
            operation="area",
            query_params=list(request.query_params.multi_items()),
        )
        if proxied is None:
            raise not_found("Collection", collectionId)
        return proxied

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
