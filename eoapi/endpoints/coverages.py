from fastapi import APIRouter, Query, Request

from pygeoapi.api import FORMAT_TYPES, F_JSON
from pygeoapi.util import url_join

from eoapi.datasets import DatasetDefinition, load_datasets
from eoapi.datasets.base import BBox, CoverageResolver, ParameterMap, SourcePayload
from eoapi.datasets.resolvers import coverage_resolvers
from eoapi.endpoints.constants import CRS84
from eoapi.endpoints.errors import invalid_parameter, not_found

router = APIRouter()

COVERAGE_RESOLVERS: dict[str, CoverageResolver] = {
    **coverage_resolvers(),
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


def _select_parameters(dataset: DatasetDefinition, range_subset: str | None) -> dict[str, dict]:
    available = dataset.parameters
    if not available:
        raise invalid_parameter(f"No parameters configured for collection '{dataset.id}'")

    if not range_subset:
        return available

    requested = [parameter.strip() for parameter in range_subset.split(",") if parameter.strip()]
    unknown = [parameter for parameter in requested if parameter not in available]
    if unknown:
        raise invalid_parameter(f"Unknown range-subset parameter(s): {', '.join(unknown)}")

    return {parameter: available[parameter] for parameter in requested}


def _resolve_dhis2eo_source(
    collection_id: str,
    parameters: ParameterMap,
    datetime_value: str,
    bbox: BBox,
) -> SourcePayload:
    resolver = COVERAGE_RESOLVERS.get(collection_id)
    if resolver is not None:
        return resolver(datetime_value, parameters, bbox)

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


@router.get("/collections/{collectionId}/coverage")
def get_collection_coverage(
    collectionId: str,
    request: Request,
    bbox: str | None = None,
    datetime_value: str | None = Query(default=None, alias="datetime"),
    range_subset: str | None = Query(
        default=None,
        alias="range-subset",
        description="Comma-separated parameter IDs. Must match keys under eoapi/datasets/<id>/<id>.yaml -> parameters",
    ),
    output_format: str = Query(default=F_JSON, alias="f"),
) -> dict:
    if output_format not in {F_JSON, "covjson"}:
        raise invalid_parameter("Only f=json and f=covjson are currently supported")

    dataset = load_datasets().get(collectionId)
    if dataset is None:
        raise not_found("Collection", collectionId)

    bbox_values = _parse_bbox(bbox, dataset.spatial_bbox)
    time_value = _parse_datetime(datetime_value, dataset.temporal_interval[0])
    parameters = _select_parameters(dataset, range_subset)
    source = _resolve_dhis2eo_source(collectionId, parameters, time_value, bbox_values)

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
        "links": _coverage_links(request, collectionId),
        "source": source,
    }
