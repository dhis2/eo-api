from datetime import date

from dhis2eo.data.chc.chirps3 import daily as chirps3_daily

from eoapi.datasets.base import BBox, ParameterMap, Point, SourcePayload
from eoapi.endpoints.errors import invalid_parameter


def _parse_sample_day(datetime_value: str) -> date:
    try:
        return date.fromisoformat(datetime_value[:10])
    except ValueError as exc:
        raise invalid_parameter("datetime must be an ISO 8601 date or datetime") from exc


def coverage_source(
    datetime_value: str,
    parameters: ParameterMap,
    bbox: BBox,
) -> SourcePayload:
    sample_day = _parse_sample_day(datetime_value)
    return {
        "backend": "dhis2eo.data.chc.chirps3.daily",
        "resolver": "url_for_day",
        "source_url": chirps3_daily.url_for_day(sample_day),
        "bbox": list(bbox),
        "variables": list(parameters.keys()),
    }


def position_source(
    datetime_value: str,
    parameters: ParameterMap,
    coords: Point,
) -> SourcePayload:
    sample_day = _parse_sample_day(datetime_value)
    return {
        "backend": "dhis2eo.data.chc.chirps3.daily",
        "resolver": "url_for_day",
        "source_url": chirps3_daily.url_for_day(sample_day),
        "point": [coords[0], coords[1]],
        "variables": list(parameters.keys()),
    }


def area_source(
    datetime_value: str,
    parameters: ParameterMap,
    bbox: BBox,
) -> SourcePayload:
    sample_day = _parse_sample_day(datetime_value)
    return {
        "backend": "dhis2eo.data.chc.chirps3.daily",
        "resolver": "url_for_day",
        "source_url": chirps3_daily.url_for_day(sample_day),
        "bbox": list(bbox),
        "variables": list(parameters.keys()),
    }
