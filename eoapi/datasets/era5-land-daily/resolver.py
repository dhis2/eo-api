from dhis2eo.data.cds.era5_land import hourly as era5_land_hourly
from dhis2eo.data.cds.era5_land import monthly as era5_land_monthly

from eoapi.datasets.base import BBox, ParameterMap, Point, SourcePayload


def coverage_source(
    datetime_value: str,
    parameters: ParameterMap,
    bbox: BBox,
) -> SourcePayload:
    return {
        "backend": "dhis2eo.data.cds.era5_land",
        "resolver": [
            era5_land_hourly.download.__name__,
            era5_land_monthly.download.__name__,
        ],
        "variables": list(parameters.keys()),
        "bbox": list(bbox),
        "note": "ERA5-Land source data is resolved via CDS download workflows in dhis2eo.",
    }


def position_source(
    datetime_value: str,
    parameters: ParameterMap,
    coords: Point,
) -> SourcePayload:
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


def area_source(
    datetime_value: str,
    parameters: ParameterMap,
    bbox: BBox,
) -> SourcePayload:
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
