from fastapi import APIRouter, HTTPException, Request

from pygeoapi import l10n
from pygeoapi.util import url_join

router = APIRouter(tags=["Collections"])

OGC_RELTYPES_BASE = "http://www.opengis.net/def/rel/ogc/1.0"
CRS84 = "http://www.opengis.net/def/crs/OGC/1.3/CRS84"

DATASETS: dict[str, dict] = {
    "chirps-daily": {
        "id": "chirps-daily",
        "title": "CHIRPS Daily Precipitation",
        "description": "Daily precipitation from CHIRPS as a gridded coverage dataset.",
        "keywords": ["CHIRPS", "precipitation", "rainfall", "coverage", "raster"],
        "spatial_bbox": [-180.0, -50.0, 180.0, 50.0],
        "temporal_interval": ["1981-01-01T00:00:00Z", None],
    },
    "era5-land-daily": {
        "id": "era5-land-daily",
        "title": "ERA5-Land Daily Climate",
        "description": "Daily ERA5-Land variables as a gridded coverage dataset.",
        "keywords": ["ERA5-Land", "temperature", "soil moisture", "coverage", "raster"],
        "spatial_bbox": [-180.0, -90.0, 180.0, 90.0],
        "temporal_interval": ["1950-01-01T00:00:00Z", None],
    },
}


def _base_url(request: Request) -> str:
    return str(request.base_url).rstrip("/")


def _locale_from_request(request: Request) -> str:
    requested = request.query_params.get("lang", "en")
    locale = l10n.str2locale(requested, silent=True)
    return l10n.locale2str(locale) if locale else "en"


def _collection_links(request: Request, collection_id: str) -> list[dict]:
    base = _base_url(request)
    collections_url = url_join(base, "collections")
    collection_url = url_join(collections_url, collection_id)
    return [
        {
            "rel": "self",
            "type": "application/json",
            "title": "This collection",
            "href": collection_url,
        },
        {
            "rel": "root",
            "type": "application/json",
            "title": "API root",
            "href": url_join(base, "/"),
        },
        {
            "rel": "parent",
            "type": "application/json",
            "title": "Collections",
            "href": collections_url,
        },
        {
            "rel": f"{OGC_RELTYPES_BASE}/coverage",
            "type": "application/json",
            "title": "Collection coverage",
            "href": url_join(collection_url, "coverage"),
        },
    ]


def _build_collection(request: Request, dataset: dict) -> dict:
    locale = _locale_from_request(request)
    return {
        "id": dataset["id"],
        "title": l10n.translate(dataset["title"], locale),
        "description": l10n.translate(dataset["description"], locale),
        "keywords": dataset["keywords"],
        "extent": {
            "spatial": {
                "bbox": [dataset["spatial_bbox"]],
                "crs": CRS84,
            },
            "temporal": {
                "interval": [dataset["temporal_interval"]],
                "trs": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian",
            },
        },
        "itemType": "coverage",
        "crs": [CRS84],
        "links": _collection_links(request, dataset["id"]),
    }


@router.get("/collections")
def get_collections(request: Request) -> dict:
    base = _base_url(request)
    collections_url = url_join(base, "collections")

    return {
        "collections": [_build_collection(request, dataset) for dataset in DATASETS.values()],
        "links": [
            {
                "rel": "self",
                "type": "application/json",
                "title": "This document",
                "href": collections_url,
            },
            {
                "rel": "root",
                "type": "application/json",
                "title": "API root",
                "href": url_join(base, "/"),
            },
        ],
    }


@router.get("/collections/{collection_id}")
def get_collection(collection_id: str, request: Request) -> dict:
    dataset = DATASETS.get(collection_id)
    if dataset is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "NotFound",
                "description": f"Collection '{collection_id}' not found",
            },
        )
    return _build_collection(request, dataset)
