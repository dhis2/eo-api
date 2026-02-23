from fastapi import APIRouter, HTTPException, Request

from pygeoapi import l10n
from pygeoapi.util import url_join
from eoapi.datasets import DatasetDefinition, load_datasets
from eoapi.endpoints.constants import CRS84, OGC_RELTYPES_BASE
from eoapi.endpoints.errors import not_found
from eoapi.endpoints.coverages import router as coverages_router
from eoapi.endpoints.edr import router as edr_router

router = APIRouter(tags=["Collections"])

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
        {
            "rel": f"{OGC_RELTYPES_BASE}/data",
            "type": "application/json",
            "title": "Collection EDR position query",
            "href": url_join(collection_url, "position"),
        },
        {
            "rel": f"{OGC_RELTYPES_BASE}/data",
            "type": "application/json",
            "title": "Collection EDR area query",
            "href": url_join(collection_url, "area"),
        },
    ]


def _build_collection(request: Request, dataset: DatasetDefinition) -> dict:
    locale = _locale_from_request(request)
    return {
        "id": dataset.id,
        "title": l10n.translate(dataset.title, locale),
        "description": l10n.translate(dataset.description, locale),
        "keywords": dataset.keywords,
        "extent": {
            "spatial": {
                "bbox": [list(dataset.spatial_bbox)],
                "crs": CRS84,
            },
            "temporal": {
                "interval": [list(dataset.temporal_interval)],
                "trs": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian",
            },
        },
        "itemType": "coverage",
        "crs": [CRS84],
        "links": _collection_links(request, dataset.id),
    }


@router.get("/collections")
def get_collections(request: Request) -> dict:
    base = _base_url(request)
    collections_url = url_join(base, "collections")
    datasets = load_datasets()

    return {
        "collections": [_build_collection(request, dataset) for dataset in datasets.values()],
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


@router.get("/collections/{collectionId}")
def get_collection(collectionId: str, request: Request) -> dict:
    dataset = load_datasets().get(collectionId)
    if dataset is None:
        raise not_found("Collection", collectionId)
    return _build_collection(request, dataset)


router.include_router(coverages_router, tags=["Collections"])
router.include_router(edr_router, tags=["Collections"])
