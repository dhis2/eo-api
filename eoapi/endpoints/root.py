from fastapi import APIRouter, Request

from pygeoapi.api import FORMAT_TYPES, F_JSON
from pygeoapi.util import url_join

router = APIRouter(tags=["Landing Page"])


@router.get("/")
def read_index(request: Request) -> dict:
    base = str(request.base_url).rstrip("/")
    return {
        "title": "DHIS2 EO API",
        "description": "OGC-aligned Earth Observation API for DHIS2 and CHAP.",
        "links": [
            {
                "rel": "self",
                "type": FORMAT_TYPES[F_JSON],
                "title": "This document",
                "href": url_join(base, "/"),
            },
            {
                "rel": "conformance",
                "type": FORMAT_TYPES[F_JSON],
                "title": "Conformance",
                "href": url_join(base, "conformance"),
            },
            {
                "rel": "data",
                "type": FORMAT_TYPES[F_JSON],
                "title": "Collections",
                "href": url_join(base, "collections"),
            },
            {
                "rel": "service-doc",
                "type": "text/html",
                "title": "OpenAPI docs",
                "href": url_join(base, "docs"),
            },
        ],
    }
