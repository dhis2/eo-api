from fastapi import APIRouter, Request

from pygeoapi.util import url_join

router = APIRouter(tags=["Conformance"])

CONFORMANCE_CLASSES = [
    "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/landing-page",
    "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/json",
    "http://www.opengis.net/spec/ogcapi-common-2/1.0/conf/collections",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/coveragejson",
    "http://www.opengis.net/spec/ogcapi-edr-1/1.1/conf/core",
    "http://www.opengis.net/spec/ogcapi-edr-1/1.1/conf/position",
    "http://www.opengis.net/spec/ogcapi-edr-1/1.1/conf/area",
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
    "http://www.opengis.net/spec/ogcapi-processes-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-processes-1/1.0/conf/json",
]


@router.get("/conformance")
def get_conformance(request: Request) -> dict:
    base = str(request.base_url).rstrip("/")
    return {
        "conformsTo": CONFORMANCE_CLASSES,
        "links": [
            {
                "rel": "self",
                "type": "application/json",
                "title": "Conformance declaration",
                "href": url_join(base, "conformance"),
            },
            {
                "rel": "root",
                "type": "application/json",
                "title": "API root",
                "href": url_join(base, "/"),
            },
        ],
    }
