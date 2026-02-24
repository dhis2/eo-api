import os
from urllib.parse import urlparse

from fastapi import APIRouter, Request

from pygeoapi.api import FORMAT_TYPES, F_JSON
from pygeoapi.util import url_join

from eoapi.dhis2_integration import dhis2_configured
from eoapi.state_store import STATE_DIR_ENV, STATE_PERSIST_ENV

router = APIRouter(tags=["Landing Page"])


def _cors_origins() -> list[str]:
    raw = os.getenv("EOAPI_CORS_ORIGINS", "*").strip()
    if not raw:
        return ["*"]
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


def _api_key_required() -> bool:
    return bool(os.getenv("EOAPI_API_KEY", "").strip())


def _internal_scheduler_enabled() -> bool:
    raw = os.getenv("EOAPI_INTERNAL_SCHEDULER_ENABLED", "true").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _state_persistence_enabled() -> bool:
    raw = os.getenv(STATE_PERSIST_ENV, "true").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _state_directory() -> str:
    return os.getenv(STATE_DIR_ENV, ".cache/state").strip() or ".cache/state"


def _dhis2_auth_mode() -> str:
    if os.getenv("EOAPI_DHIS2_TOKEN", "").strip():
        return "token"

    if os.getenv("EOAPI_DHIS2_USERNAME", "").strip() and os.getenv("EOAPI_DHIS2_PASSWORD", "").strip():
        return "basic"

    return "none"


def _dhis2_host() -> str:
    base = os.getenv("EOAPI_DHIS2_BASE_URL", "").strip()
    if not base:
        return "unset"

    parsed = urlparse(base)
    return parsed.netloc or parsed.path or "configured"


def _runtime_summary() -> dict:
    cors = _cors_origins()
    return {
        "cors": {
            "mode": "wildcard" if cors == ["*"] else "restricted",
            "origins": len(cors),
        },
        "apiKeyRequired": _api_key_required(),
        "dhis2": {
            "configured": dhis2_configured(),
            "host": _dhis2_host(),
            "authMode": _dhis2_auth_mode(),
        },
        "state": {
            "persistenceEnabled": _state_persistence_enabled(),
            "directory": _state_directory(),
        },
        "internalScheduler": {
            "enabled": _internal_scheduler_enabled(),
        },
    }


@router.get("/")
def read_index(request: Request) -> dict:
    base = str(request.base_url).rstrip("/")
    return {
        "title": "DHIS2 EO API",
        "description": "OGC-aligned Earth Observation API for DHIS2 and CHAP.",
        "runtime": _runtime_summary(),
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
                "rel": "data",
                "type": FORMAT_TYPES[F_JSON],
                "title": "Feature collections",
                "href": url_join(base, "features"),
            },
            {
                "rel": "processes",
                "type": FORMAT_TYPES[F_JSON],
                "title": "Processes",
                "href": url_join(base, "processes"),
            },
            {
                "rel": "processes",
                "type": FORMAT_TYPES[F_JSON],
                "title": "Workflows",
                "href": url_join(base, "workflows"),
            },
            {
                "rel": "service-doc",
                "type": "text/html",
                "title": "OpenAPI docs",
                "href": url_join(base, "docs"),
            },
            {
                "rel": "service",
                "type": "text/html",
                "title": "Example frontend app",
                "href": url_join(base, "example-app"),
            },
        ],
    }
