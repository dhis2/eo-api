"""Server-side HTML rendering and root resource representations for the Open Climate Service."""

import importlib.resources
import logging
from datetime import date
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from typing import Any

import jinja2
from fastapi import Request

from open_climate_service import config as api_config
from open_climate_service.data_registry.services import datasets as registry_datasets
from open_climate_service.extents.services import get_extent
from open_climate_service.ingestions.services import list_datasets

from .schemas import Link, RootResponse

_env = jinja2.Environment(loader=jinja2.BaseLoader(), autoescape=True)

_cache: dict[str, jinja2.Template] = {}

_log = logging.getLogger(__name__)

try:
    app_version = _pkg_version("open-climate-service")
except PackageNotFoundError:
    app_version = "unknown"

ROOT_RESPONSES: dict[int | str, dict[str, Any]] = {
    200: {
        "description": "Landing page (HTML) or navigation document (JSON)",
        "content": {
            "text/html": {"schema": {"type": "string"}},
            "application/json": {"schema": RootResponse.model_json_schema()},
        },
    }
}


def root_json(base: str) -> RootResponse:
    """Build the root navigation document for the JSON representation."""
    return RootResponse(
        message="Welcome to Open Climate Service",
        links=[
            Link(href=f"{base}/ogcapi/", rel="ogcapi", title="OGC API"),
            Link(href=f"{base}/stac/catalog.json", rel="stac", title="STAC Catalog"),
            Link(href=f"{base}/extent", rel="extent", title="Extent"),
            Link(href=f"{base}/ingestions", rel="ingestions", title="Ingestions"),
            Link(href=f"{base}/datasets", rel="datasets", title="Datasets"),
            Link(href=f"{base}/docs", rel="docs", title="API Docs"),
        ],
    )


def get_template(name: str) -> jinja2.Template:
    """Load and cache a Jinja2 template from the bundled templates/ directory."""
    if name not in _cache:
        resource = importlib.resources.files("open_climate_service") / "templates" / name
        _cache[name] = _env.from_string(resource.read_text(encoding="utf-8"))
    return _cache[name]


def _media_type_q(accept: str, media_type: str) -> float:
    """Return the q-value for media_type in an Accept header, or -1.0 if absent."""
    for item in accept.split(","):
        parts = item.strip().split(";")
        if parts[0].strip() != media_type:
            continue
        q = 1.0
        for param in parts[1:]:
            param = param.strip()
            if param.startswith("q="):
                try:
                    q = float(param[2:])
                except ValueError:
                    pass
        return q
    return -1.0


def wants_json(request: Request) -> bool:
    """Return True if the client prefers a JSON response over HTML.

    JSON is preferred when application/json is present with a q-value greater
    than or equal to text/html. HTML wins only when text/html has a strictly
    higher q-value, matching RFC 7231 content negotiation semantics.
    """
    if request.query_params.get("f") == "json":
        return True
    accept = request.headers.get("accept", "")
    if not accept:
        return False
    json_q = _media_type_q(accept, "application/json")
    html_q = _media_type_q(accept, "text/html")
    return json_q >= 0 and (html_q < 0 or json_q >= html_q)


def render_maps(base: str) -> str:
    """Render the map viewer page."""
    return get_template("map-viewer.html").render(base=base, name=api_config.get_name())


def _load_extent() -> dict[str, Any] | None:
    try:
        return get_extent()
    except ValueError:
        return None
    except Exception:
        _log.exception("Unexpected error loading extent")
        return None


def _load_templates() -> list[dict[str, Any]]:
    try:
        return registry_datasets.list_datasets()
    except Exception:
        _log.exception("Unexpected error loading dataset templates")
        return []


def _load_datasets() -> list[Any]:
    try:
        return list_datasets().items
    except Exception:
        _log.exception("Unexpected error loading datasets")
        return []


def render_landing(version: str, base: str) -> str:
    """Render the root landing page with live instance status."""
    return get_template("landing_page.html").render(
        version=version,
        base=base,
        name=api_config.get_name(),
        extent=_load_extent(),
        datasets=_load_datasets(),
        templates=_load_templates(),
    )


def render_manage(version: str, base: str, message: str | None = None, error: str | None = None) -> str:
    """Render the management page."""
    today = date.today().isoformat()
    year_ago = date.today().replace(year=date.today().year - 1).isoformat()
    return get_template("manage.html").render(
        version=version,
        base=base,
        name=api_config.get_name(),
        extent=_load_extent(),
        templates=_load_templates(),
        datasets=_load_datasets(),
        today=today,
        year_ago=year_ago,
        message=message,
        error=error,
    )
