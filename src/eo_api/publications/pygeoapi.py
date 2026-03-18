"""Generate pygeoapi-facing documents from backend publication state."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from ..data_manager.services.downloader import DOWNLOAD_DIR, get_zarr_path
from ..data_registry.services.datasets import get_dataset
from .schemas import PublishedResource, PublishedResourceExposure, PublishedResourceKind
from .services import collection_id_for_resource, ensure_source_dataset_publications, list_published_resources

_DEFAULT_SERVER_URL = "http://127.0.0.1:8000/ogcapi"


def build_pygeoapi_config(*, server_url: str = _DEFAULT_SERVER_URL) -> dict[str, Any]:
    """Build a minimal pygeoapi config from published resources."""
    ensure_source_dataset_publications()
    resources = list(_iter_pygeoapi_resources())
    return {
        "server": {
            "bind": {"host": "0.0.0.0", "port": 5000},
            "url": server_url,
            "mimetype": "application/json; charset=UTF-8",
            "encoding": "utf-8",
            "languages": ["en-US"],
            "limits": {"default_items": 20, "max_items": 50},
            "map": {
                "url": "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
                "attribution": "OpenStreetMap",
            },
            "gzip": True,
        },
        "logging": {"level": "ERROR"},
        "metadata": {
            "identification": {
                "title": {"en": "DHIS2 EO API"},
                "description": {"en": "Generated pygeoapi publication config from backend publication truth"},
                "keywords": {"en": ["EO", "DHIS2", "OGC"]},
                "terms_of_service": "https://dhis2.org",
                "url": "https://dhis2.org",
            },
            "license": {
                "name": "CC-BY 4.0",
                "url": "https://creativecommons.org/licenses/by/4.0/",
            },
            "provider": {"name": "DHIS2 EO API", "url": "https://dhis2.org"},
            "contact": {"name": "DHIS2", "position": "Team", "email": "climate@dhis2.org"},
        },
        "resources": {
            collection_id_for_resource(resource): _build_pygeoapi_resource(resource) for resource in resources
        },
    }


def build_pygeoapi_openapi(*, server_url: str = _DEFAULT_SERVER_URL) -> dict[str, Any]:
    """Build a minimal OpenAPI projection that reflects generated collection resources."""
    config = build_pygeoapi_config(server_url=server_url)
    resources = config["resources"]
    return {
        "openapi": "3.0.2",
        "info": {
            "title": "DHIS2 EO API",
            "description": "Generated pygeoapi OpenAPI projection from backend publication truth",
            "version": "0.1.0",
        },
        "servers": [{"url": server_url}],
        "paths": {
            "/collections": {
                "get": {
                    "summary": "Collections",
                    "operationId": "getCollections",
                    "responses": {"200": {"description": "successful operation"}},
                }
            },
            **{
                f"/collections/{resource_id}": {
                    "get": {
                        "summary": f"Collection {resource_id}",
                        "operationId": f"getCollection_{resource_id.replace('-', '_')}",
                        "responses": {"200": {"description": "successful operation"}},
                    }
                }
                for resource_id in resources
            },
        },
        "x-generated-resources": list(resources.keys()),
    }


def write_generated_pygeoapi_documents(*, server_url: str = _DEFAULT_SERVER_URL) -> tuple[Path, Path]:
    """Persist generated pygeoapi config and OpenAPI documents to disk."""
    output_dir = DOWNLOAD_DIR / "pygeoapi"
    output_dir.mkdir(parents=True, exist_ok=True)
    config_path = output_dir / "pygeoapi-config.generated.yml"
    openapi_path = output_dir / "pygeoapi-openapi.generated.yml"
    config_text = yaml.safe_dump(build_pygeoapi_config(server_url=server_url), sort_keys=False)
    config_path.write_text(config_text, encoding="utf-8")
    openapi_path.write_text(
        yaml.safe_dump(build_pygeoapi_openapi(server_url=server_url), sort_keys=False),
        encoding="utf-8",
    )
    return config_path, openapi_path


def _build_pygeoapi_resource(resource: PublishedResource) -> dict[str, Any]:
    provider = _build_provider(resource)
    return {
        "type": "collection",
        "title": {"en": resource.title},
        "description": {"en": resource.description},
        "keywords": _keywords_for_resource(resource),
        "links": _pygeoapi_links(resource),
        "extents": {
            "spatial": {"bbox": _bbox_for_resource(resource)},
            "temporal": _temporal_extent_for_resource(resource),
        },
        "providers": [provider],
        "metadata": {
            "resource_id": resource.resource_id,
            "resource_class": str(resource.resource_class),
            "dataset_id": resource.dataset_id,
            "workflow_id": resource.workflow_id,
            "job_id": resource.job_id,
            "kind": str(resource.kind),
            **resource.metadata,
        },
    }


def _bbox_for_resource(resource: PublishedResource) -> list[list[float]]:
    bbox = resource.metadata.get("bbox")
    if isinstance(bbox, list) and bbox:
        return [bbox]
    return [[-180.0, -90.0, 180.0, 90.0]]


def _temporal_extent_for_resource(resource: PublishedResource) -> dict[str, str | None]:
    metadata = resource.metadata
    start = metadata.get("time_start")
    end = metadata.get("time_end")
    if start is not None or end is not None:
        return {
            "begin": str(start) if start is not None else None,
            "end": str(end) if end is not None else None,
        }
    period_type = metadata.get("period_type")
    if period_type is not None:
        value = str(period_type)
        return {"begin": value, "end": value}
    return {"begin": None, "end": None}


def _keywords_for_resource(resource: PublishedResource) -> list[str]:
    keywords = ["EO", "DHIS2", str(resource.resource_class), str(resource.kind)]
    if resource.dataset_id is not None:
        keywords.append(resource.dataset_id)
    if resource.workflow_id is not None:
        keywords.append(resource.workflow_id)
    return keywords


def _build_provider(resource: PublishedResource) -> dict[str, Any]:
    if resource.kind == PublishedResourceKind.COVERAGE:
        dataset = get_dataset(str(resource.dataset_id))
        if dataset is None:
            raise ValueError(f"Unknown dataset_id '{resource.dataset_id}' for resource '{resource.resource_id}'")
        zarr_path = get_zarr_path(dataset)
        if zarr_path is None:
            raise ValueError(f"No zarr cache available for dataset '{resource.dataset_id}'")
        return {
            "name": "xarray",
            "type": "coverage",
            "data": str(zarr_path),
            "default": True,
        }

    if resource.kind == PublishedResourceKind.FEATURE_COLLECTION and resource.path is not None:
        suffix = Path(resource.path).suffix.lower()
        if suffix == ".geojson":
            return {
                "name": "GeoJSON",
                "type": "feature",
                "data": resource.path,
                "id_field": "id",
                "default": True,
            }

    raise ValueError(f"Resource '{resource.resource_id}' is not yet mappable to a pygeoapi provider")


def _iter_pygeoapi_resources() -> list[PublishedResource]:
    resources: list[PublishedResource] = []
    for resource in list_published_resources(exposure=PublishedResourceExposure.OGC):
        try:
            _build_provider(resource)
        except ValueError:
            continue
        resources.append(resource)
    return resources


def _pygeoapi_links(resource: PublishedResource) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    for link in resource.links:
        href = str(link.get("href", ""))
        rel = str(link.get("rel", "related"))
        if href == "":
            continue
        links.append(
            {
                "type": "application/json",
                "rel": rel,
                "title": rel.replace("-", " ").title(),
                "href": _absolute_ogc_href(href),
            }
        )
    return links


def _absolute_ogc_href(href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return f"{_DEFAULT_SERVER_URL.removesuffix('/ogcapi')}{href}"
