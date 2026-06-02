"""STAC catalogue builders backed by published artifact records."""

from __future__ import annotations

import json
import logging
import os
from copy import deepcopy
from pathlib import Path
from typing import Any

import pystac
import xarray as xr
from fastapi import HTTPException, Request
from xstac import xarray_to_stac

from open_climate_service.data_accessor.services.accessor import open_icechunk_dataset, open_zarr_dataset
from open_climate_service.data_manager.services.utils import get_time_dim, get_x_y_dims
from open_climate_service.data_registry.services import datasets as registry_datasets
from open_climate_service.ingestions import services as ingestion_services
from open_climate_service.ingestions.schemas import ArtifactFormat, ArtifactRecord
from open_climate_service.shared.time import parse_period_string_to_datetime, resolve_iso_period_step

CATALOG_TITLE = "Open Climate Service"
CATALOG_DESCRIPTION = "Published Open Climate Service GeoZarr datasets"
STAC_VERSION = "1.1.0"
DATACUBE_EXTENSION = "https://stac-extensions.github.io/datacube/v2.3.0/schema.json"
PROJECTION_EXTENSION = "https://stac-extensions.github.io/projection/v1.1.0/schema.json"
RENDER_EXTENSION = "https://stac-extensions.github.io/render/v2.0.0/schema.json"
ZARR_EXTENSION = "https://stac-extensions.github.io/zarr/v1.1.0/schema.json"
DEFAULT_STAC_LICENSE = "various"
SPATIAL_STEP_DECIMALS = 8
XSTAC_COLLECTION_CACHE_MAXSIZE = 128
logger = logging.getLogger(__name__)
_xstac_collection_cache: dict[str, dict[str, Any]] = {}


def _get_catalog_id() -> str:
    from open_climate_service import config as api_config

    return api_config.get_id()


def build_catalog(request: Request) -> dict[str, object]:
    """Build the STAC catalog document."""
    self_href = str(request.url)
    catalog_href = _abs_url(request, "/stac/catalog.json")
    links = [
        {"rel": "self", "href": self_href, "type": "application/json"},
        {"rel": "root", "href": catalog_href, "type": "application/json"},
    ]
    for dataset_id, artifact in _eligible_artifacts_by_dataset().items():
        links.append(
            {
                "rel": "child",
                "href": _abs_url(request, f"/stac/collections/{dataset_id}"),
                "title": artifact.dataset_name,
                "type": "application/json",
            }
        )
    return {
        "stac_version": STAC_VERSION,
        "type": "Catalog",
        "id": _get_catalog_id(),
        "title": CATALOG_TITLE,
        "description": CATALOG_DESCRIPTION,
        "links": links,
    }


def build_collection(dataset_id: str, request: Request) -> dict[str, object]:
    """Build one STAC collection document."""
    artifact = _eligible_artifacts_by_dataset().get(dataset_id)
    if artifact is None:
        raise HTTPException(status_code=404, detail=f"STAC collection '{dataset_id}' not found")

    source_dataset = registry_datasets.get_dataset(artifact.dataset_id) or {}
    collection_href = _abs_url(request, f"/stac/collections/{dataset_id}")
    catalog_href = _abs_url(request, "/stac/catalog.json")
    dataset_href = _abs_url(request, f"/datasets/{dataset_id}")
    zarr_href = _public_zarr_asset_href(request, dataset_id, artifact, source_dataset)

    template = _build_collection_template(
        dataset_id=dataset_id,
        artifact=artifact,
        collection_href=collection_href,
        catalog_href=catalog_href,
        dataset_href=dataset_href,
        zarr_href=zarr_href,
        source_dataset=source_dataset,
    )
    template_links = [_link_to_dict(link) for link in template.links]

    collection_payload = _build_collection_with_xstac(artifact=artifact, template=template)
    collection_payload["id"] = dataset_id
    collection_payload["type"] = "Collection"
    collection_payload["stac_version"] = STAC_VERSION
    collection_payload["description"] = template.description
    collection_payload["title"] = template.title
    renders = _build_renders(artifact, source_dataset)
    extensions = {DATACUBE_EXTENSION, ZARR_EXTENSION}
    if renders is not None:
        collection_payload["renders"] = renders
        extensions.add(RENDER_EXTENSION)
    existing_extensions = collection_payload.get("stac_extensions", [])
    if isinstance(existing_extensions, list):
        collection_payload["stac_extensions"] = sorted({*existing_extensions, *extensions})
    else:
        collection_payload["stac_extensions"] = sorted(extensions)
    collection_payload["links"] = template_links
    assets = collection_payload.setdefault("assets", {})
    zarr_from_xstac = assets.get("zarr", {}) if isinstance(assets, dict) else {}
    template_asset = _asset_to_dict(_required_zarr_asset(template))
    xarray_open_kwargs = _zarr_open_kwargs(artifact)
    collection_payload["assets"]["zarr"] = {
        **zarr_from_xstac,
        **_zarr_asset_metadata(artifact),
        "href": template_asset["href"],
        "type": template_asset.get("type"),
        "title": template_asset.get("title"),
        "roles": template_asset.get("roles"),
        "xarray:open_kwargs": xarray_open_kwargs,
    }
    collection_payload["license"] = template.license
    _remove_helper_variables(collection_payload)
    _round_spatial_steps(collection_payload)
    _override_time_step(collection_payload, resolve_iso_period_step(source_dataset))
    _override_spatial_extent_from_artifact(collection_payload, artifact)
    _override_temporal_extent_from_artifact(collection_payload, artifact)
    _sanitize_variable_attrs(collection_payload)
    return collection_payload


def _eligible_artifacts_by_dataset() -> dict[str, ArtifactRecord]:
    return ingestion_services.latest_published_zarr_artifacts_by_dataset()


def _build_collection_template(
    *,
    dataset_id: str,
    artifact: ArtifactRecord,
    collection_href: str,
    catalog_href: str,
    dataset_href: str,
    zarr_href: str,
    source_dataset: dict[str, Any],
) -> pystac.Collection:
    spatial = artifact.coverage.spatial_wgs84 or artifact.coverage.spatial
    temporal = artifact.coverage.temporal
    template = pystac.Collection(
        id=dataset_id,
        description=f"Published GeoZarr dataset for {artifact.dataset_name}",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[spatial.xmin, spatial.ymin, spatial.xmax, spatial.ymax]]),
            temporal=pystac.TemporalExtent(
                [[parse_period_string_to_datetime(temporal.start), parse_period_string_to_datetime(temporal.end)]]
            ),
        ),
        title=artifact.dataset_name,
        stac_extensions=[
            DATACUBE_EXTENSION,
            PROJECTION_EXTENSION,
            ZARR_EXTENSION,
        ],
        license=DEFAULT_STAC_LICENSE,
    )
    from open_climate_service import config as api_config

    native_crs = api_config.get_crs() or "EPSG:4326"
    template.extra_fields["keywords"] = _keywords(artifact, source_dataset)
    template.extra_fields["proj:code"] = native_crs
    template.clear_links()
    template.add_link(pystac.Link(rel="self", target=collection_href, media_type="application/json"))
    template.add_link(pystac.Link(rel="root", target=catalog_href, media_type="application/json"))
    template.add_link(pystac.Link(rel="parent", target=catalog_href, media_type="application/json"))
    template.add_link(
        pystac.Link(rel="alternate", target=dataset_href, media_type="application/json", title="Dataset detail")
    )
    template.add_asset(
        "zarr",
        pystac.Asset(
            href=zarr_href,
            media_type="application/vnd+zarr",
            title="Zarr store",
            roles=["data"],
        ),
    )
    return template


def _build_collection_with_xstac(*, artifact: ArtifactRecord, template: pystac.Collection) -> dict[str, Any]:
    cached_payload = _xstac_collection_cache.get(artifact.artifact_id)
    if cached_payload is not None:
        return deepcopy(cached_payload)

    try:
        ds = _open_published_store(artifact)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to open published Zarr store for artifact '%s'", artifact.artifact_id)
        raise HTTPException(
            status_code=503,
            detail=f"Published Zarr store for artifact '{artifact.artifact_id}' is temporarily unavailable",
        ) from exc
    try:
        x_dimension, y_dimension = get_x_y_dims(ds)
        time_dimension = get_time_dim(ds)
        result = xarray_to_stac(
            ds,
            template,
            temporal_dimension=time_dimension,
            x_dimension=x_dimension,
            y_dimension=y_dimension,
            reference_system=4326,
            # Schema validation can trigger outbound fetches for STAC extension schemas.
            validate=False,
        )
        # build_collection replaces links from the template after xstac runs, so
        # clear xstac/pystac-owned links before serialization to avoid root-link
        # resolution attempts during to_dict().
        result.clear_links()
        payload: dict[str, Any] = result.to_dict(include_self_link=False)
        _cache_xstac_collection_payload(artifact.artifact_id, payload)
        return deepcopy(payload)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to derive STAC metadata from artifact '%s'", artifact.artifact_id)
        raise HTTPException(
            status_code=503,
            detail=f"Published Zarr store for artifact '{artifact.artifact_id}' is temporarily unavailable",
        ) from exc
    finally:
        ds.close()


def _cache_xstac_collection_payload(artifact_id: str, payload: dict[str, Any]) -> None:
    if artifact_id in _xstac_collection_cache:
        _xstac_collection_cache[artifact_id] = deepcopy(payload)
        return
    if len(_xstac_collection_cache) >= XSTAC_COLLECTION_CACHE_MAXSIZE:
        oldest_artifact_id = next(iter(_xstac_collection_cache))
        _xstac_collection_cache.pop(oldest_artifact_id, None)
    _xstac_collection_cache[artifact_id] = deepcopy(payload)


def _clear_xstac_collection_cache() -> None:
    _xstac_collection_cache.clear()


def _link_to_dict(link: pystac.Link) -> dict[str, Any]:
    target = link.target
    href = target if isinstance(target, str) else link.href
    payload = {"rel": link.rel, "href": str(href)}
    if link.media_type is not None:
        payload["type"] = link.media_type
    if link.title is not None:
        payload["title"] = link.title
    return payload


def _asset_to_dict(asset: pystac.Asset) -> dict[str, Any]:
    payload: dict[str, Any] = {"href": asset.href}
    if asset.media_type is not None:
        payload["type"] = asset.media_type
    if asset.title is not None:
        payload["title"] = asset.title
    if asset.roles is not None:
        payload["roles"] = asset.roles
    payload.update(asset.extra_fields)
    return payload


def _required_zarr_asset(template: pystac.Collection) -> pystac.Asset:
    asset = template.assets.get("zarr")
    if asset is None:
        raise HTTPException(status_code=500, detail="STAC template is missing the required zarr asset")
    return asset


def _artifact_store_path(artifact: ArtifactRecord) -> str:
    if artifact.path:
        return artifact.path
    if artifact.asset_paths:
        return artifact.asset_paths[0]
    raise HTTPException(
        status_code=500,
        detail=f"Published artifact '{artifact.artifact_id}' has no readable storage path metadata",
    )


def _public_zarr_asset_href(
    request: Request,
    dataset_id: str,
    artifact: ArtifactRecord,
    source_dataset: dict[str, Any],
) -> str:
    artifact_path = _artifact_store_path(artifact)
    if artifact.format == ArtifactFormat.ICECHUNK:
        return _abs_url(request, f"/zarr/{dataset_id}")
    if _is_pyramid_zarr(artifact_path):
        return _abs_url(request, f"/zarr/{dataset_id}/0")
    return _abs_url(request, f"/zarr/{dataset_id}")


def _is_pyramid_zarr(artifact_path: str) -> bool:
    """Return True if artifact_path is a multiscale pyramid zarr store."""
    if "://" in artifact_path:
        return False
    return (Path(artifact_path) / "0").is_dir()


def _abs_url(request: Request, path: str) -> str:
    base_url = os.getenv("CLIMATE_SERVICE_BASE_URL")
    if base_url:
        return f"{base_url.rstrip('/')}{path}"
    return f"{str(request.base_url).rstrip('/')}{path}"


def _override_time_step(collection: dict[str, Any], step: str | None) -> None:
    if step is None:
        return
    dimensions = collection.setdefault("cube:dimensions", {})
    for key, value in dimensions.items():
        if isinstance(value, dict) and value.get("type") == "temporal":
            value["step"] = step
            dimensions[key] = value
            return


def _round_spatial_steps(collection: dict[str, Any]) -> None:
    dimensions = collection.get("cube:dimensions")
    if not isinstance(dimensions, dict):
        return
    for key, value in dimensions.items():
        if not isinstance(value, dict) or value.get("type") != "spatial":
            continue
        step = value.get("step")
        if isinstance(step, int | float):
            value["step"] = round(float(step), SPATIAL_STEP_DECIMALS)
            dimensions[key] = value


def _override_spatial_extent_from_artifact(collection: dict[str, Any], artifact: ArtifactRecord) -> None:
    spatial = artifact.coverage.spatial_wgs84 or artifact.coverage.spatial
    collection["extent"]["spatial"]["bbox"] = [[spatial.xmin, spatial.ymin, spatial.xmax, spatial.ymax]]


def _override_temporal_extent_from_artifact(collection: dict[str, Any], artifact: ArtifactRecord) -> None:
    temporal = artifact.coverage.temporal
    start = parse_period_string_to_datetime(temporal.start).isoformat().replace("+00:00", "Z")
    end = parse_period_string_to_datetime(temporal.end).isoformat().replace("+00:00", "Z")
    collection["extent"]["temporal"]["interval"] = [
        [
            start,
            end,
        ]
    ]
    dimensions = collection.setdefault("cube:dimensions", {})
    for key, value in dimensions.items():
        if isinstance(value, dict) and value.get("type") == "temporal":
            value["extent"] = [start, end]
            dimensions[key] = value
            return


def _sanitize_variable_attrs(collection: dict[str, Any]) -> None:
    variables = collection.get("cube:variables")
    if not isinstance(variables, dict):
        return
    for _, variable in variables.items():
        if not isinstance(variable, dict):
            continue
        attrs = variable.get("attrs")
        if not isinstance(attrs, dict):
            continue
        kept_attrs: dict[str, str] = {}
        long_name = attrs.get("long_name")
        units = attrs.get("units")
        if isinstance(long_name, str):
            kept_attrs["long_name"] = long_name
        if isinstance(units, str):
            kept_attrs["units"] = units
            variable["unit"] = units
        variable["attrs"] = kept_attrs


def _remove_helper_variables(collection: dict[str, Any]) -> None:
    variables = collection.get("cube:variables")
    if not isinstance(variables, dict):
        return
    for key in list(variables):
        variable = variables.get(key)
        if not isinstance(variable, dict):
            continue
        dimensions = variable.get("dimensions")
        # xstac can emit scalar CRS/grid-mapping helper variables with no dimensions.
        if isinstance(dimensions, list) and len(dimensions) == 0:
            variables.pop(key, None)


def _keywords(artifact: ArtifactRecord, source_dataset: dict[str, Any]) -> list[str]:
    keywords = [artifact.dataset_id, artifact.variable, "zarr", "stac"]
    for key in ("source", "short_name"):
        value = source_dataset.get(key)
        if isinstance(value, str) and value:
            keywords.append(value)
    return keywords


def _zarr_asset_metadata(artifact: ArtifactRecord) -> dict[str, object]:
    metadata: dict[str, object] = {"zarr:node_type": "group"}
    if artifact.format == ArtifactFormat.ICECHUNK:
        metadata["zarr:zarr_format"] = 3
        return metadata
    artifact_path = _artifact_store_path(artifact)
    consolidated = _zarr_consolidated_flag(artifact_path)
    if consolidated is not None:
        metadata["zarr:consolidated"] = consolidated
    if "://" in artifact_path:
        return metadata
    store_root = Path(artifact_path)
    zarr_json = store_root / "zarr.json"
    if zarr_json.exists():
        metadata["zarr:zarr_format"] = 3
    else:
        zgroup = store_root / ".zgroup"
        if zgroup.exists():
            metadata["zarr:zarr_format"] = 2
    return metadata


def _zarr_open_kwargs(artifact: ArtifactRecord) -> dict[str, bool | None]:
    if artifact.format == ArtifactFormat.ICECHUNK:
        return {"consolidated": None}
    return {"consolidated": _zarr_consolidated_flag(_artifact_store_path(artifact))}


def _open_published_store(artifact: ArtifactRecord) -> xr.Dataset:
    if artifact.format == ArtifactFormat.ICECHUNK:
        return open_icechunk_dataset(_artifact_store_path(artifact))
    return open_zarr_dataset(_artifact_store_path(artifact))


def _build_renders(artifact: ArtifactRecord, source_dataset: dict[str, Any]) -> dict[str, Any] | None:
    display = source_dataset.get("display")
    if not isinstance(display, dict):
        return None
    colormap_name = display.get("colormap")
    value_range = display.get("range")
    if not isinstance(colormap_name, str) or not isinstance(value_range, list) or len(value_range) != 2:
        return None
    render: dict[str, Any] = {
        "title": artifact.dataset_name,
        "assets": ["zarr"],
        "rescale": [[float(value_range[0]), float(value_range[1])]],
        "colormap_name": colormap_name,
        "open_climate_service:variable": artifact.variable,
    }
    nodata = display.get("nodata")
    if nodata is not None:
        render["nodata"] = float(nodata)
    units = source_dataset.get("units")
    if isinstance(units, str):
        render["open_climate_service:units"] = units
    return {"default": render}


def _zarr_consolidated_flag(artifact_path: str) -> bool | None:
    if "://" in artifact_path:
        return None

    store_root = Path(artifact_path)
    zarr_json = store_root / "zarr.json"
    if zarr_json.exists():
        try:
            payload = json.loads(zarr_json.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return "consolidated_metadata" in payload

    if (store_root / ".zmetadata").exists():
        return True
    if (store_root / ".zgroup").exists():
        return False
    return None
