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
from open_climate_service.shared.crs import canonical_crs_code, is_builtin_crs
from open_climate_service.shared.time import (
    parse_period_string_to_datetime,
    period_type_to_iso_step,
    resolve_iso_period_step,
)

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
    if artifact.format == ArtifactFormat.ICECHUNK:
        collection_payload["assets"]["icechunk"] = {
            "href": _abs_url(request, f"/icechunk/{dataset_id}"),
            "type": "application/octet-stream",
            "title": "Icechunk store (native SDK access)",
            "roles": ["data"],
            "xarray:open_kwargs": {"zarr_format": 3, "consolidated": False},
        }
    collection_payload["license"] = template.license
    _remove_helper_variables(collection_payload)
    _round_spatial_steps(collection_payload)
    # Prefer an explicit extents.temporal.resolution; fall back to the dataset's
    # period_type so openEO save_result outputs (which omit the extents block) still
    # get a temporal step — the map viewer needs it to build the time slider.
    _override_time_step(
        collection_payload,
        resolve_iso_period_step(source_dataset) or period_type_to_iso_step(source_dataset.get("period_type")),
    )
    # Spatial extent comes from the live store (set in _build_collection_with_xstac),
    # not the artifact coverage — see _wgs84_extent_from_store. Temporal still tracks the
    # artifact's materialized coverage.
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
                [
                    [
                        parse_period_string_to_datetime(temporal.start) if temporal.start else None,
                        parse_period_string_to_datetime(temporal.end) if temporal.end else None,
                    ]
                ]
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
    # WGS84 default; the store's native CRS (read from its proj:code attr in
    # _build_collection_with_xstac) overrides this. The instance config CRS is
    # never used — every dataset reports the CRS it was stored in.
    template.extra_fields["keywords"] = _keywords(artifact, source_dataset)
    template.extra_fields["proj:code"] = "EPSG:4326"
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
            media_type="application/vnd.zarr; version=3",
            title="Zarr store",
            roles=["data"],
        ),
    )
    return template


def _add_crs_render_hints(*, template: pystac.Collection, ds: xr.Dataset, store_crs: str) -> None:
    """Surface CRS render hints on the collection for projected (non-built-in) stores.

    Map clients reproject Zarr on the fly with proj4js, which resolves only the
    built-in ``EPSG:4326`` / ``EPSG:3857`` from their code — any other CRS (e.g.
    seNorge's UTM33, ``EPSG:32633``) needs a full definition, or the client has to
    fetch one at render time (an external epsg.io lookup). Publishing the definition
    in STAC removes that runtime dependency.

    Three fields are emitted:

    * ``proj:wkt2`` — the STAC Projection-extension standard, lossless CRS
      representation. It is the same information GeoZarr already carries in the CF
      ``spatial_ref`` grid-mapping's ``crs_wkt`` attribute.
    * ``open_climate_service:proj4`` — a proj4 string for direct consumption by
      proj4js. proj4 is intentionally *not* a STAC-standard field (PROJ treats proj4
      strings as lossy), so it lives under our namespace rather than ``proj:``.
      zarr-layer only accepts a built-in ``crs`` code or a ``proj4`` string today; once
      carbonplan/zarr-layer#61 lands (auto-resolving any EPSG code) this proj4 hint
      becomes unnecessary and only ``proj:wkt2`` need remain, for other STAC clients.
    * ``proj:bbox`` — the data extent in the store's native CRS. Without it, a client
      reprojecting the Zarr must fetch the x/y coordinate arrays at render time to
      derive bounds (zarr-layer's "proj4 provided without explicit bounds" warning);
      publishing it lets the client pass explicit bounds and skip that round-trip.
    """
    if is_builtin_crs(store_crs):
        return
    try:
        import warnings

        from pyproj import CRS

        # Prefer the CRS the data was actually written with (the CF grid-mapping WKT)
        # over re-deriving from the code, when the store carries it.
        wkt: str | None = None
        spatial_ref = ds.get("spatial_ref")
        if spatial_ref is not None:
            wkt = spatial_ref.attrs.get("crs_wkt") or spatial_ref.attrs.get("spatial_ref")
        crs = CRS.from_wkt(wkt) if wkt else CRS.from_user_input(store_crs)

        # Force WKT2 explicitly — pyproj's to_wkt() default can vary by version and could
        # emit WKT1 (PROJCS); the STAC Projection extension defines proj:wkt2 as WKT2.
        template.extra_fields["proj:wkt2"] = crs.to_wkt(version="WKT2_2019")
        with warnings.catch_warnings():
            # to_proj4() warns that proj4 is lossy; that's acceptable for a render hint.
            warnings.simplefilter("ignore")
            proj4 = crs.to_proj4()
        if proj4:
            template.extra_fields["open_climate_service:proj4"] = proj4.strip()
    except Exception:
        logger.warning("Could not derive CRS render hints for '%s'", store_crs, exc_info=True)

    # proj:bbox — the native-CRS extent. Prefer the GeoZarr ``spatial:bbox`` the store
    # root already carries; fall back to the x/y coordinate arrays (cell centres) for
    # stores that lack it.
    try:
        bbox = ds.attrs.get("spatial:bbox")
        if not (isinstance(bbox, (list, tuple)) and len(bbox) == 4):
            x_dim, y_dim = get_x_y_dims(ds)
            xs, ys = ds[x_dim].values, ds[y_dim].values
            bbox = [xs.min(), ys.min(), xs.max(), ys.max()]
        template.extra_fields["proj:bbox"] = [float(v) for v in bbox]
    except Exception:
        logger.warning("Could not derive proj:bbox for '%s'", store_crs, exc_info=True)


def _wgs84_extent_from_store(ds: xr.Dataset, store_crs: str, x_dim: str, y_dim: str) -> list[float] | None:
    """Return the store's spatial extent as WGS84 ``[west, south, east, north]``.

    STAC collections must report their spatial extent in WGS84. Deriving it live from the
    published store (the same coordinates ``cube:dimensions`` reads) keeps the two
    consistent and avoids the cached ``coverage`` record, which can go stale or hold a
    native-CRS (metre) bbox for projected stores.
    """
    try:
        xmin, xmax = float(ds[x_dim].min()), float(ds[x_dim].max())
        ymin, ymax = float(ds[y_dim].min()), float(ds[y_dim].max())
        if canonical_crs_code(store_crs) == "EPSG:4326":
            return [xmin, ymin, xmax, ymax]
        from pyproj import Transformer

        transformer = Transformer.from_crs(store_crs, "EPSG:4326", always_xy=True)
        west, south, east, north = transformer.transform_bounds(xmin, ymin, xmax, ymax)
        return [west, south, east, north]
    except Exception:
        logger.warning("Could not derive WGS84 extent from store for '%s'", store_crs, exc_info=True)
        return None


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
        # The store's native CRS (written by the ingest orchestrator) takes precedence
        # over the instance-wide CRS set in _build_collection_template. Plugins that
        # store data in WGS84 (e.g. CHIRPS3) should not inherit the instance UTM CRS.
        store_crs = ds.attrs.get("proj:code")
        if store_crs:
            store_crs = canonical_crs_code(store_crs)
            template.extra_fields["proj:code"] = store_crs
            _add_crs_render_hints(template=template, ds=ds, store_crs=store_crs)
        x_dimension, y_dimension = get_x_y_dims(ds)
        try:
            time_dimension: str | None = get_time_dim(ds)
        except ValueError:
            time_dimension = None  # static dataset with no time dimension
        if time_dimension is not None:
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
        else:
            # xarray_to_stac raises KeyError when temporal_dimension is None and the
            # dataset has no CF time axis (e.g. after reduce_dimension removes it).
            # Fall back to the template and build cube metadata manually so the
            # map viewer can identify the correct variable and dimensions.
            result = template
        # build_collection replaces links from the template after xstac runs, so
        # clear xstac/pystac-owned links before serialization to avoid root-link
        # resolution attempts during to_dict().
        result.clear_links()
        payload: dict[str, Any] = result.to_dict(include_self_link=False)
        if time_dimension is None:
            # xstac was skipped — inject minimal cube:dimensions and cube:variables
            # so the map viewer can resolve the variable name and spatial axes.
            payload.setdefault("cube:dimensions", _build_static_cube_dimensions(ds, x_dimension, y_dimension))
            payload.setdefault("cube:variables", _build_cube_variables(ds))
        # Declare ordinal (non-spatial, non-temporal) axes — e.g. a day-of-year
        # climatology — that xstac/the static path would otherwise drop, so the map
        # viewer can offer a slider over them.
        ordinal_dims = _build_ordinal_dimensions(ds, x_dimension, y_dimension, time_dimension)
        if ordinal_dims:
            payload.setdefault("cube:dimensions", {}).update(ordinal_dims)
        # WGS84 spatial extent from the live store — consistent with cube:dimensions and
        # immune to a stale/native-CRS cached coverage record (see _wgs84_extent_from_store).
        extent_bbox = _wgs84_extent_from_store(ds, store_crs or "EPSG:4326", x_dimension, y_dimension)
        if extent_bbox is not None:
            payload.setdefault("extent", {}).setdefault("spatial", {})["bbox"] = [extent_bbox]
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
    return _abs_url(request, f"/zarr/{dataset_id}")


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


def _build_static_cube_dimensions(ds: xr.Dataset, x_dim: str, y_dim: str) -> dict[str, Any]:
    """Build minimal cube:dimensions for a dataset with no time axis."""
    crs = ds.attrs.get("proj:code", "EPSG:4326")
    x_vals = ds[x_dim].values
    y_vals = ds[y_dim].values
    x_step = float(x_vals[1] - x_vals[0]) if len(x_vals) > 1 else None
    y_step = float(y_vals[1] - y_vals[0]) if len(y_vals) > 1 else None
    return {
        x_dim: {
            "type": "spatial",
            "axis": "x",
            "extent": [float(x_vals.min()), float(x_vals.max())],
            **({"step": x_step} if x_step is not None else {}),
            "reference_system": crs,
        },
        y_dim: {
            "type": "spatial",
            "axis": "y",
            "extent": [float(y_vals.min()), float(y_vals.max())],
            **({"step": y_step} if y_step is not None else {}),
            "reference_system": crs,
        },
    }


def _build_ordinal_dimensions(ds: xr.Dataset, x_dim: str, y_dim: str, time_dim: str | None) -> dict[str, Any]:
    """cube:dimensions entries for non-spatial, non-temporal axes (e.g. ``dayofyear``).

    The datacube/STAC machinery only declares spatial and temporal axes, so an ordinal
    coordinate like a day-of-year climatology axis would otherwise be dropped — and the
    map viewer could not slider it. Declared here with explicit ``values`` (and a ``step``
    for evenly spaced integer axes) so the viewer can step over them.
    """
    import numpy as np

    dims = getattr(ds, "dims", None)
    if not dims:
        return {}
    skip = {x_dim, y_dim} | ({time_dim} if time_dim else set())
    out: dict[str, Any] = {}
    for name in dims:
        if name in skip or name not in ds.coords:
            continue
        coord = ds[name]
        if np.issubdtype(coord.dtype, np.datetime64):
            continue  # a temporal axis — handled by xstac
        values = coord.values.tolist()
        entry: dict[str, Any] = {"type": "other", "values": values}
        if len(values) > 1 and all(isinstance(v, int) for v in values):
            # Only publish a step for a genuinely evenly spaced axis: every
            # consecutive delta must match. Deriving it from the first delta
            # alone would emit a wrong step for irregular integer coordinates.
            deltas = {b - a for a, b in zip(values, values[1:])}
            if len(deltas) == 1:
                (step,) = deltas
                if step:
                    entry["step"] = step
        out[str(name)] = entry
    return out


def _build_cube_variables(ds: xr.Dataset) -> dict[str, Any]:
    """Build cube:variables from an xr.Dataset's data variables."""
    result: dict[str, Any] = {}
    for name in ds.data_vars:
        var = ds[name]
        entry: dict[str, Any] = {
            "type": "data",
            "dimensions": [str(d) for d in var.dims],
            "unit": var.attrs.get("units"),
        }
        # Surface the CF semantics we stamp onto the store so catalog clients can
        # identify the quantity, not just its unit (#283).
        for cf_key in ("standard_name", "cell_methods"):
            value = var.attrs.get(cf_key)
            if isinstance(value, str):
                entry[cf_key] = value
        result[str(name)] = entry
    return result


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


def _override_temporal_extent_from_artifact(collection: dict[str, Any], artifact: ArtifactRecord) -> None:
    temporal = artifact.coverage.temporal

    def _fmt(v: str | None) -> str | None:
        return parse_period_string_to_datetime(v).isoformat().replace("+00:00", "Z") if v else None

    start = _fmt(temporal.start)
    end = _fmt(temporal.end)
    collection["extent"]["temporal"]["interval"] = [[start, end]]
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
        # Surface the CF semantics we stamp onto the store as top-level cube:variable
        # fields (and keep them in attrs) so catalog clients see the quantity (#283).
        for cf_key in ("standard_name", "cell_methods"):
            value = attrs.get(cf_key)
            if isinstance(value, str):
                kept_attrs[cf_key] = value
                variable[cf_key] = value
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
        return {"consolidated": True}
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
