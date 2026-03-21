"""Raster publication routes and Zarr-backed TiTiler integration."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import attr
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from rio_tiler.colormap import cmap
from rio_tiler.io.xarray import XarrayReader
from titiler.core.dependencies import ImageRenderingParams
from titiler.core.routing import EndpointScope
from titiler.xarray.dependencies import XarrayParams
from titiler.xarray.extensions import VariablesExtension
from titiler.xarray.factory import TilerFactory
from titiler.xarray.io import Reader, get_variable

from ..data_manager.services.downloader import get_zarr_path
from ..data_registry.services.datasets import get_dataset
from ..publications.schemas import PublishedResource, PublishedResourceKind
from ..publications.services import (
    collection_id_for_resource,
    ensure_source_dataset_publications,
    get_published_resource,
    get_published_resource_by_collection_id,
)
from ..shared.api_errors import api_error

router = APIRouter()

SUPPORTED_AGGREGATIONS = {"sum", "mean", "max", "min"}

RASTER_STYLE_PROFILES: dict[str, dict[str, Any]] = {
    "chirps3_precipitation_daily": {
        "colormap_name": "ylorrd",
        "rescale_by_mode": {
            "datetime": (0.0, 50.0),
            "sum": (0.0, 300.0),
            "mean": (0.0, 50.0),
            "max": (0.0, 100.0),
            "min": (0.0, 20.0),
        },
        "label": "Precipitation intensity",
    },
    "era5land_precipitation_hourly": {
        "colormap_name": "ylorrd",
        "rescale_by_mode": {
            "datetime": (0.0, 15.0),
            "sum": (0.0, 150.0),
            "mean": (0.0, 15.0),
            "max": (0.0, 40.0),
            "min": (0.0, 10.0),
        },
        "label": "Precipitation intensity",
    },
    "era5land_temperature_hourly": {
        "colormap_name": "coolwarm",
        "rescale_by_mode": {
            "datetime": (260.0, 320.0),
            "sum": (260.0, 320.0),
            "mean": (260.0, 320.0),
            "max": (260.0, 330.0),
            "min": (240.0, 310.0),
        },
        "label": "Temperature",
    },
    "worldpop_population_yearly": {
        "colormap_name": "viridis",
        "rescale_by_mode": {
            "datetime": (0.0, 500.0),
            "sum": (0.0, 1000.0),
            "mean": (0.0, 500.0),
            "max": (0.0, 1000.0),
            "min": (0.0, 100.0),
        },
        "label": "Population density",
    },
}


@router.get("/{resource_id}/capabilities")
def get_raster_capabilities(resource_id: str) -> dict[str, Any]:
    """Describe whether a published resource is TiTiler-eligible."""
    resource = _resolve_published_resource(resource_id)
    capabilities = _titiler_capabilities(resource)
    return {
        "resource_id": resource.resource_id,
        "collection_id": collection_id_for_resource(resource),
        "kind": str(resource.kind),
        "asset_format": resource.asset_format,
        "titiler": capabilities,
    }


def _resource_path_dependency(resource_id: str) -> str:
    """Resolve one published resource to a TiTiler-readable Zarr dataset path."""
    resource = _resolve_published_resource(resource_id)
    capabilities = _titiler_capabilities(resource)
    if not capabilities["eligible"]:
        raise HTTPException(
            status_code=422,
            detail=api_error(
                error="raster_publication_unsupported",
                error_code="RASTER_PUBLICATION_UNSUPPORTED",
                message=str(capabilities["reason"]),
                resource_id=resource.resource_id,
            ),
        )

    path = capabilities.get("path")
    if not isinstance(path, str) or path == "":
        raise HTTPException(
            status_code=500,
            detail=api_error(
                error="raster_publication_invalid",
                error_code="RASTER_PUBLICATION_INVALID",
                message=f"Resource '{resource.resource_id}' is missing a TiTiler dataset path",
                resource_id=resource.resource_id,
            ),
        )
    return path


@dataclass
class RasterReaderParams(XarrayParams):
    """Reader params with a user-facing temporal selector."""

    datetime: str | None = Query(
        default=None,
        description="Time slice to render for temporal datasets, for example `2024-01-01`.",
    )
    aggregation: str | None = Query(
        default=None,
        description="Temporal aggregation to apply before rendering, for example `sum` or `mean`.",
    )
    start: str | None = Query(
        default=None,
        description="Start date for temporal aggregation, for example `2024-01-01`.",
    )
    end: str | None = Query(
        default=None,
        description="End date for temporal aggregation, for example `2024-01-31`.",
    )

    def __post_init__(self) -> None:
        selector_values = list(self.sel or [])
        if self.datetime is not None:
            selector_values.append(f"time={self.datetime}")
        self.sel = selector_values or None

    def as_dict(self, exclude_none: bool = True) -> dict[Any, Any]:
        values = super().as_dict(exclude_none=exclude_none)
        values.pop("datetime", None)
        return values


@dataclass
class RasterImageRenderingParams(ImageRenderingParams):
    """Image rendering params with dataset-aware default rescaling."""

    resource_id: str = Query()
    aggregation: str | None = Query(default=None)

    def __post_init__(self) -> None:
        raw_rescale = cast(Any, self.__dict__.get("rescale"))
        if raw_rescale is None:
            profile = _style_profile_for_resource(self.resource_id)
            default_range = _default_rescale_for_profile(profile, aggregation=self.aggregation)
            if default_range is not None:
                self.__dict__["rescale"] = [f"{default_range[0]},{default_range[1]}"]
        super().__post_init__()


@attr.s
class AggregatingReader(Reader):
    """Xarray reader that can collapse a temporal dimension before rendering."""

    aggregation: str | None = attr.ib(default=None)
    start: str | None = attr.ib(default=None)
    end: str | None = attr.ib(default=None)

    def __attrs_post_init__(self) -> None:
        opener_options = {
            "group": self.group,
            "decode_times": self.decode_times,
            **self.opener_options,
        }

        self.ds = self.opener(self.src_path, **opener_options)
        self.input = get_variable(
            self.ds,
            self.variable,
            sel=self.sel,
        )

        if self.aggregation is not None:
            self.input = _aggregate_temporal_dataarray(
                self.input,
                aggregation=self.aggregation,
                start=self.start,
                end=self.end,
            )

        XarrayReader.__attrs_post_init__(self)


def _require_temporal_selector_for_rendering(request: Request, resource_id: str) -> None:
    """Require a time selector before rendering temporal datasets as images/tiles."""
    resource = _resolve_published_resource(resource_id)
    dataset = _resolve_resource_dataset(resource)
    if dataset is None or not _dataset_requires_temporal_selector(dataset):
        return

    datetime_value = request.query_params.get("datetime")
    aggregation = request.query_params.get("aggregation")
    start = request.query_params.get("start")
    end = request.query_params.get("end")

    time_selectors = [selector for selector in request.query_params.getlist("sel") if selector.startswith("time=")]

    if datetime_value and aggregation:
        raise HTTPException(
            status_code=422,
            detail=api_error(
                error="raster_temporal_query_invalid",
                error_code="RASTER_TEMPORAL_QUERY_INVALID",
                message="Use either 'datetime' or 'aggregation' with a date range, not both.",
                resource_id=resource.resource_id,
            ),
        )

    if aggregation is not None:
        if aggregation not in SUPPORTED_AGGREGATIONS:
            raise HTTPException(
                status_code=422,
                detail=api_error(
                    error="raster_temporal_query_invalid",
                    error_code="RASTER_TEMPORAL_QUERY_INVALID",
                    message=(
                        f"Unsupported aggregation '{aggregation}'. "
                        f"Supported values: {', '.join(sorted(SUPPORTED_AGGREGATIONS))}."
                    ),
                    resource_id=resource.resource_id,
                ),
            )
        if not start or not end:
            raise HTTPException(
                status_code=422,
                detail=api_error(
                    error="raster_temporal_query_invalid",
                    error_code="RASTER_TEMPORAL_QUERY_INVALID",
                    message="Temporal aggregation requires both 'start' and 'end' query parameters.",
                    resource_id=resource.resource_id,
                ),
            )
        if time_selectors:
            raise HTTPException(
                status_code=422,
                detail=api_error(
                    error="raster_temporal_query_invalid",
                    error_code="RASTER_TEMPORAL_QUERY_INVALID",
                    message="Do not combine 'aggregation' with a direct 'sel=time=...' selector.",
                    resource_id=resource.resource_id,
                ),
            )
        return

    if start or end:
        raise HTTPException(
            status_code=422,
            detail=api_error(
                error="raster_temporal_query_invalid",
                error_code="RASTER_TEMPORAL_QUERY_INVALID",
                message="Use 'start' and 'end' only together with an 'aggregation' query parameter.",
                resource_id=resource.resource_id,
            ),
        )

    if datetime_value or time_selectors:
        return

    raise HTTPException(
        status_code=422,
        detail=api_error(
            error="raster_datetime_required",
            error_code="RASTER_DATETIME_REQUIRED",
            message=(
                f"Temporal raster rendering for dataset '{dataset['id']}' requires a time selector. "
                "Use '?datetime=YYYY-MM-DD' or '?aggregation=sum&start=YYYY-MM-DD&end=YYYY-MM-DD'."
            ),
            resource_id=resource.resource_id,
        ),
    )


def _colormap_dependency(
    resource_id: str,
    colormap_name: str | None = Query(default=None, description="Named colormap override."),
    colormap: str | None = Query(default=None, description="JSON encoded custom colormap override."),
    aggregation: str | None = Query(default=None),
) -> Any:
    if colormap_name:
        return cmap.get(colormap_name)

    if colormap:
        # Delegate explicit custom colormap handling back to TiTiler callers.
        from titiler.core.dependencies import create_colormap_dependency

        return create_colormap_dependency(cmap)(colormap_name=None, colormap=colormap)

    profile = _style_profile_for_resource(resource_id)
    if profile is None:
        return None

    base_colormap = cmap.get(str(profile["colormap_name"]))
    if not isinstance(base_colormap, dict):
        return base_colormap
    default_map = cast(dict[Any, Any], base_colormap.copy())
    if str(profile["colormap_name"]) in {"ylorrd", "blues", "viridis"}:
        default_map[0] = (0, 0, 0, 0)
    return default_map


_factory = TilerFactory(
    reader=AggregatingReader,
    router_prefix="",
    path_dependency=_resource_path_dependency,
    route_dependencies=[
        (
            [
                EndpointScope(path="/preview", method="GET"),
                EndpointScope(path="/preview.{format}", method="GET"),
                EndpointScope(path="/preview/{width}x{height}.{format}", method="GET"),
                EndpointScope(path="/{tileMatrixSetId}/tilejson.json", method="GET"),
                EndpointScope(path="/tiles/{tileMatrixSetId}/{z}/{x}/{y}", method="GET"),
                EndpointScope(path="/tiles/{tileMatrixSetId}/{z}/{x}/{y}.{format}", method="GET"),
                EndpointScope(path="/tiles/{tileMatrixSetId}/{z}/{x}/{y}@{scale}x", method="GET"),
                EndpointScope(path="/tiles/{tileMatrixSetId}/{z}/{x}/{y}@{scale}x.{format}", method="GET"),
            ],
            [Depends(_require_temporal_selector_for_rendering)],
        )
    ],
    extensions=[VariablesExtension()],
    colormap_dependency=_colormap_dependency,
    render_dependency=RasterImageRenderingParams,
    reader_dependency=RasterReaderParams,
    add_viewer=False,
    add_ogc_maps=False,
    add_preview=True,
    add_part=False,
)
router.include_router(_factory.router, prefix="/{resource_id}")


def _resolve_published_resource(resource_id: str) -> PublishedResource:
    ensure_source_dataset_publications()
    resource = get_published_resource(resource_id)
    if resource is None:
        resource = get_published_resource_by_collection_id(resource_id)
    if resource is None:
        raise HTTPException(
            status_code=404,
            detail=api_error(
                error="published_resource_not_found",
                error_code="PUBLISHED_RESOURCE_NOT_FOUND",
                message=f"Unknown published resource or collection '{resource_id}'",
                resource_id=resource_id,
            ),
        )
    return resource


def _titiler_capabilities(resource: PublishedResource) -> dict[str, Any]:
    if resource.kind not in {PublishedResourceKind.COVERAGE, PublishedResourceKind.TILESET}:
        return {
            "eligible": False,
            "reader": None,
            "reason": f"Resource kind '{resource.kind}' is not raster/tile publishable",
        }

    dataset = _resolve_resource_dataset(resource)
    if dataset is None:
        return {
            "eligible": False,
            "reader": None,
            "reason": (
                "No dataset registry record is linked to this resource, so a Zarr-backed raster "
                "publication cannot be resolved."
            ),
        }

    candidate_path = _resolve_zarr_path(resource, dataset)
    if candidate_path is None:
        return {
            "eligible": False,
            "reader": "xarray",
            "reason": (
                f"No Zarr archive is available yet for dataset '{dataset['id']}'. "
                f"Build it first via '/manage/{dataset['id']}/build_zarr'."
            ),
        }

    base = f"/raster/{collection_id_for_resource(resource)}"
    return {
        "eligible": True,
        "reader": "xarray",
        "reason": None,
        "path": str(candidate_path),
        "dataset_id": dataset["id"],
        "variable_hint": dataset.get("variable"),
        "render_time_selector_required": _dataset_requires_temporal_selector(dataset),
        "supported_render_aggregations": sorted(SUPPORTED_AGGREGATIONS),
        "style_defaults": _style_profile_for_dataset(dataset),
        "endpoints": {
            "variables": f"{base}/variables",
            "info": f"{base}/info",
            "tilejson": f"{base}/WebMercatorQuad/tilejson.json",
            "tiles": f"{base}/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}.png",
            "preview": f"{base}/preview.png",
            "point": f"{base}/point/{{lon}},{{lat}}",
            "statistics": f"{base}/statistics",
        },
    }


def _resolve_resource_dataset(resource: PublishedResource) -> dict[str, Any] | None:
    if resource.dataset_id is None:
        return None
    return get_dataset(resource.dataset_id)


def _dataset_requires_temporal_selector(dataset: dict[str, Any]) -> bool:
    return bool(dataset.get("period_type"))


def _style_profile_for_resource(resource_id: str) -> dict[str, Any] | None:
    resource = _resolve_published_resource(resource_id)
    dataset = _resolve_resource_dataset(resource)
    if dataset is None:
        return None
    return _style_profile_for_dataset(dataset)


def _style_profile_for_dataset(dataset: dict[str, Any]) -> dict[str, Any] | None:
    profile = RASTER_STYLE_PROFILES.get(str(dataset["id"]))
    if profile is not None:
        return profile

    units = str(dataset.get("units") or "").lower()
    variable = str(dataset.get("variable") or "").lower()
    if "mm" in units or "precip" in variable:
        return {
            "colormap_name": "ylorrd",
            "rescale_by_mode": {
                "datetime": (0.0, 50.0),
                "sum": (0.0, 300.0),
                "mean": (0.0, 50.0),
                "max": (0.0, 100.0),
                "min": (0.0, 20.0),
            },
            "label": "Precipitation intensity",
        }

    return None


def _default_rescale_for_profile(
    profile: dict[str, Any] | None,
    *,
    aggregation: str | None,
) -> tuple[float, float] | None:
    if profile is None:
        return None
    rescale_by_mode = profile.get("rescale_by_mode", {})
    mode = aggregation or "datetime"
    range_value = rescale_by_mode.get(mode) or rescale_by_mode.get("datetime")
    if range_value is None:
        return None
    return cast(tuple[float, float], tuple(range_value))


def _aggregate_temporal_dataarray(
    data_array: Any,
    *,
    aggregation: str,
    start: str | None,
    end: str | None,
) -> Any:
    if "time" not in data_array.dims:
        raise ValueError("Temporal aggregation requires a 'time' dimension")

    time_window = data_array.sel(time=slice(start, end))
    if time_window.sizes.get("time", 0) == 0:
        raise ValueError("Temporal aggregation produced no time slices for the requested date range")

    aggregate_fn = getattr(time_window, aggregation, None)
    if aggregate_fn is None:
        raise ValueError(f"Unsupported temporal aggregation '{aggregation}'")
    return aggregate_fn(dim="time", skipna=True)


def _resolve_zarr_path(resource: PublishedResource, dataset: dict[str, Any]) -> Path | None:
    if resource.path:
        resource_path = Path(resource.path)
        if resource_path.exists():
            return resource_path

    native_output = resource.metadata.get("native_output_file")
    if isinstance(native_output, str):
        native_path = Path(native_output)
        if native_path.exists():
            return native_path

    return get_zarr_path(dataset)
