"""FastAPI router exposing dataset retrieval endpoints."""

from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from starlette.background import BackgroundTask

from ..data_registry.routes import _get_dataset_or_404
from ..shared.api_errors import api_error
from .services.accessor import (
    cleanup_file,
    get_coverage_summary,
    get_data,
    get_point_values,
    get_preview_summary,
    xarray_to_temporary_netcdf,
)

router = APIRouter()


@router.get("/{dataset_id}")
def get_file(
    dataset_id: str,
    start: str,
    end: str,
    xmin: float | None = None,
    ymin: float | None = None,
    xmax: float | None = None,
    ymax: float | None = None,
    format: str = "netcdf",
) -> FileResponse:
    """Get a dataset filtered to a timeperiod and bbox as a downloadable raster file."""
    dataset = _get_dataset_or_404(dataset_id)

    # get filtered data
    bbox: list[float] | None
    if xmin is not None and ymin is not None and xmax is not None and ymax is not None:
        bbox = [xmin, ymin, xmax, ymax]
    else:
        bbox = None
    ds = get_data(dataset, start, end, bbox)

    # save to temporary file
    if format.lower() == "netcdf":
        # convert to netcdf
        file_path = xarray_to_temporary_netcdf(ds)

    else:
        raise ValueError(f"Unsupported output format: {format}")

    # return as file
    return FileResponse(
        file_path,
        media_type="application/x-netcdf",
        filename="eo-api-raster-download.nc",
        background=BackgroundTask(cleanup_file, file_path),
    )


@router.get("/{dataset_id}/point")
def get_point_value(
    dataset_id: str,
    lon: float,
    lat: float,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, Any]:
    """Return one dataset's value series at a requested lon/lat point."""
    dataset = _get_dataset_or_404(dataset_id)
    try:
        return get_point_values(dataset, lon=lon, lat=lat, start=start, end=end)
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail=api_error(
                error="point_query_invalid",
                error_code="POINT_QUERY_INVALID",
                message=str(exc),
                resource_id=dataset_id,
            ),
        ) from exc


@router.get("/{dataset_id}/preview")
def get_dataset_preview(
    dataset_id: str,
    start: str | None = None,
    end: str | None = None,
    xmin: float | None = None,
    ymin: float | None = None,
    xmax: float | None = None,
    ymax: float | None = None,
    max_cells: int = 25,
) -> dict[str, Any]:
    """Return summary stats and a small raster sample for preview workflows."""
    dataset = _get_dataset_or_404(dataset_id)
    bbox: list[float] | None
    if any(value is not None for value in (xmin, ymin, xmax, ymax)):
        if not all(value is not None for value in (xmin, ymin, xmax, ymax)):
            raise HTTPException(
                status_code=422,
                detail=api_error(
                    error="preview_invalid",
                    error_code="PREVIEW_INVALID",
                    message="Provide all of xmin, ymin, xmax, ymax together",
                    resource_id=dataset_id,
                ),
            )
        assert xmin is not None and ymin is not None and xmax is not None and ymax is not None
        bbox = [float(xmin), float(ymin), float(xmax), float(ymax)]
    else:
        bbox = None

    try:
        return get_preview_summary(dataset, start=start, end=end, bbox=bbox, max_cells=max_cells)
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail=api_error(
                error="preview_invalid",
                error_code="PREVIEW_INVALID",
                message=str(exc),
                resource_id=dataset_id,
            ),
        ) from exc


@router.get("/{dataset_id}/coverage")
def get_dataset_coverage_summary(
    dataset_id: str,
    start: str | None = None,
    end: str | None = None,
    xmin: float | None = None,
    ymin: float | None = None,
    xmax: float | None = None,
    ymax: float | None = None,
    max_cells: int = 25,
) -> dict[str, Any]:
    """Return a lightweight coverage-style response for a raster subset."""
    dataset = _get_dataset_or_404(dataset_id)
    bbox: list[float] | None
    if any(value is not None for value in (xmin, ymin, xmax, ymax)):
        if not all(value is not None for value in (xmin, ymin, xmax, ymax)):
            raise HTTPException(
                status_code=422,
                detail=api_error(
                    error="coverage_invalid",
                    error_code="COVERAGE_INVALID",
                    message="Provide all of xmin, ymin, xmax, ymax together",
                    resource_id=dataset_id,
                ),
            )
        assert xmin is not None and ymin is not None and xmax is not None and ymax is not None
        bbox = [float(xmin), float(ymin), float(xmax), float(ymax)]
    else:
        bbox = None

    try:
        return get_coverage_summary(dataset, start=start, end=end, bbox=bbox, max_cells=max_cells)
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail=api_error(
                error="coverage_invalid",
                error_code="COVERAGE_INVALID",
                message=str(exc),
                resource_id=dataset_id,
            ),
        ) from exc
