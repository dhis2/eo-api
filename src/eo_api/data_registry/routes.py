"""FastAPI router exposing dataset endpoints."""

from typing import Any

import xarray as xr
from fastapi import APIRouter, BackgroundTasks, HTTPException, Response
from fastapi.responses import FileResponse
from starlette.background import BackgroundTask

from .services import datasets

router = APIRouter()

@router.get("/")
def list_datasets() -> list[dict[str, Any]]:
    """Return list of available datasets from registry."""
    return datasets.list_datasets()


def _get_dataset_or_404(dataset_id: str) -> dict[str, Any]:
    """Look up a dataset by ID or raise 404."""
    dataset = datasets.get_dataset(dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    return dataset


@router.get("/{dataset_id}", response_model=dict)
def get_dataset(dataset_id: str) -> dict[str, Any]:
    """Get a single dataset by ID."""
    # Note: have to import inside function to avoid circular import
    from ..data_accessor.services.accessor import get_data_coverage
    dataset = _get_dataset_or_404(dataset_id)
    coverage = get_data_coverage(dataset)
    dataset.update(coverage)
    return dataset