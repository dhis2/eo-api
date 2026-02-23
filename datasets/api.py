
from fastapi import APIRouter, HTTPException

from . import registry
from . import cache

router = APIRouter()

@router.get("/")
def list_datasets():
    """
    Returned list of available datasets from registry.
    """
    datasets = registry.list_datasets()
    return datasets

@router.get("/{dataset_id}", response_model=dict)
def get_dataset(dataset_id: str):
    """
    Get a single dataset by ID.
    """
    dataset = registry.get_dataset_with_cache_info(dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    return dataset

@router.get("/{dataset_id}/build_cache", response_model=dict)
def build_dataset_cache(dataset_id: str, start: str, end: str, overwrite: bool = False):
    """
    Download and cache dataset.
    """
    cache.build_dataset_cache(dataset_id, start=start, end=end, overwrite=overwrite)
    return {'status': 'Dataset caching request submitted for processing'}
