
from fastapi import APIRouter, HTTPException

from . import registry
from . import cache

router = APIRouter(
    prefix="/datasets",
    tags=["datasets"]
)

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
    dataset = registry.get_dataset(dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    return dataset

@router.get("/{dataset_id}/cache", response_model=dict)
def cache_dataset(dataset_id: str, start: str, end: str, variables: str):
    """
    Download and cache dataset.
    """
    cache.cache_dataset(dataset_id, start=start, end=end, variables=variables)
    return {'status': 'Dataset caching request submitted for processing'}
