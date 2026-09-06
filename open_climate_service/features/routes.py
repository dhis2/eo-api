"""FastAPI routes for the named feature collections an instance ships (CLIM-836)."""

from typing import Any

from fastapi import APIRouter, HTTPException

from open_climate_service.shared import features

router = APIRouter()


@router.get("")
def list_feature_collections() -> list[dict[str, Any]]:
    """Return every feature collection available for `load_vector_cube`.

    Read-only discovery, so a client can find the boundary sets an instance ships instead of
    guessing their ids or posting its own GeoJSON.
    """
    return features.list_collections()


@router.get("/{collection_id}")
def get_feature_collection(collection_id: str) -> dict[str, Any]:
    """Return one feature collection's metadata: feature count, columns, CRS and bounds."""
    info = features.describe(collection_id)
    if info is None:
        raise HTTPException(status_code=404, detail=f"Feature collection '{collection_id}' not found")
    return info
