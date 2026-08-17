"""FastAPI routes for the named vector collections an instance ships (CLIM-836)."""

from typing import Any

from fastapi import APIRouter, HTTPException

from open_climate_service.shared import vectors

router = APIRouter()


@router.get("")
def list_vector_collections() -> list[dict[str, Any]]:
    """Return every vector collection available for `load_vector_cube`.

    Read-only discovery, so a client can find the boundary sets an instance ships instead of
    guessing their ids or posting its own GeoJSON.
    """
    return vectors.list_collections()


@router.get("/{collection_id}")
def get_vector_collection(collection_id: str) -> dict[str, Any]:
    """Return one vector collection's metadata: feature count, columns, CRS and bounds."""
    info = vectors.describe(collection_id)
    if info is None:
        raise HTTPException(status_code=404, detail=f"Vector collection '{collection_id}' not found")
    return info
