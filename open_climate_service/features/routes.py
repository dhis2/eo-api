"""FastAPI routes for the named feature collections an instance ships (CLIM-836, CLIM-926)."""

from typing import Any

from fastapi import APIRouter, HTTPException

from open_climate_service.features.config import get_feature_templates
from open_climate_service.shared import features

router = APIRouter()


def _described(info: dict[str, Any], templates: Any) -> dict[str, Any]:
    """Merge a collection's file-derived facts with the metadata its template declares.

    Two sources because they answer different questions. The file knows its CRS, bounds, feature
    count and columns; only a human knows its licence, attribution and what it is for. A collection
    with no template is still listed — it is a real file — just with nothing authored about it.
    """
    template = templates.find(info["id"])
    return info if template is None else {**info, **template.metadata()}


@router.get("")
def list_feature_collections() -> list[dict[str, Any]]:
    """Return every feature collection this instance holds.

    Read-only discovery, so a client can find the boundary sets an instance ships instead of
    guessing their ids or posting its own GeoJSON.
    """
    templates = get_feature_templates()
    return [_described(info, templates) for info in features.list_collections()]


@router.get("/{collection_id}")
def get_feature_collection(collection_id: str) -> dict[str, Any]:
    """Return one collection's metadata: licence and provenance, plus count, columns, CRS and bounds."""
    info = features.describe(collection_id)
    if info is None:
        raise HTTPException(status_code=404, detail=f"Feature collection '{collection_id}' not found")
    return _described(info, get_feature_templates())
