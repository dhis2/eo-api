"""Routes for backend-owned publication state."""

from fastapi import APIRouter, HTTPException

from ..shared.api_errors import api_error
from .schemas import PublishedResource, PublishedResourceClass, PublishedResourceExposure, PublishedResourceListResponse
from .services import ensure_source_dataset_publications, get_published_resource, list_published_resources

router = APIRouter()


@router.get("", response_model=PublishedResourceListResponse)
def list_publications(
    resource_class: PublishedResourceClass | None = None,
    dataset_id: str | None = None,
    workflow_id: str | None = None,
    exposure: PublishedResourceExposure | None = None,
) -> PublishedResourceListResponse:
    """List backend-owned published resources."""
    ensure_source_dataset_publications()
    return PublishedResourceListResponse(
        resources=list_published_resources(
            resource_class=resource_class,
            dataset_id=dataset_id,
            workflow_id=workflow_id,
            exposure=exposure,
        )
    )


@router.get("/{resource_id}", response_model=PublishedResource)
def get_publication(resource_id: str) -> PublishedResource:
    """Get one published resource."""
    ensure_source_dataset_publications()
    resource = get_published_resource(resource_id)
    if resource is None:
        raise HTTPException(
            status_code=404,
            detail=api_error(
                error="published_resource_not_found",
                error_code="PUBLISHED_RESOURCE_NOT_FOUND",
                message=f"Unknown resource_id '{resource_id}'",
                resource_id=resource_id,
            ),
        )
    return resource
