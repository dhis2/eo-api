"""Publication serving capability policy."""

from __future__ import annotations

from dataclasses import dataclass

from .schemas import PublishedResourceExposure, PublishedResourceKind


@dataclass(frozen=True)
class PublicationServingCapability:
    """Serving support for one publication contract."""

    supported: bool
    asset_format: str
    served_by: tuple[str, ...]
    ogc_collection: bool = False
    error: str | None = None


def default_asset_format_for_kind(kind: PublishedResourceKind) -> str:
    """Default asset format for a publication kind."""
    defaults = {
        PublishedResourceKind.FEATURE_COLLECTION: "geojson",
        PublishedResourceKind.COVERAGE: "zarr",
        PublishedResourceKind.TILESET: "tiles",
        PublishedResourceKind.COLLECTION: "json",
    }
    return defaults.get(kind, "file")


def evaluate_publication_serving(
    *,
    kind: PublishedResourceKind,
    exposure: PublishedResourceExposure,
    asset_format: str | None,
) -> PublicationServingCapability:
    """Evaluate whether the server can expose a publication contract."""
    normalized_format = (asset_format or default_asset_format_for_kind(kind)).strip().lower()

    if exposure == PublishedResourceExposure.REGISTRY_ONLY:
        return PublicationServingCapability(
            supported=True,
            asset_format=normalized_format,
            served_by=("registry",),
            ogc_collection=False,
        )

    supported_matrix: dict[tuple[PublishedResourceKind, str], PublicationServingCapability] = {
        (
            PublishedResourceKind.FEATURE_COLLECTION,
            "geojson",
        ): PublicationServingCapability(
            supported=True,
            asset_format="geojson",
            served_by=("pygeoapi", "analytics"),
            ogc_collection=True,
        ),
        (
            PublishedResourceKind.COVERAGE,
            "zarr",
        ): PublicationServingCapability(
            supported=True,
            asset_format="zarr",
            served_by=("pygeoapi", "raster"),
            ogc_collection=True,
        ),
    }
    capability = supported_matrix.get((kind, normalized_format))
    if capability is not None:
        return capability

    return PublicationServingCapability(
        supported=False,
        asset_format=normalized_format,
        served_by=(),
        ogc_collection=False,
        error=(
            "Unsupported publication serving contract: "
            f"kind='{kind}', asset_format='{normalized_format}', exposure='{exposure}'"
        ),
    )
