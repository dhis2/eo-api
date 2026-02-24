"""Provider factory and public provider interfaces."""

from eoapi.processing.providers.base import RasterFetchRequest, RasterFetchResult, RasterProvider
from eoapi.processing.providers.chirps3 import Chirps3Provider
from eoapi.processing.registry import DatasetRegistryEntry


def build_provider(dataset: DatasetRegistryEntry) -> RasterProvider:
    """Instantiate provider implementation for a registry dataset entry."""

    provider_name = dataset.provider.name.strip().lower()

    if provider_name == Chirps3Provider.provider_id:
        return Chirps3Provider(**dataset.provider.options)

    raise RuntimeError(f"Unsupported provider configured for dataset '{dataset.id}': {dataset.provider.name}")


__all__ = [
    "Chirps3Provider",
    "RasterFetchRequest",
    "RasterFetchResult",
    "RasterProvider",
    "build_provider",
]
