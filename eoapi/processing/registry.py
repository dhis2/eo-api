"""Dataset registry loader derived from dataset catalog + provider mapping."""

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator

from eoapi.datasets import load_datasets

CONFIG_DIR = Path(__file__).resolve().parent / "config"
DEFAULT_PROVIDER_REGISTRY_PATH = CONFIG_DIR / "providers.yaml"


class DatasetProviderConfig(BaseModel):
    """Provider configuration for one dataset."""

    name: str = Field(min_length=1)
    options: dict[str, Any] = Field(default_factory=dict)


class DatasetRegistryEntry(BaseModel):
    """Process-facing dataset entry with provider binding."""

    id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    description: str = Field(min_length=1)
    parameters: list[str] = Field(default_factory=list)
    spatial_bbox: tuple[float, float, float, float] = (-180.0, -90.0, 180.0, 90.0)
    temporal_start: str | None = None
    temporal_end: str | None = None
    provider: DatasetProviderConfig

    @field_validator("parameters")
    @classmethod
    def _dedupe_parameters(cls, values: list[str]) -> list[str]:
        ordered = list(dict.fromkeys(value.strip() for value in values if value.strip()))
        if not ordered:
            raise ValueError("At least one parameter must be defined")
        return ordered

    @field_validator("spatial_bbox")
    @classmethod
    def _validate_bbox(cls, value: tuple[float, float, float, float]) -> tuple[float, float, float, float]:
        minx, miny, maxx, maxy = value
        if minx >= maxx or miny >= maxy:
            raise ValueError("Invalid spatial_bbox ordering")
        return value


class ProviderRegistryDocument(BaseModel):
    """Top-level provider mapping document."""

    version: str = "1.0"
    providers: dict[str, DatasetProviderConfig] = Field(default_factory=dict)


def _load_yaml_document(path: Path) -> dict[str, Any]:
    """Load and type-check a provider YAML mapping document."""

    with path.open("r", encoding="utf-8") as file_handle:
        payload = yaml.safe_load(file_handle) or {}
    if not isinstance(payload, dict):
        raise RuntimeError(f"Dataset registry '{path}' must be a YAML mapping")
    return payload


def clear_dataset_registry_cache() -> None:
    """Clear in-memory cache for registry lookups."""

    load_dataset_registry.cache_clear()


@lru_cache(maxsize=4)
def load_dataset_registry(path: str | Path | None = None) -> dict[str, DatasetRegistryEntry]:
    """Build process dataset registry from catalog metadata + provider config."""

    resolved_path = Path(path) if path is not None else DEFAULT_PROVIDER_REGISTRY_PATH
    if not resolved_path.exists():
        raise RuntimeError(f"Dataset registry not found: {resolved_path}")

    payload = _load_yaml_document(resolved_path)
    provider_doc = ProviderRegistryDocument.model_validate(payload)
    catalog = load_datasets()

    registry: dict[str, DatasetRegistryEntry] = {}
    for dataset_id, provider in provider_doc.providers.items():
        dataset = catalog.get(dataset_id)
        if dataset is None:
            raise RuntimeError(f"Provider mapping references unknown dataset id: {dataset_id}")

        temporal_start, temporal_end = dataset.temporal_interval
        registry[dataset_id] = DatasetRegistryEntry(
            id=dataset.id,
            title=dataset.title,
            description=dataset.description,
            parameters=list(dataset.parameters.keys()),
            spatial_bbox=dataset.spatial_bbox,
            temporal_start=temporal_start,
            temporal_end=temporal_end,
            provider=provider,
        )

    return registry
