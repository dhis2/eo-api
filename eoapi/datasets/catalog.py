from datetime import datetime
from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel, Field, field_validator
import yaml

DATASETS_DIR = Path(__file__).resolve().parent


class DatasetDefinition(BaseModel):
    id: str
    title: str
    description: str
    keywords: list[str] = Field(default_factory=list)
    spatial_bbox: tuple[float, float, float, float]
    temporal_interval: tuple[str, str | None]
    parameters: dict[str, dict] = Field(default_factory=dict)

    @field_validator("temporal_interval", mode="before")
    @classmethod
    def normalize_temporal_interval(cls, value):
        if not isinstance(value, (list, tuple)) or len(value) != 2:
            return value

        normalized: list[str | None] = []
        for item in value:
            if isinstance(item, datetime):
                iso_value = item.isoformat()
                if iso_value.endswith("+00:00"):
                    iso_value = iso_value.replace("+00:00", "Z")
                normalized.append(iso_value)
            else:
                normalized.append(item)

        return normalized


@lru_cache(maxsize=1)
def load_datasets() -> dict[str, DatasetDefinition]:
    datasets: dict[str, DatasetDefinition] = {}

    dataset_dirs = sorted(path for path in DATASETS_DIR.iterdir() if path.is_dir())
    for dataset_dir in dataset_dirs:
        yaml_candidates = [dataset_dir / f"{dataset_dir.name}.yml", dataset_dir / f"{dataset_dir.name}.yaml"]
        dataset_file = next((candidate for candidate in yaml_candidates if candidate.exists()), None)
        if dataset_file is None:
            fallback_files = sorted(dataset_dir.glob("*.yml")) + sorted(dataset_dir.glob("*.yaml"))
            if not fallback_files:
                continue
            dataset_file = fallback_files[0]

        with dataset_file.open("r", encoding="utf-8") as file_handle:
            payload = yaml.safe_load(file_handle) or {}

        dataset = DatasetDefinition.model_validate(payload)
        if dataset.id != dataset_dir.name:
            raise RuntimeError(
                f"Dataset id '{dataset.id}' must match dataset folder name '{dataset_dir.name}'"
            )
        if dataset.id in datasets:
            raise RuntimeError(f"Duplicate dataset id found: {dataset.id}")
        datasets[dataset.id] = dataset

    return datasets
