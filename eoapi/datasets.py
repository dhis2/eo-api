from functools import lru_cache
from pathlib import Path
from datetime import datetime

from pydantic import BaseModel, Field, field_validator
import yaml

DATASETS_DIR = Path(__file__).resolve().parents[1] / "datasets"


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

    for dataset_file in sorted(DATASETS_DIR.glob("*.yml")) + sorted(DATASETS_DIR.glob("*.yaml")):
        with dataset_file.open("r", encoding="utf-8") as file_handle:
            payload = yaml.safe_load(file_handle) or {}

        dataset = DatasetDefinition.model_validate(payload)
        if dataset.id in datasets:
            raise RuntimeError(f"Duplicate dataset id found: {dataset.id}")
        datasets[dataset.id] = dataset

    return datasets
