"""Dataset registry backed by YAML config files."""

import functools
import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent.resolve()
CONFIGS_DIR = SCRIPT_DIR / "registry"


@functools.lru_cache(maxsize=1)
def list_datasets() -> list[dict[str, Any]]:
    """Load all YAML files in the registry folder and return a flat list of datasets."""
    datasets: list[dict[str, Any]] = []
    folder = CONFIGS_DIR

    if not folder.is_dir():
        raise ValueError(f"Path is not a directory: {folder}")

    for file_path in folder.glob("*.y*ml"):
        try:
            with open(file_path, encoding="utf-8") as f:
                file_datasets = yaml.safe_load(f)
                datasets.extend(file_datasets)
        except (yaml.YAMLError, OSError):
            logger.exception("Error loading %s", file_path.name)

    return datasets


def get_dataset(dataset_id: str) -> dict[str, Any] | None:
    """Get dataset dict for a given id."""
    datasets_lookup = {d["id"]: d for d in list_datasets()}
    return datasets_lookup.get(dataset_id)
