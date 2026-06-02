"""Dataset registry backed by YAML config files."""

import importlib.resources
import logging
import sys
from pathlib import Path
from typing import Any

import yaml

from open_climate_service import config as api_config

logger = logging.getLogger(__name__)

# Overridden in tests via monkeypatch to point to a temporary directory.
# When set, only this directory is loaded (no built-ins, no config override).
CONFIGS_DIR: Path | None = None

SUPPORTED_SYNC_KINDS = {"temporal", "release", "static"}
SUPPORTED_SYNC_EXECUTIONS = {"append", "rematerialize"}


def list_datasets() -> list[dict[str, Any]]:
    """Load all dataset templates and return a flat list.

    Built-in templates from open_climate_service/data/datasets/ are always loaded. When
    plugins_dir is set in CLIMATE_SERVICE_CONFIG, templates from that directory are
    merged on top — a custom template with the same id overrides the built-in one.

    CONFIGS_DIR (test override via monkeypatch) bypasses this and loads only
    from the given directory, as tests supply a fully controlled set.
    """
    if CONFIGS_DIR is not None:
        return _load_from_dir(CONFIGS_DIR)

    merged: dict[str, dict[str, Any]] = {d["id"]: d for d in _load_builtin_datasets()}

    config = api_config.get_config()
    if config.get("templates_dir"):
        raise ValueError(
            "CLIMATE_SERVICE_CONFIG uses the removed 'templates_dir' key. "
            "Rename it to 'plugins_dir' and rename the directory from 'templates/' to 'plugins/'."
        )
    config_plugins_dir = config.get("plugins_dir")
    if config_plugins_dir:
        if not isinstance(config_plugins_dir, (str, Path)):
            raise ValueError(
                f"plugins_dir in CLIMATE_SERVICE_CONFIG must be a path string, got {type(config_plugins_dir).__name__}"
            )
        config_path = api_config.get_config_path()
        base = config_path.parent if config_path else Path()
        root = (base / config_plugins_dir).resolve()
        if not root.is_dir():
            raise ValueError(f"plugins_dir '{root}' does not exist or is not a directory")
        root_str = str(root)
        if root_str not in sys.path:
            sys.path.append(root_str)
        datasets_subdir = root / "datasets"
        if datasets_subdir.is_dir():
            for dataset in _load_from_dir(datasets_subdir):
                merged[dataset["id"]] = dataset

    return list(merged.values())


def get_dataset(dataset_id: str) -> dict[str, Any] | None:
    """Get dataset dict for a given id."""
    datasets_lookup = {d["id"]: d for d in list_datasets()}
    return datasets_lookup.get(dataset_id)


def _load_builtin_datasets() -> list[dict[str, Any]]:
    """Load built-in dataset templates from package data via importlib.resources.

    Using importlib.resources instead of __file__-relative path arithmetic ensures
    this works correctly in both editable installs and wheel installs, where the
    package lives inside site-packages with no guarantee that the project root
    directory (and its data/ folder) is accessible.
    """
    pkg = importlib.resources.files("open_climate_service") / "data" / "datasets"
    datasets: list[dict[str, Any]] = []
    for resource in pkg.iterdir():
        if not resource.name.endswith((".yaml", ".yml")):
            continue
        try:
            content = resource.read_text(encoding="utf-8")
            file_datasets = yaml.safe_load(content)
            if not isinstance(file_datasets, list):
                raise ValueError(f"{resource.name} must contain a list of dataset templates")
            for dataset in file_datasets:
                _validate_dataset_template(dataset, source=resource.name)
            datasets.extend(file_datasets)
        except Exception:
            logger.exception("Error loading %s", resource.name)
            raise
    return datasets


def _load_from_dir(folder: Path) -> list[dict[str, Any]]:
    """Load dataset templates from a directory on disk."""
    datasets: list[dict[str, Any]] = []

    if not folder.is_dir():
        raise ValueError(f"Path is not a directory: {folder}")

    for file_path in folder.glob("*.y*ml"):
        try:
            with open(file_path, encoding="utf-8") as f:
                file_datasets = yaml.safe_load(f)
                if not isinstance(file_datasets, list):
                    raise ValueError(f"{file_path.name} must contain a list of dataset templates")
                for dataset in file_datasets:
                    _validate_dataset_template(dataset, source=str(file_path))
                datasets.extend(file_datasets)
        except Exception:
            logger.exception("Error loading %s", file_path.name)
            raise

    return datasets


def _validate_dataset_template(dataset: object, *, source: str) -> None:
    """Validate registry fields required by runtime sync planning."""
    if not isinstance(dataset, dict):
        raise ValueError(f"{source} contains a non-object dataset template")

    dataset_id = dataset.get("id")
    if not isinstance(dataset_id, str) or not dataset_id:
        raise ValueError(f"{source} contains a dataset template with a missing or invalid id")
    sync_block = dataset.get("sync", {})
    sync_kind = sync_block.get("kind") if isinstance(sync_block, dict) else None
    if not isinstance(sync_kind, str) or not sync_kind:
        raise ValueError(f"Dataset template '{dataset_id}' in {source} must define sync.kind")
    if sync_kind not in SUPPORTED_SYNC_KINDS:
        supported = ", ".join(sorted(SUPPORTED_SYNC_KINDS))
        raise ValueError(
            f"Dataset template '{dataset_id}' in {source} has unsupported sync.kind "
            f"'{sync_kind}'. Supported values: {supported}"
        )

    sync_execution = sync_block.get("execution") if isinstance(sync_block, dict) else None
    if sync_execution is not None:
        if not isinstance(sync_execution, str) or not sync_execution:
            raise ValueError(f"Dataset template '{dataset_id}' in {source} has invalid sync.execution")
        if sync_execution not in SUPPORTED_SYNC_EXECUTIONS:
            supported = ", ".join(sorted(SUPPORTED_SYNC_EXECUTIONS))
            raise ValueError(
                f"Dataset template '{dataset_id}' in {source} has unsupported sync.execution "
                f"'{sync_execution}'. Supported values: {supported}"
            )

    ingestion = dataset.get("ingestion")
    if not isinstance(ingestion, dict):
        raise ValueError(f"Dataset template '{dataset_id}' in {source} must define an 'ingestion' block")
    plugin = ingestion.get("plugin")
    has_plugin = isinstance(plugin, str) and bool(plugin)
    if not has_plugin:
        raise ValueError(f"Dataset template '{dataset_id}' in {source} must define ingestion.plugin")

    sync_availability = sync_block.get("availability") if isinstance(sync_block, dict) else None
    if sync_availability is not None:
        _validate_sync_availability(sync_availability, dataset_id=dataset_id, source=source)


def _validate_sync_availability(sync_availability: object, *, dataset_id: str, source: str) -> None:
    """Validate optional source availability policy metadata."""
    if not isinstance(sync_availability, dict):
        raise ValueError(f"Dataset template '{dataset_id}' in {source} has invalid sync.availability")

    latest_available_function = sync_availability.get("latest_available_function")
    if latest_available_function is not None and (
        not isinstance(latest_available_function, str) or not latest_available_function
    ):
        raise ValueError(
            f"Dataset template '{dataset_id}' in {source} has invalid sync.availability.latest_available_function"
        )
