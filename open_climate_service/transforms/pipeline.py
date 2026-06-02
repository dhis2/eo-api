"""Shared dataset transform pipeline for managed dataset materialization."""

from __future__ import annotations

import importlib
import logging
from collections.abc import Callable
from typing import Any, cast

import xarray as xr

logger = logging.getLogger(__name__)


def run_dataset_transforms(ds: xr.Dataset, dataset: dict[str, Any]) -> xr.Dataset:
    """Apply template-declared transforms to one dataset."""
    dataset_id = dataset.get("id", "?")
    for entry in dataset.get("transforms", []):
        if isinstance(entry, str):
            func_path, params = entry, {}
        elif isinstance(entry, dict):
            if "function" not in entry:
                raise ValueError(
                    f"Transform entry in dataset '{dataset_id}' is missing required key 'function': {entry!r}"
                )
            func_path = entry["function"]
            params = entry.get("params", {})
        else:
            raise ValueError(
                f"Transform entry in dataset '{dataset_id}' must be a string or dict,"
                f" got {type(entry).__name__!r}: {entry!r}"
            )
        logger.info("Applying transform %s to dataset %s", func_path, dataset_id)
        func = _get_dynamic_function(func_path)
        ds = func(ds, dataset, **params)
    return ds


def _get_dynamic_function(full_path: str) -> Callable[..., Any]:
    module_name, func_name = full_path.rsplit(".", 1)
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise ValueError(f"Failed to import transform module '{module_name}' from '{full_path}'") from exc
    try:
        func = getattr(module, func_name)
    except AttributeError as exc:
        raise ValueError(f"Transform '{full_path}' does not exist") from exc
    if not callable(func):
        raise ValueError(f"Transform '{full_path}' is not callable")
    return cast(Callable[..., Any], func)
