"""OGC Process stage wrapper for dataset download/acquisition."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pygeoapi.process.base import ProcessorExecuteError

from eo_api.integrations.components.adapters.download import chirps3, era5, worldpop

_DOWNLOAD_ADAPTERS = {
    "chirps3": chirps3.run,
    "worldpop": worldpop.run,
    "era5": era5.run,
}


def run_download_stage(params: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    """Download/acquire dataset files for one workflow run."""
    dataset = str(params.get("dataset", "")).strip().lower()
    adapter = _DOWNLOAD_ADAPTERS.get(dataset)
    if adapter is None:
        raise ProcessorExecuteError(f"Unsupported dataset '{dataset}' in download stage")
    return adapter(params, download_dir=Path(str(context.get("download_dir", "/tmp/data"))))
