"""ERA5-Land climate data download processor."""

import logging
import os
from pathlib import Path
from typing import Any

from dhis2eo.data.cds.era5_land import hourly as era5_land_hourly
from pydantic import ValidationError
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from eo_api.routers.ogcapi.plugins.processes.schemas import ERA5LandInput, ProcessOutput

DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/tmp/data")

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    "version": "0.1.0",
    "id": "era5-land",
    "title": "ERA5-Land Climate Data",
    "description": "Download ERA5-Land hourly climate data for a bounding box and date range.",
    "jobControlOptions": ["sync-execute", "async-execute"],
    "keywords": ["climate", "ERA5-Land", "temperature", "precipitation"],
    "inputs": {
        "start": {
            "title": "Start date",
            "description": "Start date in YYYY-MM format",
            "schema": {"type": "string"},
            "minOccurs": 1,
            "maxOccurs": 1,
        },
        "end": {
            "title": "End date",
            "description": "End date in YYYY-MM format",
            "schema": {"type": "string"},
            "minOccurs": 1,
            "maxOccurs": 1,
        },
        "bbox": {
            "title": "Bounding box",
            "description": "Bounding box [west, south, east, north]",
            "schema": {"type": "array", "items": {"type": "number"}, "minItems": 4, "maxItems": 4},
            "minOccurs": 1,
            "maxOccurs": 1,
        },
        "variables": {
            "title": "Variables",
            "description": "ERA5-Land variable names",
            "schema": {"type": "array", "items": {"type": "string"}, "minItems": 1},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
        "dry_run": {
            "title": "Dry run",
            "description": "If true, return data without pushing to DHIS2",
            "schema": {"type": "boolean", "default": True},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
    },
    "outputs": {
        "result": {
            "title": "Processing result",
            "schema": {"type": "object", "contentMediaType": "application/json"},
        }
    },
}


class ERA5LandProcessor(BaseProcessor):
    """Processor for downloading ERA5-Land hourly climate data."""

    def __init__(self, processor_def: dict[str, Any]) -> None:
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data: dict[str, Any], outputs: Any = None) -> tuple[str, dict[str, Any]]:
        try:
            inputs = ERA5LandInput.model_validate(data)
        except ValidationError as e:
            raise ProcessorExecuteError(str(e)) from e

        LOGGER.info(
            "ERA5-Land download: start=%s end=%s bbox=%s variables=%s",
            inputs.start,
            inputs.end,
            inputs.bbox,
            inputs.variables,
        )

        download_dir = Path(DOWNLOAD_DIR)
        download_dir.mkdir(parents=True, exist_ok=True)

        files = era5_land_hourly.download(
            start=inputs.start,
            end=inputs.end,
            bbox=(inputs.bbox[0], inputs.bbox[1], inputs.bbox[2], inputs.bbox[3]),
            dirname=str(download_dir),
            prefix="era5",
            variables=inputs.variables,
        )
        output = ProcessOutput(
            status="completed",
            files=[str(f) for f in files],
            summary={
                "file_count": len(files),
                "variables": inputs.variables,
                "start": inputs.start,
                "end": inputs.end,
            },
            message="Data downloaded" + (" (dry run)" if inputs.dry_run else ""),
        )

        return "application/json", output.model_dump()

    def __repr__(self) -> str:
        return "<ERA5LandProcessor>"
