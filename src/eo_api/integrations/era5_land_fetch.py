"""Reusable ERA5-Land download component."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from dhis2eo.data.cds.era5_land import hourly as era5_land_hourly


def download_era5_land(
    *,
    start: str,
    end: str,
    bbox: list[float],
    variables: list[str],
    download_root: Path,
) -> dict[str, Any]:
    """Download ERA5-Land files to local storage and return metadata."""
    download_root.mkdir(parents=True, exist_ok=True)
    files = era5_land_hourly.download(
        start=start,
        end=end,
        bbox=(bbox[0], bbox[1], bbox[2], bbox[3]),
        dirname=str(download_root),
        prefix="era5",
        variables=variables,
    )
    file_paths = [str(path) for path in files]
    return {
        "files": file_paths,
        "summary": {
            "file_count": len(file_paths),
            "variables": variables,
            "start": start,
            "end": end,
        },
    }
