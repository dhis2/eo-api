"""Prefect tasks that execute OGC API processes via HTTP.

Each task triggers the corresponding pygeoapi process endpoint using
synchronous execution, which returns the result directly in the response.
"""

import logging
import os
from pathlib import Path

import httpx
import xarray as xr
from prefect import task
from prefect.artifacts import create_markdown_artifact

logger = logging.getLogger(__name__)

OGCAPI_BASE_URL = os.getenv("OGCAPI_BASE_URL", "http://localhost:8000/ogcapi")
PROCESS_TIMEOUT_SECONDS = 600


class JobFailedError(Exception):
    """Raised when an OGC API process job fails."""


def _execute_process(client: httpx.Client, process_id: str, inputs: dict) -> dict:
    """Execute an OGC process synchronously and return the result.

    Args:
        client: HTTP client configured with the OGC API base URL.
        process_id: The OGC process identifier.
        inputs: The process input parameters.

    Returns:
        The process result containing status, files, summary, and message.
    """
    response = client.post(
        f"/processes/{process_id}/execution",
        json={"inputs": inputs},
    )
    response.raise_for_status()
    result: dict = response.json()

    status = result.get("status")
    if status == "failed":
        raise JobFailedError(f"Process {process_id} failed: {result.get('message')}")

    return result


@task(retries=3, retry_delay_seconds=30, name="run-process")
def run_process(process_id: str, inputs: dict) -> dict:
    """Execute any OGC API process by its identifier."""
    with httpx.Client(base_url=OGCAPI_BASE_URL, timeout=PROCESS_TIMEOUT_SECONDS) as client:
        return _execute_process(client, process_id, inputs)


@task(name="summarize-datasets")
def summarize_datasets(process_id: str, files: list[str]) -> None:
    """Open downloaded .nc files, extract stats, publish markdown artifact."""
    if not files:
        return

    sections: list[str] = []
    for filepath in files:
        path = Path(filepath)
        if not path.exists() or path.suffix != ".nc":
            sections.append(f"### {path.name}\n\n_File not found_")
            continue

        with xr.open_dataset(filepath) as ds:
            # ERA5 uses latitude/longitude, CHIRPS3 (rioxarray) uses y/x
            lat_name = "latitude" if "latitude" in ds else "y"
            lon_name = "longitude" if "longitude" in ds else "x"
            lat = ds[lat_name].values
            lon = ds[lon_name].values
            bbox = f"[{lon.min():.2f}, {lat.min():.2f}, {lon.max():.2f}, {lat.max():.2f}]"

            # ERA5 uses valid_time, CHIRPS3 uses time
            time_name = "valid_time" if "valid_time" in ds else "time"
            time_vals = ds[time_name].values
            time_range = f"{str(time_vals[0])[:10]} to {str(time_vals[-1])[:10]}"

            # Only include variables with spatial dimensions (skip scalar metadata like number, expver)
            data_vars = [v for v in ds.data_vars if v not in ds.coords and ds[v].ndim >= 2]

            lines = [
                f"### {path.name}",
                "",
                "| Property | Value |",
                "|---|---|",
                f"| File size | {path.stat().st_size / 1024:.0f} KB |",
                f"| Time range | {time_range} |",
                f"| Timesteps | {len(time_vals)} |",
                f"| Grid | {len(lat)} x {len(lon)} |",
                f"| Bbox | {bbox} |",
            ]

            for var_name in data_vars:
                da = ds[var_name]
                units = da.attrs.get("units", "")
                vmin = float(da.min(skipna=True))
                vmean = float(da.mean(skipna=True))
                vmax = float(da.max(skipna=True))
                lines.append(f"| **{var_name}** | {vmin:.2f} / {vmean:.2f} / {vmax:.2f} {units} (min/mean/max) |")

            sections.append("\n".join(lines))

    md = f"## {process_id}\n\n" + "\n\n".join(sections)

    create_markdown_artifact(
        key=f"{process_id}-summary",
        markdown=md,
        description=f"Dataset summary for {process_id}",
    )
