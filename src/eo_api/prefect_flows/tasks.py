"""Prefect tasks that execute OGC API processes via HTTP.

Each task triggers the corresponding pygeoapi process endpoint using
synchronous execution, which returns the result directly in the response.
"""

import logging

import httpx
from prefect import task

logger = logging.getLogger(__name__)

OGCAPI_BASE_URL = "http://localhost:8000/ogcapi"
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
