"""Prefect tasks that execute OGC API processes via HTTP.

Each task triggers the corresponding pygeoapi process endpoint using
async execution, then polls the job status until completion.
"""

import logging
import time

import httpx
from prefect import task

logger = logging.getLogger(__name__)

OGCAPI_BASE_URL = "http://localhost:8000/ogcapi"
POLL_INTERVAL_SECONDS = 2
POLL_TIMEOUT_SECONDS = 600


class JobFailedError(Exception):
    """Raised when an OGC API process job fails."""


class JobTimeoutError(Exception):
    """Raised when polling for job completion exceeds the timeout."""


def _execute_process(client: httpx.Client, process_id: str, inputs: dict) -> dict:
    """Submit an async OGC process execution and poll until completion.

    Args:
        client: HTTP client configured with the OGC API base URL.
        process_id: The OGC process identifier.
        inputs: The process input parameters.

    Returns:
        The job result from /jobs/{job_id}/results.
    """
    response = client.post(
        f"/processes/{process_id}/execution",
        json={"inputs": inputs},
        headers={"Prefer": "respond-async"},
    )
    response.raise_for_status()

    job_id = response.json()["id"]
    logger.info("Process %s submitted as job %s", process_id, job_id)

    elapsed = 0.0
    while elapsed < POLL_TIMEOUT_SECONDS:
        status_response = client.get(f"/jobs/{job_id}")
        status_response.raise_for_status()

        job_info = status_response.json()
        status = job_info["jobs"][0]["status"] if "jobs" in job_info else job_info.get("status")

        if status == "successful":
            logger.info("Job %s completed successfully", job_id)
            result_response = client.get(f"/jobs/{job_id}/results")
            result_response.raise_for_status()
            result: dict = result_response.json()
            return result

        if status == "failed":
            message = job_info.get("message", "unknown error")
            raise JobFailedError(f"Job {job_id} failed: {message}")

        logger.debug("Job %s status: %s, polling again in %ss", job_id, status, POLL_INTERVAL_SECONDS)
        time.sleep(POLL_INTERVAL_SECONDS)
        elapsed += POLL_INTERVAL_SECONDS

    raise JobTimeoutError(f"Job {job_id} did not complete within {POLL_TIMEOUT_SECONDS}s")


@task(retries=3, retry_delay_seconds=30, name="run-process")
def run_process(process_id: str, inputs: dict) -> dict:
    """Execute any OGC API process by its identifier."""
    with httpx.Client(base_url=OGCAPI_BASE_URL, timeout=POLL_TIMEOUT_SECONDS) as client:
        return _execute_process(client, process_id, inputs)
