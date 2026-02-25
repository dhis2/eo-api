from datetime import UTC, datetime
from threading import Lock
from typing import Any
from uuid import uuid4

from eoapi.state_store import load_state_map, save_state_map


_JOBS: dict[str, dict[str, Any]] = load_state_map("jobs")
_LOCK = Lock()


def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def create_job(process_id: str, inputs: dict[str, Any], outputs: dict[str, Any]) -> dict[str, Any]:
    job_id = str(uuid4())
    timestamp = _now_iso()
    job = {
        "jobId": job_id,
        "processId": process_id,
        "status": "succeeded",
        "progress": 100,
        "created": timestamp,
        "updated": timestamp,
        "inputs": inputs,
        "outputs": outputs,
    }

    with _LOCK:
        _JOBS[job_id] = job
        save_state_map("jobs", _JOBS)

    return job


def create_pending_job(
    process_id: str,
    inputs: dict[str, Any],
    *,
    source: str,
    flow_run_id: str | None = None,
) -> dict[str, Any]:
    job_id = str(uuid4())
    timestamp = _now_iso()
    job = {
        "jobId": job_id,
        "processId": process_id,
        "status": "queued",
        "progress": 0,
        "created": timestamp,
        "updated": timestamp,
        "inputs": inputs,
        "outputs": {
            "importSummary": {
                "imported": 0,
                "updated": 0,
                "ignored": 0,
                "deleted": 0,
                "dryRun": bool(inputs.get("dhis2", {}).get("dryRun", True)),
            },
            "features": [],
        },
        "execution": {
            "source": source,
            "flowRunId": flow_run_id,
        },
    }

    with _LOCK:
        _JOBS[job_id] = job
        save_state_map("jobs", _JOBS)

    return job


def update_job(job_id: str, updates: dict[str, Any]) -> dict[str, Any] | None:
    with _LOCK:
        job = _JOBS.get(job_id)
        if job is None:
            return None

        for key, value in updates.items():
            if value is not None:
                job[key] = value

        job["updated"] = _now_iso()
        save_state_map("jobs", _JOBS)
        return job


def get_job(job_id: str) -> dict[str, Any] | None:
    with _LOCK:
        return _JOBS.get(job_id)


def list_jobs() -> list[dict[str, Any]]:
    """Return jobs in newest-first order."""

    with _LOCK:
        return sorted(_JOBS.values(), key=lambda job: job.get("created", ""), reverse=True)
