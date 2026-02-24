import os
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import json


def prefect_enabled() -> bool:
    return os.getenv("EOAPI_PREFECT_ENABLED", "false").lower() in {"1", "true", "yes", "on"}


def _prefect_api_url() -> str:
    value = os.getenv("EOAPI_PREFECT_API_URL", "").strip()
    if not value:
        raise RuntimeError("EOAPI_PREFECT_API_URL is required when EOAPI_PREFECT_ENABLED=true")
    return value.rstrip("/")


def _prefect_headers() -> dict[str, str]:
    headers = {
        "Content-Type": "application/json",
    }
    api_key = os.getenv("EOAPI_PREFECT_API_KEY", "").strip()
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


def _json_request(method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    url = f"{_prefect_api_url()}{path}"
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")

    request = Request(url=url, data=data, method=method)
    for key, value in _prefect_headers().items():
        request.add_header(key, value)

    try:
        with urlopen(request, timeout=20) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else {}
    except HTTPError as exc:
        detail = exc.read().decode("utf-8") if exc.fp else str(exc)
        raise RuntimeError(f"Prefect API error ({exc.code}): {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"Unable to reach Prefect API: {exc.reason}") from exc


def submit_aggregate_import_run(
    schedule_id: str,
    payload_inputs: dict[str, Any],
    trigger: str,
    eoapi_job_id: str,
) -> dict[str, Any]:
    deployment_id = os.getenv("EOAPI_PREFECT_DEPLOYMENT_ID", "").strip()
    if not deployment_id:
        raise RuntimeError("EOAPI_PREFECT_DEPLOYMENT_ID is required when EOAPI_PREFECT_ENABLED=true")

    flow_run_payload = {
        "name": f"eoapi-{schedule_id}-{eoapi_job_id}",
        "parameters": {
            "jobId": eoapi_job_id,
            "scheduleId": schedule_id,
            "trigger": trigger,
            "inputs": payload_inputs,
        },
        "tags": ["eoapi", "schedule", trigger],
    }
    return _json_request("POST", f"/api/deployments/{deployment_id}/create_flow_run", flow_run_payload)


def get_flow_run(flow_run_id: str) -> dict[str, Any]:
    return _json_request("GET", f"/api/flow_runs/{flow_run_id}")


def prefect_state_to_job_status(state_type: str | None) -> str:
    if not state_type:
        return "queued"

    normalized = state_type.upper()
    if normalized in {"PENDING", "SCHEDULED", "LATE", "PAUSED"}:
        return "queued"
    if normalized in {"RUNNING", "CANCELLING"}:
        return "running"
    if normalized in {"COMPLETED"}:
        return "succeeded"
    if normalized in {"FAILED", "CRASHED", "CANCELLED"}:
        return "failed"
    return "queued"
