# Pipeline Architecture Summary

## Overview

The eo-api combines three layers to orchestrate climate data downloads:

1. **FastAPI** -- main application, serves REST endpoints and mounts sub-apps
2. **pygeoapi** -- OGC API - Processes implementation, mounted at `/ogcapi`
3. **Prefect** -- workflow orchestration with retries, logging, and observability

Pipeline endpoints live at `/pipelines/{process_id}`. The Prefect UI is available at `/prefect/`.

## Data flow

```
POST /pipelines/era5-land-download
  |
  v
FastAPI router (/pipelines)
  |
  v
Prefect flow (via Runner deployment)
  |
  v
Prefect task: run_process()
  |  POST /ogcapi/processes/{id}/execution  (sync)
  v
pygeoapi processor (ERA5LandProcessor / CHIRPS3Processor)
  |  Downloads data via dhis2eo library
  v
Files written to DOWNLOAD_DIR  (default: /tmp/data)
  |
  v
ProcessOutput { status, files, summary, message }
  returned through the chain back to the caller
```

## Pipelines

| Pipeline | Process ID | Description |
|---|---|---|
| ERA5-Land | `era5-land-download` | Hourly reanalysis data (temperature, precipitation, etc.) via CDS API |
| CHIRPS3 | `chirps3-download` | Daily precipitation data (final or preliminary stage) |

Both processors return a `ProcessOutput` JSON object:

```json
{
  "status": "completed",
  "files": ["/tmp/data/era5_2024-01.nc"],
  "summary": { "file_count": 1, "start": "2024-01", "end": "2024-01" },
  "message": "ERA5-Land pipeline completed: 1 file(s) downloaded"
}
```

## Output location

Downloaded files are written to the directory specified by the `DOWNLOAD_DIR` environment variable (default `/tmp/data`). File naming follows the pattern `{dataset}_{year}-{month}.nc`.

## Sync vs async execution

The Prefect tasks use **synchronous** OGC process execution. The HTTP POST to `/processes/{id}/execution` blocks until the processor finishes and returns the result directly in the response body.

This was chosen over async execution (`Prefer: respond-async`) for two reasons:

- Prefect tasks already run in background workers, so there is no need for a second layer of async job management.
- pygeoapi's TinyDB job manager has a bug where `get_job_result()` returns an empty result for completed async jobs, losing the file paths from the processor output. Sync execution bypasses TinyDB storage entirely.

The httpx client timeout (`PROCESS_TIMEOUT_SECONDS = 600`) handles the wait for long-running downloads.

## Known quirks

- **pygeoapi logger noise** -- pygeoapi's `api.processes` and `l10n` loggers are suppressed to ERROR level in `main.py` to reduce log spam.
- **TinyDB results bug** -- async job results are not retrievable via `/jobs/{jobId}/results` due to a storage issue in the TinyDB manager. This is why we use sync execution.
- **Prefect UI base path** -- the embedded Prefect server is configured with `PREFECT_UI_SERVE_BASE=/prefect/` so the UI is served under the FastAPI app rather than on a separate port.
