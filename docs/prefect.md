# Prefect Pipelines

## Overview

The pipelines module provides orchestrated climate data download workflows using [Prefect](https://docs.prefect.io/). Pipelines wrap the same underlying libraries (`dhis2eo`) as the existing OGC API processors, adding automatic retries, logging, and observability.

## Architecture

The Prefect server is mounted directly into the FastAPI application -- no separate infrastructure required:

- **Pipeline endpoints**: `POST /pipelines/era5-land`, `POST /pipelines/chirps3`
- **Prefect UI**: `http://localhost:8000/prefect/` (flow runs, task runs, logs)
- **Prefect API**: `http://localhost:8000/prefect/api/`

Flows run in-process within the FastAPI server. Each pipeline endpoint triggers a Prefect flow synchronously and returns the result.

## Available Pipelines

### ERA5-Land

Downloads ERA5-Land hourly climate data.

```
POST /pipelines/era5-land
```

```json
{
  "start": "2024-01",
  "end": "2024-03",
  "bbox": [28.0, -3.0, 36.0, 4.0],
  "variables": ["2m_temperature", "total_precipitation"]
}
```

### CHIRPS3

Downloads CHIRPS3 daily precipitation data.

```
POST /pipelines/chirps3
```

```json
{
  "start": "2024-01",
  "end": "2024-03",
  "bbox": [28.0, -3.0, 36.0, 4.0],
  "stage": "final"
}
```

### Response

Both endpoints return:

```json
{
  "status": "completed",
  "files": ["/tmp/data/era5_2024-01.nc", "/tmp/data/era5_2024-02.nc"],
  "features": null,
  "message": "ERA5-Land pipeline completed: 2 file(s) downloaded"
}
```

## Adding New Pipelines

1. Add a Pydantic input model in `src/eo_api/pipelines/schemas.py`
2. Add a `@task` function in `src/eo_api/pipelines/tasks.py`
3. Add a `@flow` function in `src/eo_api/pipelines/flows.py` that chains tasks
4. Add a `POST` endpoint in `src/eo_api/pipelines/router.py`

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DOWNLOAD_DIR` | `/tmp/data` | Directory for downloaded climate data files |
| `PREFECT_API_URL` | - | URL of the Prefect API server (set to `http://localhost:8000/prefect/api` for embedded mode) |
