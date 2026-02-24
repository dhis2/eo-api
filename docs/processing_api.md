# Processing API Guide

This is the canonical guide for the processing surface in `eo-api`.

## Endpoints

### `GET /processes`
- Lists registered processes.
- Current process IDs:
  - `raster.zonal_stats`
  - `raster.point_timeseries`
  - `data.temporal_aggregate`

### `GET /processes/{processId}`
- Returns process definition, including:
  - input schema
  - output schema
  - execution links
- Includes `outputs.implementation` to declare runtime stack metadata.
- Includes a `collection` link to `/collections` so clients can discover valid `dataset_id` values.

### `POST /processes/{processId}/execution`
- Accepts:
```json
{
  "inputs": {
    "dataset_id": "chirps-daily",
    "params": ["precip"],
    "time": "2026-01-31",
    "aoi": [30.0, -10.0, 31.0, -9.0]
  }
}
```
- Returns a `202` with `jobId` and monitor link.

### `GET /jobs/{jobId}`
- Returns execution status and outputs:
  - `rows`
  - `csv`
  - `dhis2` (stub envelope)
  - `implementation` (provider/compute/formatting stack)

## Process/Collection Cross-Links

- Process -> Collection:
  - Each process definition includes a `links` entry:
    - `rel: "collection"`
    - `href: /collections`
- Collection -> Process:
  - Each local collection includes links for every registered process:
    - `rel: "process"` -> `/processes/{id}`
    - `rel: "process-execute"` -> `/processes/{id}/execution`

This keeps data discovery and process execution connected in an OGC-style workflow.

## Concrete Examples

### List processes
```bash
curl -s "http://127.0.0.1:8000/processes"
```

### Get process definition
```bash
curl -s "http://127.0.0.1:8000/processes/raster.zonal_stats"
```

### Execute zonal stats
```bash
curl -s -X POST "http://127.0.0.1:8000/processes/raster.zonal_stats/execution" \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": {
      "dataset_id": "chirps-daily",
      "params": ["precip"],
      "time": "2026-01-31",
      "aoi": [30.0, -10.0, 31.0, -9.0]
    }
  }'
```

### Execute point timeseries
```bash
curl -s -X POST "http://127.0.0.1:8000/processes/raster.point_timeseries/execution" \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": {
      "dataset_id": "chirps-daily",
      "params": ["precip"],
      "time": "2026-01-31",
      "aoi": {"bbox": [30.0, -10.0, 32.0, -8.0]}
    }
  }'
```

### Execute temporal aggregation (harmonization)
```bash
curl -s -X POST "http://127.0.0.1:8000/processes/data.temporal_aggregate/execution" \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": {
      "dataset_id": "chirps-daily",
      "params": ["precip"],
      "time": "2026-01-31",
      "frequency": "P1M",
      "aggregation": "sum"
    }
  }'
```

### Check job result
```bash
curl -s "http://127.0.0.1:8000/jobs/<jobId>"
```

Expected key output shape:
```json
{
  "outputs": {
    "provider": "chirps3",
    "from_cache": true,
    "rows": [],
    "csv": "...",
    "dhis2": {"status": "stub"},
    "implementation": {
      "provider": {"id": "chirps3", "libs": ["dhis2eo"]},
      "compute": {"libs": ["xarray"]},
      "formatting": {
        "csv": {"libs": ["python csv", "python json"]},
        "dhis2": {"libs": ["eoapi stub", "dhis2eo.integrations.pandas (planned)"]}
      }
    }
  }
}
```

## How To Interpret Results

- `status: "succeeded"` means run completed without system failure.
- `outputs.assets` shows resolved/fetched raster assets.
- `outputs.from_cache` indicates cache reuse (`true`) vs fresh fetch (`false`).
- For `raster.zonal_stats`:
  - `rows[].status = "computed"` with numeric `value` means success.
  - `rows[].status = "no_data"` means AOI/time had only null/nodata.
  - `rows[].status = "read_error"` means file(s) could not be processed.
- `dhis2.status: "stub"` means DHIS2 final mapping/export is not implemented yet.

## Internal Services and Why

- `eoapi.processing.process_catalog`
  - canonical process IDs and definition builders
- `eoapi.processing.runtime`
  - shared process dispatcher used by endpoints
- `eoapi.processing.service`
  - validation -> provider fetch -> raster op -> formatting -> job creation
- `eoapi.processing.registry`
  - dataset registry built from dataset catalog + provider mapping YAML
- `eoapi.processing.providers.base`
  - provider contracts
- `eoapi.processing.providers.chirps3`
  - CHIRPS cache-first provider
- `eoapi.processing.raster_ops`
  - process compute logic
- `eoapi.processing.formatters`
  - CSV and DHIS2 envelope formatting

## Library Responsibility Matrix

| Stage | What it does | Primary libs/tools | Where in code |
|---|---|---|---|
| Extract (provider adapters) | Fetch CHIRPS/ERA5 assets and source-specific download logic | `dhis2eo`, `earthkit`, `rioxarray` (inside adapter implementations) | `eoapi/datasets/*/resolver.py`, `eoapi/processing/providers/chirps3.py` |
| Provider cache | Reuse local files before remote fetch | local filesystem cache in `.cache/providers` | `eoapi/processing/providers/chirps3.py` |
| Process API contract | Expose process metadata + execution endpoints | OGC API - Processes style, FastAPI routing | `eoapi/endpoints/processes.py`, `eoapi/processing/process_catalog.py` |
| Transform/compute | Zonal stats, timeseries, harmonization logic | `xarray` (current zonal implementation), `rasterio`/`rioxarray`/`geopandas` (planned) | `eoapi/processing/raster_ops.py` |
| Load/formatting | Convert canonical rows to outputs | Python `csv`/`json`; `dhis2eo.integrations.pandas` planned for DHIS2 mapping | `eoapi/processing/formatters.py` |
