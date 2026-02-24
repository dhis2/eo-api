# xclim Integration (Implemented Baseline + Next Steps)

This document captures the implemented baseline for `xclim` integration in `eo-api` and the next phases toward broader indicator coverage and orchestration hardening.

## 0) Current baseline status

Implemented in API:

- Process IDs exposed under `/processes`:
  - `xclim-cdd`
  - `xclim-cwd`
  - `xclim-warm-days`
- Execution endpoint support:
  - `POST /processes/{processId}/execution`
- Job monitoring reuse:
  - `GET /jobs/{jobId}`

Current execution behavior:

- Uses lazy imports of `xclim`, `xarray`, `numpy`, and `pandas`.
- Validates required inputs (dataset/parameter/start/end/threshold/orgUnitLevel).
- Requires explicit units (precipitation in mm-based units, warm-days in Celsius units).
- Extracts EO source data through `dhis2eo` download helpers:
  - CHIRPS: `dhis2eo.data.chc.chirps3.daily.download`
  - ERA5-Land: `dhis2eo.data.cds.era5_land.hourly.download`
- Opens downloaded NetCDF outputs with `xarray`, derives daily series, and computes per-org-unit indicators.
- Produces import-ready feature payload shape + `importSummary` using existing job/result patterns.
- Includes deterministic synthetic fallback only when EO extraction fails at runtime (for example provider or credential issues).

Runtime cache:

- Downloaded EO files are cached under `EOAPI_XCLIM_CACHE_DIR` (default: `.cache/xclim`).

## 1) Goals

- Add standards-based indicator processes on top of existing EO ingestion workflows.
- Keep current raw import flows unchanged.
- Expose indicators through OGC API - Processes style endpoints.
- Produce DHIS2-ready outputs via existing `dhis2-client` integration path.

## 2) Why xclim here

- Provides standardized climate indices with explicit semantics and parameters.
- Reduces one-off custom indicator logic.
- Works directly with `xarray`, which fits existing EO data handling patterns.

## 3) Scope for first phase (2–3 indicators)

Start with indicators that map directly to climate-health use cases:

1. **Consecutive Dry Days (CDD)**
   - Typical use: drought stress monitoring.
   - Inputs: daily precipitation, threshold (for example 1 mm/day), time range.

2. **Consecutive Wet Days (CWD)**
   - Typical use: flood/vector suitability signal.
   - Inputs: daily precipitation, threshold, time range.

3. **Warm Days Above Threshold**
   - Typical use: heat stress indicator.
   - Inputs: daily 2m temperature, threshold (for example 35°C), time range.

These three provide immediate value while keeping compute and validation scope manageable.

## 4) Process catalog additions

Process IDs (implemented):

- `xclim-cdd`
- `xclim-cwd`
- `xclim-warm-days`

Endpoints (implemented):

- `GET /processes`
- `GET /processes/{processId}`
- `POST /processes/{processId}/execution`
- `GET /jobs/{jobId}`

## 5) Proposed execution contract

Common request fields:

- `datasetId` (`chirps-daily` or `era5-land-daily`)
- `parameter` (`precip` or `2m_temperature`)
- `start`, `end`
- `orgUnitLevel` or `featureCollectionId`
- `threshold` (unit explicit)
- `aggregation` (usually `sum` or `mean` for post-index rollups)
- `dhis2`:
  - `dataElementId`
  - `dryRun` (default `true`)

Common output fields:

- `indicatorName`
- `period`
- `orgUnitCount`
- `importSummary`
- link to result features and payload preview

## 6) Data and unit handling requirements

- Require explicit units in process inputs (for example `degC`, `mm/day`).
- Reject ambiguous or missing units.
- Avoid silent conversions.
- Document assumptions in process descriptions.

Unit examples:

- ERA5-Land temperature is often Kelvin in source workflows; convert explicitly before threshold logic.
- Precipitation threshold units must match transformed daily precipitation units.

## 7) Pipeline placement

Insert `xclim` after EO extraction and before DHIS2 payload generation:

1. Extract EO source data (`dhis2eo`)
2. Harmonize units/CRS/time axis
3. Compute `xclim` indicator on gridded data
4. Aggregate indicator result to org units
5. Build DHIS2 payload
6. Import (or dry-run)

This keeps EO extraction concerns separate from indicator logic.

## 8) Validation strategy

For each indicator process, add deterministic tests for:

- threshold boundary behavior
- missing/nodata handling
- unit mismatch rejection
- expected period formatting for DHIS2 payloads
- process contract errors (`InvalidParameterValue`, `NotFound`)

## 9) Incremental rollout

Phase 1:

- add `xclim-cdd` and `xclim-warm-days`
- dry-run imports only by default
- process contract + deterministic tests

Phase 2:

- add `xclim-cwd`
- enable scheduled runs through existing schedule/orchestrator path
- add payload preview endpoint if not already enabled

Phase 3:

- evaluate additional indices (for example hot spell duration, percentile-based extremes)
- add bias-adjustment entry points where needed

## 10) Example process payloads

### `xclim-cdd`

```json
{
  "inputs": {
    "datasetId": "chirps-daily",
    "parameter": "precip",
    "start": "2026-01-01",
    "end": "2026-01-31",
    "orgUnitLevel": 2,
    "threshold": { "value": 1.0, "unit": "mm/day" },
    "dhis2": {
      "dataElementId": "<CDD_DATA_ELEMENT_ID>",
      "dryRun": true
    }
  }
}
```

### `xclim-warm-days`

```json
{
  "inputs": {
    "datasetId": "era5-land-daily",
    "parameter": "2m_temperature",
    "start": "2026-01-01",
    "end": "2026-01-31",
    "orgUnitLevel": 2,
    "threshold": { "value": 35.0, "unit": "degC" },
    "dhis2": {
      "dataElementId": "<WARM_DAYS_DATA_ELEMENT_ID>",
      "dryRun": true
    }
  }
}
```

## 11) Dependency and packaging note

`xclim` is now added as a project dependency. Keep CI/environment checks focused on compatibility across the `xclim` + `xarray` + `numpy` stack.

## 12) Copy/paste workflow example

Use this to create a custom workflow that runs aggregate import first, then CDD and warm-days indicators.

```json
{
  "name": "climate-indicators-monthly-workflow",
  "steps": [
    {
      "name": "aggregate-precip",
      "processId": "eo-aggregate-import",
      "payload": {
        "inputs": {
          "datasetId": "chirps-daily",
          "parameters": ["precip"],
          "datetime": "2026-01-31T00:00:00Z",
          "orgUnitLevel": 2,
          "aggregation": "mean",
          "dhis2": {
            "dataElementId": "<AGGREGATE_DATA_ELEMENT_ID>",
            "dryRun": true
          }
        }
      }
    },
    {
      "name": "cdd",
      "processId": "xclim-cdd",
      "payload": {
        "inputs": {
          "datasetId": "chirps-daily",
          "parameter": "precip",
          "start": "2026-01-01",
          "end": "2026-01-31",
          "orgUnitLevel": 2,
          "threshold": { "value": 1.0, "unit": "mm/day" },
          "dhis2": {
            "dataElementId": "<CDD_DATA_ELEMENT_ID>",
            "dryRun": true
          }
        }
      }
    },
    {
      "name": "warm-days",
      "processId": "xclim-warm-days",
      "payload": {
        "inputs": {
          "datasetId": "era5-land-daily",
          "parameter": "2m_temperature",
          "start": "2026-01-01",
          "end": "2026-01-31",
          "orgUnitLevel": 2,
          "threshold": { "value": 35.0, "unit": "degC" },
          "dhis2": {
            "dataElementId": "<WARM_DAYS_DATA_ELEMENT_ID>",
            "dryRun": true
          }
        }
      }
    }
  ]
}
```

Suggested API flow:

1. `POST /workflows` with the JSON above.
2. `POST /workflows/{workflowId}/run` for immediate execution.
3. `POST /schedules` with `workflowId` for recurring execution.

Workflow-target schedule payload example:

```json
{
  "name": "nightly-climate-workflow",
  "cron": "0 0 * * *",
  "timezone": "UTC",
  "enabled": true,
  "workflowId": "<WORKFLOW_ID>"
}
```
