# DHIS2 Pipeline Milestones

This document presents the `dhis2.pipeline` process as a milestone-based walkthrough.

Goal:
- Input: DHIS2 GeoJSON org unit features
- Output: DHIS2 `dataValueSet` payload

## Milestone 0: OGC Discovery (Features + Collections + Processes)

What happens:
- Client discovers org unit feature source, raster collection, and executable process.
- Client identifies:
  - feature collection: `dhis2-org-units`
  - data collection: `chirps-daily`
  - process: `dhis2.pipeline`

Endpoints:
- `GET /features`
- `GET /features/dhis2-org-units/items`
- `GET /collections`
- `GET /collections/{collectionId}`
- `GET /processes`
- `GET /processes/dhis2.pipeline`

Code:
- `eoapi/endpoints/collections.py`
- `eoapi/endpoints/processes.py`
- `eoapi/processing/process_catalog.py`
- `eoapi/processing/pipeline.py` (`get_pipeline_definition`)

## Milestone 1: OGC Process Execute

What happens:
- Client posts one request to execute `dhis2.pipeline`.
- Request includes GeoJSON features + dataset/time/params + DHIS2 mapping fields.

Endpoint:
- `POST /processes/dhis2.pipeline/execution`

Input shape (core fields):
- `features` (GeoJSON FeatureCollection or Feature list)
- `dataset_id` (example: `chirps-daily`)
- `params` (example: `["precip"]`)
- `time` (example: `2026-01-31`)
- `aggregation` (example: `mean`)
- `data_element` (DHIS2 UID)

Code:
- `eoapi/endpoints/processes.py` (runtime dispatch)
- `eoapi/processing/pipeline.py` (`execute_dhis2_pipeline`)

## Milestone 2: Process Input Validation and Normalization

What happens:
- Validate input payload with pydantic model.
- Validate dataset exists in registry.
- Validate requested params exist for dataset.
- Normalize features into list form.
- Resolve per-feature orgUnit UID + geometry bbox.

Code:
- `PipelineInputs` in `eoapi/processing/pipeline.py`
- `_extract_feature_list`, `_extract_ou_id`, `_bbox_from_geometry`
- `load_dataset_registry()` and `_resolve_requested_params(...)`

Failure modes:
- `400 InvalidParameterValue` for malformed `features` / invalid schema
- `404 NotFound` for unknown dataset

## Milestone 3: Collection-to-Provider Data Access

What happens:
- For each feature bbox and each requested parameter:
  - provider fetches raster assets (cache-first where supported)
- For CHIRPS, provider uses `dhis2eo` adapter under the hood.

Code:
- `build_provider(...)` from `eoapi.processing.providers`
- `RasterFetchRequest`
- CHIRPS provider: `eoapi/processing/providers/chirps3.py`

Notes:
- This stage is dataset/provider specific.
- Errors are collected per-feature in pipeline `errors`.

## Milestone 4: Process Compute (Zonal Statistics per Feature)

What happens:
- For each feature:
  - run zonal statistics over its bbox
  - produce row(s) with computed value/status
- Rows are enriched with `orgUnit` context.

Code:
- `zonal_stats_stub(...)` in `eoapi/processing/raster_ops.py`
- Called from `execute_dhis2_pipeline(...)`

Row semantics:
- `status=computed` + numeric `value` => successful statistic
- `status=no_data` => all null/nodata in slice
- `status=read_error` / `missing_assets` => fetch/read issue

## Milestone 5: Process Output Formatting (DHIS2 Contract)

What happens:
- Convert computed rows to DHIS2 `dataValues`.
- One dataValue per computed row, mapped to:
  - `dataElement`
  - `orgUnit`
  - `period` (`YYYYMMDD`)
  - `value`
  - optional `categoryOptionCombo`
- Wrap as `dataValueSet`.

Code:
- `rows_to_dhis2(...)` in `eoapi/processing/formatters.py`
- Assembled in `eoapi/processing/pipeline.py`

Output fields:
- `outputs.dataValueSet`
- `outputs.rows`
- `outputs.summary` (`features`, `computed`, `errors`)
- `outputs.errors`

## Milestone 6: OGC Job Persistence and Retrieval

What happens:
- Create job record and return inline execution result.
- Job can be re-read later from `/jobs/{jobId}`.

Endpoints:
- `POST /processes/dhis2.pipeline/execution` (returns job inline)
- `GET /jobs/{jobId}`

Code:
- `create_job(...)` in `eoapi/jobs.py`
- `/jobs` handlers in `eoapi/endpoints/processes.py`

## Milestone 7: Optional Downstream DHIS2 Import

What happens:
- `dataValueSet` payload can be posted to DHIS2 `/api/dataValueSets`.
- This is intentionally decoupled from process execution.

Code available:
- `eoapi/dhis2_integration.py` (`import_data_values_to_dhis2`)

## End-to-End Summary

```text
OGC Features (discover org unit geometries)
  -> OGC Collections (discover dataset)
  -> OGC Processes (discover + execute dhis2.pipeline)
  -> Provider fetch (cache-first)
  -> Process compute (zonal stats per feature bbox)
  -> Process output formatting (DHIS2 dataValueSet)
  -> OGC Jobs (retrieve persisted result)
```

This is the full "collection -> process -> job -> DHIS2 payload" pipeline.
