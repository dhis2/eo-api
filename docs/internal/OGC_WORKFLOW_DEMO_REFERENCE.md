# OGC Workflow Demo Reference

## Purpose

This document is a stable operator/demo reference for the current branch shape.

It answers:

1. how to fetch CHIRPS3 and WorldPop data
2. how to run workflow-backed executions
3. how to inspect jobs and results
4. what the difference is between processes and jobs
5. how to browse published collections
6. why both `/ogcapi` and `/pygeoapi` currently exist

## Current Route Model

The current runtime split is:

1. `/workflows` = native workflow control plane
2. `/ogcapi` = native OGC API - Processes / Jobs adapter
3. `/pygeoapi` = mounted collection/items browse shell for published resources
4. `/publications` = publication registry
5. `/raster` = raster capabilities / rendering layer

Important current note:

Some older docs still refer to `/ogcapi/collections`. In the current codebase, collection browsing is under `/pygeoapi/collections`.

## Dataset IDs Used In Examples

1. `chirps3_precipitation_daily`
2. `worldpop_population_yearly`

WorldPop usually needs `country_code`.

## A. Direct Dataset Fetch Examples

These examples fetch/cache source data directly. They do not create workflow jobs.

### A1. Fetch CHIRPS3

```bash
curl -X POST http://localhost:8000/components/download-dataset \
  -H 'Content-Type: application/json' \
  -d '{
    "dataset_id": "chirps3_precipitation_daily",
    "start": "2024-01-01",
    "end": "2024-01-31",
    "overwrite": false
  }'
```

### A2. Fetch WorldPop

```bash
curl -X POST http://localhost:8000/components/download-dataset \
  -H 'Content-Type: application/json' \
  -d '{
    "dataset_id": "worldpop_population_yearly",
    "start": "2020",
    "end": "2020",
    "country_code": "ETH",
    "overwrite": false
  }'
```

### A3. Alternative Legacy Download Routes

These older routes still exist, but they are not job-backed and are less aligned with the newer component/workflow model.

```bash
curl "http://localhost:8000/manage/chirps3_precipitation_daily/download?start=2024-01-01&end=2024-01-31"
curl "http://localhost:8000/manage/worldpop_population_yearly/download?start=2020&end=2020"
```

## B. Workflow Execution Example

This is the canonical native workflow execution shape:

Important current note:

`publishable` in workflow policy does not automatically publish every successful run.
The caller must explicitly opt in with `"publish": true`, and the workflow must also allow publication.

```bash
curl -X POST http://localhost:8000/workflows/dhis2-datavalue-set \
  -H 'Content-Type: application/json' \
  -d '{
    "request": {
      "workflow_id": "dhis2_datavalue_set_v1",
      "dataset_id": "chirps3_precipitation_daily",
      "start_date": "2024-01-01",
      "end_date": "2024-01-31",
      "org_unit_level": 3,
      "data_element": "abc123def45",
      "temporal_resolution": "monthly",
      "temporal_reducer": "sum",
      "spatial_reducer": "mean",
      "publish": true,
      "dry_run": true
    }
  }'
```

If this succeeds, the response includes a `run_id`. That `run_id` is the native workflow job ID.

Important current note:

This path depends on DHIS2-backed feature resolution. If DHIS2 is unavailable or misconfigured, this request can fail with `503`.

## C. Processes vs Jobs

A `process` is a reusable executable capability.

A `job` is one concrete execution of a process.

In the current OGC layer, there is one exposed generic process:

1. `generic-dhis2-workflow`

So the model is:

1. process = "what can this server execute?"
2. job = "one recorded run of that execution capability"

The native workflow layer also persists jobs, but it does not use OGC process terminology in its route naming.

## D. How To Inspect Jobs And Results

### D1. Native Workflow View

List jobs:

```bash
curl http://localhost:8000/workflows/jobs
```

Get one job:

```bash
curl http://localhost:8000/workflows/jobs/{job_id}
```

Get persisted result payload:

```bash
curl http://localhost:8000/workflows/jobs/{job_id}/result
```

Get persisted trace/log payload:

```bash
curl http://localhost:8000/workflows/jobs/{job_id}/trace
```

Delete one job and cascade its owned artifacts:

```bash
curl -X DELETE http://localhost:8000/workflows/jobs/{job_id}
```

### D2. OGC View Over The Same Execution Layer

List OGC processes:

```bash
curl http://localhost:8000/ogcapi/processes
```

Describe the exposed generic process:

```bash
curl http://localhost:8000/ogcapi/processes/generic-dhis2-workflow
```

List OGC jobs:

```bash
curl http://localhost:8000/ogcapi/jobs
```

Get one OGC job:

```bash
curl http://localhost:8000/ogcapi/jobs/{job_id}
```

Get OGC job results:

```bash
curl http://localhost:8000/ogcapi/jobs/{job_id}/results
```

Get OGC job results plus extra native metadata:

```bash
curl "http://localhost:8000/ogcapi/jobs/{job_id}/results?extended=true"
```

Download the native output artifact if available:

```bash
curl -OJ http://localhost:8000/ogcapi/jobs/{job_id}/download
```

## E. How To Browse Published Collections

### E1. Inspect Publication Registry

List publications:

```bash
curl http://localhost:8000/publications
```

Get CHIRPS3 source publication:

```bash
curl http://localhost:8000/publications/dataset-chirps3_precipitation_daily
```

Get WorldPop source publication:

```bash
curl http://localhost:8000/publications/dataset-worldpop_population_yearly
```

### E2. Browse Published Collections

List collections:

```bash
curl http://localhost:8000/pygeoapi/collections
curl "http://localhost:8000/pygeoapi/collections?f=html"
```

Open CHIRPS3 collection:

```bash
curl http://localhost:8000/pygeoapi/collections/chirps3_precipitation_daily
```

Open WorldPop collection:

```bash
curl http://localhost:8000/pygeoapi/collections/worldpop_population_yearly
```

### E3. Raster-Specific Checks

These two source datasets are primarily coverage/raster resources, so their raster capabilities are also important:

```bash
curl http://localhost:8000/raster/chirps3_precipitation_daily/capabilities
curl http://localhost:8000/raster/worldpop_population_yearly/capabilities
```

### E4. Derived Workflow Output Collections

If a publishable workflow run succeeds, the system can register a derived publication:

1. the workflow result is persisted as a job
2. a `PublishedResource` may be created
3. that publication may become visible under `/pygeoapi/collections/{collection_id}`

The easiest path is:

1. run a publishable workflow
2. open `/workflows/jobs/{job_id}`
3. follow its `collection` link if present

## F. Why Both `/ogcapi` And `/pygeoapi` Exist

Current intent:

1. `/ogcapi` is the native canonical OGC process/job surface
2. `/pygeoapi` is the current generic browse shell for collection/items publication

This means:

1. execution semantics stay FastAPI-owned
2. publication truth stays FastAPI-owned through `PublishedResource`
3. generic collection/items browsing is still delegated to `pygeoapi` where it adds value

This is a pragmatic transition state, not necessarily the final public shape.

## G. Publication Bridge Lifecycle

The publication bridge is file-backed and explicit.

Current lifecycle:

1. a source dataset or workflow output becomes eligible for publication
2. the backend registers publication truth as a `PublishedResource` JSON record
3. the pygeoapi projection layer reads those publication records
4. the system generates pygeoapi YAML/OpenAPI documents from that publication state
5. the mounted `/pygeoapi` app serves collections/items from that generated configuration

In short:

```text
workflow/source dataset
  -> publication registration
  -> PublishedResource JSON
  -> generated pygeoapi YAML
  -> /pygeoapi collection
```

Current storage/projection locations:

1. publication state:
   - `data/downloads/published_resources/*.json`
2. generated pygeoapi projection:
   - `data/downloads/pygeoapi/pygeoapi-config.generated.yml`
   - `data/downloads/pygeoapi/pygeoapi-openapi.generated.yml`

Important architectural point:

The JSON publication record is the source of truth.
The pygeoapi YAML is generated serving configuration, not the primary publication database.

## H. Can The System Eventually Live With Just One?

Yes.

The accepted architectural direction is:

1. `/ogcapi` remains canonical
2. `/pygeoapi` is secondary and potentially transitional

That means the long-term convergence options are:

1. move collection/resource routes into native FastAPI under `/ogcapi`
2. or keep pygeoapi as an internal implementation component while exposing one canonical `/ogcapi` surface

For now, keeping both is reasonable because it preserves a clean native process/job model while still reusing pygeoapi's browse capabilities.

## I. Short Demo Sequence

Good operator/demo flow:

1. `GET /workflows`
2. `GET /ogcapi/processes`
3. `GET /publications`
4. `GET /pygeoapi/collections?f=html`
5. `GET /pygeoapi/collections/chirps3_precipitation_daily`
6. `GET /raster/chirps3_precipitation_daily/capabilities`
7. run one workflow if DHIS2 is available
8. `GET /workflows/jobs/{job_id}`
9. `GET /ogcapi/jobs/{job_id}/results`
10. browse the derived collection if the run published one
