---
name: eo-pipeline-orchestration
description: Define scheduled and custom EO data pipelines with optional pre/post-processing and DHIS2-ready outputs.
---

# EO Pipeline Orchestration

## Use this skill when

- Creating recurring ingestion workflows
- Adding custom pre/post-processing pipeline steps
- Designing orchestration handoffs (Airflow/Prefect)

## Canonical pipeline stages

1. Discover dataset and validate metadata (from `eoapi/datasets/<dataset-id>/<dataset-id>.yaml` definitions)
2. Extract data for time/area window
3. Cache source/intermediate artifacts as files when needed
4. Transform and harmonize units/CRS
5. Aggregate to org unit geometries
6. Optional post-process (e.g., consecutive rainy days)
7. Produce import-ready output for DHIS2
8. Trigger import or publish for downstream ingestion

## Orchestration guidance

- Treat each stage as an idempotent task where possible
- Persist execution metadata and lineage
- Use retries/backoff for transient provider failures
- Surface execution status and partial failure details via API
- Use `dhis2eo` for EO/climate processing tasks inside pipeline stages
- Use `dhis2-python-client` for DHIS2 import/export interactions
- Push processed outputs to DHIS2 via Web API as the default load path
- Use DHIS2 Data Store for pipeline/configuration metadata where appropriate
- Promote reusable orchestration helpers to upstream libraries when they are broadly applicable beyond `eo-api`

## Data integrity checks

- Nodata handling rules are explicit
- CRS mismatches are detected and resolved deterministically
- Aggregation method and temporal windows are logged
- Dataset definition schema is validated before runs (e.g. `make validate-datasets`)

## MVP constraints

- Prefer simple, inspectable DAGs over highly dynamic graphs
- Prioritize reliable daily scheduled runs for climate and population flows
