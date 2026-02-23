# Copilot Instructions for `eo-api`

## Project context

- `eo-api` is a DHIS2 Earth Observation integration API.
- Primary goal: unified, no-code-friendly EO data ingestion and processing for DHIS2 and CHAP.
- Typical capabilities: dataset discovery, process execution, raster preview/tiling, aggregation to org units, and scheduled pipelines.
- DHIS2 Maps app and DHIS2 Climate app are primary consumers of `eo-api`.
- `eo-api` should replace functionality currently sourced via Google Earth Engine for these apps.

## Current implemented baseline (keep in sync)

- Dataset discovery is implemented via:
  - `GET /collections`
  - `GET /collections/{collection_id}`
- OGC API - Coverages baseline is implemented via:
  - `GET /collections/{collection_id}/coverage`
- Collections and coverages are split into separate endpoint modules:
  - `eoapi/endpoints/collections.py`
  - `eoapi/endpoints/coverages.py`
- Shared endpoint constants/errors live in:
  - `eoapi/endpoints/constants.py`
  - `eoapi/endpoints/errors.py`
- Dataset metadata is file-driven from top-level `datasets/*.yaml` and validated by Pydantic (`eoapi/datasets.py`).
- Dataset validation command is available via `make validate-datasets`.
- Tests currently include endpoint error contract tests and run via `make test`.

## Product priorities

- Favor end-to-end data flow correctness over feature breadth.
- Keep MVP scope tight: discovery, process execution, aggregation, and import-ready outputs.
- Design for extensibility of process catalog and new datasets.
- Preserve consumer-facing compatibility for Maps and Climate app integration paths.

## Technical direction (default assumptions)

- Python + FastAPI for core API services.
- `dhis2-python-client` is the default library for DHIS2 Web API interactions.
- `dhis2eo` is the default library for EO and climate data processing/integration.
- OGC API - Processes style endpoints for process execution.
- TiTiler-compatible patterns for COG/STAC raster access where relevant.
- File-based caching is the default mechanism for reusable source/intermediate EO artifacts when needed.
- Airflow/Prefect-oriented orchestration hooks for scheduled or long-running jobs.
- Container-first development and deployment.

## Coding guidelines

- Use clear, explicit typing for Python functions and models where practical.
- Keep functions focused and side effects minimal.
- Validate API payloads and return actionable error responses.
- Prefer configuration via environment variables over hardcoded values.
- Avoid introducing heavy dependencies unless required for core functionality.

## API design guidance

- Keep endpoint naming resource-oriented and consistent.
- Preserve backward compatibility when changing response schemas.
- For async/long-running operations, expose job status instead of blocking calls.
- Include stable identifiers for datasets, processes, and executions.
- Treat Maps app and Climate app contracts as first-class compatibility targets.
- For collections/coverages, prefer OGC API - Common and OGC API - Coverages compatible response structures and link relations.
- Keep collections and coverages handlers decoupled, with shared helpers/constants in dedicated modules.

## Geospatial/data handling guidance

- Preserve CRS assumptions explicitly and document transformations.
- Make aggregation semantics explicit (e.g., mean/sum/min/max, temporal window).
- Avoid silent unit conversions; require explicit metadata or configuration.
- Prefer standards-aligned metadata (STAC/OGC) where feasible.

## DHIS2 integration guidance

- Keep DHIS2-specific mapping isolated from core EO processing logic.
- Route DHIS2 Web API calls through `dhis2-python-client` unless a clear gap requires direct HTTP usage.
- Processed EO outputs should be pushed to DHIS2 database through the DHIS2 Web API.
- DHIS2 Data Store may be used to store EO API configuration metadata where appropriate.
- Produce import-ready, schema-validated payloads.
- Support dry-run/preview modes before committing imports.

## EO library usage guidance

- Reuse `dhis2eo` primitives for EO/climate extraction, transformation, and aggregation where applicable.
- Avoid re-implementing EO processing logic already covered by `dhis2eo`.

## Upstream contribution guidance

- `eo-api` can drive changes in `dhis2-python-client` and `dhis2eo` when gaps block product requirements.
- Prefer upstream contributions over long-lived local patches.
- Preserve backward compatibility in upstream changes unless a documented breaking change is intentional.
- When temporary local workarounds are needed, track follow-up tasks to remove them after upstream release.

## Testing and validation

- Add targeted tests for transformations, aggregations, and API contracts.
- Prioritize deterministic tests for geospatial processing logic.
- Validate edge cases: missing geometries, nodata handling, CRS mismatch, large AOIs.

## What to avoid

- Do not add unrelated UI layers or non-essential platform features.
- Do not couple orchestration internals tightly to API request handlers.
- Do not hardcode provider-specific assumptions that block future data sources.

## Documentation expectations

- Update docs when adding endpoints, process parameters, or output schema changes.
- Include example requests/responses for new process execution paths.
- Keep `README.md` concise and place endpoint examples in `API_EXAMPLES.md`.
- Keep dataset schema documentation in `datasets/README.md`.
