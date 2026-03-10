# OGC Migration Plan

## Purpose

This plan defines how `eo-api` moves from an internal component chain to a clean OGC-facing platform that supports:

- one generic execution process for DHIS2-oriented workflows,
- discoverable data access APIs (Collections, EDR, Tiles),
- asynchronous job management aligned with OGC API - Processes.

## Current Baseline (March 10, 2026)

- Generic process exists: `generic-dhis2-workflow`.
- Internal component chain exists and is stable:
  1. `features`
  2. `download`
  3. `temporal_aggregation`
  4. `spatial_aggregation`
  5. `dhis2_payload_builder`
- Dataset capabilities endpoint exists:
  - `GET /ogcapi/processes/generic-dhis2-workflow/capabilities`

## Target OGC Surface

Expose the right interface for the right job:

- `Processes` for computation and orchestration.
- `Collections` for discoverable datasets and metadata.
- `EDR` for subsetting/querying environmental data by spatiotemporal patterns.
- `Tiles` for visualization delivery (via TiTiler or equivalent tile backend).

Do not expose every internal component as a public process yet. Keep one primary orchestration process and promote components later only when independently valuable.

## Process vs Collection vs EDR vs Tiles

Use this decision rule:

- If it executes workflow logic, reducers, joins, or payload generation: `Process`.
- If it publishes dataset catalog and static metadata: `Collection`.
- If clients need query patterns like point/area/trajectory/corridor extraction: `EDR`.
- If clients need slippy-map rendering and map UX performance: `Tiles`.

## OGC API - Processes alignment

### Part 3: Workflows and Chaining

Adopt Part 3 semantics by modeling pipeline execution as composable workflow stages. In our architecture:

- `generic-dhis2-workflow` is the public workflow process.
- internal stages are chain nodes selected/handled by capability-aware routing.
- stage pass-through/exit behavior remains internal but traceable in execution metadata.

### Part 4: Job Management

Adopt stronger async lifecycle as first-class:

- support async execution (`Prefer: respond-async`) for long runs,
- keep robust job polling/status endpoints,
- return stable links to results/artifacts,
- define retention policy and cancellation semantics.

## Migration Phases

## Phase 1: Harden process contract (now)

- Keep `generic-dhis2-workflow` as primary process ID.
- Finalize typed input model (Pydantic) and clear validation errors.
- Standardize execution summary fields:
  - status, step traces, exit reason, produced artifact references.
- Ensure capabilities endpoint remains source of truth for dataset/provider/integration support.

Exit criteria:

- deterministic behavior across `chirps3`, `era5`, `worldpop`,
- stable dry-run responses and trace semantics,
- docs/examples for adding a dataset adapter.

## Phase 2: OGC data discoverability

- Add OGC Collections records for supported datasets and derived outputs.
- Expose collection metadata links from process responses where possible.
- Introduce EDR endpoints for high-value query patterns (point/area/time series extraction).

Exit criteria:

- clients can discover data without reading code,
- workflows and data access are linked through documented identifiers.

## Phase 3: Tile delivery and map integration

- Add tile serving for raster outputs via TiTiler integration.
- Standardize output registration path convention:
  - collections / assets / COG references / tile endpoints.
- Add process response links to tiles for immediate visualization.

Exit criteria:

- map client can execute process and visualize output without manual file handling.

## Phase 4: Full async job model

- Move long-running workflows to async-first execution.
- Validate job metadata, result links, retry/cancel behavior.
- Add operational controls: retention windows, artifact cleanup, observability.

Exit criteria:

- reliable long-running production operations with clear job lifecycle.

## Recommended Public API Shape

### Keep now

- `POST /ogcapi/processes/generic-dhis2-workflow/execution`
- `GET /ogcapi/processes/generic-dhis2-workflow/capabilities`

### Add next

- dataset `Collections` entries with rich metadata and links.
- targeted `EDR` endpoints for common extraction patterns.
- tile endpoints for raster outputs.

## Internal Architecture Guidance

- Keep `component -> adapter -> service` layering.
- Keep dataset-specific rules in adapters/services, not in orchestration executor.
- Keep workflow runtime focused on cross-cutting concerns:
  - tracing, step control, timing, error normalization.
- Keep capability catalog split:
  - `provider_capabilities`: what the source ecosystem supports.
  - `integration_capabilities`: what `eo-api + dhis2eo` currently supports.

## Risks and Mitigations

- Risk: exposing too many process IDs early increases maintenance burden.
  - Mitigation: single primary process now; expand only for clear product value.
- Risk: async job behavior differs across backends.
  - Mitigation: conformance tests for execution, status, results, cancellation.
- Risk: output artifacts not consistently discoverable.
  - Mitigation: enforce output metadata contract and link relations in every run.

## Immediate Next Actions

1. Add this plan to team review and agree phase boundaries.
2. Define Collections metadata model for datasets and workflow outputs.
3. Design EDR MVP for point/area extraction backed by existing datasets.
4. Draft TiTiler integration contract for generated rasters/COGs.
5. Implement async job pilot for `generic-dhis2-workflow` and validate job/result lifecycle.
