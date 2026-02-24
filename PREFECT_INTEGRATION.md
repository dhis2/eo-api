# Prefect Integration (Minimal Design)

This document proposes a minimal way to integrate Prefect orchestration into `eo-api` for no-code scheduled and long-running EO workflows.

**Constraint:** This project uses only open-source Prefect (self-hosted). Paid managed services are out of scope.

## 1) Scope and intent

- Keep `eo-api` as API/control plane.
- Use Prefect as orchestration plane for:
  - API-triggered process executions
  - recurring schedules (daily/monthly/yearly)
- Preserve current OGC-aligned API contracts and existing `/jobs/{jobId}` model.
- Reuse `dhis2eo` and `dhis2-client` inside flow/task steps.

## 2) Why Prefect for this project

- Python-native flow definitions align with current FastAPI codebase.
- Lower operational overhead than Airflow for early-stage product development.
- Good fit for API-triggered runs and dynamic parameters from process execution requests.
- Built-in state model can be translated cleanly into API job states.

## 3) Minimal architecture

Components:

- `eo-api` (FastAPI)
  - validates process execution payloads
  - creates stable `jobId`
  - triggers Prefect deployment/flow run
  - exposes status/result via `/jobs/{jobId}`
- Self-hosted Prefect server + workers (open-source deployment)
  - executes EO pipeline flows
- Shared storage/cache
  - intermediate EO files and result artifacts
- State mapping store
  - maps `jobId` ↔ `flow_run_id`

Suggested mapping fields:

- `jobId` (API)
- `flowName` (Prefect flow/deployment)
- `flowRunId` (Prefect)
- `status` (`queued|running|succeeded|failed`)
- `progress` (0–100)
- `resultRef` (artifact path/link)

## 4) Flow boundary and task contract

Recommended flow name:

- `eo_aggregate_import_v1`

Recommended tasks:

1. `validate_inputs`
2. `load_org_units`
3. `extract_source_data`
4. `aggregate_to_org_units`
5. `build_dhis2_payload`
6. `import_to_dhis2`
7. `publish_results`

Task IO contract:

- Inputs: `jobId`, `datasetId`, `parameters`, `datetime/start/end`, `orgUnitLevel`, `aggregation`, `dhis2` options
- Outputs (JSON-serializable):
  - `orgUnitCount`
  - `rowCount`
  - `payloadPreviewPath`
  - `importSummary`
  - `resultFeaturesPath`

## 5) API interaction model

### Trigger from process endpoint

- Endpoint: `POST /processes/eo-aggregate-import/execution`
- Behavior:
  - create `jobId`
  - submit Prefect flow run with request inputs
  - persist `jobId` ↔ `flowRunId`
  - return `202` with monitor/result links

### Monitor through job endpoint

- Endpoint: `GET /jobs/{jobId}`
- Behavior:
  - lookup `flowRunId`
  - fetch Prefect state
  - map state to API status
  - return status/progress/import summary/result links

State mapping (minimal):

- `queued` → Prefect: `Scheduled`, `Pending`
- `running` → Prefect: `Running`
- `succeeded` → Prefect: `Completed`
- `failed` → Prefect: `Failed`, `Crashed`, `Cancelled`

## 6) Scheduling model

Use Prefect deployments/schedules for:

- daily ERA5-Land import
- daily CHIRPS import
- yearly WorldPop refresh

Each schedule should support parameterized deployment inputs (dataset, period, org unit level, dryRun).

## 7) Operational controls

- Retries/backoff per task for transient failures (provider/network).
- Task and flow-level timeouts.
- Idempotent import guardrails (same period/org unit reruns do not duplicate).
- Structured artifacts for payload previews and import summaries.
- `dryRun=true` default for safety in no-code workflows.

## 8) Security and secret handling

- Store credentials in self-hosted Prefect blocks/secret store or environment-managed secret backend.
- Do not log sensitive credentials in flow/task logs.
- Redact sensitive fields in API and worker logs.

## 9) Suggested rollout phases

Phase 1 (minimal):

- one Prefect-backed flow for `eo-aggregate-import`
- API trigger + `/jobs/{jobId}` polling
- one scheduled daily ERA5 dry-run deployment

Phase 2:

- CHIRPS and WorldPop schedules
- richer progress reporting per step
- payload preview retrieval endpoint

Phase 3:

- multi-tenant schedule configuration
- stronger alerting/notification integration
- policy-based retries and escalation

## 10) Fit with current eo-api direction

- Supports no-code scheduled workflows without overloading request handlers.
- Keeps endpoint contracts stable for DHIS2 Maps and Climate app paths.
- Aligns with product priority on end-to-end correctness and reproducibility.
- Preserves strategic library usage (`dhis2eo`, `dhis2-client`) with clear task boundaries.
