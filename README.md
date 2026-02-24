# eo-api

DHIS2 EO API allows data from multiple sources (primarily earth observation data) to be extracted, transformed and loaded into DHIS2 and the Chap Modelling Platform.

## Setup

### Using uv (recommended)

Install dependencies (requires [uv](https://docs.astral.sh/uv/)):

`uv sync`

Start the app:

`uv run uvicorn main:app --reload`

### Using pip (alternative)

If you can't use uv (e.g. mixed conda/forge environments):

```
python -m venv .venv
source .venv/bin/activate
pip install -e .
uvicorn main:app --reload
```

### Using conda

```
conda create -n dhis2-eo-api python=3.13
conda activate dhis2-eo-api
pip install -e .
uvicorn main:app --reload
```

### Makefile targets

- `make sync` — install dependencies with uv
- `make run` — start the app with uv
- `make validate-datasets` — validate all dataset YAML files against the Pydantic schema
- `make test` — run Python tests with pytest

## Example frontend app

A minimal browser UI is available at:

- `http://127.0.0.1:8000/example-app`

This app demonstrates creating and running scheduled imports (for example nightly precipitation/temperature imports aggregated to org units) using the API endpoints.

## Documentation

- API usage examples: [`API_EXAMPLES.md`](API_EXAMPLES.md)
  - Runtime summary example for `GET /`: [`API_EXAMPLES.md#landing-page-runtime-summary`](API_EXAMPLES.md#landing-page-runtime-summary)
- Project presentation deck: [`PRESENTATION.md`](PRESENTATION.md)
- Executive presentation deck (10 slides): [`PRESENTATION_EXECUTIVE.md`](PRESENTATION_EXECUTIVE.md)
- Dataset schema and resolver conventions: [`eoapi/datasets/README.md`](eoapi/datasets/README.md)
- Product requirements and scope: [`PRD.md`](PRD.md)
- Prefect orchestration design: [`PREFECT_INTEGRATION.md`](PREFECT_INTEGRATION.md)
- xclim indicator integration design: [`XCLIM_INTEGRATION.md`](XCLIM_INTEGRATION.md)
- Repository coding guidance for AI edits: [`.github/copilot-instructions.md`](.github/copilot-instructions.md)

## Prefect runtime configuration

To enable Prefect-backed schedule runs (`POST /schedules/{scheduleId}/run`), set:

- `EOAPI_PREFECT_ENABLED=true`
- `EOAPI_PREFECT_API_URL=<prefect-api-base-url>`
- `EOAPI_PREFECT_DEPLOYMENT_ID=<deployment-id-for-eo_aggregate_import_v1>`
- `EOAPI_PREFECT_API_KEY=<optional-api-token>`

If Prefect is disabled (or unavailable), schedule runs fall back to local in-process execution.

## Internal scheduler runtime

Recurring schedules can also run from an internal cron worker in this API process.

- `EOAPI_INTERNAL_SCHEDULER_ENABLED` (optional, default `true`)
- `EOAPI_INTERNAL_SCHEDULER_POLL_SECONDS` (optional, default `30`)

## DHIS2 runtime configuration

To enable live DHIS2 org-unit retrieval and `dataValueSets` import:

- `EOAPI_DHIS2_BASE_URL=<https://your-dhis2-host>`
- Either `EOAPI_DHIS2_TOKEN=<api-token>`
- Or `EOAPI_DHIS2_USERNAME=<username>` and `EOAPI_DHIS2_PASSWORD=<password>`
- Optional: `EOAPI_DHIS2_TIMEOUT_SECONDS=20`

Behavior:

- If DHIS2 is configured, org units are fetched from DHIS2 for `/features/dhis2-org-units/items`.
- If DHIS2 is not configured or unavailable, built-in sample org units are used as fallback.
- Process imports (`dryRun=false`) send `dataValues` to DHIS2 `POST /api/dataValueSets`.
- Process imports (`dryRun=true`) are validated locally and return import-ready payload summaries without writing to DHIS2.

## State persistence

Jobs, schedules, and workflows now persist to JSON state files.

- `EOAPI_STATE_DIR` (optional, default `.cache/state`)
- `EOAPI_STATE_PERSIST` (optional, default `true`; set `false` to disable persistence)

## API security and CORS

- `EOAPI_CORS_ORIGINS` (optional, default `*`) comma-separated allowed origins
- `EOAPI_API_KEY` (optional): if set, write operations (`POST`, `PATCH`, `PUT`, `DELETE`) require `X-API-Key` header

## External OGC federation (collections)

To merge external OGC API - Common collections into local `/collections`, set:

- `EOAPI_EXTERNAL_OGC_SERVICES=<json-array>`

Example:

```json
[
  {
    "id": "demo-provider",
    "title": "Demo OGC Provider",
    "url": "https://example-ogc.test",
    "headers": {
      "X-Client-Id": "eo-api"
    },
    "apiKeyEnv": "DEMO_OGC_API_KEY",
    "authScheme": "Bearer",
    "timeoutSeconds": 20,
    "retries": 1,
    "operations": ["coverage", "position"]
  }
]
```

Provider config fields:

- `headers` (optional): static headers to include in upstream requests.
- `apiKeyEnv` (optional): environment variable name containing upstream API key/token.
- `authScheme` (optional, default `Bearer`): used in `Authorization` header as `<scheme> <token>`; use `none` to send token without a scheme.
- `timeoutSeconds` (optional, default `20`): request timeout per upstream call.
- `retries` (optional, default `0`): number of retries for transient upstream/network failures.
- `operations` (optional): explicit allowlist for proxied operations; supported values are `coverage`, `position`, `area`. If omitted, all operations are allowed. If any unknown value is provided, that provider config is rejected.

Current behavior:

- `/collections` returns local + external collections.
- External collections use federated IDs: `ext:<providerId>:<sourceCollectionId>`.
- `/collections/{collectionId}` supports those federated external IDs.
- `/collections/{collectionId}/coverage` proxies to upstream for federated IDs.
- `/collections/{collectionId}/position` and `/collections/{collectionId}/area` proxy to upstream for federated IDs.

## Workflow JSON schema (MVP)

Custom workflows are created via `POST /workflows` using this shape:

```json
{
  "name": "climate-indicators-workflow",
  "steps": [
    {
      "name": "aggregate-step",
      "processId": "eo-aggregate-import",
      "payload": {
        "inputs": { "...": "process-specific inputs" }
      }
    },
    {
      "name": "indicator-step",
      "processId": "xclim-cdd",
      "payload": {
        "inputs": { "...": "process-specific inputs" }
      }
    }
  ]
}
```

Notes:

- `steps` run sequentially in the order provided.
- `processId` must match an existing process ID from `GET /processes`.
- `payload.inputs` must match the target process execution contract.
- Schedules can target either aggregate-import inputs or a `workflowId`.

## Dataset definitions

Collection metadata for `/collections` is defined in YAML files under `eoapi/datasets/`.

Each dataset uses `eoapi/datasets/<dataset-id>/<dataset-id>.yaml` with matching resolver code in `eoapi/datasets/<dataset-id>/resolver.py`.

For schema details, examples, and current dataset files, see [`eoapi/datasets/README.md`](eoapi/datasets/README.md).
