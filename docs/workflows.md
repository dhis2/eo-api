# Workflows

A **workflow** is a reusable, named openEO process graph stored on the server. Instead of repeating the same chain of processes every time, you define it once, give it a name and parameters, and call it by name from any openEO client in any language.

Workflows appear in `GET /process_graphs` and are callable directly by `process_id` in a process graph — exactly like any standard openEO process.

---

## Anatomy of a workflow

A workflow is a JSON file with an `id`, a list of `parameters`, and a `process_graph`:

```json
{
  "id": "monthly_precipitation",
  "summary": "Aggregate daily precipitation to monthly totals",
  "parameters": [
    {
      "name": "collection_id",
      "description": "Dataset id to load",
      "schema": { "type": "string" }
    }
  ],
  "process_graph": {
    "load": {
      "process_id": "load_collection",
      "arguments": { "id": { "from_parameter": "collection_id" } }
    },
    "aggregate": {
      "process_id": "aggregate_temporal_period",
      "arguments": {
        "data": { "from_node": "load" },
        "period": "month",
        "reducer": {
          "process_graph": {
            "sum": {
              "process_id": "sum",
              "arguments": { "data": { "from_parameter": "data" } },
              "result": true
            }
          }
        }
      },
      "result": true
    }
  }
}
```

Calling it from a process graph is a single node:

```json
{
  "process_graph": {
    "1": {
      "process_id": "monthly_precipitation",
      "arguments": { "collection_id": "chirps3_precipitation_daily" },
      "result": true
    }
  }
}
```

---

## Three sources of workflows

Workflows are loaded from three places, each overriding the previous on id collision:

| Source | Location | Loaded |
|---|---|---|
| Built-in | `open_climate_service/plugins/workflows/` | At startup |
| Instance plugin | `plugins_dir/workflows/` | On each request |
| Runtime-registered | `PUT /process_graphs/{id}` | Immediately |

**Instance plugin workflows** (files in `plugins_dir/workflows/`) are re-read on every `GET /process_graphs` call — no restart needed to pick up changes.

**Runtime-registered workflows** are created via the `PUT /process_graphs/{id}` API and stored in the instance data directory. They disappear if the data directory is wiped.

---

## Creating a workflow

### Via file (recommended for instance repos)

Add a `.json` file to your instance `plugins/workflows/` directory:

```
my-instance/
└── plugins/
    └── workflows/
        └── aggregate_for_dhis2.json
```

Configure `plugins_dir` in `climate-service.yaml`:

```yaml
plugins_dir: ./plugins/
```

The workflow appears in `GET /process_graphs` immediately on the next request — no restart required.

### Via API

```bash
curl -X PUT http://localhost:8000/process_graphs/monthly_precipitation \
  -H "Content-Type: application/json" \
  -d @monthly_precipitation.json
```

---

## Using a workflow

### Python (openEO client)

```python
from openeo import connect

conn = connect("http://localhost:8000")

result = conn.execute({
    "process_graph": {
        "1": {
            "process_id": "monthly_precipitation",
            "arguments": { "collection_id": "chirps3_precipitation_daily" },
            "result": True
        }
    }
})
```

### JavaScript (openEO JS client)

```javascript
import { OpenEO } from "@openeo/js-client";

const conn = await OpenEO.connect("http://localhost:8000");

const process = await conn.buildProcess((builder) =>
  builder.monthly_precipitation("chirps3_precipitation_daily")
);

const result = await conn.computeResult(process);
```

The JS client discovers available workflows via `GET /process_graphs` automatically — `monthly_precipitation` appears alongside standard openEO processes.

---

## Listing workflows

```bash
GET /process_graphs
```

Returns all available workflows: built-ins, instance plugins, and runtime-registered, merged together.

---

## API reference

| Method | Path | Description |
|---|---|---|
| `GET` | `/process_graphs` | List all workflows |
| `GET` | `/process_graphs/{id}` | Get one workflow |
| `PUT` | `/process_graphs/{id}` | Create or replace a workflow |
| `DELETE` | `/process_graphs/{id}` | Delete a runtime-registered workflow |

> Note: `DELETE` only removes runtime-registered workflows. Workflows loaded from `plugins_dir/workflows/` files persist until the file is removed — deleting them via the API has no effect.
