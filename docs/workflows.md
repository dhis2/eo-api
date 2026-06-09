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

## Built-in workflows

Open Climate Service ships with ready-to-use workflows for aggregating **any published GeoZarr dataset** to a set of **GeoJSON features** (typically DHIS2 organisation units) and exporting DHIS2-ready output:

| Workflow | Output |
|---|---|
| `aggregate_to_dhis2_json` | DHIS2 `dataValueSet` JSON |
| `aggregate_to_chap_csv` | CHAP wide CSV (`time_period`, `location`, one column per variable) |

Both run `load_collection → aggregate_spatial → save_result`: they load the dataset over a time range, compute a spatial statistic of the variable within each feature, and emit one value per feature per time step. Each feature's GeoJSON `id` becomes the DHIS2 `orgUnit` (CHAP `location`), and each time step becomes the DHIS2 `period` (CHAP `time_period`).

### Parameters

| Name | Workflows | Default | Description |
|---|---|---|---|
| `dataset_id` | both | — | Published GeoZarr collection to aggregate (see `/datasets`) |
| `temporal_extent` | both | — | `[start, end]` ISO-8601 dates |
| `geometries` | both | — | GeoJSON `FeatureCollection`; each feature's `id` is the org unit / location |
| `data_element_id` | DHIS2 only | — | DHIS2 data element id assigned to every value |
| `method` | both | `mean` | Spatial aggregation method: `mean`, `min`, `max`, or `sum` |
| `period_type` | both | `month` | Period type used to format each time step: `day`, `week`, `month`, `quarter`, `year` |

> `period_type` **formats** each native time step into a DHIS2 period — it does not re-aggregate in time. Pick a dataset whose native temporal resolution matches the period you want (e.g. a monthly dataset for monthly values).

### Example

Mean monthly precipitation per district, as DHIS2 data values:

```json
{
  "process": {
    "process_graph": {
      "agg": {
        "process_id": "aggregate_to_dhis2_json",
        "arguments": {
          "dataset_id": "era5land_precipitation_monthly",
          "temporal_extent": ["2025-01-01", "2025-12-31"],
          "geometries": { "type": "FeatureCollection", "features": [ "...org units..." ] },
          "data_element_id": "fbfJHSPpUQD",
          "method": "mean",
          "period_type": "month"
        },
        "result": true
      }
    }
  }
}
```

Submit it to `POST /result` (synchronous) or `POST /jobs` (batch); the result is a DHIS2 `dataValueSet` ready to POST to the DHIS2 Web API. For CHAP CSV, call `aggregate_to_chap_csv` with the same arguments minus `data_element_id`.

---

## Mapping change between two periods

`temporal_change` computes the per-pixel **net change** of a variable between the first and last time step in a range (`last − first`) and publishes it as a new single-band GeoZarr dataset — for example population change between two census years, or NDVI change between two dekads. Positive values are increases and negative values decreases, so the result suits a diverging colormap centred on zero.

It runs `load_collection → reduce_dimension → save_result`, reducing the time dimension away to a 2-D `(y, x)` raster. Only the earliest and latest time step in `temporal_extent` contribute; any steps in between are ignored.

### Parameters

| Name | Description |
|---|---|
| `dataset_id` | Published GeoZarr collection to load (see `/datasets`) |
| `output_dataset_id` | Id of the change dataset to publish (needs a dataset template — see below) |
| `variable` | Variable/band name carried through to the published dataset |
| `temporal_extent` | `[start, end]` ISO-8601 dates; the change is `value(last) − value(first)` within this range |

The `output_dataset_id` must have a **dataset template** registered on the instance: a YAML in `plugins_dir/datasets/` with `sync: {kind: static}` and a `display` block. No ingestion plugin (`.py`) is needed — the data is produced by the workflow, not ingested:

```yaml
- id: worldpop_population_change
  name: Population change 2015–2030
  variable: population
  period_type: yearly
  sync:
    kind: static
  display:
    colormap: RdBu
    range: [-500, 500]
```

### Example

Zarr output cannot be produced synchronously, so submit it as a **batch job** (`POST /jobs`, then `POST /jobs/{id}/results`):

```json
{
  "process": {
    "process_graph": {
      "change": {
        "process_id": "temporal_change",
        "arguments": {
          "dataset_id": "worldpop_population_yearly",
          "output_dataset_id": "worldpop_population_change",
          "variable": "population",
          "temporal_extent": ["2015-01-01", "2030-12-31"]
        },
        "result": true
      }
    }
  }
}
```

When the job finishes, the new change dataset appears under `/datasets` and on the map viewer.

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
