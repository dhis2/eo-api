# Processes

The Open Climate Service exposes two complementary processing interfaces:

- **openEO process graphs** — the primary interface for data analysis. Submit a DAG of composable operations via `POST /result` (synchronous) or `POST /jobs` (batch). 120+ standard processes are available out of the box. See the [openEO guide](openeo.md).
- **Native processes** — custom plugin functions registered via YAML, callable synchronously at `POST /processes/{id}/execution` and from openEO process graphs. Shaped to align progressively with [OGC API Processes](https://ogcapi.ogc.org/processes/).

This page documents the native processes. For process graph execution, see [openeo.md](openeo.md).

---

---

## How processes work

A process takes parameters from the JSON request body, executes a computation, and returns a JSON response. The result is typically a new derived dataset artifact that can be opened via the Zarr endpoint or published to the OGC API catalog.

```
POST /processes/{id}/execution
Content-Type: application/json

{ ...parameters... }
```

Available processes are listed at `GET /processes`.

---

## Built-in process: `resample`

The `resample` process aggregates a source dataset to a coarser temporal resolution. It is the primary way to produce daily totals from hourly data, weekly averages from daily data, and so on.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `source_dataset_id` | string | Yes | ID of the source managed dataset to resample |
| `frequency` | string | Yes | Target temporal resolution as a pandas frequency alias (e.g. `1D`, `W-MON`, `MS`) |
| `method` | string | Yes | Aggregation method: `mean`, `sum`, `min`, or `max` |
| `start` | string | Yes | Start of the period range to resample (ISO 8601 date or datetime) |
| `end` | string | No | End of the period range. Defaults to today's UTC date |
| `overwrite` | boolean | No | Re-materialize the derived dataset even if it already exists. Default: `false` |
| `publish` | boolean | No | Publish the result to the OGC API catalog after materializing. Default: `true` |

**Supported frequency aliases:**

| Alias | Meaning |
| --- | --- |
| `1D`, `7D`, `10D`, … | Every N calendar days |
| `W-MON` | ISO weeks starting Monday |
| `MS` | Calendar months (month-start) |
| `QS` | Calendar quarters (quarter-start) |
| `YS` | Calendar years (year-start) |

### Derived dataset ID

The derived dataset is stored under an auto-generated ID:

```
{source_dataset_id}_{frequency_slug}_{method}
```

Where `frequency_slug` is the frequency alias lowercased with non-alphanumeric characters replaced by `_` and leading/trailing underscores stripped. For example:

| Source | Frequency | Method | Derived ID |
| --- | --- | --- | --- |
| `chirps3_precipitation_daily` | `W-MON` | `sum` | `chirps3_precipitation_daily_w_mon_sum` |
| `era5land_temperature_hourly` | `1D` | `mean` | `era5land_temperature_hourly_1d_mean` |
| `chirps3_precipitation_daily` | `MS` | `sum` | `chirps3_precipitation_daily_ms_sum` |

### Idempotency

If a derived dataset artifact already exists for the requested `source_dataset_id`, `frequency`, `method`, and time range, the process returns the existing artifact without re-materializing it. Pass `overwrite: true` to force a rebuild.

### Example: daily mean temperature from hourly ERA5-Land

```bash
curl -s -X POST http://127.0.0.1:8000/processes/resample/execution \
  -H "Content-Type: application/json" \
  -d '{
    "source_dataset_id": "era5land_temperature_hourly",
    "frequency": "1D",
    "method": "mean",
    "start": "2024-01-01",
    "end": "2024-01-31",
    "publish": true
  }' | jq
```

Response:

```json
{
  "artifact_id": "3f2a1b4c-8e7d-4f9a-b2c1-0d5e6f7a8b9c",
  "status": "completed",
  "dataset": {
    "dataset_id": "era5land_temperature_hourly_1d_mean",
    "dataset_name": "era5land_temperature_hourly_1d_mean",
    "variable": "t2m",
    "period_type": "daily",
    ...
  }
}
```

### Example: weekly precipitation totals from CHIRPS daily

```bash
curl -s -X POST http://127.0.0.1:8000/processes/resample/execution \
  -H "Content-Type: application/json" \
  -d '{
    "source_dataset_id": "chirps3_precipitation_daily",
    "frequency": "W-MON",
    "method": "sum",
    "start": "2024-01-01",
    "end": "2024-03-31",
    "publish": true
  }' | jq
```

### Incomplete edge periods

The resampler automatically drops leading and trailing periods that are not fully covered by the source data. For example, if the source daily dataset starts on a Wednesday and you resample to weekly (Monday–Sunday), the first Monday-anchored week is dropped because it only has data from Wednesday onward.

This means the realized time range of the derived artifact may be shorter than the requested range if the source data does not fully cover the first or last target period.

### Opening the derived dataset

Once materialized, the derived dataset can be opened like any other managed dataset:

```python
from open_climate_service.client import Client

api = Client("http://127.0.0.1:8000")
ds = api.open("era5land_temperature_hourly_1d_mean")
print(ds)
```

Or directly via the Zarr endpoint:

```bash
# open in Python
import xarray as xr
ds = xr.open_zarr("http://127.0.0.1:8000/zarr/era5land_temperature_hourly_1d_mean", consolidated=True)
```

---

## Custom processes

You can register additional processes from a `plugins_dir/processes/` directory. See [Extensibility — Processes](extensibility.md#processes) for the YAML format and execution function contract.
