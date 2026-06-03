# Adding custom datasets

This guide explains how to add a new dataset source to your Open Climate Service instance — for example a national meteorological service, a regional satellite product, or a custom model output.

The built-in dataset templates (CHIRPS3, ERA5-Land, WorldPop) ship as package data. Custom datasets are layered on top by pointing `plugins_dir` in your `climate-service.yaml` at a plugins directory.

## Overview

Adding a custom dataset involves two things:

1. **A streaming plugin** — a Python class that probes the source, enumerates periods, and fetches one period at a time as an `xarray.Dataset`.
2. **A dataset template YAML** — a file that describes the dataset and tells the API which plugin class to use.

Place both in your `plugins/datasets/` directory:

```
plugins/
└── datasets/
    ├── enacts_rainfall.yaml
    └── enacts.py          # the plugin class
```

## Step 1: Write the streaming plugin

The plugin class must implement three async methods. The framework handles resume, concurrency, store commits, artifact registration, and publication.

```python
# plugins/datasets/enacts.py
import xarray as xr
from open_climate_service.streaming.protocol import GridSpec

class ENACTSRainfallPlugin:
    max_concurrency = 2    # how many periods to fetch concurrently
    commit_batch_size = 10 # commit to the Zarr store after this many periods

    async def probe(self, bbox: list[float], **params) -> GridSpec:
        """Return the native grid specification for the given bounding box."""
        ...

    async def periods(self, start: str, end: str) -> list[str]:
        """Return the list of period ids available between start and end."""
        ...

    async def fetch_period(
        self, period_id: str, bbox: list[float], **params
    ) -> xr.Dataset:
        """Fetch one period and return it as an xarray Dataset."""
        ...
```

**`probe`** — called once before fetching begins. Returns a `GridSpec` describing the native resolution and CRS of the source. The framework uses this to determine chunking and reprojection.

**`periods`** — returns an ordered list of period identifiers (typically ISO 8601 date strings) that the source has available between `start` and `end`. The framework uses this list to determine which periods are missing and need to be fetched.

**`fetch_period`** — fetches exactly one period and returns it as an `xarray.Dataset`. The framework appends it directly to the Icechunk-backed Zarr store. The function should not write to disk.

`**params` in both `probe` and `fetch_period` receives the `params` dict from the YAML template, making it possible to reuse the same class for multiple variables.

## Step 2: Create a dataset template YAML

```yaml
# plugins/datasets/enacts_rainfall.yaml
- id: enacts_rainfall_daily
  name: ENACTS Rainfall (daily)
  short_name: Rainfall
  variable: rainfall
  period_type: daily
  sync:
    kind: temporal
    execution: append
  ingestion:
    plugin: datasets.enacts.ENACTSRainfallPlugin
  units: mm
  resolution: 4 km x 4 km
  source: ENACTS
  source_url: https://enacts.example.org
```

### Template field reference

**Identity**

| Field        | Required | Description |
| ------------ | -------- | ----------- |
| `id`         | Yes | Unique template identifier. This becomes the dataset ID in the API |
| `name`       | Yes | Full human-readable name shown in API responses and STAC metadata |
| `short_name` | No  | Short label used in compact displays |
| `variable`   | Yes | Name of the data variable in the Zarr store (e.g. `precip`, `t2m`, `rainfall`) |
| `source`     | No  | Name of the upstream data source |
| `source_url` | No  | URL to the upstream dataset documentation or landing page |

**Period and sync**

| Field | Required | Description |
| ----- | -------- | ----------- |
| `period_type` | Yes | Temporal resolution: `hourly`, `daily`, `monthly`, `yearly` |
| `sync.kind` | Yes | `temporal` — data grows over time; `release` — versioned releases; `static` — never synced |
| `sync.execution` | No | `append` — new time steps appended to existing store; `rematerialize` — full rebuild on each sync |
| `sync.availability` | No | Provider availability policy — see below |

**Sync availability** — how the API determines the latest available data:

```yaml
sync:
  kind: temporal
  execution: append
  availability:
    latest_available_function: open_climate_service.providers.availability.lagged_latest_available
    lag_hours: 48
```

| Field | Description |
| ----- | ----------- |
| `latest_available_function` | Dotted path to a built-in availability function in `open_climate_service.providers.availability` |
| `lag_hours` / `lag_days` | Data is delayed by this many hours or days |
| `allow_future` | Allow requesting future dates (e.g. forecasts or projections). Default: `false` |

Omit `sync.availability` entirely for `static` datasets or when you always want to sync up to the requested end date.

**Ingestion**

| Field | Required | Description |
| ----- | -------- | ----------- |
| `ingestion.plugin` | Yes | Dotted path to the streaming plugin class |
| `ingestion.params` | No | Extra keyword arguments forwarded to `probe` and `fetch_period` as `**params`, and to the plugin constructor |

Multiple templates can share the same plugin class and differ only in `params`:

```yaml
- id: era5land_temperature_hourly
  ingestion:
    plugin: open_climate_service.plugins.datasets.era5_land.ERA5LandHourlySingleBandPlugin
    params:
      variable: 2m_temperature

- id: era5land_precipitation_hourly
  ingestion:
    plugin: open_climate_service.plugins.datasets.era5_land.ERA5LandPrecipitationPlugin
    params:
      variable: total_precipitation
```

**Spatial and temporal extents** — declares what the source dataset covers. Used to validate ingest requests before hitting the provider:

```yaml
extents:
  spatial:
    bbox: [-180, -50, 180, 50]   # [xmin, ymin, xmax, ymax] in WGS84
    crs: http://www.opengis.net/def/crs/OGC/1.3/CRS84
  temporal:
    begin: "1981-01-01"
    end: "2030-12-31"            # omit if ongoing
    trs: http://www.opengis.net/def/uom/ISO-8601/0/Gregorian
    resolution: P1D              # ISO 8601 duration: PT1H, P1D, P1M, P1Y
```

**Units and display**

| Field | Required | Description |
| ----- | -------- | ----------- |
| `units` | No | Physical units of the stored data (e.g. `mm`, `degC`, `m`) |
| `resolution` | No | Human-readable spatial resolution (e.g. `5 km x 5 km`) |
| `display.colormap` | No | Colormap name for map rendering (e.g. `blues`, `rdbu_r`) |
| `display.range` | No | `[min, max]` display range for the colormap |
| `display.nodata` | No | No-data / fill value |

## Step 3: Point the instance at your plugins directory

Add `plugins_dir` to your `climate-service.yaml`:

```yaml
extent:
  name: Rwanda
  bbox: [28.8, -2.9, 30.9, -1.0]

data_dir: ./data
plugins_dir: ./plugins/
```

All `*.yaml` files in `plugins_dir/datasets/` are loaded and merged with the built-in templates. Custom templates are additive — the built-ins remain available unless you deliberately override one by using the same `id`.

Since `plugins_dir` is added to `sys.path`, the plugin class at `datasets.enacts.ENACTSRainfallPlugin` is importable without installing a package.

## Step 4: Ingest and publish

Once the API is running with `CLIMATE_SERVICE_CONFIG` pointing to your updated config:

```bash
curl -s -X POST http://127.0.0.1:8000/ingestions \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_id": "enacts_rainfall_daily",
    "start": "2024-01-01",
    "end": "2024-01-31",
    "publish": true
  }' | jq
```

Verify it appears in the STAC catalog:

```bash
curl -s http://127.0.0.1:8000/stac/catalog.json | jq '.links[] | select(.rel == "child")'
```
