# Adding custom datasets

This guide explains how to add a new dataset source to your Open Climate Service instance — for example a national meteorological service, a regional satellite product, or a custom model output.

The built-in dataset templates (CHIRPS3, ERA5-Land, WorldPop) ship as package data. Custom datasets are layered on top by pointing `plugins_dir` in your `climate-service.yaml` at a plugins directory. That directory serves two purposes: YAML dataset templates go in its `datasets/` subfolder, and Python modules placed directly under it are importable by their dotted path (e.g. `mypackage.sources.download`) without installing them as a package.

## Overview

Adding a custom dataset involves two things:

1. **A download function** — a Python function that downloads data and writes it as one or more NetCDF files to a given directory.
2. **A dataset template YAML** — a file that describes the dataset and tells the API which download function to call.

## Step 1: Write the download function

The download function must be importable as a dotted Python path. The API calls it with keyword arguments and ignores the return value — the function is expected to write NetCDF files to `dirname` using `prefix` as the filename prefix.

```python
# mypackage/sources/enacts.py
from pathlib import Path

def download(
    *,
    start: str,         # ISO 8601 date or datetime
    end: str,
    dirname: Path,      # directory to write output files into
    prefix: str,        # filename prefix (use e.g. f"{prefix}_{year}.nc")
    overwrite: bool,
    bbox: list[float],  # [xmin, ymin, xmax, ymax] — include only if your source needs it
    **kwargs: object,   # absorbs default_params from the YAML template
) -> None:
    """Download ENACTS rainfall and write NetCDF files to dirname."""
    ...
```

**Required parameters** — always passed by the API:

| Parameter   | Type       | Description |
| ----------- | ---------- | ----------- |
| `start`     | `str`      | Start of the requested time range (ISO 8601) |
| `end`       | `str`      | End of the requested time range (ISO 8601) |
| `dirname`   | `Path`     | Directory to write output NetCDF files into |
| `prefix`    | `str`      | Filename prefix for output files |
| `overwrite` | `bool`     | Whether to overwrite existing cached files |

**Optional parameters** — passed only when present in the function signature:

| Parameter      | Type            | Description |
| -------------- | --------------- | ----------- |
| `bbox`         | `list[float]`   | Bounding box as `[xmin, ymin, xmax, ymax]` — include this if your source requires a spatial filter |
| `country_code` | `str`           | ISO 3166-1 alpha-3 code — include this if your source (e.g. WorldPop) requires a country code |

Any extra keyword arguments from `ingestion.default_params` in the YAML template are forwarded as additional kwargs.

The API normalises coordinate names at write time: `valid_time` → `time`, `lat`/`latitude` → `y`, `lon`/`longitude` → `x`. Using the canonical names in your output avoids any ambiguity, but upstream names are handled automatically.

Install your package in the same environment as the Open Climate Service:

```bash
pip install ./mypackage
```

## Step 2: Create a dataset template YAML

Create a directory for your custom templates and add a YAML file. Each file contains a list of templates (even if there is only one):

```yaml
# datasets/enacts_rainfall.yaml
- id: enacts_rainfall_daily
  name: ENACTS Rainfall (daily)
  short_name: Rainfall
  variable: rainfall
  period_type: daily
  sync:
    kind: temporal
    execution: append
  ingestion:
    function: mypackage.sources.enacts.download
  units: mm
  resolution: 4 km x 4 km
  source: ENACTS
  source_url: https://enacts.example.org
```

### Template field reference

**Identity**

| Field        | Required | Description |
| ------------ | -------- | ----------- |
| `id`         | Yes | Unique template identifier. This becomes the dataset ID in the API, e.g. `enacts_rainfall_daily` |
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
| `ingestion.function` | Legacy | Dotted path to the download function |
| `ingestion.plugin` | New path | Dotted path to a streaming plugin class |
| `ingestion.default_params` | No | Extra keyword arguments applied to the selected ingestion contract (legacy download function and/or streaming plugin) |

The platform currently supports two ingestion contracts:

- `ingestion.function` for the legacy download-to-NetCDF path
- `ingestion.plugin` for the new per-period streaming path

The streaming plugin contract is:

```python
class MyStreamingPlugin:
    max_concurrency = 4
    commit_batch_size = 30

    async def probe(self, bbox: list[float], **params) -> GridSpec:
        ...

    async def periods(self, start: str, end: str) -> list[str]:
        ...

    async def fetch_period(self, period_id: str, bbox: list[float], **params) -> xr.Dataset:
        ...
```

For streaming plugins, `ingestion.default_params` may be used both:

- as constructor kwargs when the plugin class is instantiated
- as `**params` forwarded into `probe(...)` and `fetch_period(...)`

Current limitation: only the initial streaming ingest path is implemented.
Plugin-backed datasets currently rematerialize on sync rather than using delta
append. Rechunking, pyramid behavior, and full migration away from
`ingestion.function` for all remaining built-ins are follow-up work.

**Transforms** — applied after download, before writing to Zarr:

```yaml
transforms:
  - open_climate_service.transforms.kelvin_to_celsius
  - mypackage.transforms.my_custom_transform
```

See [Transforms](transforms.md) for the full pipeline description, built-in options, and how to write a custom transform.

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

If an ingest request's bounding box has no overlap with `extents.spatial.bbox`, the API returns HTTP 400 immediately. Partial overlap is allowed — the provider will return data for the intersecting area.

**Units and display**

| Field | Required | Description |
| ----- | -------- | ----------- |
| `units` | No | Physical units of the stored data (e.g. `mm`, `degC`, `m`) |
| `resolution` | No | Human-readable spatial resolution (e.g. `5 km x 5 km`) |
| `display.colormap` | No | Colormap name for map rendering (e.g. `blues`, `rdbu_r`) |
| `display.range` | No | `[min, max]` display range for the colormap |
| `display.nodata` | No | No-data / fill value |

**Multiscale pyramid** — pyramid Zarr stores are built automatically when the ingested dataset's spatial dimensions exceed 2048×2048 pixels. No YAML configuration is required; the pyramid level count is derived from the data shape and coarsening always uses mean aggregation.

## Step 3: Point the instance at your plugins directory

Add `plugins_dir` to your `climate-service.yaml` and place your YAML file in the `datasets/` subfolder:

```
plugins/
└── datasets/
    └── enacts_rainfall.yaml
```

```yaml
extent:
  name: Rwanda
  bbox: [28.8, -2.9, 30.9, -1.0]

data_dir: ./data
plugins_dir: ./plugins/
```

All `*.yaml` files in `plugins_dir/datasets/` are loaded and merged with the built-in templates (CHIRPS3, ERA5-Land, WorldPop). Custom templates are additive — the built-ins remain available unless you deliberately override one by using the same `id`.

## Step 4: Ingest and publish

Once the API is running with `CLIMATE_SERVICE_CONFIG` pointing to your updated config, ingest as usual:

```bash
curl -s -X POST http://127.0.0.1:8000/ingestions \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_id": "enacts_rainfall_daily",
    "start": "2024-01-01",
    "end": "2024-01-31",
    "prefer_zarr": true,
    "publish": true
  }' | jq
```

Verify it appears in the STAC catalog:

```bash
curl -s http://127.0.0.1:8000/stac/catalog.json | jq '.links[] | select(.rel == "child")'
```

## Minimal example

The smallest valid template for a static dataset with no sync:

```yaml
- id: my_static_dataset
  name: My static dataset
  variable: value
  period_type: daily
  sync:
    kind: static
  ingestion:
    function: mypackage.sources.my_source.download
```
