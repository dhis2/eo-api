# Architecture

This document explains how the Open Climate Service is structured, why it is structured that way, and what the consequences are of each design decision. It is written for developers who will maintain or extend the platform over time.

---

## Core concepts

The platform has four first-class concepts. Understanding the distinction between them is the foundation for understanding everything else.

### Dataset template

A **template** is a YAML blueprint that describes a data source. Built-ins live in `open_climate_service/plugins/datasets/` inside the package (loaded via `importlib.resources`). Custom templates live in `{plugins_dir}/datasets/` where `plugins_dir` is set in `climate-service.yaml`. It has no state — it describes what _could_ be ingested, not what _has been_ ingested.

A template defines:

- the dataset identifier and display metadata
- the variable name, units, and period type
- how to ingest the data (`ingestion.plugin` — a dotted path to a streaming plugin class)
- any data transformations applied inside the plugin before returning
- what sync strategy to use (`sync.kind`, `sync.execution`)

Templates are config, not code. If a template needs custom logic, the logic goes into a Python function referenced by dotted path from the YAML.

### Streaming ingest

Datasets are ingested via a per-period streaming contract. The plugin probes the source grid, enumerates periods, and fetches one period at a time as an `xarray.Dataset`. The framework handles resume, concurrency, store commits, artifact persistence, and publication.

The streaming engine lives in `open_climate_service.streaming`, while `open_climate_service.ingestions` is the application-facing layer that owns routes, artifact records, and publication state.

### Artifact

An **artifact** is the internal record of a completed data ingestion. It is the persistence layer — not a public API concept. Each ingestion produces exactly one artifact, which records:

- what dataset template it came from
- the exact spatial extent and time range that was materialized
- where the data lives on disk (path to the zarr store or netCDF files)
- when it was created
- whether it has been published

Multiple artifacts can exist for the same dataset template if data was ingested at different times (they form the version history). The most recent artifact for a given `dataset_id` is what the public API serves.

Artifacts are stored in `{data_dir}/artifacts/records.json`, where `data_dir` is the path configured in `climate-service.yaml`. This is an internal implementation detail — consumers should never depend on artifact IDs or artifact paths directly.

### Managed dataset

A **managed dataset** is the public-facing view of the most recent artifact for a given template. It is what `/datasets`, `/zarr`, and `/stac` expose. When an operator ingests or syncs a dataset, the managed dataset view updates to reflect the new artifact — the public ID stays stable.

The relationship is: one template → many artifacts over time → one managed dataset (the latest).

### Extent

The **extent** is the spatial bounding box configured for this Open Climate Service instance. It is set once in `climate-service.yaml` and does not change at runtime. Every ingestion is automatically scoped to this extent — operators do not specify it per-request.

This is a deliberate design constraint: each instance serves one place. A Sierra Leone instance serves Sierra Leone. Multi-country coverage requires multiple instances.

---

## Data lifecycle

```
Template (YAML)
    │
    │  POST /ingestions  (or  POST /sync)
    ▼
Ingestion
    │    probe source grid
    │    enumerate periods
    │    fetch missing periods
    │    append each period directly to Icechunk-backed Zarr v3
    │
    │  compute coverage (spatial + temporal extent of actual data)
    ▼
Artifact (internal record)
    │
    │  publish=true
    ▼
Managed dataset (public API)
    ├── /datasets/{id}         — native metadata
    ├── /zarr/{id}             — raw zarr store access
    └── /stac/collections/{id} — STAC discovery
```

The plugin owns source probing, period enumeration, and fetching one period as an `xarray.Dataset`. The framework owns job callbacks, artifact persistence, publication metadata, and all public API integration. Plugins do not need to know about Zarr conventions or STAC.

---

## Sync kinds

The `sync.kind` field in a template determines how a managed dataset is kept current.

| `sync.kind` | On each sync                            | Use when                                                          |
| ----------- | --------------------------------------- | ----------------------------------------------------------------- |
| `temporal`  | Append new time steps, or rematerialize | Historical record that grows over time (CHIRPS, ERA5-Land)        |
| `release`   | Rematerialize if a newer release exists | Versioned releases where each year/version is discrete (WorldPop) |
| `static`    | Never synced                            | One-time fixed dataset with no updates                            |

### The sync execution modes

Within `sync.kind: temporal`, two execution modes control what happens when new data is available:

- `append` — downloads only the missing time range and appends it to the existing artifact
- `rematerialize` — discards the existing artifact and rebuilds it from scratch

`append` is efficient for large historical datasets (avoid re-downloading years of data on each sync). `rematerialize` is appropriate when old data may change retroactively (e.g. reanalysis products that are corrected after the fact).

### Availability clamping

Each plugin's `periods(start, end)` method is responsible for returning only periods that are actually available from the source. The sync planner calls `plugin.periods()` before deciding the sync action and uses the result to clamp `target_end` to what the plugin reports as available, avoiding jobs that would fetch nothing.

---

## Processes and jobs

### openEO process catalog

`GET /processes` serves the openEO process catalog — standard processes from `openeo-processes-dask` plus any `@process`-decorated plugin functions loaded from `plugins_dir/processes/`. Processes are executed via openEO process graphs submitted to `POST /jobs` (async batch) or `POST /result` (synchronous).

### Ingestion and sync

Ingestion and sync are domain operations with their own dedicated routes — they are not exposed as openEO processes.

```text
POST /ingestions                  → synchronous, or
POST /ingestions + Prefer: respond-async → 202 + Location: /ingestions/jobs/{id}

POST /sync/{dataset_id}           → synchronous, or
POST /sync/{dataset_id} + Prefer: respond-async → 202 + Location: /ingestions/jobs/{id}
```

### Job

A job is the operational state of one async execution. The same job runtime is shared between async ingestion, async sync, and openEO batch jobs (the latter tracked at `/jobs/{id}`; native ingestion/sync jobs tracked at `/ingestions/jobs/{id}`).

Jobs provide:

- status tracking
- progress reporting
- cursor/checkpoint persistence
- cancellation
- retry and recovery after restart
- a durable result or error record

---

## The plugin contract

The platform has three plugin types. Each has a narrow contract — the framework handles everything else automatically.

### Streaming plugin

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

Responsibilities are split: the plugin knows the source; the orchestrator owns resume, concurrency, and store commits; `open_climate_service.ingestions` owns artifacts, publication, and API responses.

Multiple YAML templates can reference the same plugin class and differentiate via `ingestion.params`. This is the intended pattern for sources that expose multiple variables:

```yaml
# era5land_temperature_hourly.yaml
ingestion:
  plugin: dhis2eo.streaming.era5_land.ERA5LandPlugin
  params:
    variable: 2m_temperature

# era5land_precipitation_hourly.yaml
ingestion:
  plugin: dhis2eo.streaming.era5_land.ERA5LandPlugin
  params:
    variable: total_precipitation
```

### Named openEO process (`@process`)

```python
from open_climate_service.process import process

@process(summary="My custom index")
def my_index(data: xr.DataArray, thresh: float = 0.5) -> xr.DataArray:
    """Compute a custom climate index."""
    return (data > thresh).astype(float)
```

Functions decorated with `@process` and placed in `plugins_dir/processes/` are registered as named openEO processes. They appear in `GET /processes` and are callable directly by `process_id` in any openEO process graph. Parameter types and defaults are derived from the function signature.

---

## GeoZarr root attributes

Every zarr artifact must have GeoZarr root attributes for map rendering to work correctly. These are written into `zarr.json` at the store root:

- `spatial:bbox` — `[xmin, ymin, xmax, ymax]` in the native CRS
- `proj:code` — the CRS EPSG code (e.g. `EPSG:32633` for UTM, `EPSG:4326` for WGS84)
- `zarr_conventions` — GeoZarr convention declaration

The map viewer reads `spatial:bbox` and `proj:code` to determine where to position tiles on the map.

**The framework writes these attributes — plugins do not.** They are written after reprojection, using the actual coordinate bounds of the final written data and the instance CRS.

---

## Artifact deduplication and version history

When a new ingestion request arrives, the framework checks whether an existing artifact already covers the requested scope:

- same `dataset_id`
- same bbox (from the configured extent)
- overlapping time range

If a match exists and `overwrite=false`, the existing artifact is returned without re-downloading. If `overwrite=true`, the existing artifact is replaced.

The artifact store keeps the full history of records for sync deduplication and provenance. Old artifacts are not deleted automatically. For long-running instances, `records.json` grows over time. The long-term direction is a proper transactional store, but for the current scale (tens of artifacts per instance) a JSON file is adequate.

---

## What the framework guarantees

Plugin code (streaming plugins, `@process` functions) can rely on the following being handled automatically by the framework:

| Concern                                               | Where handled                               |
| ----------------------------------------------------- | ------------------------------------------- |
| Coordinate name normalisation (`lat` → `y`, etc.)     | `build_dataset_zarr`                        |
| Reprojection to instance CRS                          | `reproject_to_instance_crs`                 |
| Zarr chunking (auto-sized from `extents.temporal.resolution`) | `_compute_time_space_chunks`         |
| Multiscale pyramid generation (when dims > 2048×2048) | `build_dataset_zarr`                        |
| GeoZarr root attributes (`spatial:bbox`, `proj:code`) | `build_dataset_zarr`                        |
| Artifact coverage computation                         | `_coverage_from_dataset`                    |
| Artifact record persistence                           | `_store_artifact`                           |
| STAC publication                                      | `publish_artifact_record` if `publish=true` |
| STAC collection generation                            | Dynamic from artifact record                |

Plugin code only needs to produce data. Everything else is the framework's responsibility.

---

## Consequences of design choices

### Single extent per instance

Each instance is configured for one place. This keeps the data model simple (no per-artifact extent tags) and the zarr stores small (country-scale downloads rather than global). The trade-off is that a national ministry with sub-national data needs either runs multiple instances or configures a single instance at national extent.

### Temporal gaps are not allowed

The sync engine validates that new data connects to the end of the existing artifact before appending. If a gap exists, the sync fails rather than silently producing a dataset with a hole. This is a deliberate constraint: downstream consumers (DHIS2, CHAP) depend on continuous time series and should not receive data with silent gaps.

### The append execution mode avoids re-downloading history

`append` downloads only the missing range and rebuilds the full zarr from all cached files. This means the local cache (NetCDF files in `data/downloads/`) is the source of truth for the full time series; the zarr is a derived view. If the cache is deleted, a rematerialize is required to recover.
