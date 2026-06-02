# Architecture

This document explains how the Open Climate Service is structured, why it is structured that way, and what the consequences are of each design decision. It is written for developers who will maintain or extend the platform over time.

---

## Core concepts

The platform has four first-class concepts. Understanding the distinction between them is the foundation for understanding everything else.

### Dataset template

A **template** is a YAML blueprint that describes a data source. Built-ins live in `open_climate_service/data/datasets/` inside the package (loaded via `importlib.resources`). Custom templates live in `{plugins_dir}/datasets/` where `plugins_dir` is set in `climate-service.yaml`. It has no state — it describes what _could_ be ingested, not what _has been_ ingested.

A template defines:

- the dataset identifier and display metadata
- the variable name, units, and period type
- how to ingest the data (`ingestion.function` today, `ingestion.plugin` for the
  new streaming path)
- what transforms to apply (`transforms`)
- what sync strategy to use (`sync.kind`, `sync.execution`)

Templates are config, not code. If a template needs custom logic, the logic goes into a Python function referenced by dotted path from the YAML.

### Streaming ingest

The platform is currently in a transition between two ingestion strategies:

- the legacy download-and-rebuild path based on `ingestion.function`
- the new per-period streaming path based on `ingestion.plugin`

The new path is implemented internally in `open_climate_service.streaming`, while
`open_climate_service.ingestions` remains the application-facing layer that owns routes,
artifact records, and publication state.

For the first implementation slice:

- CHIRPS3 initial ingest uses the streaming path
- CHIRPS3 no longer depends on `ingestion.function`
- data is written directly into flat Icechunk-backed Zarr v3 stores with
  GeoZarr metadata
- resume is based on committed store state plus an optional job cursor
- sync is not yet store-based; plugin-backed datasets currently rematerialize on
  sync rather than using delta append
- `/zarr/{dataset_id}` serving for Icechunk-backed datasets is not yet exposed
- rechunking and pyramid behavior are deferred

This split is intentional. It keeps the first streaming implementation
end-to-end for one source without mixing in the later sync and storage-finality
work.

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

A **managed dataset** is the public-facing view of the most recent artifact for a given template. It is what `/datasets`, `/zarr`, `/stac`, and `/ogcapi` expose. When an operator ingests or syncs a dataset, the managed dataset view updates to reflect the new artifact — the public ID stays stable.

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
    │  legacy path:
    │    call ingestion function → NetCDF files on disk
    │    apply transforms
    │    reproject to instance CRS
    │    write GeoZarr store
    │
    │  streaming path:
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
    ├── /stac/collections/{id} — STAC discovery
    └── /ogcapi/collections/{id} — OGC API access
```

The legacy ingestion function is called identically by both `POST /ingestions`
and `POST /sync` — the framework invokes it the same way regardless of the
trigger. A correctly written ingestion function works for both without any
changes.

For the legacy path, the framework is responsible for everything from "write
zarr" onward. An ingestion function only needs to write NetCDF files to a given
directory. The framework then:

1. reads and normalises the coordinate names
2. applies transforms (unit conversion, etc.)
3. reprojects to the instance CRS
4. builds the zarr store with auto-computed chunking
5. writes GeoZarr root attributes (`spatial:bbox`, `proj:code`) so map clients can position tiles
6. computes artifact coverage (spatial bounds + time range) from the written data
7. stores the artifact record
8. publishes the managed dataset through pygeoapi if `publish=true`

This division means that ingestion functions do not need to know about zarr
conventions, STAC, OGC, or pygeoapi. They write data files; the framework
handles everything else.

For the streaming path, the division is different: the plugin owns source
probing, period enumeration, and fetching one period as an `xarray.Dataset`.
The framework still owns job callbacks, artifact persistence, publication
metadata, and all public API integration.

---

## Processes, execution, and jobs

The platform is moving toward a shared process-based execution model.

The hierarchy is:

```text
/processes
  |
  +-- /processes/ingestion
  |      |
  |      +-- /execution
  |             |
  |             +-- creates or runs a job
  |                    |
  |                    +-- calls the ingestion execution function
  |                           |
  |                           +-- open_climate_service.ingestions.services
  |                                  |
  |                                  +-- open_climate_service.streaming      (new path)
  |                                  +-- legacy download path       (old path)
  |
  +-- /processes/resample
         |
         +-- /execution
                |
                +-- creates or runs a job
                       |
                       +-- calls the resample execution function
                              |
                              +-- open_climate_service.processing.services
                              +-- open_climate_service.ingestions.services
```

The important distinction is:

- **process** — a named operation the system can perform
- **execution** — an invocation of that operation
- **job** — the persisted runtime record of one execution

### Process

A process is the catalog-level concept. It defines:

- the public operation id, for example `ingestion` or `resample`
- the input and output contract
- whether sync and/or async execution is supported
- the Python function that implements the operation

Examples:

- `ingestion` materializes a managed dataset from a dataset template
- `resample` derives a new managed dataset from an existing one

### Execution

`/execution` means “run this process now”.

Execution is an invocation surface shared by all processes. It is not specific
to ingestion, resampling, or any one domain operation. This gives the platform
one consistent way to run long-lived work.

### Job

A job is the operational state of one execution. Jobs sit at the runtime layer,
not at the domain layer.

Jobs provide:

- status tracking
- progress reporting
- cursor/checkpoint persistence
- cancellation
- retry and recovery after restart
- a durable result or error record

This is why jobs belong under execution: they describe *how one invocation is
running*, not *what the invocation means*.

### Domain processes on the shared runtime

Both ingestion and resampling use the same execution substrate:

```text
process definition
    -> execution request
        -> job runtime
            -> domain service
                -> artifact / dataset update
```

For ingestion:

- the domain goal is to materialize a managed dataset
- the implementation may use the new `streaming` engine or the old download
  path depending on the dataset contract

For resampling:

- the domain goal is to derive a new managed dataset from an existing one
- the implementation uses the processing/resample services, then persists the
  result through the same artifact layer

The jobs framework is therefore horizontal. It does not need to know whether a
process is ingestion, resampling, or something else later. It only needs to run
the registered execution function and persist lifecycle state.

### Current API stance

For ingestion specifically:

- `/processes/ingestion/execution` is the forward execution path
- `/ingestions` is the legacy synchronous surface and may be reworked or removed later

This keeps the public domain noun consistent (`ingestion`) while moving actual
runtime execution onto the shared async process + job framework.

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

Providers publish data on a delay. The `sync.availability` block in a template tells the sync engine how far back from today data is reliably available:

```yaml
sync:
  kind: temporal
  execution: append
  availability:
    latest_available_function: open_climate_service.providers.availability.lagged_latest_available
    lag_hours: 120
```

Before executing a sync, the engine calls the availability function to clamp the target end date. This prevents requesting data that has not yet been published, which would leave temporal gaps.

---

## The plugin contract

The platform has four extension points. Each one has a narrow contract — the framework handles everything else automatically.

### Ingestion function

```python
def download(
    *,
    start: str,       # ISO 8601 date or datetime
    end: str,
    dirname: Path,    # write output files here
    prefix: str,      # use as filename prefix, e.g. f"{prefix}_{year}.nc"
    overwrite: bool,
    bbox: list[float],  # optional — only if the source needs a spatial filter
    **kwargs,           # default_params from the YAML template
) -> None:
    # Write one or more NetCDF files to dirname.
```

The function writes NetCDF files. The framework reads them, normalises coordinate names, applies transforms, reprojects to the instance CRS, builds the zarr, writes GeoZarr attributes, computes coverage, and registers the artifact.

The ingestion function is called identically by `POST /ingestions` and `POST /sync`. The caller makes no difference to the function — it always receives the same parameters.

**Reusing ingestion logic across templates**: multiple YAML templates can reference the same Python function and differentiate via `default_params`. This is the intended pattern for sources that have the same fetching logic but expose different variables:

```yaml
# era5land_temperature_hourly.yaml
ingestion:
  function: dhis2eo.data.era5_land.download
  default_params:
    variable: 2m_temperature

# era5land_precipitation_hourly.yaml
ingestion:
  function: dhis2eo.data.era5_land.download
  default_params:
    variable: total_precipitation
```

No framework changes are needed to support a new variable from the same source.

### Streaming plugin

The new streaming path replaces the download-function contract with a narrower
three-method plugin:

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

Responsibilities are intentionally split:

- the plugin knows the source
- the orchestrator knows resume, concurrency, and store commits
- `open_climate_service.ingestions` knows artifacts, publication, and API responses

Ticket 1 only uses this contract for direct CHIRPS3 ingest. Sync reuse and
broader source migration are follow-up work.

### Transform function

```python
def my_transform(ds: xr.Dataset, dataset: dict) -> xr.Dataset:
    # Receive the dataset after download, return a modified dataset.
    # Modify ds[dataset["variable"]] values and variable attributes.
    # Do not modify dataset-level ds.attrs — the framework manages those.
```

Transforms are applied in order after the ingestion function returns, before the zarr is written. They receive the full xarray Dataset and the template dict. They return a modified Dataset. They do not write to disk.

### Process execution function

```python
def execute(*, source_dataset_id: str, **kwargs) -> dict:
    # Run a named operation (e.g. temporal resampling).
    # Return a JSON-serialisable result dict.
```

Processes are named operations triggered via `POST /processes/{id}/execution`. They are broader than single-dataset transforms — they can read one managed dataset and produce another (e.g. daily → monthly aggregation).

---

## The transform pipeline

Transforms are applied at a consistent point in the ingestion lifecycle:

1. ingestion function writes raw NetCDF files to disk
2. framework reads and normalises the data into an xarray Dataset
3. `_run_transforms(ds, dataset)` applies each declared transform in order
4. result is reprojected to instance CRS
5. zarr store is written with auto-computed chunking
6. framework writes GeoZarr root attributes
7. framework computes coverage from the zarr

Transforms see post-download, pre-reproject data. They should only modify data values and variable-level attributes. The framework writes dataset-level attributes (GeoZarr) after the transform pipeline completes.

---

## GeoZarr root attributes

Every zarr artifact must have GeoZarr root attributes for map rendering to work correctly. These are written into `zarr.json` at the store root:

- `spatial:bbox` — `[xmin, ymin, xmax, ymax]` in the native CRS
- `proj:code` — the CRS EPSG code (e.g. `EPSG:32633` for UTM, `EPSG:4326` for WGS84)
- `zarr_conventions` — GeoZarr convention declaration

The map viewer reads `spatial:bbox` and `proj:code` to determine where to position tiles on the map.

**The framework writes these attributes — plugins do not.** They are written in `build_dataset_zarr` after transforms and reprojection, using the actual coordinate bounds of the final written data and the instance CRS.

---

## CRS handling

The instance CRS is configured in `climate-service.yaml`:

```yaml
extent:
  name: Norway
  bbox: [3.0, 57.0, 32.0, 72.5]
  crs: EPSG:32633 # optional; defaults to EPSG:4326
```

Downloaded data is reprojected from the source CRS (`source_crs` in the template, default `EPSG:4326`) to the instance CRS during ingestion. The stored zarr is always in the instance CRS.

If no `crs` is set in the config, data is stored in `EPSG:4326` (WGS84). This is the correct default for instances that do not need a metric CRS.

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

Plugin code (ingestion functions, transforms, processes) can rely on the following being handled automatically by the framework:

| Concern                                               | Where handled                               |
| ----------------------------------------------------- | ------------------------------------------- |
| Coordinate name normalisation (`lat` → `y`, etc.)     | `build_dataset_zarr`                        |
| Reprojection to instance CRS                          | `reproject_to_instance_crs`                 |
| Zarr chunking (auto-sized from `extents.temporal.resolution`) | `_compute_time_space_chunks`         |
| Multiscale pyramid generation (when dims > 2048×2048) | `build_dataset_zarr`                        |
| GeoZarr root attributes (`spatial:bbox`, `proj:code`) | `build_dataset_zarr`                        |
| Artifact coverage computation                         | `_coverage_from_dataset`                    |
| Artifact record persistence                           | `_store_artifact`                           |
| pygeoapi publication                                  | `publish_artifact_record` if `publish=true` |
| STAC collection generation                            | Dynamic from artifact record                |

Plugin code only needs to produce data files. Everything else is the framework's responsibility.

---

## Consequences of design choices

### Single extent per instance

Each instance is configured for one place. This keeps the data model simple (no per-artifact extent tags) and the zarr stores small (country-scale downloads rather than global). The trade-off is that a national ministry with sub-national data needs either runs multiple instances or configures a single instance at national extent.

### Temporal gaps are not allowed

The sync engine validates that new data connects to the end of the existing artifact before appending. If a gap exists, the sync fails rather than silently producing a dataset with a hole. This is a deliberate constraint: downstream consumers (DHIS2, CHAP) depend on continuous time series and should not receive data with silent gaps.

### The append execution mode avoids re-downloading history

`append` downloads only the missing range and rebuilds the full zarr from all cached files. This means the local cache (NetCDF files in `data/downloads/`) is the source of truth for the full time series; the zarr is a derived view. If the cache is deleted, a rematerialize is required to recover.

### Transforms run after download, before reproject

Transforms see raw downloaded values in the source CRS and source units. The order is: download → transform → reproject → write zarr.
