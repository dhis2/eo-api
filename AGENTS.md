# Open Climate Service

Agent context for the `open-climate-service` repository. Provider-agnostic — intended to be readable by any AI coding assistant.

## Project overview

The Open Climate Service is a FastAPI-based REST API that downloads, processes, and serves climate and Earth Observation data as GeoZarr stores.

Key concepts:

- **Dataset templates** — YAML files in `data/datasets/` describing a data source (variable, period type, download function). These are blueprints.
- **Artifacts / managed datasets** — ingested instances of a template for a specific spatial extent and time range. Exposed under `/datasets` and `/zarr/{dataset_id}`.
- **Extent** — a single named spatial bounding box configured at instance setup time (`id`, `bbox`, optional `country_code`). Exposed at `GET /extent`.
- **GeoZarr stores** — datasets are stored as chunked Zarr v3 archives with GeoZarr spatial attributes. Flat stores for small extents; multiscale pyramids for large ones. Served chunk-by-chunk over HTTP with no specialised server middleware.

## Repository layout

```
open_climate_service/
  data_manager/     # download and zarr build (downloader.py)
  data_accessor/    # open zarr / netcdf for read (accessor.py)
  data_registry/    # dataset template YAML loading
  ingestions/       # artifact lifecycle: create, list, sync, publish
  publications/     # pygeoapi config generation
  extents/          # spatial extent config
  shared/           # dhis2 adapter, time utils
  main.py           # FastAPI app, CORS middleware, route registration
data/datasets/      # dataset template YAMLs (chirps3.yaml, worldpop.yaml, …)
config/pygeoapi/    # pygeoapi base config
tests/
docs/
```

## Development

```bash
make run      # start uvicorn with --reload (also generates pygeoapi OpenAPI spec)
make lint     # ruff check + ruff format + mypy + pyright
make test     # pytest
make start    # docker compose up --build
```

The `.env` file is required for `make run` and `make openapi`. Copy `.env.example` if it exists.

## Dataset templates

Each YAML in `data/datasets/` defines a dataset template. The `ingestion` block controls download and zarr build behaviour:

```yaml
ingestion:
  function: dhis2eo.data.worldpop.pop_total.yearly.download
  default_params: {} # passed to the download function
```

`build_dataset_zarr` in `data_manager/downloader.py` builds a multiscale Zarr pyramid when the spatial dimensions exceed 2048×2048 pixels; otherwise it writes a flat chunked zarr with chunk sizes derived from the dataset's temporal resolution.

The ingestion interface is being redesigned as a plugin protocol (see GitHub issue #64) — the `ingestion.function` convention will be replaced by a three-method async plugin (`probe`, `periods`, `fetch_period`).

## pygeoapi

pygeoapi is mounted at `/ogcapi` as a sub-application. Its config is generated dynamically from published artifacts by `publications/services.py` and written to `data/pygeoapi/pygeoapi-config.yml`.

The config is regenerated on each `publish_artifact` call and also at startup via `ensure_pygeoapi_base_config()`.

## Active design work

- **#64** — streaming ingest via Icechunk; per-period writes; no intermediate files
- **#111** — async job execution; OGC API Processes; progress reporting
- **#137** — spatial aggregation to DHIS2 org units; multi-dataset grid alignment

## Commit conventions

- Use conventional commits (`feat:`, `fix:`, `docs:`, `chore:`, `refactor:`, `test:`)
- No attribution lines in commit messages
- No emojis anywhere — not in commits, code, comments, or documentation
