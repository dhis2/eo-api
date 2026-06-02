# Open Climate Service

Climate and Earth Observation data is distributed across dozens of providers — each with different APIs, data formats, and access mechanisms. The Open Climate Service unifies this fragmented landscape behind a single, consistent interface.

Each instance is configured for a specific country or region, and all data extraction, processing, and storage is scoped to that spatial extent. It abstracts data access across heterogeneous sources (CHIRPS, ERA5, WorldPop, and others), stores outputs as GeoZarr, and exposes them through standards-based endpoints.

The platform is designed to operate independently of DHIS2 and can be deployed on local, cloud-hosted, or sovereign country infrastructure. See [docs/setup_guide.md](docs/setup_guide.md) for a step-by-step setup walkthrough, [docs/user_guide.md](docs/user_guide.md) for data access examples, [docs/managed_data_api_guide.md](docs/managed_data_api_guide.md) for the full API reference, and [docs/roadmap.md](docs/roadmap.md) for the planned development steps.

> **Status: active development.** Current focus is on dataset ingestion, sync workflows, and GeoZarr storage. APIs and data models may change without notice.

## Setup

### Using uv (recommended)

Install dependencies (requires [uv](https://docs.astral.sh/uv/)):

```
uv sync
```

Copy `.env.example` to `.env` and adjust values as needed. Environment variables are loaded automatically from `.env` at runtime. See `.env.example` for the full list of available options.

Start the app:

```
uv run uvicorn open_climate_service.main:app --reload
```

### Using pip

If you cannot use uv (e.g. mixed conda/forge environments):

```
python -m venv .venv
source .venv/bin/activate
pip install -e .
python -m uvicorn open_climate_service.main:app --reload
```

### Using conda

```
conda create -n dhis2-climate-service python=3.13
conda activate dhis2-climate-service
pip install -e .
python -m uvicorn open_climate_service.main:app --reload
```

## Development

Common Makefile targets:

| Target         | Description                                      |
| -------------- | ------------------------------------------------ |
| `make sync`    | Install dependencies with uv                     |
| `make run`     | Start the app with uvicorn (hot reload)          |
| `make lint`    | Check linting, formatting, and types             |
| `make fix`     | Autofix ruff lint and format issues              |
| `make test`    | Run the test suite with pytest                   |
| `make openapi` | Regenerate the pygeoapi OpenAPI spec             |
| `make start`   | Build and start the Docker stack                 |
| `make restart` | Tear down, rebuild, and restart the Docker stack |

## Endpoints

Once running, the API is available at:

| Endpoint                                  | Description                                |
| ----------------------------------------- | ------------------------------------------ |
| `http://localhost:8000/`                  | Navigation document                        |
| `http://localhost:8000/health`            | Health check                               |
| `http://localhost:8000/docs`              | Interactive API documentation (Swagger UI) |
| `http://localhost:8000/extent`            | Configured spatial extent                  |
| `http://localhost:8000/datasets`          | Managed dataset catalogue                  |
| `http://localhost:8000/stac/catalog.json` | STAC catalog for published GeoZarr data    |
| `http://localhost:8000/zarr/{dataset_id}` | GeoZarr store for a managed dataset        |
| `http://localhost:8000/ogcapi`            | OGC API root                               |

## STAC

Published GeoZarr datasets are discoverable under `/stac` as one STAC Collection per dataset. Each collection includes a `zarr` asset with direct xarray-compatible access metadata derived from the live Zarr store.

Discover available datasets and open one with xarray:

The catalog is populated once at least one dataset has been ingested and published (see [docs/setup_guide.md](docs/setup_guide.md)).

```python
import httpx
import xarray as xr

catalog = httpx.get("http://127.0.0.1:8000/stac/catalog.json").json()
children = [link for link in catalog["links"] if link["rel"] == "child"]

if not children:
    print("No published datasets found. Run an ingestion first.")
else:
    for link in children:
        print(link["title"], "—", link["href"])

    collection = httpx.get(children[0]["href"]).json()
    asset = collection["assets"]["zarr"]
    ds = xr.open_zarr(
        asset["href"],
        consolidated=asset["xarray:open_kwargs"]["consolidated"],
    )
    print(ds)
```

## pygeoapi

The OGC API is served by pygeoapi, mounted at `/ogcapi`. Its configuration is generated dynamically from published artifacts and written to the resolved runtime data directory (for Docker: `/app/data/pygeoapi/pygeoapi-config.yml`).

The base configuration is bundled with the package at `open_climate_service/data/pygeoapi/base.yml` and does not need to be copied or managed separately.

To validate the configuration manually:

```
PYTHONPATH="$(pwd)/src" uv run pygeoapi config validate -c data/pygeoapi/pygeoapi-config.yml
```

Regenerate after changes to publication logic:

```
make openapi
```
