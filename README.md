# eo-api

DHIS2 EO API allows data from multiple sources (primarily earth observation data) to be extracted, transformed and loaded into DHIS2 and the Chap Modelling Platform.

## Setup

### Using uv (recommended)

Install dependencies (requires [uv](https://docs.astral.sh/uv/)):

`uv sync`

Environment variables are loaded automatically from `.env` (via `python-dotenv`).
Copy `.env.example` to `.env` and adjust values as needed.

Key environment variables (used by the OGC API DHIS2 plugin):

- `DHIS2_BASE_URL` -- DHIS2 API base URL (defaults to play server in `.env.example`)
- `DHIS2_USERNAME` -- DHIS2 username
- `DHIS2_PASSWORD` -- DHIS2 password

Start the app:

`uv run uvicorn eo_api.main:app --reload`

### Using pip (alternative)

If you can't use uv (e.g. mixed conda/forge environments):

```
python -m venv .venv
source .venv/bin/activate
pip install -e .
uvicorn eo_api.main:app --reload
```

### Using conda

```
conda create -n dhis2-eo-api python=3.13
conda activate dhis2-eo-api
pip install -e .
uvicorn eo_api.main:app --reload
```

### Makefile targets

- `make sync` -- install dependencies with uv
- `make run` -- start the app with uvicorn
- `make lint` -- run ruff linting and format checks
- `make test` -- run tests with pytest
- `make openapi` -- generate pygeoapi OpenAPI spec
- `make start` -- start the Docker stack (builds images first)
- `make restart` -- tear down, rebuild, and start the Docker stack from scratch

### pygeoapi instructions

To validate the configuration:

```
pygeoapi config validate -c pygeoapi-config.yml`
```

Run after changes are made in pygeoapi-config.yml:

`make openapi` or

```
PYTHONPATH="$(pwd)" uv run pygeoapi openapi generate ./pygeoapi-config.yml > pygeoapi-openapi.yml
```

### Endpoints

Root endpoint:

http://127.0.0.1:8000/ -> Welcome to DHIS2 EO API

Docs:

http://127.0.0.1:8000/docs

OGC API

http://127.0.0.1:8000/ogcapi

Examples:

COG info:

http://127.0.0.1:8000/cog/info?url=https%3A%2F%2Fdata.chc.ucsb.edu%2Fproducts%2FCHIRPS%2Fv3.0%2Fdaily%2Ffinal%2Frnl%2F2026%2Fchirps-v3.0.rnl.2026.01.31.tif

COG preview:

http://127.0.0.1:8000/cog/preview.png?url=https%3A%2F%2Fdata.chc.ucsb.edu%2Fproducts%2FCHIRPS%2Fv3.0%2Fdaily%2Ffinal%2Frnl%2F2026%2Fchirps-v3.0.rnl.2026.01.31.tif&max_size=2048&colormap_name=delta

Tile:

http://127.0.0.1:8000/cog/tiles/WebMercatorQuad/4/5/5.png?url=https%3A%2F%2Fdata.chc.ucsb.edu%2Fproducts%2FCHIRPS%2Fv3.0%2Fdaily%2Ffinal%2Frnl%2F2026%2Fchirps-v3.0.rnl.2026.01.31.tif&colormap_name=delta

---

CHIRPS COG test file:

https://data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/final/rnl/2026/chirps-v3.0.rnl.2026.01.31.tif
