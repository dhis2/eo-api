# eo-api

DHIS2 EO API allows data from multiple sources (primarily earth observation data) to be extracted, transformed and loaded into DHIS2 and the Chap Modelling Platform.

## Setup

### Using uv (recommended)

Install dependencies (requires [uv](https://docs.astral.sh/uv/)):

`uv sync`

Environment variables are loaded automatically from `.env` (via `python-dotenv`).
Copy `.env.example` to `.env` and adjust values as needed.

Start the app:

`uv run uvicorn main:app --reload`

### Using pip (alternative)

If you can't use uv (e.g. mixed conda/forge environments):

```
python -m venv .venv
source .venv/bin/activate
pip install -e .
uvicorn main:app --reload
```

### Using conda

```
conda create -n dhis2-eo-api python=3.13
conda activate dhis2-eo-api
pip install -e .
uvicorn main:app --reload
```

### Makefile targets

- `make sync` — install dependencies with uv
- `make run` — start the app with uv
- `make run-pygeoapi PYGEOAPI_CONFIG=/absolute/path/to/pygeoapi-config.yml` — start app with pygeoapi mounted at `/ogcapi`
- `make run-pygeoapi-example` — start app with pygeoapi mounted at `/ogcapi` using `./pygeoapi-config.yml`

### Minimal pygeoapi downstream integration

This project supports the pygeoapi downstream-application pattern (mounting pygeoapi in FastAPI):

- Set `PYGEOAPI_CONFIG` to your pygeoapi config file path.
- Start the API with `make run-pygeoapi PYGEOAPI_CONFIG=/absolute/path/to/pygeoapi-config.yml`.
- Access pygeoapi endpoints under `/ogcapi` (for example `/ogcapi`, `/ogcapi/conformance`, `/ogcapi/collections`).

Example:

```bash
make run-pygeoapi PYGEOAPI_CONFIG="/absolute/path/to/pygeoapi-config.yml"
```

If you keep your config at repo root as `pygeoapi-config.yml`, you can run:

```bash
make run-pygeoapi-example
```

A standalone minimal example is available at `examples/pygeoapi_downstream_fastapi.py`.

Root endpoint:

http://127.0.0.1:8000/ -> Welcome to DHIS2 EO API

Docs:

http://127.0.0.1:8000/docs

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
