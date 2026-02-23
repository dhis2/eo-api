# eo-api

DHIS2 EO API allows data from multiple sources (primarily earth observation data) to be extracted, transformed and loaded into DHIS2 and the Chap Modelling Platform.

## Setup

### Using uv (recommended)

Install dependencies (requires [uv](https://docs.astral.sh/uv/)):

`uv sync`

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
- `make validate-datasets` — validate all dataset YAML files against the Pydantic schema
- `make test` — run Python tests with pytest

## API examples

See [`API_EXAMPLES.md`](API_EXAMPLES.md) for docs, collections, STAC, and COG request examples.

## Documentation

- API usage examples: [`API_EXAMPLES.md`](API_EXAMPLES.md)
- Dataset schema and resolver conventions: [`eoapi/datasets/README.md`](eoapi/datasets/README.md)
- Product requirements and scope: [`PRD.md`](PRD.md)
- Repository coding guidance for AI edits: [`.github/copilot-instructions.md`](.github/copilot-instructions.md)

## Dataset definitions

Collection metadata for `/collections` is defined in YAML files under `eoapi/datasets/`.

Each dataset uses `eoapi/datasets/<dataset-id>/<dataset-id>.yaml` with matching resolver code in `eoapi/datasets/<dataset-id>/resolver.py`.

For schema details, examples, and current dataset files, see [`eoapi/datasets/README.md`](eoapi/datasets/README.md).
