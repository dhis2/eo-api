# Instance guide

An **instance repository** packages the configuration and plugins for a specific operational context — a country, a region, or an organisation — and references open-climate-service as a versioned dependency rather than including it directly.

This keeps the core service separate from context-specific concerns, and means your configuration lives in its own repository that can be versioned, shared, and deployed independently.

## When to use this pattern

Use an instance repository when you:

- Want to add custom datasets not included in open-climate-service (e.g. national meteorological data)
- Want to track your configuration in version control separately from the open-climate-service codebase
- Want to pin your service to a specific version of open-climate-service and upgrade deliberately
- Want to share your configuration with others, or deploy across multiple environments

If you only need to run climate-service with built-in datasets and no custom plugins, the [setup guide](setup_guide.md) (cloning open-climate-service directly) is simpler.

---

## Repository structure

```
my-climate-service/
├── pyproject.toml          # declares open-climate-service as a dependency
├── uv.lock                 # locked dependency tree for reproducible installs
├── Makefile                # install / run shortcuts
├── climate-service.yaml        # instance config: extent, CRS, data_dir, plugins_dir
├── .env.example            # committed template for environment variables
├── .gitignore
├── plugins/
│   ├── datasets/           # custom dataset template YAMLs
│   ├── <source>/           # custom download / ingestion functions
│   │   ├── __init__.py
│   │   └── daily.py
│   ├── transforms/         # custom transform functions
│   │   ├── __init__.py
│   │   └── my_transform.py
│   └── processes/          # custom process YAMLs + Python functions
│       ├── my_process.yaml
│       └── my_process.py
└── data/                   # gitignored — downloaded files and Zarr stores
```

---

## Step 1: Create the repository

```bash
mkdir my-climate-service
cd my-climate-service
git init
```

## Step 2: Declare open-climate-service as a dependency

Create `pyproject.toml`:

```toml
[project]
name = "my-climate-service"
version = "0.1.0"
requires-python = ">=3.13"
description = "Open Climate Service instance for [context]"
dependencies = [
    "open-climate-service @ git+https://github.com/dhis2/open-climate-service.git",
]

[tool.uv]
package = false

[tool.uv.sources]
open-climate-service = { git = "https://github.com/dhis2/open-climate-service.git", branch = "main" }
```

The `package = false` setting tells uv that this repository is not itself a Python package — it only declares dependencies. The `[tool.uv.sources]` block pins open-climate-service to the `main` branch on GitHub. To pin to a specific tag or commit instead, use `rev = "v0.2.0"`. Installing directly from git is the intended path for now — the package name is reserved on PyPI but no stable releases have been published there yet.

Install dependencies:

```bash
uv sync
```

This creates a `.venv` and a `uv.lock` file. Commit `uv.lock` so that everyone working with this repository installs exactly the same versions.

## Step 3: Add a Makefile

```makefile
.DEFAULT_GOAL := help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

install: ## Install dependencies with uv
	uv sync

run: ## Start the API with uvicorn
	set -a && . ./.env && set +a && \
		uv run uvicorn open_climate_service.main:app --reload --reload-include "*.yaml" --reload-include "*.yml" --port 8000
```

## Step 4: Configure the instance

Create `climate-service.yaml`:

```yaml
extent:
  name: My Region
  bbox: [28.8, -2.9, 30.9, -1.0]    # [xmin, ymin, xmax, ymax] in WGS84
  country_code: RWA                   # ISO 3166-1 alpha-3, required for WorldPop

data_dir: ./data
crs: EPSG:4326                         # target CRS for Zarr outputs (optional, defaults to EPSG:4326)
plugins_dir: ./plugins/
```

| Field | Required | Description |
| ----- | -------- | ----------- |
| `extent.bbox` | Yes | Bounding box in WGS84 decimal degrees |
| `extent.name` | No | Human-readable label shown in API responses |
| `extent.country_code` | No | ISO 3166-1 alpha-3 — required for WorldPop downloads |
| `data_dir` | Yes | Directory for downloaded files and Zarr stores, resolved relative to the config file |
| `crs` | No | EPSG code for the output Zarr CRS. Defaults to `EPSG:4326` |
| `plugins_dir` | No | Directory containing custom datasets, functions, and processes |

To find the bounding box for a region, [bboxfinder.com](http://bboxfinder.com) is a useful tool.

Create `.env`:

```bash
CLIMATE_SERVICE_CONFIG=/absolute/path/to/my-climate-service/climate-service.yaml
```

And a committed `.env.example` as a template:

```bash
CLIMATE_SERVICE_CONFIG=/path/to/my-climate-service/climate-service.yaml
```

## Step 5: Add a .gitignore

```
.env
.venv/
data/
__pycache__/
*.pyc
.DS_Store
```

## Step 6: Run the instance

```bash
make install
make run
```

Visit `http://localhost:8000` to confirm the API is running. The `/extent` endpoint should return your configured bounding box.

---

## Adding plugins

Plugins extend the instance with custom datasets, download functions, transforms, and processes. They live in `plugins_dir` and are loaded automatically at startup. The `plugins_dir` is added to `sys.path`, so Python modules placed directly inside it are importable.

```
plugins/
├── datasets/
│   └── enacts_rainfall.yaml    # custom dataset template
├── enacts/
│   ├── __init__.py
│   └── daily.py                # download function referenced in the YAML
├── transforms/
│   ├── __init__.py
│   └── enacts.py               # transform function
└── processes/
    ├── spatial_stats.yaml
    └── spatial_stats.py
```

See [Extensibility](extensibility.md) for the full specification of each extension point, and [Adding custom datasets](adding_custom_datasets.md) for the dataset template field reference and download function contract.

---

## Keeping open-climate-service up to date

To pull the latest changes from the `main` branch:

```bash
uv lock --upgrade-package open-climate-service
uv sync
```

Commit the updated `uv.lock`. Everyone working with this repository will get the same updated version after running `uv sync`.

To pin to a specific commit for a more controlled upgrade:

```toml
[tool.uv.sources]
open-climate-service = { git = "https://github.com/dhis2/open-climate-service.git", rev = "abc1234" }
```

---

## Deployment

For production deployments, the same repository can be used directly on a server:

```bash
git clone https://github.com/your-org/my-climate-service.git
cd my-climate-service
cp .env.example .env   # fill in absolute paths and credentials
uv sync
make run
```

A Docker-based deployment guide is in progress.
