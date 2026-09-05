# Instance guide

An **instance repository** packages the configuration and plugins for a specific operational context — a country, a region, or an organisation — and references open-climate-service as a versioned dependency rather than including it directly.

This keeps the core service separate from context-specific concerns, and means your configuration lives in its own repository that can be versioned, shared, and deployed independently.

## When to use this pattern

Use an instance repository when you:

- Want to add custom datasets not included in open-climate-service (e.g. national meteorological data)
- Want to track your configuration in version control separately from the open-climate-service codebase
- Want to pin your service to a specific version of open-climate-service and upgrade deliberately
- Want to share your configuration with others, or deploy across multiple environments

This is the recommended path for running an actual climate service. If you only want to
try Open Climate Service locally with the built-in datasets, the [quick start](setup_guide.md)
(cloning open-climate-service directly) is faster.

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
│   ├── datasets/           # dataset templates (.yaml) + plugin classes (.py)
│   │   ├── enacts_rainfall.yaml
│   │   └── enacts.py
│   ├── processes/          # @process-decorated functions (.py)
│   │   └── my_process.py
│   └── workflows/          # reusable process graph compositions (.json)
│       └── my_workflow.json
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
requires-python = ">=3.12"
description = "Open Climate Service instance for [context]"
dependencies = [
    "open-climate-service[server]==0.1.0",
]

[tool.uv]
package = false

# Required for the [server] extra to resolve: these relax upstream transitive pins
# that uv applies but pip cannot — openeo-pg-parser-networkx pins geojson-pydantic<2
# (we need >=2.1.0), and openeo-processes-dask pins an older zarr
# (Open-EO/openeo-processes-dask#376). Drop entries as upstream releases catch up.
override-dependencies = [
    "geojson-pydantic>=2.1.0",
    "zarr>=3.1.6",
    "pyarrow>=19.0",
    "xarray>=2025.12.0",
    "numpy>=2.2",
    "dask>=2024.1.0",
    "dask-geopandas>=0.4",
    "geopandas>=1.1",
    "xvec>=0.3",
    "rioxarray>=0.17",
    "pystac>=1.10",
]
```

The `package = false` setting tells uv that this repository is not itself a Python package — it only declares dependencies. It depends on the released `open-climate-service[server]` from PyPI, pinned here to `0.1.0`; bump the version to upgrade. The `override-dependencies` block is required for `uv` to resolve the `[server]` extra (see the comment above) — this is also why `pip install` is not a supported install path for `[server]`. To track the latest unreleased code instead of a release, add a `[tool.uv.sources]` entry pinning open-climate-service to git (`open-climate-service = { git = "https://github.com/dhis2/open-climate-service.git", branch = "main" }`) and change the dependency to `open-climate-service[server]`.

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
		uv run uvicorn open_climate_service.main:app --reload --reload-include "*.yaml" --reload-include "*.yml" --port 9000
```

## Step 4: Configure the instance

Create `climate-service.yaml`:

```yaml
id: rwanda-climate-service             # unique identifier for this instance
name: Rwanda Climate Service           # display name shown in the web UI

extent:
  name: Rwanda
  bbox: [28.8, -2.9, 30.9, -1.0]    # [xmin, ymin, xmax, ymax] in WGS84
  country_code: RWA                   # ISO 3166-1 alpha-3, required for WorldPop

data_dir: ./data
plugins_dir: ./plugins/
```

| Field | Required | Description |
| ----- | -------- | ----------- |
| `id` | No | Unique instance identifier used as the STAC catalog id. Lowercase, hyphen-separated (e.g. `rwanda-climate-service`). Defaults to `open-climate-service` |
| `name` | No | Display name shown in the web UI. Defaults to `Open Climate Service` |
| `extent.bbox` | Yes | Bounding box in WGS84 decimal degrees |
| `extent.name` | No | Human-readable label shown in API responses |
| `extent.country_code` | No | ISO 3166-1 alpha-3 — required for WorldPop downloads |
| `data_dir` | Yes | Directory for downloaded files and Zarr stores, resolved relative to the config file. A `features/` subdirectory here holds the instance's [feature collections](openeo.md#feature-collections) |
| `plugins_dir` | No | Directory containing `datasets/`, `processes/`, and `workflows/` plugin subdirectories |
| `read_only` | No | Set `true` to refuse all state-changing requests — see [Read-only instances](#read-only-instances). Defaults to `false` |
| `scheduler` | No | Instance-level scheduled dataset-sync configuration. See [Scheduled dataset synchronization](scheduled_sync.md) |
| `automation` | No | Event-driven workflow bindings for successful dataset updates. See [Dataset-update workflow automation](workflow_automation.md) |

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

Visit `http://127.0.0.1:9000` to confirm the service is running. From there, open
`/manage` to ingest data and `/map` to view it — see [Using the web interface](web_interface.md).
The `/extent` endpoint should return your configured bounding box.

---

## Adding plugins

Plugins extend the instance with custom datasets, processes, workflows, and feature providers. They live in `plugins_dir` and are loaded automatically. The `plugins_dir` is added to `sys.path`, so Python modules placed directly inside it are importable.

```
plugins/
├── datasets/
│   ├── enacts_rainfall.yaml    # custom dataset template
│   └── enacts.py               # streaming plugin class
├── processes/
│   └── spatial_stats.py        # @process-decorated functions
├── workflows/
│   └── aggregate_for_dhis2.json
└── features/
    └── national_registry.py    # @feature_provider-decorated functions
```

See [Extensibility](extensibility.md) for the plugin types, and [Adding custom datasets](adding_custom_datasets.md) for the dataset template field reference and streaming plugin contract.

---

## Declared feature sets

Zonal aggregation needs geometry. Passing a GeoJSON `FeatureCollection` with every request works for a hand-made call and not for a schedule: a country's hierarchy is megabytes, it would have to be pasted into `climate-service.yaml`, and it goes stale whenever the hierarchy changes.

Instead, describe a feature set in a **template**, exactly as you would a dataset. Templates are `features/*.yaml` in `plugins_dir`, beside the providers that fetch them:

```
plugins/features/
├── districts.yaml          # the template
└── dhis2_hierarchy.py      # the @feature_provider it names
```

```yaml
- id: districts
  name: District boundaries
  description: Level-2 organisation units, as held by the national DHIS2 instance.
  license: CC-BY-4.0
  attribution: Ministry of Health
  source_url: https://dhis2.example.org
  provider: dhis2
  params: { level: 2 }
  ttl_seconds: 86400        # optional — default: 24 hours
```

| Field | Required | Description |
| ----- | -------- | ----------- |
| `id` | Yes | The name workflows and triggers reference. Must be unique |
| `name` / `description` | No | Shown at `GET /features` |
| `license` | No | SPDX id or URI. Some sources require this — Overture buildings are ODbL, and share-alike applies to anything derived and served |
| `attribution` | No | Credit line to surface wherever the layer is shown |
| `source` / `source_url` | No | Where it came from |
| `keywords` | No | For catalogue search |
| `provider` | No | A registered feature provider. Omit for a file you place in the store by hand |
| `params` | No | Passed to the provider as keyword arguments |
| `ttl_seconds` | No | How long a provider-backed set is reused before refetching. Defaults to 24 hours |
| `id_property` | No | Property whose value becomes each feature's id. Required when the provider does not already set correct ids |

A template with no `provider` carries metadata only: nothing refreshes the file, and it exists so a collection an admin placed can still declare its licence.

A trigger then references the id instead of carrying geometry:

```yaml
automation:
  workflow_triggers:
    - id: chirps-to-dhis2
      on_update_of: chirps3_precipitation_daily
      workflow_id: aggregate_to_dhis2_json
      arguments:
        geometries: { from_features: districts }
```

At submission this becomes a `load_features` **node** in the process graph, referenced by the workflow — not a copy of the geometry:

```json
{"features_districts": {"process_id": "load_features", "arguments": {"id": "districts"}},
 "workflow": {"process_id": "aggregate_to_dhis2_json",
              "arguments": {"geometries": {"from_node": "features_districts"}}, "result": true}}
```

The job's description records which version of each set it ran against (`against features districts@release-2026-09`), which is what explains why one run covered 47 districts and the next covered 48.

There is **one** store, `<data_dir>/features/`. A provider-backed set updates its entry there rather than filling a separate cache — the same shape as a dataset plugin writing into `downloads/`. Each provider-maintained entry gets a JSON sidecar recording the runtime facts: provider, version and fetch time. A file an admin drops in has no sidecar, and a provider refuses to overwrite it.

The `stored` provider reads that same store, so an instance with no external system configured can still declare feature sets and schedule aggregation against boundaries it ships itself.

Past versions are not kept. Recording *which* boundaries a run used needs a version string, not an archive — and re-running a scheduled push is usually a repair that wants current boundaries anyway.

Feature ids must identify exactly one feature. A null or duplicate id fails loudly, because the id becomes the location label an export writes against — two features under one id would be pushed as one organisation unit, silently discarding a value.

----- | -------- | ----------- |
| `id` | Yes | The name workflows and triggers reference. Must be unique |
| `provider` | Yes | A registered feature provider. `stored` ships with OCS; others come from plugins |
| `params` | No | Passed to the provider as keyword arguments |
| `ttl_seconds` | No | How long a resolved set is reused before refetching. Defaults to 24 hours |
| `id_property` | No | Property whose value becomes each feature's id. Required when the provider does not already set correct ids |

A trigger then references the id instead of carrying geometry:

```yaml
automation:
  workflow_triggers:
    - id: chirps-to-dhis2
      on_update_of: chirps3_precipitation_daily
      workflow_id: aggregate_to_dhis2_json
      arguments:
        geometries: { from_features: districts }
```

At submission this becomes a `load_features` node, not a copy of the geometry — so the job record stays small:

```json
{"process_id": "load_features", "arguments": {"id": "districts"}}
```

The job's description records which version of each set it ran against (`against features districts@release-2026-09`), which is what explains why one run covered 47 districts and the next covered 48.

There is **one** store, `<data_dir>/features/`. A provider-backed set updates its entry there rather than filling a separate cache — the same shape as a dataset plugin writing into `downloads/`. Each provider-maintained entry gets a JSON sidecar recording its provider, version and fetch time; a file an admin drops in has no sidecar, and a provider refuses to overwrite it.

The `stored` provider reads that same store, so an instance with no external system configured can still declare feature sets and schedule aggregation against boundaries it ships itself.

Past versions are not kept. Recording *which* boundaries a run used needs a version string, not an archive — and re-running a scheduled push is usually a repair that wants current boundaries anyway.

Feature ids must identify exactly one feature. A null or duplicate id fails loudly, because the id becomes the location label an export writes against — two features under one id would be pushed as one organisation unit, silently discarding a value.

---

## Upgrading and troubleshooting

### Upgrading

To move to a newer release, bump the pin in `pyproject.toml` (e.g. `open-climate-service[server]==0.2.0`), then:

```bash
uv lock --upgrade-package open-climate-service
uv sync
```

Commit the updated `uv.lock` so everyone gets the same versions.

> **`uv sync` alone does not upgrade.** It only makes the environment match the lockfile. To actually move to a newer version you must re-lock first (`uv lock --upgrade-package open-climate-service`) — this is true both for a pinned release and for a git source.

If you track the latest unreleased code via a `[tool.uv.sources]` git entry, `uv lock --upgrade-package open-climate-service` re-resolves to the current branch head. To pin to a specific commit instead:

```toml
[tool.uv.sources]
open-climate-service = { git = "https://github.com/dhis2/open-climate-service.git", rev = "abc1234" }
```

Pinning to a released version (`==X.Y.Z`) is recommended over tracking `main`: a release is reproducible, whereas `main`'s dependency tree shifts over time and can change under you between syncs.

### Troubleshooting

- **Always run inside the synced environment** — use `make run` or `uv run uvicorn …`, never a bare `uvicorn`/`python`. If a stray virtual environment is activated (`echo $VIRTUAL_ENV`), deactivate it; uv warns when `VIRTUAL_ENV` doesn't match the project's `.venv`.

- **Never `pip install` into the venv.** `uv sync` makes the environment *exactly* match the lockfile and removes anything else — so hand-installed packages disappear on the next sync, and a missing package "comes back" after every upgrade. If something is genuinely needed, add it to `[project] dependencies` (or the right extra/group) and re-lock.

- **`ModuleNotFoundError` for `xvec`, `odc`, `dask_geopandas`, `planetary_computer`, `pystac_client`, `stac_validator`, … when running a job** means the `[server]` extra is not fully installed. `openeo-processes-dask` eagerly imports its whole implementation stack, so all of these must be present. Don't add them one by one — the `[server]` extra is the complete, maintained set. Ensure your dependency is `open-climate-service[server]` (with the extra), then:
  ```bash
  uv lock --upgrade-package open-climate-service
  uv sync
  ```
  Verify the stack is complete:
  ```bash
  uv run python -c "import xvec, odc.geo, dask_geopandas, planetary_computer, pystac_client, stac_validator; print('ok')"
  ```

- **`pip install open-climate-service[server]` does not work** — the `[server]` extra needs dependency overrides (for upstream version caps) that `uv` applies but `pip` cannot. Install the server stack with **uv** (this guide) or **Docker**. The base client and `[xarray]` extras install fine with `pip`.

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

For containerised deployment, the core open-climate-service repository ships a `Dockerfile`
and a `compose.yml` that can serve as a starting point for packaging an instance. A
dedicated instance Docker guide is planned.

### Read-only instances

For an instance that should be browsable but not changeable — a public demo, a shared
reference endpoint:

```yaml
read_only: true
```

Every state-changing request is refused with `403`, the openEO capabilities document stops
advertising the endpoints that would refuse, and `GET /info` reports `read_only: true`.

Still available: the catalogue and metadata (`/collections`, `/stac`, `/datasets`,
`/processes`, `/process_graphs`, `/extent`), the data (`/zarr/…`, `/icechunk/…`, downloads),
the landing page and `/map`, and `POST /result` for synchronous openEO process graphs.

Refused: ingestion and sync, the `/manage` console, stored process graph writes, and batch
jobs.

Read-only applies to HTTP only, so ingestion becomes an operator task on the host. Until a
CLI command exists, run it inside the container or virtualenv:

```python
from open_climate_service.ingestions.processes import execute_ingestion

execute_ingestion(dataset_id="era5land_temperature_monthly", start="2016-01", end="2016-12")
```
