# Extensibility

The Open Climate Service is designed around a consistent plugin pattern: built-in behaviour lives in the package, and custom behaviour is layered on top through a `plugins_dir` directory and Python dotted paths — without forking or patching core code.

The same pattern applies at every extension point:

| Extension point | How to extend | Plugin location |
| --------------- | ------------- | --------------- |
| [Dataset templates](#dataset-templates) | YAML files | `plugins_dir/datasets/` |
| [Ingestion functions](#ingestion-functions) | Python function, dotted path in YAML | any importable path |
| [Streaming plugins](#streaming-plugins) | Python class, dotted path in YAML | any importable path |
| [Transform functions](#transform-functions) | Python function, dotted path in YAML | any importable path |
| [Processes](#processes) | YAML file + Python function | `plugins_dir/processes/` |

---

## Dataset templates

Dataset templates are YAML files that describe a data source. Built-ins live in the package (`open_climate_service/plugins/datasets/`). Custom templates are loaded from `plugins_dir/datasets/`.

```
plugins/
└── datasets/
    └── enacts_rainfall.yaml
```

```yaml
# climate-service.yaml
plugins_dir: ./plugins/
```

All `*.yaml` files in `plugins_dir/datasets/` are merged with the built-ins. A custom template with the same `id` as a built-in overrides it — useful for adjusting lag times, display ranges, or availability settings on an existing dataset.

See [Adding custom datasets](adding_custom_datasets.md) for the full template field reference.

---

## Ingestion functions

The `ingestion.function` field in a dataset template is a dotted Python path to the download function that fetches data for that dataset.

```yaml
ingestion:
  function: mypackage.sources.enacts.download
```

The function must follow the download function contract (see [Adding custom datasets](adding_custom_datasets.md#step-1-write-the-download-function)). It can live anywhere that is importable — either an installed package or a module placed directly under `plugins_dir` (which is automatically added to `sys.path`).

## Streaming plugins

The `ingestion.plugin` field is a dotted Python path to a class implementing
the new per-period streaming ingest contract:

```yaml
ingestion:
  plugin: mypackage.sources.chirps3.CHIRPS3DailyPlugin
  default_params:
    stage: final
```

The class must expose:

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

`ingestion.default_params` are applied in two places:

- they are passed to the plugin constructor as configuration kwargs
- they are also forwarded into `probe(...)` and `fetch_period(...)` as `**params`

Plugin authors may therefore keep source configuration in constructor state,
per-call kwargs, or both.

Streaming plugins are source adapters, not framework replacements. They should
know how to enumerate and fetch source periods, while the framework handles
resume, job callbacks, store commits, and artifact registration.

Current scope note: the streaming path is implemented for initial ingest.
Plugin-backed datasets currently rematerialize on sync, and broader sync reuse
plus full migration away from `ingestion.function` are follow-up work.

---

## Transform functions

Transforms are functions applied to a dataset after download and before the Zarr store is written. They are declared as a list of dotted paths in the dataset template:

```yaml
transforms:
  - open_climate_service.transforms.kelvin_to_celsius
  - mypackage.transforms.clamp_negatives
```

Each transform receives the `xr.Dataset` and the dataset template dict, and returns a (possibly modified) `xr.Dataset`:

```python
import xarray as xr
from typing import Any

def clamp_negatives(ds: xr.Dataset, dataset: dict[str, Any]) -> xr.Dataset:
    varname = dataset["variable"]
    return ds.assign({varname: ds[varname].clip(min=0)})
```

Dict entries with params are also supported:

```yaml
transforms:
  - function: mypackage.transforms.scale
    params:
      factor: 0.01
```

The `params` dict is forwarded as keyword arguments to the transform function. Custom transforms can live in any importable package or in a module under `plugins_dir`.

For the built-in transforms and a full description of the pipeline, see [Transforms](transforms.md).

---

## Processes (UDFs)

Custom data transformations are implemented as **UDFs** (User Defined Functions) — Python files placed in `plugins_dir/processes/`. A UDF receives a datacube, transforms it, and returns a datacube. It is the standard openEO extension point for custom indices, unit conversions, or special aggregations.

```
plugins/
└── processes/
    └── my_transform.py
```

UDF files are served by the Open Climate Service so that openEO process graphs can reference them by URL via the standard `run_udf` process. Since `plugins_dir` is on `sys.path`, they are also importable as Python modules.

---

## Workflows (UDPs)

Reusable pipeline compositions are implemented as **UDPs** (User Defined Processes) — JSON process graph files placed in `plugins_dir/workflows/`. A UDP is a named, parameterised composition of existing openEO processes callable by name from any openEO client.

```
plugins/
└── workflows/
    └── my_workflow.json
```

UDP files are loaded on startup and appear in `GET /process_graphs` alongside runtime-registered UDPs. A plugin UDP with the same `id` as a built-in overrides it.

---

## What is not pluggable

**Availability functions** (`sync.availability.latest_available_function`) accept a dotted path but only resolve built-in functions in `open_climate_service.providers.availability`. Plugin paths are not reliably supported — the path is resolved without `plugins_dir` on `sys.path`. Use one of the built-in availability functions instead, or open an issue if a new provider cadence is needed.

See [issue #95](https://github.com/dhis2/open-climate-service/issues/95) for the planned fix.
