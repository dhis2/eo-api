# Extensibility

The Open Climate Service supports three plugin types, all following the same pattern: place files in the appropriate subdirectory of `plugins_dir` and the service picks them up automatically — no forking or patching of core code required.

| Plugin type | Location | Format |
| ----------- | -------- | ------ |
| Datasets | `plugins_dir/datasets/` | `.yaml` + `.py` |
| Processes | `plugins_dir/processes/` | `.py` |
| Workflows | `plugins_dir/workflows/` | `.json` |

---

## Datasets

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

A Python plugin class is declared alongside the YAML using the `ingestion.plugin` dotted path. Any data transformations (unit conversion, clamping, etc.) are applied inside `fetch_period` before the `xr.Dataset` is returned — no separate framework-level declaration is needed.

Plugin modules can live in any importable package or directly under `plugins_dir`, which is automatically added to `sys.path`.

See [Adding custom datasets](adding_custom_datasets.md) for the full template field reference, streaming plugin contract and transform function signature.

---

## Processes

Custom processes are Python functions decorated with `@process` and placed in `plugins_dir/processes/`. They appear in `GET /processes` alongside standard openEO processes and are callable directly by `process_id` in any process graph — no `run_udf` indirection needed.

```python
# plugins/processes/climate_indices.py
import xarray as xr
from open_climate_service.process import process

@process(summary="Maximum consecutive dry days")
def cdd(pr: xr.DataArray, thresh: str = "1mm/day") -> xr.DataArray:
    """Maximum number of consecutive days with precipitation below threshold."""
    import xclim.atmos
    return xclim.atmos.maximum_consecutive_dry_days(pr, thresh=thresh)
```

The `@process` decorator derives the process id (function name), summary, parameter names, types, and defaults from the function signature and docstring. Use explicit metadata to override:

```python
@process(
    summary="Custom summary",
    parameters={"thresh": {"description": "Precipitation threshold"}},
)
def cdd(pr: xr.DataArray, thresh: str = "1mm/day") -> xr.DataArray:
    ...
```

A plugin process with the same id as a standard openEO process overrides it. The server must be restarted to pick up new process files.

---

## Workflows

Reusable pipeline compositions are implemented as **UDPs** (User Defined Processes) — JSON process graph files placed in `plugins_dir/workflows/`. A UDP is a named, parameterised composition of existing openEO processes callable by name from any openEO client.

```
plugins/
└── workflows/
    └── my_workflow.json
```

Workflow JSON files are loaded on each request to `GET /process_graphs`, so changes on disk take effect without restarting the server. A plugin workflow with the same `id` as a built-in overrides it.
