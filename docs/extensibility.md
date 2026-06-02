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

### Built-in xclim layer

All 179 [xclim](https://xclim.readthedocs.io) climate indicators are auto-registered at startup — no code or YAML required. They appear in `GET /processes` with full metadata (summary, parameter descriptions, types, and defaults) read directly from xclim. See [Climate indices](climate_indices.md) for usage examples.

### Custom process plugins

Python functions decorated with `@process` and placed in `plugins_dir/processes/` appear in `GET /processes` alongside standard openEO processes and are callable directly by `process_id` in any process graph — no `run_udf` indirection needed.

```python
# plugins/processes/my_indices.py
import xarray as xr
from open_climate_service.process import process

@process(summary="Cumulative rainfall anomaly")
def rainfall_anomaly(pr: xr.DataArray, baseline_mean: float = 0.0) -> xr.DataArray:
    """Deviation of precipitation from a baseline mean."""
    return pr - baseline_mean
```

The `@process` decorator derives the process id (function name), summary, parameter names, types, and defaults from the function signature and docstring. Use explicit metadata to override descriptions or add schema hints:

```python
@process(
    summary="Cumulative rainfall anomaly",
    parameters={"baseline_mean": {"description": "Long-term mean precipitation (kg m-2 s-1)."}},
)
def rainfall_anomaly(pr: xr.DataArray, baseline_mean: float = 0.0) -> xr.DataArray:
    ...
```

### Override an existing process

A plugin process with the same id as an existing process (xclim auto-registered or standard openEO) overrides it. This is useful for adjusting default parameters or adding domain-specific documentation without forking core code:

```python
# plugins/processes/climate_indices.py — lower CDD threshold for Rwanda
import xarray as xr
import xclim.indicators.atmos as xclim_atmos
from open_climate_service.process import process

@process(summary="Consecutive dry days (Rwanda threshold)")
def cdd(pr: xr.DataArray, thresh: str = "0.5 mm/day", freq: str = "MS") -> xr.DataArray:
    """CDD with a lower threshold suited to Rwanda's dry season definition."""
    return xclim_atmos.maximum_consecutive_dry_days(pr, thresh=thresh, freq=freq)
```

### Resolution order

| Priority | Source |
|---|---|
| 1 (lowest) | xclim auto-registered indicators |
| 2 | Built-in file plugins (`open_climate_service/plugins/processes/`) |
| 3 (highest) | Instance plugins (`plugins_dir/processes/`) |

The server must be restarted to pick up new or changed process files.

---

## Workflows

Reusable pipeline compositions are implemented as **UDPs** (User Defined Processes) — JSON process graph files placed in `plugins_dir/workflows/`. A UDP is a named, parameterised composition of existing openEO processes callable by name from any openEO client.

```
plugins/
└── workflows/
    └── my_workflow.json
```

Workflow JSON files are loaded on each request to `GET /process_graphs`, so changes on disk take effect without restarting the server. A plugin workflow with the same `id` as a built-in overrides it.
