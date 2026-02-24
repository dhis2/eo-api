# Processing Architecture Modules

This package implements the architecture direction from `docs/architecture.md`:

- OGC API Processes skeleton metadata and execution contracts
- Dataset registry loader backed by YAML
- Provider interface for cache-first data access
- CHIRPS3 provider implementation

## Layout

- `process_catalog.py`: Process IDs and OGC metadata payload builders
- `runtime.py`: Shared process runtime used by endpoint dispatch
- `service.py`: Process execution skeleton using registry + provider
- `registry.py`: Dataset registry built from `eoapi/datasets/*` + provider mapping YAML
- `raster_ops.py`: Raster operation stubs (`zonal_stats`, `point_timeseries`)
- `formatters.py`: Output formatter stubs (CSV and DHIS2 payload)
- `providers/base.py`: Provider contracts (`RasterProvider`)
- `providers/chirps3.py`: CHIRPS3 cache-first provider
- `config/providers.yaml`: Dataset-to-provider mapping

## Runtime visibility

Process job outputs include an `implementation` block that reports the runtime stack used by stage:

- provider/adapters (for example `dhis2eo`)
- compute libraries (for example `xarray` for zonal stats)
- output formatting layer (CSV now, DHIS2 adapter path)

See `docs/processing_api.md#library-responsibility-matrix` for the stage-by-stage responsibilities.
