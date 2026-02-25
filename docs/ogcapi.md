# OGC API and pygeoapi

## OGC API overview

OGC API is a family of standards from the [Open Geospatial Consortium](https://www.ogc.org/) that define RESTful interfaces for geospatial data. Each standard covers a specific data type or interaction pattern:

| Standard | Purpose |
|---|---|
| [Features](https://ogcapi.ogc.org/features/) | Vector feature access (GeoJSON, etc.) |
| [Coverages](https://ogcapi.ogc.org/coverages/) | Gridded / raster data |
| [EDR](https://ogcapi.ogc.org/edr/) | Environmental Data Retrieval (point, trajectory, corridor queries) |
| [Processes](https://ogcapi.ogc.org/processes/) | Server-side processing / workflows |
| [Maps](https://ogcapi.ogc.org/maps/) | Rendered map images |
| [Tiles](https://ogcapi.ogc.org/tiles/) | Tiled data (vector and map tiles) |
| [Records](https://ogcapi.ogc.org/records/) | Catalogue / metadata search |

All standards share a common core: JSON/HTML responses, OpenAPI-described endpoints, and content negotiation. The full specification catalogue is at <https://ogcapi.ogc.org>.

## pygeoapi

[pygeoapi](https://pygeoapi.io) is a Python server that implements the OGC API standards listed above. It is the OGC Reference Implementation for OGC API - Features.

In this project pygeoapi is mounted as a sub-application at `/ogcapi`. The integration is minimal -- a single re-export in `src/eo_api/routers/ogcapi.py`:

```python
from pygeoapi.starlette_app import APP as pygeoapi_app

app = pygeoapi_app  # mounted by the main FastAPI app
```

All dataset and behaviour configuration happens in YAML, not Python code.

- pygeoapi docs: <https://docs.pygeoapi.io>
- Source: <https://github.com/geopython/pygeoapi>

## Configuration

pygeoapi is configured through a single YAML file whose path is set by the `PYGEOAPI_CONFIG` environment variable. The repo ships a default config at `pygeoapi-config.yml`.

### Top-level sections

```yaml
server:     # host, port, URL, limits, CORS, languages, admin toggle
logging:    # log level and optional log file
metadata:   # service identification, contact, license
resources:  # datasets and processes exposed by the API
```

### `server`

Controls runtime behaviour -- bind address, public URL, response encoding, language negotiation, pagination limits, and the optional admin API.

```yaml
server:
  bind:
    host: 127.0.0.1
    port: 5000
  url: http://127.0.0.1:8000/ogcapi
  mimetype: application/json; charset=UTF-8
  encoding: utf-8
  languages:
    - en-US
    - fr-CA
  limits:
    default_items: 20
    max_items: 50
  admin: false
```

### `metadata`

Service-level identification, contact details, and license. Supports multilingual values.

```yaml
metadata:
  identification:
    title:
      en: DHIS2 EO API
    description:
      en: OGC API compliant geospatial data API
  provider:
    name: DHIS2 EO API
    url: https://dhis2.org
  contact:
    name: DHIS2 Climate Team
    email: climate@dhis2.org
```

### `resources`

Each key under `resources` defines a collection or process. A collection needs at minimum a `type`, `title`, `description`, `extents`, and one or more `providers`.

```yaml
resources:
  lakes:
    type: collection
    title: Large Lakes
    description: lakes of the world, public domain
    extents:
      spatial:
        bbox: [-180, -90, 180, 90]
        crs: http://www.opengis.net/def/crs/OGC/1.3/CRS84
    providers:
      - type: feature
        name: GeoJSON
        data: tests/data/ne_110m_lakes.geojson
        id_field: id
```

Full configuration reference: <https://docs.pygeoapi.io/en/latest/configuration.html>

## Resource types

The `type` field on a provider determines which OGC API standard the collection exposes.

| Provider type | OGC API standard | Description |
|---|---|---|
| `feature` | Features | Vector data (points, lines, polygons). Backends include CSV, GeoJSON, PostGIS, Elasticsearch, and others. |
| `coverage` | Coverages | Gridded / raster data. Backends include rasterio, xarray, and S3-hosted COGs. |
| `map` | Maps | Rendered map images, typically proxied from an upstream WMS via `WMSFacade`. |
| `process` | Processes | Server-side processing tasks. Defined by a `processor` rather than a `providers` list. |

A single collection can have multiple providers (e.g. both `feature` and `tile` on the same resource).

## CQL filtering

pygeoapi supports [CQL2](https://docs.ogc.org/is/21-065r2/21-065r2.html) text filters on collections backed by a CQL-capable provider. Filters are passed as query parameters:

```
?filter=<expression>
```

The `dhis2-org-units-cql` collection exposes this capability. Its filterable properties are `name`, `code`, `shortName`, `level`, and `openingDate`.

### Supported operators

| Category | Operators | Example |
|---|---|---|
| Comparison | `=`, `<>`, `<`, `<=`, `>`, `>=` | `level=2` |
| Pattern matching | `LIKE`, `ILIKE` (`%` = any chars, `_` = single char) | `name LIKE '%Hospital%'` |
| Range | `BETWEEN ... AND ...` | `level BETWEEN 2 AND 3` |
| Set membership | `IN (...)` | `level IN (1,2)` |
| Null checks | `IS NULL`, `IS NOT NULL` | `code IS NOT NULL` |
| Logical | `AND`, `OR`, `NOT` | `level=3 AND name LIKE '%CH%'` |

String values must be enclosed in **single quotes**.

### Example queries

Exact match on level:

```
/ogcapi/collections/dhis2-org-units-cql/items?filter=level=2
```

String match on name:

```
/ogcapi/collections/dhis2-org-units-cql/items?filter=name='0002 CH Mittaphap'
```

LIKE (case-sensitive pattern):

```
/ogcapi/collections/dhis2-org-units-cql/items?filter=name LIKE '%Hospital%'
```

ILIKE (case-insensitive pattern):

```
/ogcapi/collections/dhis2-org-units-cql/items?filter=name ILIKE '%hospital%'
```

Combined filter with AND:

```
/ogcapi/collections/dhis2-org-units-cql/items?filter=level=3 AND name LIKE '%CH%'
```

BETWEEN range:

```
/ogcapi/collections/dhis2-org-units-cql/items?filter=level BETWEEN 2 AND 3
```

IN set membership:

```
/ogcapi/collections/dhis2-org-units-cql/items?filter=level IN (1,2)
```

NULL check combined with comparison:

```
/ogcapi/collections/dhis2-org-units-cql/items?filter=code IS NULL AND level=5
```

## Processes

OGC API - Processes exposes server-side processing tasks. Each process defines typed inputs and outputs and can be executed synchronously or asynchronously via `POST`.

### Available processes

| Process | ID | Description |
|---|---|---|
| ERA5-Land | `era5-land-download` | Download ERA5-Land hourly climate data (temperature, precipitation, etc.) |
| CHIRPS3 | `chirps3-download` | Download CHIRPS3 daily precipitation data |

### Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/ogcapi/processes` | List all available processes |
| GET | `/ogcapi/processes/{processId}` | Describe a process (inputs, outputs, metadata) |
| POST | `/ogcapi/processes/{processId}/execution` | Execute a process (sync or async) |
| GET | `/ogcapi/jobs` | List all jobs |
| GET | `/ogcapi/jobs/{jobId}` | Get job status |
| GET | `/ogcapi/jobs/{jobId}/results` | Get job results |
| DELETE | `/ogcapi/jobs/{jobId}` | Cancel or delete a job |

### Common inputs

All climate processes share these inputs:

| Input | Type | Required | Description |
|---|---|---|---|
| `start` | string | yes | Start date in `YYYY-MM` format |
| `end` | string | yes | End date in `YYYY-MM` format |
| `bbox` | array[number] | yes | Bounding box `[west, south, east, north]` |
| `dry_run` | boolean | no | If true (default), return data without pushing to DHIS2 |

### ERA5-Land (`era5-land-download`)

Downloads ERA5-Land hourly climate data via the CDS API.

Additional inputs:

| Input | Type | Required | Default | Description |
|---|---|---|---|---|
| `variables` | array[string] | no | `["2m_temperature", "total_precipitation"]` | ERA5-Land variable names |

Example request:

```bash
curl -X POST http://localhost:8000/ogcapi/processes/era5-land-download/execution \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": {
      "start": "2024-01",
      "end": "2024-03",
      "bbox": [32.0, -2.0, 35.0, 1.0],
      "variables": ["2m_temperature"],
      "dry_run": true
    }
  }'
```

### CHIRPS3 (`chirps3-download`)

Downloads CHIRPS3 daily precipitation data.

Additional inputs:

| Input | Type | Required | Default | Description |
|---|---|---|---|---|
| `stage` | string | no | `"final"` | Product stage: `"final"` or `"prelim"` |

Example request:

```bash
curl -X POST http://localhost:8000/ogcapi/processes/chirps3-download/execution \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": {
      "start": "2024-01",
      "end": "2024-03",
      "bbox": [32.0, -2.0, 35.0, 1.0],
      "stage": "final",
      "dry_run": true
    }
  }'
```

### Process output

All processes return a JSON object with:

```json
{
  "status": "completed",
  "files": ["path/to/file1.nc", "path/to/file2.nc"],
  "summary": {
    "file_count": 2,
    "start": "2024-01",
    "end": "2024-03"
  },
  "message": "Data downloaded (dry run)"
}
```

## Async execution and job management

Climate data downloads (ERA5-Land, CHIRPS3) can take minutes. To avoid HTTP timeouts, processes support asynchronous execution via the `Prefer: respond-async` header.

### Submitting an async request

Add the `Prefer: respond-async` header to a normal execution request. The server returns `201 Created` with a `Location` header pointing to the job status endpoint.

```bash
curl -X POST http://localhost:8000/ogcapi/processes/chirps3-download/execution \
  -H "Prefer: respond-async" \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": {
      "start": "2024-01",
      "end": "2024-01",
      "bbox": [32, -2, 35, 1]
    }
  }'
```

Response (`201 Created`):

```json
{
  "jobID": "abc123",
  "status": "accepted",
  "type": "process",
  "message": "Job accepted",
  "...": "..."
}
```

The `Location` response header contains the job URL, e.g. `/ogcapi/jobs/abc123`.

### Polling job status

```bash
curl http://localhost:8000/ogcapi/jobs/{jobId}
```

The `status` field progresses through: `accepted` -> `running` -> `successful` (or `failed`).

### Retrieving results

Once status is `successful`:

```bash
curl http://localhost:8000/ogcapi/jobs/{jobId}/results
```

### Listing all jobs

```bash
curl http://localhost:8000/ogcapi/jobs
```

### Deleting a job

```bash
curl -X DELETE http://localhost:8000/ogcapi/jobs/{jobId}
```

### Synchronous execution (default)

Without the `Prefer` header, requests execute synchronously and return results directly. This is unchanged from before.

## Plugin system

pygeoapi uses a plugin architecture so that new data backends, output formats, and processing tasks can be added without modifying the core.

### Plugin categories

| Category | Base class | Purpose |
|---|---|---|
| **provider** | `pygeoapi.provider.base.BaseProvider` | Data access (read features, coverages, tiles, etc.) |
| **formatter** | `pygeoapi.formatter.base.BaseFormatter` | Output format conversion (e.g. CSV export) |
| **process** | `pygeoapi.process.base.BaseProcessor` | Server-side processing logic |
| **process_manager** | `pygeoapi.process.manager.base.BaseManager` | Job tracking and async execution |

### How loading works

In the YAML config the `name` field on a provider or processor identifies the plugin. pygeoapi resolves it in two ways:

1. **Short name** -- a built-in alias registered in pygeoapi's plugin registry (e.g. `GeoJSON`, `CSV`, `rasterio`).
2. **Dotted Python path** -- a fully-qualified class name for custom plugins (e.g. `mypackage.providers.MyProvider`).

### Plugin directory layout

Custom plugins live under `src/eo_api/routers/ogcapi/plugins/`, organized by type:

```
plugins/
  __init__.py
  providers/            # Data access plugins (BaseProvider subclasses)
    __init__.py
    dhis2_common.py     # Shared DHIS2 models and helpers
    dhis2_org_units.py  # Feature provider for DHIS2 org units
    dhis2_org_units_cql.py  # Feature provider with CQL filter support
    dhis2eo.py          # EDR provider stub
  processes/            # Processing plugins (BaseProcessor subclasses)
    __init__.py
    schemas.py          # Pydantic models for process inputs/outputs
    era5_land.py        # ERA5-Land download processor
    chirps3.py          # CHIRPS3 download processor
```

### Creating a custom provider

A custom provider subclasses the appropriate base class and implements the required methods.

```python
from pygeoapi.provider.base import BaseProvider


class MyProvider(BaseProvider):
    def __init__(self, provider_def):
        super().__init__(provider_def)

    def get(self, identifier, **kwargs):
        ...

    def query(self, **kwargs):
        ...
```

Reference it in the config by dotted path:

```yaml
providers:
  - type: feature
    name: eo_api.routers.ogcapi.plugins.providers.my_provider.MyProvider
    data: /path/to/data
```

### Creating a custom processor

A custom processor subclasses `BaseProcessor`, defines `PROCESS_METADATA`, and implements `execute()`:

```python
from pygeoapi.process.base import BaseProcessor

PROCESS_METADATA = {
    "version": "0.1.0",
    "id": "my-process",
    "title": "My Process",
    "jobControlOptions": ["sync-execute"],
    "inputs": { ... },
    "outputs": { ... },
}


class MyProcessor(BaseProcessor):
    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data, outputs=None):
        # Validate inputs, run processing, return (mimetype, result)
        return "application/json", {"status": "completed"}
```

Reference it in the config:

```yaml
resources:
  my-process:
    type: process
    processor:
      name: eo_api.routers.ogcapi.plugins.processes.my_process.MyProcessor
```

## References

- OGC API standards catalogue: <https://ogcapi.ogc.org>
- OGC API - Features spec: <https://ogcapi.ogc.org/features/>
- OGC API - Coverages spec: <https://ogcapi.ogc.org/coverages/>
- OGC API - EDR spec: <https://ogcapi.ogc.org/edr/>
- OGC API - Processes spec: <https://ogcapi.ogc.org/processes/>
- pygeoapi documentation: <https://docs.pygeoapi.io>
- pygeoapi configuration guide: <https://docs.pygeoapi.io/en/latest/configuration.html>
- pygeoapi data publishing guide: <https://docs.pygeoapi.io/en/latest/data-publishing/>
- pygeoapi plugins: <https://docs.pygeoapi.io/en/latest/plugins.html>
- Community plugins wiki: <https://github.com/geopython/pygeoapi/wiki/CommunityPlugins>
- pygeoapi source: <https://github.com/geopython/pygeoapi>
