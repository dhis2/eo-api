# OGC API and pygeoapi

## OGC API overview

OGC API is a family of standards from the [Open Geospatial Consortium](https://www.ogc.org/) that define RESTful interfaces for geospatial data. Each standard covers a specific data type or interaction pattern:

| Standard                                       | Purpose                                                            |
| ---------------------------------------------- | ------------------------------------------------------------------ |
| [Features](https://ogcapi.ogc.org/features/)   | Vector feature access (GeoJSON, etc.)                              |
| [Coverages](https://ogcapi.ogc.org/coverages/) | Gridded / raster data                                              |
| [EDR](https://ogcapi.ogc.org/edr/)             | Environmental Data Retrieval (point, trajectory, corridor queries) |
| [Processes](https://ogcapi.ogc.org/processes/) | Server-side processing / workflows                                 |
| [Maps](https://ogcapi.ogc.org/maps/)           | Rendered map images                                                |
| [Tiles](https://ogcapi.ogc.org/tiles/)         | Tiled data (vector and map tiles)                                  |
| [Records](https://ogcapi.ogc.org/records/)     | Catalogue / metadata search                                        |

All standards share a common core: JSON/HTML responses, OpenAPI-described endpoints, and content negotiation. The full specification catalogue is at <https://ogcapi.ogc.org>.

## pygeoapi

[pygeoapi](https://pygeoapi.io) is a Python server that implements the OGC API standards listed above. It is the OGC Reference Implementation for OGC API - Features.

In this project pygeoapi is mounted as a sub-application at `/ogcapi` by `open_climate_service/pygeoapi_app.py`. All dataset and behaviour configuration happens in YAML, not Python code.

- pygeoapi docs: <https://docs.pygeoapi.io>
- Source: <https://github.com/geopython/pygeoapi>

## Configuration

pygeoapi is configured through a single generated YAML file whose path is set by the `PYGEOAPI_CONFIG` environment variable. The generated file lives under `{data_dir}/pygeoapi/pygeoapi-config.yml` and is derived at startup from the base config at `open_climate_service/pygeoapi/base.yml`.

### Top-level sections

```yaml
server: # host, port, URL, limits, CORS, languages, admin toggle
logging: # log level and optional log file
metadata: # service identification, contact, license
resources: # datasets and processes exposed by the API
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
      en: Open Climate Service
    description:
      en: OGC API compliant geospatial data API
  provider:
    name: Open Climate Service
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

| Provider type | OGC API standard | Description                                                                                               |
| ------------- | ---------------- | --------------------------------------------------------------------------------------------------------- |
| `feature`     | Features         | Vector data (points, lines, polygons). Backends include CSV, GeoJSON, PostGIS, Elasticsearch, and others. |
| `coverage`    | Coverages        | Gridded / raster data. Backends include rasterio, xarray, and S3-hosted COGs.                             |
| `map`         | Maps             | Rendered map images, typically proxied from an upstream WMS via `WMSFacade`.                              |
| `process`     | Processes        | Server-side processing tasks. In Open Climate Service, the native `/processes` surface is authoritative; pygeoapi process support is not the primary process runtime. |

A single collection can have multiple providers (e.g. both `feature` and `tile` on the same resource).

## CQL filtering

pygeoapi supports [CQL2](https://docs.ogc.org/is/21-065r2/21-065r2.html) text filters on collections backed by a CQL-capable provider. Filters are passed as query parameters:

```
?filter=<expression>
```

The `dhis2-org-units-cql` collection exposes this capability. Its filterable properties are `name`, `code`, `shortName`, `level`, and `openingDate`.

### Supported operators

| Category         | Operators                                            | Example                        |
| ---------------- | ---------------------------------------------------- | ------------------------------ |
| Comparison       | `=`, `<>`, `<`, `<=`, `>`, `>=`                      | `level=2`                      |
| Pattern matching | `LIKE`, `ILIKE` (`%` = any chars, `_` = single char) | `name LIKE '%Hospital%'`       |
| Range            | `BETWEEN ... AND ...`                                | `level BETWEEN 2 AND 3`        |
| Set membership   | `IN (...)`                                           | `level IN (1,2)`               |
| Null checks      | `IS NULL`, `IS NOT NULL`                             | `code IS NOT NULL`             |
| Logical          | `AND`, `OR`, `NOT`                                   | `level=3 AND name LIKE '%CH%'` |

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

Dataset ingestion and analysis are handled by the native API (`POST /ingestions`, `POST /sync`, openEO `POST /jobs`) — not by pygeoapi processes. The `/ogcapi/processes` endpoint is available via pygeoapi but no custom processes are currently registered.

## Plugin system

pygeoapi uses a plugin architecture so that new data backends, output formats, and processing tasks can be added without modifying the core.

### Plugin categories

| Category            | Base class                                  | Purpose                                             |
| ------------------- | ------------------------------------------- | --------------------------------------------------- |
| **provider**        | `pygeoapi.provider.base.BaseProvider`       | Data access (read features, coverages, tiles, etc.) |
| **formatter**       | `pygeoapi.formatter.base.BaseFormatter`     | Output format conversion (e.g. CSV export)          |
| **process**         | `pygeoapi.process.base.BaseProcessor`       | Server-side processing logic                        |
| **process_manager** | `pygeoapi.process.manager.base.BaseManager` | Job tracking and async execution                    |

### How loading works

In the YAML config the `name` field on a provider or processor identifies the plugin. pygeoapi resolves it in two ways:

1. **Short name** -- a built-in alias registered in pygeoapi's plugin registry (e.g. `GeoJSON`, `CSV`, `rasterio`).
2. **Dotted Python path** -- a fully-qualified class name for custom plugins (e.g. `mypackage.providers.MyProvider`).

### Plugin directory layout

Custom plugins live anywhere importable by dotted path. Reference them by fully-qualified class name in the YAML config (see below).

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
    name: open_climate_service.routers.ogcapi.plugins.providers.my_provider.MyProvider
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
      name: open_climate_service.routers.ogcapi.plugins.processes.my_process.MyProcessor
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
