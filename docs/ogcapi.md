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
?filter=<expression>&filter-lang=cql-text
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
/ogcapi/collections/dhis2-org-units-cql/items?filter=level=2&filter-lang=cql-text
```

String match on name:

```
/ogcapi/collections/dhis2-org-units-cql/items?filter=name='0002 CH Mittaphap'&filter-lang=cql-text
```

LIKE (case-sensitive pattern):

```
/ogcapi/collections/dhis2-org-units-cql/items?filter=name LIKE '%Hospital%'&filter-lang=cql-text
```

ILIKE (case-insensitive pattern):

```
/ogcapi/collections/dhis2-org-units-cql/items?filter=name ILIKE '%hospital%'&filter-lang=cql-text
```

Combined filter with AND:

```
/ogcapi/collections/dhis2-org-units-cql/items?filter=level=3 AND name LIKE '%CH%'&filter-lang=cql-text
```

BETWEEN range:

```
/ogcapi/collections/dhis2-org-units-cql/items?filter=level BETWEEN 2 AND 3&filter-lang=cql-text
```

IN set membership:

```
/ogcapi/collections/dhis2-org-units-cql/items?filter=level IN (1,2)&filter-lang=cql-text
```

NULL check combined with comparison:

```
/ogcapi/collections/dhis2-org-units-cql/items?filter=code IS NULL AND level=5&filter-lang=cql-text
```

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

1. **Short name** -- a built-in alias registered in pygeoapi's plugin registry (e.g. `GeoJSON`, `CSV`, `rasterio`, `HelloWorld`).
2. **Dotted Python path** -- a fully-qualified class name for custom plugins (e.g. `mypackage.providers.MyProvider`).

### Creating a custom plugin

A custom provider needs to subclass the appropriate base class and implement the required methods.

```python
from pygeoapi.provider.base import BaseProvider


class MyProvider(BaseProvider):
    """Custom feature provider."""

    def __init__(self, provider_def):
        super().__init__(provider_def)
        # provider_def contains the YAML provider block

    def get(self, identifier, **kwargs):
        # Return a single feature by ID
        ...

    def query(self, **kwargs):
        # Return a FeatureCollection matching the query parameters
        ...
```

Reference it in the config by dotted path:

```yaml
providers:
  - type: feature
    name: mypackage.providers.MyProvider
    data: /path/to/data
```

For processes, subclass `BaseProcessor` and set `PROCESS_METADATA` as a class-level dict describing inputs and outputs:

```python
from pygeoapi.process.base import BaseProcessor

PROCESS_METADATA = {
    "version": "0.1.0",
    "id": "my-process",
    "title": "My Process",
    "inputs": { ... },
    "outputs": { ... },
}


class MyProcessor(BaseProcessor):
    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):
        # Process input data and return results
        ...
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
