## Ingestion Artifact API Guide

This guide covers the native FastAPI surface for `ingestions`, `artifacts`, and `collections`, and shows how that native API connects to the standards-facing `/ogcapi` publication layer. It is intended as a practical walkthrough of the current branch behavior: 

- ingest data from upstream sources, 
- persist managed artifacts locally as Zarr when possible, with NetCDF fallback, 
- expose native artifact and collection state, 
- and browse published gridded data through OGC API Coverages.

## Sample Requests And Expected Formats

This section gives concrete example payloads and quick checks that can be run manually.

### 1. Ingest CHIRPS3 by bbox

Request:

```bash
curl -s -X POST http://127.0.0.1:8000/ingestions \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_id": "chirps3_precipitation_daily",
    "start": "2024-01-01",
    "end": "2024-01-31",
    "bbox": [-13.5, 6.9, -10.1, 10.0],
    "prefer_zarr": true,
    "publish": true
  }' | jq
```

Expected request format:

- HTTP method: `POST`
- content type: `application/json`
- body: JSON object

Expected response format:

- content type: JSON
- shape:

```json
{
  "ingestion_id": "uuid",
  "status": "completed",
  "artifact": {
    "artifact_id": "uuid",
    "dataset_id": "chirps3_precipitation_daily",
    "format": "zarr",
    "request_scope": {
      "start": "2024-01-01",
      "end": "2024-01-31",
      "bbox": [-13.5, 6.9, -10.1, 10.0],
      "country_code": null
    },
    "publication": {
      "status": "published",
      "collection_id": "chirps3_precipitation_daily-bbox-9aa80782",
      "pygeoapi_path": "/ogcapi/collections/chirps3_precipitation_daily-bbox-9aa80782"
    }
  }
}
```

What this means:

- `ingestion_id` currently matches the created artifact identity and is the handle a client can use to look the result up again later.
- `status = "completed"` means this branch is treating the request as a synchronous create call, not as a queued long-running job.
- `artifact.format = "zarr"` tells the client that the canonical stored result is a Zarr store rather than a single NetCDF file.
- `request_scope` is the logical definition of what was requested. It is more important than the artifact path because it explains why this artifact exists.
- `request_scope.bbox` tells us this artifact represents a geographic subset, not a whole-dataset global publication.
- `publication.status = "published"` means the artifact is already exposed through the standards-facing `/ogcapi` layer.
- `publication.collection_id` is the stable public identity for the published dataset slice. It is the name downstream OGC clients should care about.
- `publication.pygeoapi_path` is the standards-facing route, which matters more for interoperable client access than the native FastAPI bookkeeping routes.

### 2. Ingest WorldPop by country

Request:

```bash
curl -s -X POST http://127.0.0.1:8000/ingestions \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_id": "worldpop_population_yearly",
    "start": "2020",
    "end": "2020",
    "country_code": "SLE",
    "prefer_zarr": true,
    "publish": true
  }' | jq
```

Expected request format:

- HTTP method: `POST`
- content type: `application/json`
- body: JSON object

Expected response format:

- content type: JSON
- shape similar to CHIRPS3, but with:
  - `dataset_id = "worldpop_population_yearly"`
  - `request_scope.country_code = "SLE"`
  - stable collection id like `worldpop_population_yearly-country-sle`

What this means:

- `country_code = "SLE"` expresses a semantic scope, not just a geometry clip. It says this artifact is the Sierra Leone slice by country identity.
- a country-based scope is useful because it is easier to reproduce and reason about than an arbitrary bbox supplied by a client.
- the stable collection id derived from the country code is important because it creates a durable publication identity that can be reused across repeated refreshes.
- for products like population, this kind of scope is likely closer to how downstream DHIS2 users think about the data than a raw bounding box.

### 3. List artifacts

Request:

```bash
curl -s http://127.0.0.1:8000/artifacts | jq
```

Expected response format:

- content type: JSON
- shape:

```json
{
  "items": [
    {
      "artifact_id": "uuid",
      "dataset_id": "chirps3_precipitation_daily",
      "format": "zarr",
      "path": "/abs/path/to/store.zarr",
      "request_scope": {
        "start": "2024-01-01",
        "end": "2024-01-31",
        "bbox": [-13.5, 6.9, -10.1, 10.0],
        "country_code": null
      },
      "coverage": {
        "spatial": {
          "xmin": -13.52,
          "ymin": 6.92,
          "xmax": -10.12,
          "ymax": 10.02
        },
        "temporal": {
          "start": "2024-01-01",
          "end": "2024-01-31"
        }
      }
    }
  ]
}
```

What this means:

- `/artifacts` is the inventory of managed EO assets owned by this service.
- `artifact_id` is the internal stable identifier for a stored result, regardless of whether it is published through OGC.
- `path` points to the canonical on-disk representation. For Zarr, this is a store root, not a downloadable single file.
- `request_scope` tells us the logical query that produced the artifact; it is the basis for idempotency and future deduplication decisions.
- `coverage` describes what data actually exists in the artifact after processing, which may matter more than the original request if clipping or source constraints altered the realized extent.
- `coverage.spatial` is the realized data footprint, useful for publication metadata, previews, and sanity checks.
- `coverage.temporal` is the realized time span present in the stored artifact, which is especially important for time-series analytics and repeat imports.

### 4. List native collections

Request:

```bash
curl -s http://127.0.0.1:8000/collections | jq
```

Expected response format:

- content type: JSON
- shape:

```json
{
  "items": [
    {
      "collection_id": "chirps3_precipitation_daily-bbox-9aa80782",
      "dataset_id": "chirps3_precipitation_daily",
      "latest_artifact_id": "uuid",
      "artifact_count": 1,
      "pygeoapi_path": "/ogcapi/collections/chirps3_precipitation_daily-bbox-9aa80782"
    }
  ]
}
```

What this means:

- `/collections` is not the raw OGC endpoint; it is the native registry view of what EO API has published.
- `collection_id` is the public logical dataset identity for a stable published scope.
- `latest_artifact_id` tells us which artifact currently represents the freshest backing data for that collection.
- `artifact_count` tells us whether the collection has publication history behind it or is currently backed by a single artifact only.
- `pygeoapi_path` is the canonical standards-facing entrypoint for interoperable clients.
- this endpoint is useful for management and debugging because it exposes the relation between internal artifact history and public collection identity.

### 5. Get native collection detail

Request:

```bash
curl -s http://127.0.0.1:8000/collections/chirps3_precipitation_daily-bbox-9aa80782 | jq
```

Expected response format:

- content type: JSON
- includes:
  - collection summary
  - `artifacts` array
  - artifact history entries with `artifact_api_path`

Shape:

```json
{
  "collection_id": "chirps3_precipitation_daily-bbox-9aa80782",
  "latest_artifact_id": "uuid",
  "artifact_count": 1,
  "artifacts": [
    {
      "artifact_id": "uuid",
      "format": "zarr",
      "request_scope": {
        "start": "2024-01-01",
        "end": "2024-01-31",
        "bbox": [-13.5, 6.9, -10.1, 10.0],
        "country_code": null
      },
      "artifact_api_path": "/artifacts/uuid"
    }
  ]
}
```

What this means:

- this endpoint explains the history behind a published collection, not just its current public face.
- the top-level collection fields summarize the current effective publication.
- the `artifacts` array shows which concrete stored artifacts have been associated with the same logical collection over time.
- `artifact_api_path` is the bridge back from the public collection abstraction to the exact managed artifact record.
- this is important for auditability: a client can tell not only what is published, but what stored artifact version currently stands behind that publication.

### 6. List published OGC collections

Request:

```bash
curl -s "http://127.0.0.1:8000/ogcapi/collections?f=json" | jq
```

Expected response format:

- content type: `application/json`
- pygeoapi collection list document
- includes collection `id`, `title`, `links`, and `extent`

What this means:

- this is the standards-facing collection inventory intended for generic OGC clients, not just EO API-specific tooling.
- `id` is the public interoperable identifier a standards client uses in follow-up requests.
- `links` tell a client what related resources are available next, such as collection metadata or coverage access.
- `extent` tells a client the advertised spatial and temporal footprint of the published collection.
- this endpoint is the one that matters for external interoperability; `/collections` is the native operational mirror.

### 7. Inspect OGC collection metadata

Request:

```bash
curl -s "http://127.0.0.1:8000/ogcapi/collections/chirps3_precipitation_daily-bbox-9aa80782?f=json" | jq
```

Expected response format:

- content type: `application/json`
- pygeoapi collection document
- includes:
  - OGC links
  - coverage links
  - extra native links back to:
    - `/collections/{collection_id}`
    - `/artifacts/{artifact_id}`

What this means:

- this is the public metadata document for one published collection.
- OGC links describe the standards-defined navigation options available to clients.
- coverage links tell us this collection is being served primarily as gridded coverage data rather than as a feature collection.
- the extra native links are useful because they connect the public OGC view back to EO API's internal artifact and registry model.
- this document is where a client learns both what the collection is and how to retrieve or inspect it further.

### 8. Read coverage JSON

Request:

```bash
curl -s "http://127.0.0.1:8000/ogcapi/collections/chirps3_precipitation_daily-bbox-9aa80782/coverage?f=json" | jq
```

Expected response format:

- content type: `application/prs.coverage+json` or JSON-compatible response
- OGC Coverage JSON document
- for CHIRPS3:
  - spatial axes `x` and `y`
  - temporal axis `t`
  - `ranges.precip`

Representative shape:

```json
{
  "type": "Coverage",
  "domain": {
    "type": "Domain",
    "domainType": "Grid",
    "axes": {
      "x": {"start": -13.52, "stop": -10.12, "num": 69},
      "y": {"start": 10.02, "stop": 6.92, "num": 63},
      "t": {"values": ["2024-01-01T00:00:00.000000000", "..."]}
    }
  },
  "ranges": {
    "precip": {
      "type": "NdArray"
    }
  }
}
```

What this means:

- this is the actual gridded data view, not just collection metadata.
- `domain.axes.x`, `domain.axes.y`, and `domain.axes.t` describe the coordinate system of the coverage payload.
- `num` on spatial axes tells us grid size along each dimension, which gives a fast sense of the resolution and response weight.
- `ranges.precip` tells us the measured variable being served and where the numeric array payload lives conceptually.
- this response is the standards-aligned way to access published EO grid content without exposing the raw Zarr structure directly.
- for downstream consumers, this is the important “data access” contract, while `/collections/{id}` is the “metadata and discovery” contract.

### 9. Browse raw Zarr store

Request:

```bash
curl -s http://127.0.0.1:8000/artifacts/<artifact_id>/zarr | jq
```

Expected response format:

- content type: JSON
- top-level Zarr directory listing

Shape:

```json
{
  "artifact_id": "uuid",
  "dataset_id": "chirps3_precipitation_daily",
  "format": "zarr",
  "store_root": "/abs/path/to/store.zarr",
  "entries": [
    {"name": "precip", "kind": "directory", "href": "/artifacts/uuid/zarr/precip"},
    {"name": "time", "kind": "directory", "href": "/artifacts/uuid/zarr/time"},
    {"name": "x", "kind": "directory", "href": "/artifacts/uuid/zarr/x"},
    {"name": "y", "kind": "directory", "href": "/artifacts/uuid/zarr/y"},
    {"name": "zarr.json", "kind": "file", "href": "/artifacts/uuid/zarr/zarr.json"}
  ]
}
```

What this means:

- this is the raw managed-artifact view of the Zarr store, not the OGC publication view.
- `store_root` tells us where the canonical Zarr store exists on disk inside EO API-managed storage.
- each `entries[].href` is part of the same Zarr namespace and can be followed to inspect deeper directories or fetch objects.
- directory names like `precip`, `time`, `x`, and `y` reflect the Zarr structure of arrays and coordinates, which is useful for Zarr-aware tooling and debugging.
- this endpoint is primarily about artifact transparency: it shows that the service is preserving Zarr as Zarr instead of hiding it behind a repackaged export.

### 10. Inspect Zarr metadata conveniently

Request:

```bash
curl -s http://127.0.0.1:8000/artifacts/<artifact_id>/zarr/precip/zarr.json | jq
```

Expected response format:

- content type: JSON
- inline metadata JSON for the Zarr array or store

Use cases:

- inspect store metadata without browser download behavior
- browse directories such as:
  - `/artifacts/<artifact_id>/zarr/precip`
  - `/artifacts/<artifact_id>/zarr/time`

What this means:

- a metadata file like `zarr.json` explains how an array or group is organized internally.
- these metadata documents tell us things like shape, chunking, codecs, attributes, and hierarchy, depending on what level is being inspected.
- that information is important for debugging performance, validating publication assumptions, and confirming that the stored artifact matches what downstream Zarr-aware clients expect.
- this is especially useful when the public OGC layer works, but we need to understand the actual underlying data-cube representation.

### 11. Raw Zarr object access

Request:

```bash
curl -i http://127.0.0.1:8000/artifacts/<artifact_id>/zarr/precip/zarr.json
```

Expected response format:

- raw file access under the Zarr store
- actual content type depends on the stored object and browser/client behavior
- this route is the raw artifact contract, not the convenience inspection route

What this means:

- this is the lowest-level artifact-serving behavior in the branch.
- the same namespace can expose both metadata objects and chunk/data objects because that matches how a Zarr store is actually organized.
- clients that understand Zarr can treat EO API as a real store-backed source rather than as a system that only emits derived APIs.
- this matters strategically because it keeps the artifact plane honest: the service is not only publishing OGC products, it is also preserving the native stored data form.

## What Is Deferred For Team Discussion

These are the main next-step questions to discuss with the team:

1. For same dataset + same scope + extended time range:
   - create a bigger replacement artifact?
   - append to canonical scope store?
   - keep artifact history as manifests over shared storage?

2. For one collection with multiple artifacts behind it:
   - always use latest artifact by default?
   - allow historical selection by request?
   - eventually resolve from multiple artifacts?

3. When to add feature collections:
   - after raw coverage path is fully stable
   - likely as derived outputs, not as another encoding of the raw grid

## Short Summary

This branch now proves a real end-to-end slice:

1. download EO grids
2. store them locally as managed artifacts
3. prefer Zarr where possible
4. auto-publish them through `pygeoapi`
5. browse them through `/ogcapi`
6. inspect artifact and collection state through native FastAPI
7. serve raw Zarr as Zarr
