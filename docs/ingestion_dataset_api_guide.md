# EO API Ingestion and Dataset Guide

This guide describes the current native FastAPI surface for EO API and how it relates to the standards-facing `pygeoapi` publication layer.

The current public story is:

- ingest a managed dataset with `POST /ingestions`
- discover configured extents with `/extents`
- discover managed datasets with `/datasets`
- access raw Zarr data with `/zarr/{dataset_id}`
- access standards-facing publication with `/ogcapi/...`

Internal artifacts still exist as a storage and provenance model, but they are not part of the public API contract.

## Main Public Endpoints

- `POST /ingestions`
- `GET /ingestions/{ingestion_id}`
- `GET /extents`
- `GET /extents/{extent_id}`
- `GET /datasets`
- `GET /datasets/{dataset_id}`
- `GET /datasets/{dataset_id}/download`
- `GET /zarr/{dataset_id}`
- `GET /zarr/{dataset_id}/{relative_path}`
- `POST /sync/{dataset_id}`
- `GET /ogcapi/collections`
- `GET /ogcapi/collections/{dataset_id}`
- `GET /ogcapi/collections/{dataset_id}/coverage`

## 1. Discover configured extents

Configured extents are setup-time EO API configuration. They are read-only at runtime and are identified by `extent_id`.

Example:

```bash
curl -s http://127.0.0.1:8000/extents | jq
```

Example response:

```json
{
  "kind": "ExtentList",
  "items": [
    {
      "extent_id": "sle",
      "name": "Sierra Leone",
      "description": "National extent for Sierra Leone.",
      "bbox": [-13.5, 6.9, -10.1, 10.0]
    }
  ]
}
```

What this means:

- `extent_id` is the public EO API handle for a configured spatial extent
- `bbox` is the resolved spatial extent exposed publicly
- provider-specific hints may exist internally in extent config, but they are not part of the public extent response

## 2. Ingest a dataset

The public ingestion contract now takes:

- `dataset_id`
- `start`
- optional `end`
- optional `extent_id`
- `overwrite`
- `prefer_zarr`
- `publish`

Raw `bbox` and `country_code` are no longer part of the public ingestion payload.

### Example: CHIRPS3

```bash
curl -s -X POST http://127.0.0.1:8000/ingestions \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_id": "chirps3_precipitation_daily",
    "start": "2024-01-01",
    "end": "2024-01-31",
    "extent_id": "sle",
    "overwrite": false,
    "prefer_zarr": true,
    "publish": true
  }' | jq
```

### Example: WorldPop

```bash
curl -s -X POST http://127.0.0.1:8000/ingestions \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_id": "worldpop_population_yearly",
    "start": "2020",
    "end": "2020",
    "extent_id": "sle",
    "overwrite": false,
    "prefer_zarr": true,
    "publish": true
  }' | jq
```

Example response:

```json
{
  "ingestion_id": "a7e06c93-ba78-4c74-b772-160927fdb463",
  "status": "completed",
  "dataset": {
    "dataset_id": "chirps3_precipitation_daily-extent-sle",
    "source_dataset_id": "chirps3_precipitation_daily",
    "dataset_name": "Total precipitation (CHIRPS3)",
    "short_name": "Total precipitation",
    "variable": "precip",
    "period_type": "daily",
    "units": "mm",
    "resolution": "5 km x 5 km",
    "source": "CHIRPS v3",
    "source_url": "https://www.chc.ucsb.edu/data/chirps3",
    "extent": {
      "spatial": {
        "xmin": -13.52499751932919,
        "ymin": 6.92499920912087,
        "xmax": -10.124997468665242,
        "ymax": 10.02499925531447
      },
      "temporal": {
        "start": "2024-01-01",
        "end": "2024-01-31"
      }
    },
    "last_updated": "2026-04-01T09:03:28.691120Z",
    "links": [
      {
        "href": "/datasets/chirps3_precipitation_daily-extent-sle",
        "rel": "self",
        "title": "Dataset detail"
      },
      {
        "href": "/zarr/chirps3_precipitation_daily-extent-sle",
        "rel": "zarr",
        "title": "Zarr store"
      },
      {
        "href": "/ogcapi/collections/chirps3_precipitation_daily-extent-sle",
        "rel": "ogc-collection",
        "title": "OGC collection"
      }
    ],
    "publication": {
      "status": "published",
      "published_at": "2026-04-01T09:03:28.692230Z"
    }
  }
}
```

What this means:

- `ingestion_id` is the handle for the ingestion event lookup route
- `status = "completed"` means this branch still treats ingestion synchronously
- `dataset` is a public managed dataset summary, not an internal artifact record
- `extent` is realized data coverage, not just the configured bbox
- `links` point to the native dataset metadata, native Zarr access, and standards-facing OGC collection

## 3. Ingestion failure behavior

Ingestion should fail gracefully with a structured API error, not a raw 500 stack trace.

Current behavior:

- invalid or missing spatial/config inputs return `400`
- dataset/provider execution failures return `502`

Example cases:

- a provider requires a country code and the resolved extent config does not provide one
- a dataset requires a bbox and no bbox can be resolved
- the upstream provider fails at download time

Example error response:

```json
{
  "detail": "Upstream dataset download failed: provider timeout"
}
```

## 4. Discover managed datasets

`GET /datasets` is the native managed-data catalog.

Example:

```bash
curl -s http://127.0.0.1:8000/datasets | jq
```

Example response:

```json
{
  "kind": "DatasetList",
  "items": [
    {
      "dataset_id": "chirps3_precipitation_daily-extent-sle",
      "source_dataset_id": "chirps3_precipitation_daily",
      "dataset_name": "Total precipitation (CHIRPS3)",
      "short_name": "Total precipitation",
      "variable": "precip",
      "period_type": "daily",
      "units": "mm",
      "resolution": "5 km x 5 km",
      "source": "CHIRPS v3",
      "source_url": "https://www.chc.ucsb.edu/data/chirps3",
      "extent": {
        "spatial": {
          "xmin": -13.52499751932919,
          "ymin": 6.92499920912087,
          "xmax": -10.124997468665242,
          "ymax": 10.02499925531447
        },
        "temporal": {
          "start": "2024-01-01",
          "end": "2024-01-31"
        }
      },
      "last_updated": "2026-04-01T09:03:28.691120Z",
      "links": [
        {
          "href": "/datasets/chirps3_precipitation_daily-extent-sle",
          "rel": "self",
          "title": "Dataset detail"
        },
        {
          "href": "/zarr/chirps3_precipitation_daily-extent-sle",
          "rel": "zarr",
          "title": "Zarr store"
        },
        {
          "href": "/ogcapi/collections/chirps3_precipitation_daily-extent-sle",
          "rel": "ogc-collection",
          "title": "OGC collection"
        }
      ],
      "publication": {
        "status": "published",
        "published_at": "2026-04-01T09:03:28.692230Z"
      }
    }
  ]
}
```

What this means:

- `/datasets` is the public native catalog of managed datasets
- `items` is wrapped in a `kind` envelope for consistency and self-description
- dataset items contain public metadata and access links only
- internal artifact ids, filesystem paths, and downloader implementation details are intentionally omitted

## 5. Get dataset detail

`GET /datasets/{dataset_id}` returns the full managed dataset detail view.

Example:

```bash
curl -s http://127.0.0.1:8000/datasets/chirps3_precipitation_daily-extent-sle | jq
```

What this adds beyond the list response:

- full dataset metadata
- publication summary
- slim `versions` history derived from internal records

The detailed dataset response is where version history belongs. The ingestion response stays as a summary.

## 6. Access raw Zarr data

If the latest managed dataset version is Zarr-backed, the canonical native raw-data route is `/zarr/{dataset_id}`.

Examples:

```bash
curl -s http://127.0.0.1:8000/zarr/chirps3_precipitation_daily-extent-sle | jq
curl -s http://127.0.0.1:8000/zarr/chirps3_precipitation_daily-extent-sle/zarr.json | jq
```

The listing response exposes:

- `kind`
- `dataset_id`
- `format`
- `path`
- `entries`

What this means:

- `/zarr/{dataset_id}` is for raw native data access
- dataset metadata remains under `/datasets`
- entry links stay inside the canonical `/zarr/{dataset_id}/...` namespace
- internal artifact ids and local filesystem roots are not exposed

## 7. Access published OGC collections

Published datasets are exposed only through `/ogcapi`.

Examples:

```bash
curl -s "http://127.0.0.1:8000/ogcapi/collections?f=json" | jq
curl -s "http://127.0.0.1:8000/ogcapi/collections/chirps3_precipitation_daily-extent-sle?f=json" | jq
curl -s "http://127.0.0.1:8000/ogcapi/collections/chirps3_precipitation_daily-extent-sle/coverage?f=json" | jq
```

What this means:

- `/ogcapi` is the only public collection surface
- native FastAPI no longer exposes `/collections`
- dataset responses include links to `/ogcapi/collections/{dataset_id}`, but the collection resource itself lives only under `pygeoapi`

## 8. `/sync`

`POST /sync/{dataset_id}` exists and is part of the intended public product shape, but its behavior is still the main refinement area.

Current intent:

- sync a managed dataset forward from its latest available period
- preserve stable managed dataset identity
- return the updated dataset view

This route should be treated as present but still under active behavioral design.

## Manual Test Sequence

For a clean manual test, this is the best sequence to run:

1. `GET /extents`
2. `POST /ingestions` with CHIRPS3
3. `GET /datasets`
4. `GET /datasets/{dataset_id}`
5. `GET /zarr/{dataset_id}`
6. `GET /ogcapi/collections`
7. `GET /ogcapi/collections/{dataset_id}`
8. `GET /ogcapi/collections/{dataset_id}/coverage`
9. `POST /ingestions` with WorldPop

Good demo payloads:

- CHIRPS3:
  - `dataset_id = "chirps3_precipitation_daily"`
  - `extent_id = "sle"`
  - `start = "2024-01-01"`
  - `end = "2024-01-31"`
- WorldPop:
  - `dataset_id = "worldpop_population_yearly"`
  - `extent_id = "sle"`
  - `start = "2020"`
  - `end = "2020"`

## Summary

The current branch is no longer an artifact-first API.

The public contract is now:

- ingest with `/ingestions`
- discover extents with `/extents`
- discover managed datasets with `/datasets`
- access raw native data with `/zarr/{dataset_id}`
- access standards-facing publication with `/ogcapi`

Artifacts remain internal because EO API still needs storage and provenance records behind ingestion and publication, but those internals are no longer exposed as first-class public resources.
