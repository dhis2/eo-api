# Zarr and GeoZarr

This document explains why the Open Climate Service uses Zarr as its primary storage format, how Zarr stores are structured and served, and how GeoZarr root attributes enable map rendering.

---

## What is Zarr?

[Zarr](https://zarr.dev) is an open storage format for chunked, compressed N-dimensional arrays. A Zarr store is a directory tree: array metadata lives in `zarr.json` files, and the data itself is split into independent chunk files. Each chunk is compressed independently and can be read in a single HTTP request.

Zarr is designed to work natively in cloud object stores as well as on local disk — the directory layout is the same in both cases. The [Zarr v3 specification](https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html) is the current standard.

---

## What is GeoZarr?

[GeoZarr](https://github.com/zarr-developers/geozarr-spec) is a draft convention that adds spatial context to Zarr stores. A plain Zarr array has no concept of geography — it is just numbers in a grid. GeoZarr defines a small set of root attributes (`spatial:bbox`, `proj:code`, `zarr_conventions`) that tell a client where the grid is located on Earth and in which coordinate reference system.

---

## Why Zarr

Climate datasets are large, multi-dimensional arrays: a daily precipitation dataset covering a country at 5 km resolution for 10 years has roughly 3 600 time steps and hundreds of thousands of spatial pixels. Serving this efficiently from a REST API requires a format that supports:

- **Chunk-level random access** — a client requesting one time step should not have to read the entire file. Zarr stores data in independent, addressable chunks; a request for a single date reads only the relevant chunk.
- **HTTP-native serving** — each chunk is a separate file on disk. A standard `GET /zarr/{dataset_id}/{chunk_path}` serves it with a regular `FileResponse`. No specialised server software is needed.
- **Cloud compatibility** — the same directory layout works on local disk and cloud storage without code changes.
- **Multiscale pyramids** — GeoZarr defines a multiscales convention that allows a store to contain multiple resolution levels. Map clients request only the level that matches their current zoom, avoiding full-resolution downloads.

---

## ARCO: Analysis-Ready, Cloud-Optimized

The stores produced by the Open Climate Service are an instance of the **ARCO** pattern — a term from the climate science community describing datasets that are simultaneously ready for analysis and optimised for cloud access.

The two halves of the term map directly onto the choices described in this document:

**Analysis-ready** means a consumer can open the data and start computing without preprocessing:

- Dimension names are normalised to `(time, x, y)` regardless of the source convention.
- All datasets in an instance share a single coordinate reference system.
- Units are standardised by the transform pipeline (e.g. Kelvin → Celsius).

**Cloud-optimized** means the data can be accessed efficiently over HTTP without downloading the whole file. The Zarr and GeoZarr formats provide all the necessary properties — chunk-level access, HTTP-native serving, multiscale pyramids, and cloud compatibility.

The Open Climate Service targets the same access pattern at country scale for arbitrary source datasets.

---

## Store layout on disk

Each managed dataset has exactly one Zarr store on disk, stored under `{data_dir}/downloads/{dataset_id}.zarr`. The store is either:

- **Flat** — a single-resolution Zarr store with dimensions `(time, x, y)`
- **Pyramid** — a multi-resolution Zarr store with levels `0/`, `1/`, `2/`, … where `0/` is the full resolution

The flat vs. pyramid decision is made at build time based on spatial size (see [Multiscale pyramids](#multiscale-pyramids) below).

---

## Chunk sizing

Chunks are sized to match expected access patterns. The goal is that reading one time step for the full spatial extent fits in one round-trip, and that full time series for a small area also fits in one round-trip.

Time chunk sizes are derived from the dataset's `extents.temporal.resolution` field, an ISO 8601 duration (e.g. `P1D`, `PT1H`, `P1M`). When present and valid, the duration is converted to approximate hours and mapped to a natural analysis window:

| Duration tier       | Approximate hours | Target window | Example                     |
| ------------------- | ----------------- | ------------- | --------------------------- |
| Sub-daily           | < 24 h            | ~1 week       | `PT1H` (hourly) → 168 steps |
| Daily to sub-weekly | 24 h – 168 h      | ~1 month      | `P1D` (daily) → 30 steps    |
| Weekly and coarser  | ≥ 168 h           | ~1 year       | `P1M` (monthly) → 12 steps  |

This calculation is fully data-driven: any dataset — including custom or plugin datasets — only needs to declare `extents.temporal.resolution` and the correct chunk size is computed automatically. If the field is absent or not a valid ISO 8601 duration, a warning is logged and the time chunk falls back to the dataset's `period_type`.

Spatial chunks are capped at 512 × 512 pixels — a pragmatic compromise between tile rendering (which benefits from smaller chunks) and analysis workloads (which benefit from larger ones). For small extents where the full spatial dimension is smaller than 512 pixels, the entire dimension fits in one chunk.

Dimension names are normalised to `(time, x, y)` before writing, regardless of the source naming convention (`lat`/`lon`, `latitude`/`longitude`, etc.).

---

## Multiscale pyramids

For large spatial extents, a flat zarr would require a map viewer to download the entire spatial extent at full resolution on every tile request. The platform builds a multiscale pyramid when the spatial dimensions exceed **2048 × 2048 pixels**.

Pyramid levels are computed as:

```
levels = ceil(log2(max_dim / 512))   # clamped to [2, 8]
```

Where 512 is the target tile size in pixels. Each level halves the resolution in both spatial dimensions using mean downsampling. Level `0/` is always the full resolution.

Both flat and pyramid stores are written in **Zarr v3** format.

---

## GeoZarr root attributes

A plain Zarr store has no concept of spatial coordinates. A map viewer opening it has no way to know where to position tiles on a map. GeoZarr addresses this by writing a small set of attributes into `zarr.json` at the store root:

| Attribute          | Example value             | Purpose                        |
| ------------------ | ------------------------- | ------------------------------ |
| `spatial:bbox`     | `[3.0, 57.0, 32.0, 72.5]` | Bounding box in the native CRS |
| `proj:code`        | `EPSG:4326`               | CRS of the stored coordinates  |
| `zarr_conventions` | `[{...}]`                 | Convention declarations        |

These attributes are computed from the actual coordinate bounds of the written data and the instance CRS. They are always written by the framework after transforms and reprojection have run. This guarantees they always reflect the final stored data.

`zarr_conventions` for a flat store contains the base GeoZarr convention declaration. For pyramid stores it also includes a multiscales entry that declares the level structure.

---

## CRS handling

The instance CRS is configured in `climate-service.yaml`:

```yaml
extent:
  bbox: [3.0, 57.0, 32.0, 72.5]
  crs: EPSG:32633 # optional; defaults to EPSG:4326
```

Datasets are always stored in the instance CRS. During ingestion, data is reprojected from its source CRS (declared as `source_crs` in the template, default `EPSG:4326`) to the instance CRS. The stored `spatial:bbox` is therefore in the instance CRS — UTM eastings and northings for a UTM instance, degrees for a WGS84 instance.

STAC metadata also stores the WGS84 bounding box alongside the native bbox, so catalogue clients that expect geographic coordinates always get one regardless of the instance CRS.

---

## How Zarr stores are served

The `/zarr/{dataset_id}/` endpoint serves individual files from the Zarr store directory using FastAPI's `FileResponse`. The ZarrLayer client issues one HTTP request per chunk file it needs.

```
GET /zarr/{dataset_id}/zarr.json          → root metadata (JSON)
GET /zarr/{dataset_id}/precip/c/0/0/0     → chunk at time=0, x=0, y=0
GET /zarr/{dataset_id}/time/c/0           → time coordinate chunk
```

Metadata files (`zarr.json`) are returned as `application/json`. All other files — chunk data — are returned as `application/octet-stream`. Directory paths return a JSON listing of their contents.

This design means the zarr store is served by ordinary file serving — there is no zarr-specific server middleware.

---

## Fill values and NaN handling

When writing float data to Zarr, missing data is stored as IEEE `NaN`. The map viewer uses the zarr `fill_value` attribute (which defaults to `NaN` for float arrays) to render missing pixels as transparent.
