---
marp: true
theme: default
paginate: true
title: EO-API Architecture Direction
description: OGC-First, Lightweight, Provider-Based Design
---

# EO-API Architecture Direction
## OGC-First, Lightweight, Provider-Based Design

---

# 🎯 Problem Statement

We are building **eo-api** to:

- Fetch EO / climate datasets (starting with CHIRPS3)
- Accept AOIs (GeoJSON, bbox, DHIS2 org units)
- Compute zonal aggregates / time series
- Output results for:
  - CHAP modelling engine (CSV)
  - DHIS2 `dataValueSets`
  - ML pipelines
- Later: support visualization

We need:

- Clear architecture
- Standard alignment
- Low overlap between contributors
- Long-term extensibility

---

# 🧭 Guiding Principles

1. Follow **OGC standards**
2. Keep implementation **lightweight**
3. Separate **data access** from **computation**
4. Keep dataset definitions in **one place**
5. Make outputs **portable**
6. Design for **multiple datasets**

---

# 🧩 Phased Approach

## Phase 1 — Data & Processing

- Dataset registry
- Cache-first fetching
- AOI input (GeoJSON + bbox)
- Zonal stats & timeseries
- Canonical rows output
- CSV + DHIS2 export

## Phase 2 — Visualization

- COG conversion (if needed)
- TiTiler integration
- OGC API – Tiles (optional)

---

# 🏛️ Public API Surface

## OGC API – Processes

Endpoints:

- `GET /processes`
- `GET /processes/{id}`
- `POST /processes/{id}/execution`
- (Later) `/jobs`

Processes represent **algorithms**, not datasets.

---

# 🔧 Core Processes

Initial generic processes:

- `raster.zonal_stats`
- `raster.point_timeseries`

Inputs:

- `dataset_id`
- `params`
- `time`
- `aoi`

Outputs:

- Canonical rows
- Optional CSV
- Optional DHIS2 payload

---

# 🧠 Internal Architecture (Inspired by pygeoapi)

```text
Dataset Registry → Provider → Cache → Raster Ops → Formatter
```

Library split (details in `docs/processing_api.md#library-responsibility-matrix`):

- Extract/adapters: `dhis2eo`, `earthkit`
- Compute: `xarray` now; `rasterio`/`rioxarray`/`geopandas` as process implementations expand
- Output mapping: CSV/JSON now, DHIS2 mapping adapter integration next
