# Dataset metadata

This folder contains dataset definitions used by the `/collections` endpoints.

Definitions are loaded from `*.yaml` files and validated by the `DatasetDefinition` Pydantic model in `eoapi/endpoints/collections.py`.

## Required schema

- `id` (string): unique collection identifier used in `/collections/{id}`
- `title` (string): human-readable collection title
- `description` (string): collection description
- `spatial_bbox` (array of 4 numbers): `[minx, miny, maxx, maxy]` in CRS84
- `temporal_interval` (array of 2 values): `[start_iso8601, end_iso8601_or_null]`

## Optional schema

- `keywords` (array of strings): tags used for discovery

## Example

```yaml
id: my-dataset-daily
title: My Dataset Daily
description: Daily gridded variable for demonstration.
keywords:
  - climate
  - precipitation
  - coverage
spatial_bbox:
  - -180.0
  - -90.0
  - 180.0
  - 90.0
temporal_interval:
  - 2000-01-01T00:00:00Z
  - null
```

## Current definitions

- `chirps-daily.yaml`
- `era5-land-daily.yaml`
