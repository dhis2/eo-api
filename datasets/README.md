# Dataset metadata

This folder contains dataset definitions used by the `/collections` endpoints.

Definitions are loaded from `*.yaml` files and validated by the `DatasetDefinition` Pydantic model in `eoapi/datasets.py`.

## Required schema

- `id` (string): unique collection identifier used in `/collections/{id}`
- `title` (string): human-readable collection title
- `description` (string): collection description
- `spatial_bbox` (array of 4 numbers): `[minx, miny, maxx, maxy]` in CRS84
- `temporal_interval` (array of 2 values): `[start_iso8601, end_iso8601_or_null]`

## Optional schema

- `keywords` (array of strings): tags used for discovery
- `parameters` (object): shared parameter definitions used by both Coverages and EDR endpoints

### `parameters` object shape

Each key is a parameter ID (for example `precip` or `2m_temperature`) and value is a CoverageJSON/EDR-compatible parameter object, for example:

```yaml
parameters:
  precip:
    type: Parameter
    description:
      en: Daily precipitation
    unit:
      label:
        en: mm/day
    observedProperty:
      label:
        en: Precipitation
```

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
parameters:
  precip:
    type: Parameter
    description:
      en: Daily precipitation
    unit:
      label:
        en: mm/day
    observedProperty:
      label:
        en: Precipitation
```

## Current definitions

- `chirps-daily.yaml`
- `era5-land-daily.yaml`
