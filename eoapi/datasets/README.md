# Dataset metadata

This folder contains dataset definitions used by the `/collections` endpoints.

Definitions are loaded from dataset-specific subfolders and validated by the `DatasetDefinition` Pydantic model in `eoapi/datasets/catalog.py`.

## Folder layout

Each dataset has its own folder named after the dataset ID:

- `eoapi/datasets/<dataset-id>/<dataset-id>.yaml`
- `eoapi/datasets/<dataset-id>/resolver.py`

Example:

- `eoapi/datasets/chirps-daily/chirps-daily.yaml`
- `eoapi/datasets/chirps-daily/resolver.py`

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

- `chirps-daily/chirps-daily.yaml`
- `era5-land-daily/era5-land-daily.yaml`

## Adding a new dataset resolver module

When adding a new dataset, create a folder under `eoapi/datasets/` named exactly as the dataset ID and include a `resolver.py` module for dataset-specific source integration logic.

Expected resolver functions in the module:

- `coverage_source(datetime_value, parameters, bbox)`
- `position_source(datetime_value, parameters, coords)`
- `area_source(datetime_value, parameters, bbox)`

These should follow the shared resolver contracts in `eoapi/datasets/base.py`.

Resolver registration is automatic via `eoapi/datasets/resolvers.py`, which scans dataset folders and loads `resolver.py` by dataset ID.
