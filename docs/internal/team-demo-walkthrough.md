# Team Demo Walkthrough

## 30-Second Opener

This branch now tells a cleaner story than before. FastAPI is the native control and metadata plane for ingestions, extents, datasets, sync, and raw Zarr access. `pygeoapi` is mounted inside the same app and remains the standards-facing publication plane under `/ogcapi`. The end-to-end proof is: ingest a dataset for a configured extent, expose it natively as a managed dataset, expose raw data through `/zarr/{dataset_id}`, and publish it immediately as an OGC collection.

## 3-Minute Opening Script

The public EO API story is now built around five concepts:

1. `POST /ingestions`
2. `GET /extents`
3. `GET /datasets`
4. `GET /zarr/{dataset_id}`
5. `GET /ogcapi/collections/{dataset_id}`

If we start in [src/eo_api/main.py](/home/abyot/coding/EO/eo-api-pygeoapi-publication/src/eo_api/main.py), the app mounts the native FastAPI routes first and then mounts `pygeoapi` under `/ogcapi`.

Configured spatial extents are now explicit and read-only. In [data/extents.yaml](/home/abyot/coding/EO/eo-api-pygeoapi-publication/data/extents.yaml) we define instance extents like `sle`, and [src/eo_api/extents/routes.py](/home/abyot/coding/EO/eo-api-pygeoapi-publication/src/eo_api/extents/routes.py) exposes them through `/extents`. That means ingestion no longer needs raw `bbox` or `country_code` in the public payload.

The main create flow starts at `POST /ingestions`, implemented in [src/eo_api/ingestions/routes.py](/home/abyot/coding/EO/eo-api-pygeoapi-publication/src/eo_api/ingestions/routes.py). A request now says: dataset template, time range, and configured extent. The route resolves the extent internally, downloads data when needed, prefers Zarr when possible, persists an internal record, and returns a public dataset view rather than an artifact record.

That public dataset is available under `/datasets/{dataset_id}`. `/datasets` is now the native catalog of managed datasets, not a registry of internal files. It returns dataset metadata, current extent, last updated time, access links, publication state, and a slim version summary.

Raw Zarr access has been separated cleanly from dataset metadata. If a managed dataset is Zarr-backed, the canonical native raw-data route is `/zarr/{dataset_id}`. That route exposes a browseable listing and file access without leaking internal artifact ids or local filesystem roots.

Publication still happens through [src/eo_api/publications/services.py](/home/abyot/coding/EO/eo-api-pygeoapi-publication/src/eo_api/publications/services.py). When a dataset is published, EO API regenerates the pygeoapi resources and refreshes the mounted pygeoapi app in process. The result becomes immediately visible under `/ogcapi/collections/{dataset_id}`.

So the current story is simple: ingest by dataset template plus configured extent, browse metadata under `/datasets`, inspect raw data under `/zarr/{dataset_id}`, and browse standards-facing publication under `/ogcapi`.

## Architecture Walkthrough

### 1. App Composition

Start in [src/eo_api/main.py](/home/abyot/coding/EO/eo-api-pygeoapi-publication/src/eo_api/main.py).

The app mounts:

- `/`
- `/extents`
- `/registry`
- `/manage`
- `/retrieve`
- `/ingestions`
- `/datasets`
- `/zarr`
- `/sync`
- `/ogcapi`

The public focus of the branch is:

- `/ingestions`
- `/sync`
- `/datasets`
- `/zarr/{dataset_id}`
- `/ogcapi`

### 2. Extent Configuration

Open:

- [data/extents.yaml](/home/abyot/coding/EO/eo-api-pygeoapi-publication/data/extents.yaml)
- [src/eo_api/extents/services.py](/home/abyot/coding/EO/eo-api-pygeoapi-publication/src/eo_api/extents/services.py)
- [src/eo_api/extents/routes.py](/home/abyot/coding/EO/eo-api-pygeoapi-publication/src/eo_api/extents/routes.py)

The important point is that extents are now setup-time configuration, not runtime write resources.

That gives us:

- stable `extent_id`
- explicit bbox configuration
- read-only discovery through the API
- no dependency on passing raw spatial selectors in the public ingestion payload

### 3. Ingestion Flow

Open [src/eo_api/ingestions/routes.py](/home/abyot/coding/EO/eo-api-pygeoapi-publication/src/eo_api/ingestions/routes.py).

The current public ingestion contract is:

```json
{
  "dataset_id": "chirps3_precipitation_daily",
  "start": "2024-01-01",
  "end": "2024-01-31",
  "extent_id": "sle",
  "overwrite": false,
  "prefer_zarr": true,
  "publish": true
}
```

Two concrete demo examples fit the current setup well.

CHIRPS3 for Sierra Leone:

```json
{
  "dataset_id": "chirps3_precipitation_daily",
  "start": "2024-01-01",
  "end": "2024-01-31",
  "extent_id": "sle",
  "overwrite": false,
  "prefer_zarr": true,
  "publish": true
}
```

WorldPop for Sierra Leone:

```json
{
  "dataset_id": "worldpop_population_yearly",
  "start": "2020",
  "end": "2020",
  "extent_id": "sle",
  "overwrite": false,
  "prefer_zarr": true,
  "publish": true
}
```

These two examples are useful together because they show the same API contract working across:

- a daily climate raster with temporal range behavior
- a yearly population raster with a much simpler time axis

The route resolves:

1. the dataset template
2. the configured extent
3. the internal download/materialization flow

Then it returns:

- `ingestion_id`
- `status`
- `dataset`

The key point to emphasize in the demo is that the public response now returns a dataset view, not an internal artifact record.

For the live walkthrough, CHIRPS3 is the better primary example because it makes temporal coverage and future `/sync` behavior easier to explain. WorldPop is the useful secondary example because it shows that the same contract also works for annual static-like datasets and still publishes them through the same `/datasets`, `/zarr`, and `/ogcapi` surfaces.

### 4. Managed Dataset Model

Open:

- [src/eo_api/ingestions/services.py](/home/abyot/coding/EO/eo-api-pygeoapi-publication/src/eo_api/ingestions/services.py)
- [src/eo_api/ingestions/schemas.py](/home/abyot/coding/EO/eo-api-pygeoapi-publication/src/eo_api/ingestions/schemas.py)

There is still an internal artifact persistence model under the hood, but it is no longer the public API story.

The public dataset model now carries:

- dataset identity
- source dataset template id
- display metadata
- extent
- last updated
- links
- publication summary
- slim version history in dataset detail

This is why `/datasets` is now the native metadata plane for managed EO datasets.

### 5. Raw Zarr Access

Open [src/eo_api/ingestions/routes.py](/home/abyot/coding/EO/eo-api-pygeoapi-publication/src/eo_api/ingestions/routes.py) and [src/eo_api/ingestions/services.py](/home/abyot/coding/EO/eo-api-pygeoapi-publication/src/eo_api/ingestions/services.py).

The important route pair is:

- `GET /zarr/{dataset_id}`
- `GET /zarr/{dataset_id}/{relative_path}`

This is the canonical native raw-data surface now.

Important demo point:

- dataset metadata lives under `/datasets`
- raw store access lives under `/zarr`
- we no longer bury Zarr under the dataset detail route tree

### 6. pygeoapi Publication

Open [src/eo_api/publications/services.py](/home/abyot/coding/EO/eo-api-pygeoapi-publication/src/eo_api/publications/services.py).

Publication still works by:

1. assigning a stable managed dataset id
2. regenerating pygeoapi resources from published internal records
3. regenerating the pygeoapi OpenAPI document
4. refreshing the mounted pygeoapi ASGI app

The public result is only surfaced under:

- `/ogcapi/collections`
- `/ogcapi/collections/{dataset_id}`
- `/ogcapi/collections/{dataset_id}/coverage`

We no longer maintain a native FastAPI `/collections` surface.

### 7. `/sync`

`/sync/{dataset_id}` exists and is part of the intended public story, but it is still the main refinement area. It should be presented as present but still under active behavioral design.

That means the live demo should mention it, but not overclaim final semantics yet.

## Suggested Live Demo Order

1. `GET /`
2. [src/eo_api/main.py](/home/abyot/coding/EO/eo-api-pygeoapi-publication/src/eo_api/main.py)
3. `GET /extents`
4. `POST /ingestions` with `chirps3_precipitation_daily`
5. `GET /datasets/chirps3_precipitation_daily_sle`
6. `GET /zarr/chirps3_precipitation_daily_sle`
7. `GET /ogcapi/collections/chirps3_precipitation_daily_sle`
8. `GET /ogcapi/collections/chirps3_precipitation_daily_sle/coverage`
9. `POST /ingestions` with `worldpop_population_yearly`
10. `GET /datasets/worldpop_population_yearly_sle`
11. `GET /zarr/worldpop_population_yearly_sle`
12. `GET /ogcapi/collections/worldpop_population_yearly_sle`

## Presentation Notes

- keep emphasizing the split between native metadata/control and standards-facing publication
- say clearly that artifacts still exist internally, but are not part of the public contract anymore
- describe `extent_id` as instance configuration, not as an upstream provider concept
- describe `/zarr/{dataset_id}` as the native raw-data route
- describe `/sync` as the next area to refine, not a finalized workflow contract
- use CHIRPS3 first for the richer narrative, then WorldPop to show the same contract works for a different EO source and temporal grain
