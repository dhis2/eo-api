# Using the web interface

Every Open Climate Service instance ships with a small built-in web interface — no
separate application to install. Once the instance is running, open its root URL
(`http://127.0.0.1:8000` by default) and the landing page links to everything below.

| Page | URL | What it does |
| ---- | --- | ------------ |
| Landing page | `/` | Instance overview — name, extent, available dataset templates, and ingested datasets |
| **Manage** | `/manage` | Ingest data, sync datasets forward, and see what is already ingested |
| **Map viewer** | `/map` | View published datasets on an interactive map |
| openEO editor | `/openeo` | Redirects to the openEO Web Editor, pre-connected to this instance |
| API docs | `/docs` | Interactive Swagger documentation for the REST API |

The interface is intended for operators setting up and curating an instance. Everything
it does is also available through the REST API, so the same operations can be scripted or
scheduled — see the [API reference](managed_data_api_guide.md).

---

## Managing data (`/manage`)

The management page has two parts: an **ingest form** and a **dataset status table**.

### Ingesting a dataset

1. Choose a **dataset template** from the dropdown (the list comes from the templates
   registered for this instance — the built-in catalogue plus any custom dataset plugins).
2. Enter a **start** date. Daily datasets take `YYYY-MM-DD`, monthly `YYYY-MM`, yearly
   `YYYY`.
3. Optionally enter an **end** date — if left blank it defaults to today.
4. Leave **Publish after ingestion** checked to make the dataset immediately discoverable
   (via STAC) and visible in the map viewer. Uncheck it to ingest without publishing.
5. Check **Overwrite if already ingested** to replace an existing store for the same scope.
6. Click ingest. Progress streams live as the data downloads and materialises; the page
   confirms when it finishes (or shows the error if it fails).

You do **not** enter a bounding box — ingestion always uses the spatial extent configured
for the instance in `climate-service.yaml`.

### Dataset status and sync

Below the form, a table lists every dataset already ingested, with its period type,
temporal coverage, and publication status. Each row has a **Sync** button that advances
that dataset to the latest available upstream data — appending missing periods for
temporal datasets, or rematerialising a newer release. You can set an optional cutoff
date; otherwise sync goes as far as the source allows. Sync progress streams live, the
same way ingestion does.

---

## The map viewer (`/map`)

The map viewer renders **published** datasets directly in the browser from their GeoZarr
stores (using MapLibre and zarr-layer), so only datasets ingested with publishing enabled
appear here.

- **Dataset selector** — pick any published dataset from the dropdown.
- **Time slider** — datasets with a time dimension get a slider to step through periods;
  the current period and the number of steps are shown above it.
- **Legend** — a colour bar with the value range and units, derived from the dataset's
  metadata (including the colour scheme defined in its template).
- **Source and units** — shown alongside the legend for context.
- The map fits to the instance's configured extent on load.

If a dataset doesn't show up, confirm it was ingested with **Publish** enabled — only
published datasets are listed.

---

## When to use the API instead

The web interface is the quickest way to set up and curate an instance by hand. For
**automation, scheduling, or programmatic access** — recurring ingestion, scripted sync,
or integrating with other systems — use the REST API directly. The management forms map
one-to-one onto the `POST /ingestions` and `POST /sync/{dataset_id}` endpoints documented
in the [API reference](managed_data_api_guide.md). To read and analyse published data, see
[Accessing data](user_guide.md).
