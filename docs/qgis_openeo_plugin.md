# openEO QGIS Plugin

The [openEO QGIS plugin](https://github.com/Open-EO/openeo-qgis-plugin) lets you connect to any openEO-compatible backend — including Open Climate Service — directly from QGIS. You can browse collections, manage batch jobs, load results as layers, and work with web services without leaving QGIS.

---

## Installation

1. Open **Plugins → Manage and Install Plugins**.
2. Search for **OpenEO** and install the plugin.
3. After installation, an **openEO** entry appears in the **QGIS Browser panel** — this is where all plugin functionality lives.

!!! note "macOS"
    macOS users need **qpip version 1.3.0 or higher**. Version 2.0 or later of the plugin is recommended.

---

## Connecting to Open Climate Service

1. Open the **Browser panel** (if not visible: **View → Panels → Browser**).
2. Find the **openEO** entry and right-click it.
3. Select **New openEO Connection**.
4. Enter a connection name and your instance URL:

   ```
   http://your-instance:8000
   ```

5. Choose an authentication method:
   - **Basic Authentication** — enter credentials directly
   - **OpenID Connect** — browser-based login (recommended)

   For local instances, no authentication is required.

A success notification appears in QGIS and the **Batch Jobs** and **Web Services** entries become expandable under the connection.

!!! warning "Security"
    Basic authentication credentials and tokens are stored as plain text. On shared systems, log out after use by right-clicking the connection and selecting **Log out** or **Remove Connection**.

---

## Browsing collections

Expand the **Collections** section under your connection to see all available datasets. Collections that support tile map service previews show a checkerboard icon instead of the default cube icon.

Open Climate Service collections include datasets such as `chirps3_precipitation_daily`, `era5land_temperature_hourly`, and `worldpop_population_yearly`.

**Available actions:**

- Right-click a collection → **Details** to view metadata in the browser
- Right-click → **Add Layer to Project** or drag the collection into the map canvas to load a preview

---

## Batch jobs

The **Batch Jobs** section lists all jobs associated with your account.

**Working with jobs:**

- Right-click a job → **View Logs** to see execution details and errors
- Right-click a job → **Download Results To..** to save results locally (default: system downloads folder)
- Right-click → **Add Results to Project** to load one or all result files directly as QGIS layers

Result types handled by the plugin:

| Format | Type |
|---|---|
| GeoTIFF | Raster layer |
| NetCDF | Raster layer |
| GeoJSON / Shapefile | Vector layer |
| Zarr | File (not directly loadable as a layer) |

---

## Web services

The **Web Services** section lists tile services and other OGC services created by your account on the backend.

- View service details and configuration via right-click
- Add enabled services directly to your QGIS project as map layers

---

## Running a process graph from the openEO Web Editor

The QGIS plugin focuses on browsing, managing jobs, and loading results. To build and submit process graphs, use the [openEO Web Editor](https://editor.openeo.org) connected to the same backend, or the [openEO Python client](openeo.md). Results submitted from any client appear under **Batch Jobs** in the QGIS plugin once complete.

---

## Further reading

- [openEO QGIS plugin documentation](https://openeo.org/documentation/1.0/qgis/)
- [openEO QGIS plugin on GitHub](https://github.com/Open-EO/openeo-qgis-plugin)
- [Open Climate Service openEO guide](openeo.md)
- [Available climate indices](climate_indices.md)
