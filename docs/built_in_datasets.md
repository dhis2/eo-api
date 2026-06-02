# Built-in datasets

The Open Climate Service ships with four built-in dataset templates covering precipitation, temperature, and population. Each template describes a data source and the rules for downloading, transforming, and syncing it. They are available in every instance without any additional configuration.

To ingest a built-in dataset for your configured extent, see the [API reference](managed_data_api_guide.md). To add datasets beyond these four, see [Adding custom datasets](adding_custom_datasets.md).

---

## CHIRPS v3 — daily precipitation

| Property | Value |
| --- | --- |
| **Dataset ID** | `chirps3_precipitation_daily` |
| **Variable** | `precip` |
| **Units** | mm |
| **Period** | Daily |
| **Spatial coverage** | Global land, 50°S–50°N |
| **Spatial resolution** | ~5 km |
| **Record start** | 1981-01-01 |
| **Source** | [CHIRPS v3](https://www.chc.ucsb.edu/data/chirps3) |

CHIRPS (Climate Hazards Group InfraRed Precipitation with Station data) v3 is a quasi-global daily precipitation dataset merging satellite thermal infrared imagery with station observations. It is widely used for drought monitoring, food security analysis, and WASH planning in low- and middle-income countries.

**Sync behaviour** — new data is ingested incrementally as it becomes available. CHIRPS has a nominal publication lag of around 3–7 days, so data through yesterday is not always present. The API uses a custom availability function that checks the actual latest available date from the CHIRPS server before each sync.

**Transforms** — none applied; data is stored as received in mm.

---

## ERA5-Land — 2 m temperature (hourly)

| Property | Value |
| --- | --- |
| **Dataset ID** | `era5land_temperature_hourly` |
| **Variable** | `t2m` |
| **Units** | °C |
| **Period** | Hourly |
| **Spatial coverage** | Global |
| **Spatial resolution** | ~9 km |
| **Record start** | 1950-01-01 |
| **Source** | [ERA5-Land Reanalysis via DestinE Earth Data Hub](https://earthdatahub.destine.eu/collections/era5/datasets/reanalysis-era5-land) |

ERA5-Land is a global atmospheric reanalysis produced by ECMWF. The 2 m temperature variable (`t2m`) represents the air temperature 2 metres above the land surface, including corrections for topography relative to the ERA5 pressure levels.

**Sync behaviour** — new hours are appended incrementally. ERA5-Land is published with a nominal 5-day lag; the API will not request data closer than 120 hours to the current time.

**Transforms** — raw values are in Kelvin. The `kelvin_to_celsius` transform is applied at ingest time, so stored values are in °C.

---

## ERA5-Land — total precipitation (hourly)

| Property | Value |
| --- | --- |
| **Dataset ID** | `era5land_precipitation_hourly` |
| **Variable** | `tp` |
| **Units** | mm |
| **Period** | Hourly |
| **Spatial coverage** | Global |
| **Spatial resolution** | ~9 km |
| **Record start** | 1950-01-01 |
| **Source** | [ERA5-Land Reanalysis via DestinE Earth Data Hub](https://earthdatahub.destine.eu/collections/era5/datasets/reanalysis-era5-land) |

Total precipitation (`tp`) from ERA5-Land is an accumulated hourly value representing the sum of large-scale and convective precipitation falling onto the land surface. It is useful as a high-resolution complement to CHIRPS for countries outside CHIRPS's 50°N–50°S band, or for sub-daily analysis.

**Sync behaviour** — same 5-day lag as ERA5-Land temperature; hours are appended incrementally.

**Transforms** — raw values are in metres per hour. The `metres_to_mm` transform converts to mm at ingest time.

---

## WorldPop Global2 — total population (yearly)

| Property | Value |
| --- | --- |
| **Dataset ID** | `worldpop_population_yearly` |
| **Variable** | `pop_total` |
| **Units** | people |
| **Period** | Yearly |
| **Spatial coverage** | Global |
| **Spatial resolution** | ~100 m |
| **Record start** | 2015 |
| **Record end** | 2030 |
| **Source** | [WorldPop Global2](https://hub.worldpop.org/project/categories?id=3) |

WorldPop Global2 provides gridded population estimates and projections at 100 m resolution. Each raster year represents estimated residential population counts. Years up to and including the present are backward-modelled estimates; years beyond the present are forward projections.

**Sync behaviour** — population data is released year by year, not as a continuous stream. The API uses a `release`-kind sync that checks each calendar year separately. Future years (projections) are also requestable, since the underlying data covers through 2030.

**Transforms** — none applied; values are stored as received (population counts per pixel).

---

## Derived datasets

In addition to the four built-in sources, the API can produce **derived datasets** by resampling any ingested dataset to a coarser temporal resolution. Derived datasets are created on demand via the `resample` process. See [Processes](processes.md) for details.
