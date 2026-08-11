# Built-in datasets

The Open Climate Service ships with built-in dataset templates covering precipitation, temperature, and population. Each template describes a data source and the rules for downloading, transforming, and syncing it. They are available in every instance without any additional configuration.

To ingest a built-in dataset for your configured extent, see the [API reference](managed_data_api_guide.md). To add datasets beyond these, see [Adding custom datasets](adding_custom_datasets.md).

---

## CHIRPS v3 — daily precipitation

| Property               | Value                                              |
| ---------------------- | -------------------------------------------------- |
| **Dataset ID**         | `chirps3_precipitation_daily`                      |
| **Variable**           | `precip`                                           |
| **Units**              | mm                                                 |
| **Period**             | Daily                                              |
| **Spatial coverage**   | Global land, 50°S–50°N                             |
| **Spatial resolution** | ~5 km                                              |
| **Record start**       | 1981-01-01                                         |
| **Source**             | [CHIRPS v3](https://www.chc.ucsb.edu/data/chirps3) |

CHIRPS (Climate Hazards Group InfraRed Precipitation with Station data) v3 is a quasi-global daily precipitation dataset merging satellite thermal infrared imagery with station observations. It is widely used for drought monitoring, food security analysis, and WASH planning in low- and middle-income countries.

**Sync behaviour** — new data is ingested incrementally as it becomes available. CHIRPS has a nominal publication lag of around 3–7 days, so data through yesterday is not always present. The API uses a custom availability function that checks the actual latest available date from the CHIRPS server before each sync.

**Transforms** — none applied; data is stored as received in mm.

---

## ERA5-Land — temperature and precipitation

ERA5-Land provides temperature and precipitation at hourly, daily, and monthly resolution. Nine dataset templates are available covering both variables and all resolutions, with options for UTC or local-timezone daily aggregation.

See **[ERA5-Land datasets](era5_land_datasets.md)** for the full reference, including dataset IDs, coverage, lag times, and guidance on choosing the right dataset for your use case.

---

## WorldPop Global2 — total population (yearly)

| Property               | Value                                                                |
| ---------------------- | -------------------------------------------------------------------- |
| **Dataset ID**         | `worldpop_population_global2_R2025A_100m`                            |
| **Variable**           | `pop_total`                                                          |
| **Units**              | people                                                               |
| **Period**             | Yearly                                                               |
| **Spatial coverage**   | Global                                                               |
| **Spatial resolution** | ~100 m                                                               |
| **Record start**       | 2015                                                                 |
| **Record end**         | 2030                                                                 |
| **Source**             | [WorldPop Global2](https://hub.worldpop.org/project/categories?id=3) |

WorldPop Global2 provides gridded population estimates and projections at 100 m resolution. Each raster year represents estimated residential population counts. Years up to and including the present are backward-modelled estimates; years beyond the present are forward projections.

**Sync behaviour** — population data is released year by year, not as a continuous stream. The API uses a `release`-kind sync that checks each calendar year separately. Future years (projections) are also requestable, since the underlying data covers through 2030.

**Transforms** — none applied; values are stored as received (population counts per pixel).

---

## WorldPop Global2 — population by age and sex (yearly)

| Property               | Value                                                                                     |
| ---------------------- | ----------------------------------------------------------------------------------------- |
| **Dataset ID**         | `worldpop_agesex_global2_R2025A_100m`                                                     |
| **Variable**           | `population` (over `sex` and `age_group` dimensions)                                      |
| **Units**              | people                                                                                    |
| **Period**             | Yearly                                                                                    |
| **Spatial coverage**   | Per-country (set `extent.country_code`)                                                   |
| **Spatial resolution** | ~100 m                                                                                    |
| **Record start**       | 2015                                                                                      |
| **Record end**         | 2030                                                                                      |
| **Source**             | [WorldPop Global2 age & sex structures](https://hub.worldpop.org/project/categories?id=8) |

Population disaggregated by sex and 5-year age band. Population is the quantity; sex and age are both disaggregation dimensions of it — so WorldPop's ~40 per-(sex, age) GeoTIFFs per country-year are combined into a **single `population` variable** over a `sex` dimension (`female`, `male`) and an ordinal `age_group` dimension (the lower bound of each band: 0, 1, 5, 10, … 90).

---

## NOAA GEFS — 35-day ensemble forecast

| Property               | Value                                                                              |
| ---------------------- | ---------------------------------------------------------------------------------- |
| **Dataset IDs**        | `gefs_temperature_forecast`, `gefs_precipitation_forecast`                          |
| **Variables**          | `temperature_2m`, `precipitation_surface`                                           |
| **Units**              | °C, mm/day                                                                          |
| **Period**             | Daily, up to 35 days ahead                                                          |
| **Spatial coverage**   | Global                                                                              |
| **Spatial resolution** | 0.25° (~28 km)                                                                      |
| **Record start**       | 2020-10-01                                                                          |
| **Source**             | [NOAA GEFS via dynamical.org](https://dynamical.org/catalog/noaa-gefs-forecast-35-day/) |

A 31-member ensemble forecast, issued daily. Unlike every other built-in dataset these are
**forecast cubes**: two temporal axes rather than one, so the archive keeps what each past run
predicted instead of overwriting it. See [Forecast datasets](adding_custom_datasets.md#forecast-datasets-two-temporal-axes)
for the shape and what it means for coverage and sync.

**Ensemble spread** — members are reduced to the 10th, 50th and 90th percentiles on a `quantile`
axis, rather than to a single mean. The spread is most of what an ensemble forecast is for: a
median of 2 mm with a 90th percentile of 40 mm is a different decision from one with a 90th
percentile of 3 mm.

**Transforms** — upstream steps are 3-hourly, widening to 6-hourly partway through the run, so
they are aggregated to whole forecast days; a trailing day the run does not fully cover is
dropped rather than reported short. Precipitation arrives as an average rate in `kg m-2 s-1` and
is integrated over the day to give a total in mm.

Requires no credentials, which makes this the easiest way to see forecast handling end to end.

---

## Temporal resampling

Any ingested dataset can be resampled to a coarser temporal resolution (e.g. hourly → daily, daily → monthly) using the standard openEO `aggregate_temporal_period` process in a process graph. See [Processes](processes.md) for an example.
